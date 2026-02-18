/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.remoting;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.Channel;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.future.FutureTaskExt;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.common.thread.ThreadPoolStatusMonitor;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.activity.AckMessageActivity;
import org.apache.rocketmq.proxy.remoting.activity.ChangeInvisibleTimeActivity;
import org.apache.rocketmq.proxy.remoting.activity.ClientManagerActivity;
import org.apache.rocketmq.proxy.remoting.activity.ConsumerManagerActivity;
import org.apache.rocketmq.proxy.remoting.activity.GetTopicRouteActivity;
import org.apache.rocketmq.proxy.remoting.activity.PopMessageActivity;
import org.apache.rocketmq.proxy.remoting.activity.PullMessageActivity;
import org.apache.rocketmq.proxy.remoting.activity.RecallMessageActivity;
import org.apache.rocketmq.proxy.remoting.activity.SendMessageActivity;
import org.apache.rocketmq.proxy.remoting.activity.TransactionActivity;
import org.apache.rocketmq.proxy.remoting.channel.RemotingChannelManager;
import org.apache.rocketmq.proxy.remoting.pipeline.AuthenticationPipeline;
import org.apache.rocketmq.proxy.remoting.pipeline.AuthorizationPipeline;
import org.apache.rocketmq.proxy.remoting.pipeline.ContextInitPipeline;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Remoting 协议服务端, 负责处理客户端请求并与 Nameserver、Broker 通信
 */
public class RemotingProtocolServer implements StartAndShutdown, RemotingProxyOutClient {
    /**
     * Remoting 协议服务日志记录器
     */
    private final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    /**
     * 消息处理核心组件
     */
    protected final MessagingProcessor messagingProcessor;
    /**
     * Remoting 通道管理器
     */
    protected final RemotingChannelManager remotingChannelManager;
    /**
     * 客户端事件监听服务
     */
    protected final ChannelEventListener clientHousekeepingService;
    /**
     * 默认 Remoting 服务端实现
     */
    protected final RemotingServer defaultRemotingServer;

    /**
     * 主题路由查询处理器
     */
    protected final GetTopicRouteActivity getTopicRouteActivity;
    /**
     * 客户端管理处理器
     */
    protected final ClientManagerActivity clientManagerActivity;
    /**
     * 消费者管理处理器
     */
    protected final ConsumerManagerActivity consumerManagerActivity;
    /**
     * 发送消息处理器
     */
    protected final SendMessageActivity sendMessageActivity;
    /**
     * 消息撤回处理器
     */
    protected final RecallMessageActivity recallMessageActivity;
    /**
     * 事务消息处理器
     */
    protected final TransactionActivity transactionActivity;
    /**
     * 拉消息处理器
     */
    protected final PullMessageActivity pullMessageActivity;
    /**
     * POP 消息处理器
     */
    protected final PopMessageActivity popMessageActivity;
    /**
     * ACK 处理器
     */
    protected final AckMessageActivity ackMessageActivity;
    /**
     * 修改不可见时长处理器
     */
    protected final ChangeInvisibleTimeActivity changeInvisibleTimeActivity;

    /**
     * 发送消息线程池 CPU * 4
     */
    protected final ThreadPoolExecutor sendMessageExecutor;
    /**
     * 拉消息线程池 CPU * 4
     */
    protected final ThreadPoolExecutor pullMessageExecutor;
    /**
     * 心跳线程池 CPU * 2
     */
    protected final ThreadPoolExecutor heartbeatExecutor;
    /**
     * 位点更新线程池 CPU * 4
     */
    protected final ThreadPoolExecutor updateOffsetExecutor;
    /**
     * 路由查询线程池 CPU * 2
     */
    protected final ThreadPoolExecutor topicRouteExecutor;
    /**
     * 默认请求线程池 CPU * 4
     */
    protected final ThreadPoolExecutor defaultExecutor;
    /**
     * 定时清理任务线程池 1
     */
    protected final ScheduledExecutorService timerExecutor;

    /**
     * 构造 Remoting 协议服务, 初始化处理器与线程池
     *
     * @param messagingProcessor 消息处理核心组件
     */
    public RemotingProtocolServer(MessagingProcessor messagingProcessor) {
        this.messagingProcessor = messagingProcessor;
        this.remotingChannelManager = new RemotingChannelManager(this, messagingProcessor.getProxyRelayService());

        RequestPipeline pipeline = createRequestPipeline(messagingProcessor);
        this.getTopicRouteActivity = new GetTopicRouteActivity(pipeline, messagingProcessor);
        this.clientManagerActivity = new ClientManagerActivity(pipeline, messagingProcessor, remotingChannelManager);
        this.consumerManagerActivity = new ConsumerManagerActivity(pipeline, messagingProcessor);
        this.sendMessageActivity = new SendMessageActivity(pipeline, messagingProcessor);
        this.recallMessageActivity = new RecallMessageActivity(pipeline, messagingProcessor);
        this.transactionActivity = new TransactionActivity(pipeline, messagingProcessor);
        this.pullMessageActivity = new PullMessageActivity(pipeline, messagingProcessor);
        this.popMessageActivity = new PopMessageActivity(pipeline, messagingProcessor);
        this.ackMessageActivity = new AckMessageActivity(pipeline, messagingProcessor);
        this.changeInvisibleTimeActivity = new ChangeInvisibleTimeActivity(pipeline, messagingProcessor);

        ProxyConfig config = ConfigurationManager.getProxyConfig();
        NettyServerConfig defaultServerConfig = new NettyServerConfig();
        defaultServerConfig.setListenPort(config.getRemotingListenPort());
        TlsSystemConfig.tlsTestModeEnable = config.isTlsTestModeEnable();
        System.setProperty(TlsSystemConfig.TLS_TEST_MODE_ENABLE, Boolean.toString(config.isTlsTestModeEnable()));
        TlsSystemConfig.tlsServerCertPath = config.getTlsCertPath();
        System.setProperty(TlsSystemConfig.TLS_SERVER_CERTPATH, config.getTlsCertPath());
        TlsSystemConfig.tlsServerKeyPath = config.getTlsKeyPath();
        System.setProperty(TlsSystemConfig.TLS_SERVER_KEYPATH, config.getTlsKeyPath());

        this.clientHousekeepingService = new ClientHousekeepingService(this.clientManagerActivity);

        if (config.isEnableRemotingLocalProxyGrpc()) {
            this.defaultRemotingServer = new MultiProtocolRemotingServer(defaultServerConfig, this.clientHousekeepingService);
        } else {
            this.defaultRemotingServer = new NettyRemotingServer(defaultServerConfig, this.clientHousekeepingService);
        }
        this.registerRemotingServer(this.defaultRemotingServer);

        this.sendMessageExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getRemotingSendMessageThreadPoolNums(),
            config.getRemotingSendMessageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            "RemotingSendMessageThread",
            config.getRemotingSendThreadPoolQueueCapacity(),
            new ThreadPoolHeadSlowTimeMillsMonitor(config.getRemotingWaitTimeMillsInSendQueue())
        );

        this.pullMessageExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getRemotingPullMessageThreadPoolNums(),
            config.getRemotingPullMessageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            "RemotingPullMessageThread",
            config.getRemotingPullThreadPoolQueueCapacity(),
            new ThreadPoolHeadSlowTimeMillsMonitor(config.getRemotingWaitTimeMillsInPullQueue())
        );

        this.updateOffsetExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getRemotingUpdateOffsetThreadPoolNums(),
            config.getRemotingUpdateOffsetThreadPoolNums(),
            1,
            TimeUnit.MINUTES,
            "RemotingUpdateOffsetThread",
            config.getRemotingUpdateOffsetThreadPoolQueueCapacity(),
            new ThreadPoolHeadSlowTimeMillsMonitor(config.getRemotingWaitTimeMillsInUpdateOffsetQueue())
        );

        this.heartbeatExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getRemotingHeartbeatThreadPoolNums(),
            config.getRemotingHeartbeatThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            "RemotingHeartbeatThread",
            config.getRemotingHeartbeatThreadPoolQueueCapacity(),
            new ThreadPoolHeadSlowTimeMillsMonitor(config.getRemotingWaitTimeMillsInHeartbeatQueue())
        );

        this.topicRouteExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getRemotingTopicRouteThreadPoolNums(),
            config.getRemotingTopicRouteThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            "RemotingTopicRouteThread",
            config.getRemotingTopicRouteThreadPoolQueueCapacity(),
            new ThreadPoolHeadSlowTimeMillsMonitor(config.getRemotingWaitTimeMillsInTopicRouteQueue())
        );

        this.defaultExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getRemotingDefaultThreadPoolNums(),
            config.getRemotingDefaultThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            "RemotingDefaultThread",
            config.getRemotingDefaultThreadPoolQueueCapacity(),
            new ThreadPoolHeadSlowTimeMillsMonitor(config.getRemotingWaitTimeMillsInDefaultQueue())
        );

        this.timerExecutor = ThreadUtils.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("RemotingServerScheduler-%d").build()
        );
        this.timerExecutor.scheduleAtFixedRate(this::cleanExpireRequest, 10, 10, TimeUnit.SECONDS);
    }

    /**
     * 注册 Remoting 请求码与对应处理器
     *
     * @param remotingServer Remoting 服务端
     */
    protected void registerRemotingServer(RemotingServer remotingServer) {
        remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendMessageActivity, this.sendMessageExecutor);
        remotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendMessageActivity, this.sendMessageExecutor);
        remotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, sendMessageActivity, this.sendMessageExecutor);
        remotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendMessageActivity, sendMessageExecutor);

        remotingServer.registerProcessor(RequestCode.END_TRANSACTION, transactionActivity, sendMessageExecutor);
        remotingServer.registerProcessor(RequestCode.RECALL_MESSAGE, recallMessageActivity, sendMessageExecutor);

        remotingServer.registerProcessor(RequestCode.HEART_BEAT, clientManagerActivity, this.heartbeatExecutor);
        remotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientManagerActivity, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.CHECK_CLIENT_CONFIG, clientManagerActivity, this.defaultExecutor);

        remotingServer.registerProcessor(RequestCode.PULL_MESSAGE, pullMessageActivity, this.pullMessageExecutor);
        remotingServer.registerProcessor(RequestCode.LITE_PULL_MESSAGE, pullMessageActivity, this.pullMessageExecutor);
        remotingServer.registerProcessor(RequestCode.POP_MESSAGE, pullMessageActivity, this.pullMessageExecutor);

        remotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, consumerManagerActivity, this.updateOffsetExecutor);
        remotingServer.registerProcessor(RequestCode.ACK_MESSAGE, consumerManagerActivity, this.updateOffsetExecutor);
        remotingServer.registerProcessor(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, consumerManagerActivity, this.updateOffsetExecutor);
        remotingServer.registerProcessor(RequestCode.GET_CONSUMER_CONNECTION_LIST, consumerManagerActivity, this.updateOffsetExecutor);

        remotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerManagerActivity, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.GET_MAX_OFFSET, consumerManagerActivity, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.GET_MIN_OFFSET, consumerManagerActivity, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, consumerManagerActivity, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, consumerManagerActivity, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.LOCK_BATCH_MQ, consumerManagerActivity, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.UNLOCK_BATCH_MQ, consumerManagerActivity, this.defaultExecutor);

        remotingServer.registerProcessor(RequestCode.GET_ROUTEINFO_BY_TOPIC, getTopicRouteActivity, this.topicRouteExecutor);
    }

    /**
     * 关闭 Remoting 服务与全部线程池资源
     *
     * @throws Exception 关闭异常
     */
    @Override
    public void shutdown() throws Exception {
        this.defaultRemotingServer.shutdown();
        this.remotingChannelManager.shutdown();
        this.sendMessageExecutor.shutdown();
        this.pullMessageExecutor.shutdown();
        this.heartbeatExecutor.shutdown();
        this.updateOffsetExecutor.shutdown();
        this.topicRouteExecutor.shutdown();
        this.defaultExecutor.shutdown();
    }

    /**
     * 启动 Remoting 通道管理与服务端监听
     *
     * @throws Exception 启动异常
     */
    @Override
    public void start() throws Exception {
        this.remotingChannelManager.start();
        this.defaultRemotingServer.start();
    }

    /**
     * 向客户端异步透传请求并返回未来结果
     *
     * @param channel 客户端通道
     * @param request 请求命令
     * @param timeoutMillis 超时时间
     * @return 异步响应结果
     */
    @Override
    public CompletableFuture<RemotingCommand> invokeToClient(Channel channel, RemotingCommand request,
        long timeoutMillis) {
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        try {
            this.defaultRemotingServer.invokeAsync(channel, request, timeoutMillis, new InvokeCallback() {
                /**
                 * 回调完成通知, 当前无需额外处理
                 *
                 * @param responseFuture 响应 future
                 */
                @Override
                public void operationComplete(ResponseFuture responseFuture) {

                }

                /**
                 * 回调成功通知, 将结果传递给外层 future
                 *
                 * @param response 响应命令
                 */
                @Override
                public void operationSucceed(RemotingCommand response) {
                    future.complete(response);
                }

                /**
                 * 回调失败通知, 将异常传递给外层 future
                 *
                 * @param throwable 失败原因
                 */
                @Override
                public void operationFail(Throwable throwable) {
                    future.completeExceptionally(throwable);
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    /**
     * 构建请求处理管道, 包含上下文初始化与鉴权能力
     *
     * @param messagingProcessor 消息处理核心组件
     * @return 请求处理管道
     */
    protected RequestPipeline createRequestPipeline(MessagingProcessor messagingProcessor) {
        RequestPipeline pipeline = (ctx, request, context) -> {
        };
        // add pipeline
        // the last pipe add will execute at the first
        // 添加请求处理管道, 后追加的管线优先执行
        AuthConfig authConfig = ConfigurationManager.getAuthConfig();
        if (authConfig != null) {
            pipeline = pipeline
                    .pipe(new AuthorizationPipeline(authConfig, messagingProcessor))
                    .pipe(new AuthenticationPipeline(authConfig, messagingProcessor));
        }
        return pipeline.pipe(new ContextInitPipeline());
    }

    /**
     * 线程池队头慢请求监控器
     */
    protected class ThreadPoolHeadSlowTimeMillsMonitor implements ThreadPoolStatusMonitor {

        /**
         * 队头任务最大等待阈值
         */
        private final long maxWaitTimeMillsInQueue;

        /**
         * 构建监控器并设置阈值
         *
         * @param maxWaitTimeMillsInQueue 最大等待阈值
         */
        public ThreadPoolHeadSlowTimeMillsMonitor(long maxWaitTimeMillsInQueue) {
            this.maxWaitTimeMillsInQueue = maxWaitTimeMillsInQueue;
        }

        /**
         * 返回监控指标名称
         *
         * @return 指标名称
         */
        @Override
        public String describe() {
            return "headSlow";
        }

        /**
         * 计算当前线程池队头慢请求时长
         *
         * @param executor 线程池
         * @return 慢请求时长
         */
        @Override
        public double value(ThreadPoolExecutor executor) {
            return headSlowTimeMills(executor.getQueue());
        }

        /**
         * 判断是否需要打印线程堆栈
         *
         * @param executor 线程池
         * @param value 当前指标值
         * @return 是否打印线程堆栈
         */
        @Override
        public boolean needPrintJstack(ThreadPoolExecutor executor, double value) {
            return value > maxWaitTimeMillsInQueue;
        }
    }

    /**
     * 计算队头任务等待时长
     *
     * @param q 任务队列
     * @return 等待时长, 异常时返回 -1
     */
    protected long headSlowTimeMills(BlockingQueue<Runnable> q) {
        try {
            long slowTimeMills = 0;
            final Runnable peek = q.peek();
            if (peek != null) {
                RequestTask rt = castRunnable(peek);
                slowTimeMills = rt == null ? 0 : System.currentTimeMillis() - rt.getCreateTimestamp();
            }

            if (slowTimeMills < 0) {
                slowTimeMills = 0;
            }

            return slowTimeMills;
        } catch (Exception e) {
            log.error("error when headSlowTimeMills.", e);
        }
        return -1;
    }

    /**
     * 清理各业务线程池中的超时请求
     */
    protected void cleanExpireRequest() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();

        cleanExpiredRequestInQueue(this.sendMessageExecutor, config.getRemotingWaitTimeMillsInSendQueue());
        cleanExpiredRequestInQueue(this.pullMessageExecutor, config.getRemotingWaitTimeMillsInPullQueue());
        cleanExpiredRequestInQueue(this.heartbeatExecutor, config.getRemotingWaitTimeMillsInHeartbeatQueue());
        cleanExpiredRequestInQueue(this.updateOffsetExecutor, config.getRemotingWaitTimeMillsInUpdateOffsetQueue());
        cleanExpiredRequestInQueue(this.topicRouteExecutor, config.getRemotingWaitTimeMillsInTopicRouteQueue());
        cleanExpiredRequestInQueue(this.defaultExecutor, config.getRemotingWaitTimeMillsInDefaultQueue());
    }

    /**
     * 清理指定线程池中等待超时的请求
     *
     * @param threadPoolExecutor 目标线程池
     * @param maxWaitTimeMillsInQueue 最大等待阈值
     */
    protected void cleanExpiredRequestInQueue(ThreadPoolExecutor threadPoolExecutor, long maxWaitTimeMillsInQueue) {
        while (true) {
            try {
                BlockingQueue<Runnable> blockingQueue = threadPoolExecutor.getQueue();
                if (!blockingQueue.isEmpty()) {
                    final Runnable runnable = blockingQueue.peek();
                    if (null == runnable) {
                        break;
                    }
                    final RequestTask rt = castRunnable(runnable);
                    if (rt == null || rt.isStopRun()) {
                        break;
                    }

                    final long behind = System.currentTimeMillis() - rt.getCreateTimestamp();
                    if (behind >= maxWaitTimeMillsInQueue) {
                        if (blockingQueue.remove(runnable)) {
                            rt.setStopRun(true);
                            rt.returnResponse(ResponseCode.SYSTEM_BUSY,
                                String.format("[TIMEOUT_CLEAN_QUEUE]broker busy, start flow control for a while, period in queue: %sms, size of queue: %d", behind, blockingQueue.size()));
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            } catch (Throwable ignored) {
            }
        }
    }

    /**
     * 将队列任务转换为 RequestTask
     *
     * @param runnable 队列任务
     * @return RequestTask 或 null
     */
    private RequestTask castRunnable(final Runnable runnable) {
        try {
            if (runnable instanceof FutureTaskExt) {
                FutureTaskExt futureTaskExt = (FutureTaskExt) runnable;
                return (RequestTask) futureTaskExt.getRunnable();
            }
            return null;
        } catch (Throwable e) {
            log.error("castRunnable exception. class:{}", runnable.getClass().getName(), e);
        }

        return null;
    }
}
