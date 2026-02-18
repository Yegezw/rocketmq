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

package org.apache.rocketmq.proxy.grpc.v2;

import apache.rocketmq.v2.*;
import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.pipeline.AuthenticationPipeline;
import org.apache.rocketmq.proxy.grpc.pipeline.AuthorizationPipeline;
import org.apache.rocketmq.proxy.grpc.pipeline.ContextInitPipeline;
import org.apache.rocketmq.proxy.grpc.pipeline.RequestPipeline;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcProxyException;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseWriter;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * gRPC MessagingService 应用实现, 负责请求管道执行, 线程池调度与统一响应回写
 */
public class GrpcMessagingApplication extends MessagingServiceGrpc.MessagingServiceImplBase implements StartAndShutdown {
    /**
     * Proxy 模块日志记录器
     */
    private final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    /**
     * gRPC 消息活动聚合入口
     */
    private final GrpcMessingActivity grpcMessingActivity;

    /**
     * 请求处理流水线
     */
    protected final RequestPipeline requestPipeline;

    /**
     * CPU * 1 + 10000; 路由请求线程池
     */
    protected ThreadPoolExecutor routeThreadPoolExecutor;
    /**
     * CPU * 1 + 10000; 生产相关请求线程池
     */
    protected ThreadPoolExecutor producerThreadPoolExecutor;
    /**
     * CPU * 1 + 10000; 消费相关请求线程池
     */
    protected ThreadPoolExecutor consumerThreadPoolExecutor;
    /**
     * CPU * 1 + 10000; 客户端管理请求线程池
     */
    protected ThreadPoolExecutor clientManagerThreadPoolExecutor;
    /**
     * CPU * 1 + 10000; 事务相关请求线程池
     */
    protected ThreadPoolExecutor transactionThreadPoolExecutor;


    /**
     * 构造 gRPC 消息应用实例并初始化线程池
     *
     * @param grpcMessingActivity 消息活动入口
     * @param requestPipeline 请求处理流水线
     */
    protected GrpcMessagingApplication(GrpcMessingActivity grpcMessingActivity, RequestPipeline requestPipeline) {
        this.grpcMessingActivity = grpcMessingActivity;
        this.requestPipeline = requestPipeline;

        ProxyConfig config = ConfigurationManager.getProxyConfig();
        this.routeThreadPoolExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getGrpcRouteThreadPoolNums(),
            config.getGrpcRouteThreadPoolNums(),
            1,
            TimeUnit.MINUTES,
            "GrpcRouteThreadPool",
            config.getGrpcRouteThreadQueueCapacity()
        );
        this.producerThreadPoolExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getGrpcProducerThreadPoolNums(),
            config.getGrpcProducerThreadPoolNums(),
            1,
            TimeUnit.MINUTES,
            "GrpcProducerThreadPool",
            config.getGrpcProducerThreadQueueCapacity()
        );
        this.consumerThreadPoolExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getGrpcConsumerThreadPoolNums(),
            config.getGrpcConsumerThreadPoolNums(),
            1,
            TimeUnit.MINUTES,
            "GrpcConsumerThreadPool",
            config.getGrpcConsumerThreadQueueCapacity()
        );
        this.clientManagerThreadPoolExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getGrpcClientManagerThreadPoolNums(),
            config.getGrpcClientManagerThreadPoolNums(),
            1,
            TimeUnit.MINUTES,
            "GrpcClientManagerThreadPool",
            config.getGrpcClientManagerThreadQueueCapacity()
        );
        this.transactionThreadPoolExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getGrpcTransactionThreadPoolNums(),
            config.getGrpcTransactionThreadPoolNums(),
            1,
            TimeUnit.MINUTES,
            "GrpcTransactionThreadPool",
            config.getGrpcTransactionThreadQueueCapacity()
        );

        this.init();
    }

    /**
     * 初始化线程池拒绝策略
     */
    protected void init() {
        GrpcTaskRejectedExecutionHandler rejectedExecutionHandler = new GrpcTaskRejectedExecutionHandler();
        this.routeThreadPoolExecutor.setRejectedExecutionHandler(rejectedExecutionHandler);
        this.routeThreadPoolExecutor.setRejectedExecutionHandler(rejectedExecutionHandler);
        this.producerThreadPoolExecutor.setRejectedExecutionHandler(rejectedExecutionHandler);
        this.consumerThreadPoolExecutor.setRejectedExecutionHandler(rejectedExecutionHandler);
        this.clientManagerThreadPoolExecutor.setRejectedExecutionHandler(rejectedExecutionHandler);
        this.transactionThreadPoolExecutor.setRejectedExecutionHandler(rejectedExecutionHandler);
    }

    /**
     * 创建 gRPC 消息应用实例
     *
     * @param messagingProcessor 消息处理器
     * @return gRPC 消息应用实例
     */
    public static GrpcMessagingApplication create(MessagingProcessor messagingProcessor) {
        RequestPipeline pipeline = (context, headers, request) -> {
        };
        // add pipeline
        // the last pipe add will execute at the first
        // 添加请求处理流水线, 最后追加的节点最先执行
        AuthConfig authConfig = ConfigurationManager.getAuthConfig();
        if (authConfig != null) {
            pipeline = pipeline
                .pipe(new AuthorizationPipeline(authConfig, messagingProcessor))
                .pipe(new AuthenticationPipeline(authConfig, messagingProcessor));
        }
        pipeline = pipeline.pipe(new ContextInitPipeline());
        return new GrpcMessagingApplication(new DefaultGrpcMessingActivity(messagingProcessor), pipeline);
    }

    /**
     * 构建流控响应状态
     *
     * @return 流控状态
     */
    protected Status flowLimitStatus() {
        return ResponseBuilder.getInstance().buildStatus(Code.TOO_MANY_REQUESTS, "flow limit");
    }

    /**
     * 将异常转换为统一状态码
     *
     * @param t 异常对象
     * @return 转换后的状态
     */
    protected Status convertExceptionToStatus(Throwable t) {
        return ResponseBuilder.getInstance().buildStatus(t);
    }

    /**
     * 在目标线程池执行任务, 并在提交前执行请求流水线
     *
     * @param executor 目标执行线程池
     * @param context Proxy 上下文
     * @param request 请求对象
     * @param runnable 业务执行逻辑
     * @param responseObserver 响应观察者
     * @param statusResponseCreator 状态响应构造器
     * @param <V> 请求类型
     * @param <T> 响应类型
     */
    protected <V, T> void addExecutor(ExecutorService executor, ProxyContext context, V request, Runnable runnable,
        StreamObserver<T> responseObserver, Function<Status, T> statusResponseCreator) {
        if (request instanceof GeneratedMessageV3) {
            requestPipeline.execute(context, GrpcConstants.METADATA.get(Context.current()), (GeneratedMessageV3) request);
            validateContext(context);
        } else {
            log.error("[BUG]grpc request pipe is not been executed");
        }
        executor.submit(new GrpcTask<>(runnable, context, request, responseObserver, statusResponseCreator.apply(flowLimitStatus())));
    }

    /**
     * 回写响应结果, 异常场景统一写回错误状态
     *
     * @param context Proxy 上下文
     * @param request 请求对象
     * @param response 正常响应
     * @param responseObserver 响应观察者
     * @param t 异常对象
     * @param errorResponseCreator 错误响应构造器
     * @param <V> 请求类型
     * @param <T> 响应类型
     */
    protected <V, T> void writeResponse(ProxyContext context, V request, T response, StreamObserver<T> responseObserver,
        Throwable t, Function<Status, T> errorResponseCreator) {
        if (t != null) {
            ResponseWriter.getInstance().write(
                responseObserver,
                errorResponseCreator.apply(convertExceptionToStatus(t))
            );
        } else {
            ResponseWriter.getInstance().write(responseObserver, response);
        }
    }

    /**
     * 创建本次请求上下文
     *
     * @return Proxy 上下文
     */
    protected ProxyContext createContext() {
        return ProxyContext.create();
    }

    /**
     * 校验请求上下文关键字段
     *
     * @param context Proxy 上下文
     */
    protected void validateContext(ProxyContext context) {
        if (StringUtils.isBlank(context.getClientID())) {
            throw new GrpcProxyException(Code.CLIENT_ID_REQUIRED, "client id cannot be empty");
        }
    }

    /**
     * 处理路由查询请求
     *
     * @param request 路由查询请求
     * @param responseObserver 响应观察者
     */
    @Override
    public void queryRoute(QueryRouteRequest request, StreamObserver<QueryRouteResponse> responseObserver) {
        Function<Status, QueryRouteResponse> statusResponseCreator = status -> QueryRouteResponse.newBuilder().setStatus(status).build();
        ProxyContext context = createContext();
        try {
            this.addExecutor(this.routeThreadPoolExecutor,
                context,
                request,
                () -> grpcMessingActivity.queryRoute(context, request)
                    .whenComplete((response, throwable) -> writeResponse(context, request, response, responseObserver, throwable, statusResponseCreator)),
                responseObserver,
                statusResponseCreator);
        } catch (Throwable t) {
            writeResponse(context, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    /**
     * 处理心跳请求
     *
     * @param request 心跳请求
     * @param responseObserver 响应观察者
     */
    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        Function<Status, HeartbeatResponse> statusResponseCreator = status -> HeartbeatResponse.newBuilder().setStatus(status).build();
        ProxyContext context = createContext();
        try {
            this.addExecutor(this.clientManagerThreadPoolExecutor,
                context,
                request,
                () -> grpcMessingActivity.heartbeat(context, request)
                    .whenComplete((response, throwable) -> writeResponse(context, request, response, responseObserver, throwable, statusResponseCreator)),
                responseObserver,
                statusResponseCreator);
        } catch (Throwable t) {
            writeResponse(context, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    /**
     * 处理发送消息请求
     *
     * @param request 发送请求
     * @param responseObserver 响应观察者
     */
    @Override
    public void sendMessage(SendMessageRequest request, StreamObserver<SendMessageResponse> responseObserver) {
        Function<Status, SendMessageResponse> statusResponseCreator = status -> SendMessageResponse.newBuilder().setStatus(status).build();
        ProxyContext context = createContext();
        try {
            this.addExecutor(this.producerThreadPoolExecutor,
                context,
                request,
                () -> grpcMessingActivity.sendMessage(context, request)
                    .whenComplete((response, throwable) -> writeResponse(context, request, response, responseObserver, throwable, statusResponseCreator)),
                responseObserver,
                statusResponseCreator);
        } catch (Throwable t) {
            writeResponse(context, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    /**
     * 处理消费分配查询请求
     *
     * @param request 分配查询请求
     * @param responseObserver 响应观察者
     */
    @Override
    public void queryAssignment(QueryAssignmentRequest request,
        StreamObserver<QueryAssignmentResponse> responseObserver) {
        Function<Status, QueryAssignmentResponse> statusResponseCreator = status -> QueryAssignmentResponse.newBuilder().setStatus(status).build();
        ProxyContext context = createContext();
        try {
            this.addExecutor(this.routeThreadPoolExecutor,
                context,
                request,
                () -> grpcMessingActivity.queryAssignment(context, request)
                    .whenComplete((response, throwable) -> writeResponse(context, request, response, responseObserver, throwable, statusResponseCreator)),
                responseObserver,
                statusResponseCreator);
        } catch (Throwable t) {
            writeResponse(context, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    /**
     * 处理流式拉取消息请求
     *
     * @param request 拉取请求
     * @param responseObserver 响应观察者
     */
    @Override
    public void receiveMessage(ReceiveMessageRequest request, StreamObserver<ReceiveMessageResponse> responseObserver) {
        Function<Status, ReceiveMessageResponse> statusResponseCreator = status -> ReceiveMessageResponse.newBuilder().setStatus(status).build();
        ProxyContext context = createContext();
        try {
            this.addExecutor(this.consumerThreadPoolExecutor,
                context,
                request,
                () -> grpcMessingActivity.receiveMessage(context, request, responseObserver),
                responseObserver,
                statusResponseCreator);
        } catch (Throwable t) {
            writeResponse(context, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    /**
     * 处理消息确认请求
     *
     * @param request 确认请求
     * @param responseObserver 响应观察者
     */
    @Override
    public void ackMessage(AckMessageRequest request, StreamObserver<AckMessageResponse> responseObserver) {
        Function<Status, AckMessageResponse> statusResponseCreator = status -> AckMessageResponse.newBuilder().setStatus(status).build();
        ProxyContext context = createContext();
        try {
            this.addExecutor(this.consumerThreadPoolExecutor,
                context,
                request,
                () -> grpcMessingActivity.ackMessage(context, request)
                    .whenComplete((response, throwable) -> writeResponse(context, request, response, responseObserver, throwable, statusResponseCreator)),
                responseObserver,
                statusResponseCreator);
        } catch (Throwable t) {
            writeResponse(context, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    /**
     * 处理转发死信请求
     *
     * @param request 转发请求
     * @param responseObserver 响应观察者
     */
    @Override
    public void forwardMessageToDeadLetterQueue(ForwardMessageToDeadLetterQueueRequest request,
        StreamObserver<ForwardMessageToDeadLetterQueueResponse> responseObserver) {
        Function<Status, ForwardMessageToDeadLetterQueueResponse> statusResponseCreator = status -> ForwardMessageToDeadLetterQueueResponse.newBuilder().setStatus(status).build();
        ProxyContext context = createContext();
        try {
            this.addExecutor(this.producerThreadPoolExecutor,
                context,
                request,
                () -> grpcMessingActivity.forwardMessageToDeadLetterQueue(context, request)
                    .whenComplete((response, throwable) -> writeResponse(context, request, response, responseObserver, throwable, statusResponseCreator)),
                responseObserver,
                statusResponseCreator);
        } catch (Throwable t) {
            writeResponse(context, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    /**
     * 处理结束事务请求
     *
     * @param request 事务结束请求
     * @param responseObserver 响应观察者
     */
    @Override
    public void endTransaction(EndTransactionRequest request, StreamObserver<EndTransactionResponse> responseObserver) {
        Function<Status, EndTransactionResponse> statusResponseCreator = status -> EndTransactionResponse.newBuilder().setStatus(status).build();
        ProxyContext context = createContext();
        try {
            this.addExecutor(this.transactionThreadPoolExecutor,
                context,
                request,
                () -> grpcMessingActivity.endTransaction(context, request)
                    .whenComplete((response, throwable) -> writeResponse(context, request, response, responseObserver, throwable, statusResponseCreator)),
                responseObserver,
                statusResponseCreator);
        } catch (Throwable t) {
            writeResponse(context, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    /**
     * 处理客户端终止通知请求
     *
     * @param request 终止通知请求
     * @param responseObserver 响应观察者
     */
    @Override
    public void notifyClientTermination(NotifyClientTerminationRequest request,
        StreamObserver<NotifyClientTerminationResponse> responseObserver) {
        Function<Status, NotifyClientTerminationResponse> statusResponseCreator = status -> NotifyClientTerminationResponse.newBuilder().setStatus(status).build();
        ProxyContext context = createContext();
        try {
            this.addExecutor(this.clientManagerThreadPoolExecutor,
                context,
                request,
                () -> grpcMessingActivity.notifyClientTermination(context, request)
                    .whenComplete((response, throwable) -> writeResponse(context, request, response, responseObserver, throwable, statusResponseCreator)),
                responseObserver,
                statusResponseCreator);
        } catch (Throwable t) {
            writeResponse(context, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    /**
     * 处理修改不可见时长请求
     *
     * @param request 时长变更请求
     * @param responseObserver 响应观察者
     */
    @Override
    public void changeInvisibleDuration(ChangeInvisibleDurationRequest request,
        StreamObserver<ChangeInvisibleDurationResponse> responseObserver) {
        Function<Status, ChangeInvisibleDurationResponse> statusResponseCreator = status -> ChangeInvisibleDurationResponse.newBuilder().setStatus(status).build();
        ProxyContext context = createContext();
        try {
            this.addExecutor(this.consumerThreadPoolExecutor,
                context,
                request,
                () -> grpcMessingActivity.changeInvisibleDuration(context, request)
                    .whenComplete((response, throwable) -> writeResponse(context, request, response, responseObserver, throwable, statusResponseCreator)),
                responseObserver,
                statusResponseCreator);
        } catch (Throwable t) {
            writeResponse(context, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    /**
     * 处理撤回消息请求
     *
     * @param request 撤回请求
     * @param responseObserver 响应观察者
     */
    @Override
    public void recallMessage(RecallMessageRequest request, StreamObserver<RecallMessageResponse> responseObserver) {
        Function<Status, RecallMessageResponse> statusResponseCreator =
            status -> RecallMessageResponse.newBuilder().setStatus(status).build();
        ProxyContext context = createContext();
        try {
            this.addExecutor(this.producerThreadPoolExecutor, // reuse producer thread pool
                // 复用生产请求线程池
                context,
                request,
                () -> grpcMessingActivity.recallMessage(context, request)
                    .whenComplete((response, throwable) ->
                        writeResponse(context, request, response, responseObserver, throwable, statusResponseCreator)),
                responseObserver,
                statusResponseCreator);
        } catch (Throwable t) {
            writeResponse(context, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    /**
     * 处理遥测双向流请求
     *
     * @param responseObserver 服务端响应观察者
     * @return 客户端请求观察者
     */
    @Override
    public StreamObserver<TelemetryCommand> telemetry(StreamObserver<TelemetryCommand> responseObserver) {
        Function<Status, TelemetryCommand> statusResponseCreator = status -> TelemetryCommand.newBuilder().setStatus(status).build();
        ContextStreamObserver<TelemetryCommand> responseTelemetryCommand = grpcMessingActivity.telemetry(responseObserver);
        return new StreamObserver<TelemetryCommand>() {
            /**
             * 处理遥测上行消息
             *
             * @param value 遥测命令
             */
            @Override
            public void onNext(TelemetryCommand value) {
                ProxyContext context = createContext();
                try {
                    addExecutor(clientManagerThreadPoolExecutor,
                        context,
                        value,
                        () -> responseTelemetryCommand.onNext(context, value),
                        responseObserver,
                        statusResponseCreator);
                } catch (Throwable t) {
                    writeResponse(context, value, null, responseObserver, t, statusResponseCreator);
                }
            }

            /**
             * 处理遥测流异常
             *
             * @param t 异常对象
             */
            @Override
            public void onError(Throwable t) {
                responseTelemetryCommand.onError(t);
            }

            /**
             * 处理遥测流完成事件
             */
            @Override
            public void onCompleted() {
                responseTelemetryCommand.onCompleted();
            }
        };
    }

    /**
     * 关闭消息活动与业务线程池
     *
     * @throws Exception 关闭异常
     */
    @Override
    public void shutdown() throws Exception {
        this.grpcMessingActivity.shutdown();

        this.routeThreadPoolExecutor.shutdown();
        this.routeThreadPoolExecutor.shutdown();
        this.producerThreadPoolExecutor.shutdown();
        this.consumerThreadPoolExecutor.shutdown();
        this.clientManagerThreadPoolExecutor.shutdown();
        this.transactionThreadPoolExecutor.shutdown();
    }

    /**
     * 启动消息活动组件
     *
     * @throws Exception 启动异常
     */
    @Override
    public void start() throws Exception {
        this.grpcMessingActivity.start();
    }

    /**
     * gRPC 任务包装对象, 用于封装执行上下文与拒绝响应
     *
     * @param <V> 请求类型
     * @param <T> 响应类型
     */
    protected static class GrpcTask<V, T> implements Runnable {

        /**
         * 业务执行逻辑
         */
        protected final Runnable runnable;
        /**
         * Proxy 上下文
         */
        protected final ProxyContext context;
        /**
         * 请求对象
         */
        protected final V request;
        /**
         * 任务被拒绝时的响应对象
         */
        protected final T executeRejectResponse;
        /**
         * 响应观察者
         */
        protected final StreamObserver<T> streamObserver;

        /**
         * 构造 gRPC 任务对象
         *
         * @param runnable 业务执行逻辑
         * @param context Proxy 上下文
         * @param request 请求对象
         * @param streamObserver 响应观察者
         * @param executeRejectResponse 任务被拒绝时的响应对象
         */
        public GrpcTask(Runnable runnable, ProxyContext context, V request, StreamObserver<T> streamObserver,
            T executeRejectResponse) {
            this.runnable = runnable;
            this.context = context;
            this.streamObserver = streamObserver;
            this.request = request;
            this.executeRejectResponse = executeRejectResponse;
        }

        /**
         * 执行业务任务
         */
        @Override
        public void run() {
            this.runnable.run();
        }
    }

    /**
     * gRPC 任务拒绝处理器, 在线程池饱和时回写流控响应
     */
    protected class GrpcTaskRejectedExecutionHandler implements RejectedExecutionHandler {

        /**
         * 构造任务拒绝处理器
         */
        public GrpcTaskRejectedExecutionHandler() {

        }

        /**
         * 处理任务拒绝事件
         *
         * @param r 被拒绝任务
         * @param executor 触发拒绝的线程池
         */
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if (r instanceof GrpcTask) {
                try {
                    GrpcTask grpcTask = (GrpcTask) r;
                    writeResponse(grpcTask.context, grpcTask.request, grpcTask.executeRejectResponse, grpcTask.streamObserver, null, null);
                } catch (Throwable t) {
                    log.warn("write rejected error response failed", t);
                }
            }
        }
    }
}
