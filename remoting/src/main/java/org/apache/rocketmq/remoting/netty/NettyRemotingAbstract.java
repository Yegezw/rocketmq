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
package org.apache.rocketmq.remoting.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.opentelemetry.api.common.AttributesBuilder;
import org.apache.rocketmq.common.*;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ExceptionUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.metrics.RemotingMetricsManager;
import org.apache.rocketmq.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

import javax.annotation.Nullable;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.*;

/**
 * 请求方式: 同步请求, 异步请求, 单向请求
 */
public abstract class NettyRemotingAbstract {

    /**
     * Remoting logger instance.
     * <br>
     * Remoting 日志实例
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);

    /**
     * Semaphore to limit maximum number of on-going one-way requests, which protects system memory footprint.
     * <br>
     * 用于限制进行中的单向请求最大数量的信号量, 以保护系统内存占用
     */
    protected final Semaphore semaphoreOneway;

    /**
     * Semaphore to limit maximum number of on-going asynchronous requests, which protects system memory footprint.
     * <br>
     * 用于限制进行中的异步请求最大数量的信号量, 以保护系统内存占用
     */
    protected final Semaphore semaphoreAsync;

    /**
     * This map caches all on-going requests.
     * <br>
     * 该映射用于缓存所有进行中的请求
     */
    protected final ConcurrentMap<Integer /* opaque */, ResponseFuture> responseTable =
        new ConcurrentHashMap<>(256);

    /**
     * This container holds all processors per request code, aka, for each incoming request, we may look up the
     * responding processor in this map to handle the request.
     * <br>
     * key = {@link org.apache.rocketmq.remoting.protocol.RequestCode}<br>
     * 该容器按请求码保存所有处理器, 对每个入站请求, 可在该映射中查找对应处理器执行处理
     */
    protected final HashMap<Integer/* request code */, Pair<NettyRequestProcessor, ExecutorService>> processorTable =
        new HashMap<>(64);

    /**
     * Executor to feed netty events to user defined {@link ChannelEventListener}.
     * <br>
     * 用于将 Netty 事件投递给用户自定义 {@link ChannelEventListener} 的执行器
     */
    protected final NettyEventExecutor nettyEventExecutor = new NettyEventExecutor();

    /**
     * The default request processor to use in case there is no exact match in {@link #processorTable} per request
     * code.
     * <br>
     * 当 {@link #processorTable} 中不存在按请求码精确匹配的处理器时, 使用该默认处理器
     */
    protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessorPair;

    /**
     * SSL context via which to create {@link SslHandler}.
     * <br>
     * 用于创建 {@link SslHandler} 的 SSL 上下文
     */
    protected volatile SslContext sslContext;

    /**
     * custom rpc hooks
     * <br>
     * 自定义 RPC Hook 列表
     */
    protected List<RPCHook> rpcHooks = new ArrayList<>();

    /**
     * 请求处理流水线, 用于在处理器执行前扩展请求处理逻辑
     */
    protected RequestPipeline requestPipeline;

    /**
     * 关闭状态标记, true 表示当前节点处于关闭流程中
     */
    protected AtomicBoolean isShuttingDown = new AtomicBoolean(false);

    static {
        // 初始化 Netty 日志适配器
        NettyLogger.initNettyLogger();
    }

    /**
     * Constructor, specifying capacity of one-way and asynchronous semaphores.
     * <br>
     * 构造方法, 用于指定单向与异步信号量容量
     *
     * @param permitsOneway Number of permits for one-way requests.<br>单向请求可用许可数量
     * @param permitsAsync  Number of permits for asynchronous requests.<br>异步请求可用许可数量
     */
    public NettyRemotingAbstract(final int permitsOneway, final int permitsAsync) {
        this.semaphoreOneway = new Semaphore(permitsOneway, true);
        this.semaphoreAsync = new Semaphore(permitsAsync, true);
    }

    /**
     * Custom channel event listener.
     * <br>
     * 获取自定义 Channel 事件监听器
     *
     * @return custom channel event listener if defined; null otherwise.<br>
     * 若已定义则返回自定义监听器, 否则返回 null
     */
    public abstract ChannelEventListener getChannelEventListener();

    /**
     * Put a netty event to the executor.
     * <br>
     * 将 Netty 事件投递到执行器
     *
     * @param event Netty event instance.<br>Netty 事件实例
     */
    public void putNettyEvent(final NettyEvent event) {
        this.nettyEventExecutor.putNettyEvent(event);
    }

    /**
     * Entry of incoming command processing.
     *
     * <p>
     * <strong>Note:</strong>
     * The incoming remoting command may be
     * <ul>
     * <li>An inquiry request from a remote peer component;</li>
     * <li>A response to a previous request issued by this very participant.</li>
     * </ul>
     * </p>
     * <br>入站命令处理入口
     * <br>注意: 入站 Remoting 命令可能是来自远端的请求, 也可能是对本端先前请求的响应
     *
     * @param ctx Channel handler context.<br>Channel 处理上下文
     * @param msg incoming remoting command.<br>入站 Remoting 命令
     */
    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) {
        if (msg != null) {
            switch (msg.getType()) {
                case REQUEST_COMMAND:
                    processRequestCommand(ctx, msg);
                    break;
                case RESPONSE_COMMAND:
                    processResponseCommand(ctx, msg);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * 执行请求前 RPC Hook
     *
     * @param addr    远端地址
     * @param request 请求命令
     */
    protected void doBeforeRpcHooks(String addr, RemotingCommand request) {
        if (rpcHooks.size() > 0) {
            for (RPCHook rpcHook : rpcHooks) {
                rpcHook.doBeforeRequest(addr, request);
            }
        }
    }

    /**
     * 执行响应后 RPC Hook
     *
     * @param addr     远端地址
     * @param request  请求命令
     * @param response 响应命令
     */
    public void doAfterRpcHooks(String addr, RemotingCommand request, RemotingCommand response) {
        if (rpcHooks.size() > 0) {
            for (RPCHook rpcHook : rpcHooks) {
                rpcHook.doAfterResponse(addr, request, response);
            }
        }
    }

    /**
     * 写回响应命令, 使用默认回调
     *
     * @param channel  目标通道
     * @param request  原始请求
     * @param response 响应命令
     */
    public static void writeResponse(Channel channel, RemotingCommand request, @Nullable RemotingCommand response) {
        writeResponse(channel, request, response, null);
    }

    /**
     * 写回响应命令, 并在写完成后触发回调
     *
     * @param channel  目标通道
     * @param request  原始请求
     * @param response 响应命令
     * @param callback 写回完成后的回调
     */
    public static void writeResponse(Channel channel, RemotingCommand request, @Nullable RemotingCommand response,
        Consumer<Future<?>> callback) {
        // 响应为空时直接返回, 避免空写操作
        if (response == null) {
            return;
        }
        // 构建指标标签, 记录请求码与响应码维度
        AttributesBuilder attributesBuilder = RemotingMetricsManager.newAttributesBuilder()
            .put(LABEL_IS_LONG_POLLING, request.isSuspended())
            .put(LABEL_REQUEST_CODE, RemotingHelper.getRequestCodeDesc(request.getCode()))
            .put(LABEL_RESPONSE_CODE, RemotingHelper.getResponseCodeDesc(response.getCode()));
        // 单向请求无需真正回写, 只记录延迟指标
        if (request.isOnewayRPC()) {
            attributesBuilder.put(LABEL_RESULT, RESULT_ONEWAY);
            RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
            return;
        }
        // 将响应与请求 opaque 对齐并标记为响应类型
        response.setOpaque(request.getOpaque());
        response.markResponseType();
        try {
            // 异步写回响应, 并在监听器中记录结果
            channel.writeAndFlush(response).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    log.debug("Response[request code: {}, response code: {}, opaque: {}] is written to channel{}",
                        request.getCode(), response.getCode(), response.getOpaque(), channel);
                } else {
                    log.error("Failed to write response[request code: {}, response code: {}, opaque: {}] to channel{}",
                        request.getCode(), response.getCode(), response.getOpaque(), channel, future.cause());
                }
                attributesBuilder.put(LABEL_RESULT, RemotingMetricsManager.getWriteAndFlushResult(future));
                RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
                if (callback != null) {
                    callback.accept(future);
                }
            });
        } catch (Throwable e) {
            // 写通道异常, 打印上下文并记录失败指标
            log.error("process request over, but response failed", e);
            log.error(request.toString());
            log.error(response.toString());
            attributesBuilder.put(LABEL_RESULT, RESULT_WRITE_CHANNEL_FAILED);
            RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
        }
    }

    /**
     * Process incoming request command issued by remote peer.
     * <br>
     * 处理来自远端的请求命令
     *
     * @param ctx channel handler context.<br>Channel 处理上下文
     * @param cmd request command.<br>请求命令
     */
    public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        // 根据请求码查找处理器, 未命中时回退到默认处理器
        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
        final Pair<NettyRequestProcessor, ExecutorService> pair = null == matched ? this.defaultRequestProcessorPair : matched;
        final int opaque = cmd.getOpaque();

        // 处理器不存在时直接返回不支持错误
        if (pair == null) {
            String error = " request type " + cmd.getCode() + " not supported";
            final RemotingCommand response =
                RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
            response.setOpaque(opaque);
            writeResponse(ctx.channel(), cmd, response);
            log.error(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + error);
            return;
        }

        Runnable run = buildProcessRequestHandler(ctx, cmd, pair, opaque);

        // 关闭过程中, 对新版本客户端返回 GO_AWAY 以触发迁移
        if (isShuttingDown.get()) {
            if (cmd.getVersion() > MQVersion.Version.V5_3_1.ordinal()) {
                final RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.GO_AWAY,
                    "please go away");
                response.setOpaque(opaque);
                writeResponse(ctx.channel(), cmd, response);
                log.info("proxy is shutting down, write response GO_AWAY. channel={}, requestCode={}, opaque={}", ctx.channel(), cmd.getCode(), opaque);
                return;
            }
        }

        // 处理器主动拒绝请求时返回系统繁忙
        if (pair.getObject1().rejectRequest()) {
            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                "[REJECTREQUEST]system busy, start flow control for a while");
            response.setOpaque(opaque);
            writeResponse(ctx.channel(), cmd, response);
            return;
        }

        try {
            final RequestTask requestTask = new RequestTask(run, ctx.channel(), cmd);
            //async execute task, current thread return directly
            //异步执行任务, 当前线程直接返回
            pair.getObject2().submit(requestTask);
        } catch (RejectedExecutionException e) {
            // 线程池繁忙时按周期打印告警, 并回写过载响应
            if ((System.currentTimeMillis() % 10000) == 0) {
                log.warn(RemotingHelper.parseChannelRemoteAddr(ctx.channel())
                    + ", too many requests and system thread pool busy, RejectedExecutionException "
                    + pair.getObject2().toString()
                    + " request code: " + cmd.getCode());
            }

            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                "[OVERLOAD]system busy, start flow control for a while");
            response.setOpaque(opaque);
            writeResponse(ctx.channel(), cmd, response);
        } catch (Throwable e) {
            // 请求处理异常时仅记录失败指标, 具体响应由处理链决定
            AttributesBuilder attributesBuilder = RemotingMetricsManager.newAttributesBuilder()
                .put(LABEL_REQUEST_CODE, RemotingHelper.getRequestCodeDesc(cmd.getCode()))
                .put(LABEL_RESULT, RESULT_PROCESS_REQUEST_FAILED);
            RemotingMetricsManager.rpcLatency.record(cmd.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
        }
    }

    /**
     * 构建请求处理任务
     *
     * @param ctx    Channel 处理上下文
     * @param cmd    请求命令
     * @param pair   请求处理器与执行器对
     * @param opaque 请求唯一标识
     * @return 可提交到线程池执行的处理任务
     */
    private Runnable buildProcessRequestHandler(ChannelHandlerContext ctx, RemotingCommand cmd,
        Pair<NettyRequestProcessor, ExecutorService> pair, int opaque) {
        return () -> {
            Exception exception = null;
            RemotingCommand response;
            String remoteAddr = null;

            try {
                // 解析远端地址并执行前置 Hook
                remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                try {
                    doBeforeRpcHooks(remoteAddr, cmd);
                } catch (AbortProcessException e) {
                    throw e;
                } catch (Exception e) {
                    exception = e;
                }

                // 执行可选请求流水线
                if (this.requestPipeline != null) {
                    this.requestPipeline.execute(ctx, cmd);
                }

                // 前置流程无异常时执行业务处理器, 否则构造系统错误响应
                if (exception == null) {
                    response = pair.getObject1().processRequest(ctx, cmd);
                } else {
                    response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, null);
                }

                // 执行后置 Hook
                try {
                    doAfterRpcHooks(remoteAddr, cmd, response);
                } catch (AbortProcessException e) {
                    throw e;
                } catch (Exception e) {
                    exception = e;
                }

                // 后置 Hook 出错时统一抛出, 进入异常处理分支
                if (exception != null) {
                    throw exception;
                }

                // 正常路径写回响应
                writeResponse(ctx.channel(), cmd, response);
            } catch (AbortProcessException e) {
                // 处理器主动中止时按异常携带的错误码回写
                response = RemotingCommand.createResponseCommand(e.getResponseCode(), e.getErrorMessage());
                response.setOpaque(opaque);
                writeResponse(ctx.channel(), cmd, response);
            } catch (Throwable e) {
                // 未知异常时记录日志, 非单向请求回写系统错误
                log.error("process request exception, remoteAddr: {}", remoteAddr, e);
                log.error(cmd.toString());

                if (!cmd.isOnewayRPC()) {
                    response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR,
                        UtilAll.exceptionSimpleDesc(e));
                    response.setOpaque(opaque);
                    writeResponse(ctx.channel(), cmd, response);
                }
            }
        };
    }

    /**
     * Process response from remote peer to the previous issued requests.
     * <br>
     * 处理来自远端的响应命令
     *
     * @param ctx channel handler context.<br>Channel 处理上下文
     * @param cmd response command instance.<br>响应命令实例
     */
    public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        // 使用 opaque 匹配请求与响应
        final int opaque = cmd.getOpaque();
        final ResponseFuture responseFuture = responseTable.get(opaque);
        if (responseFuture != null) {
            responseFuture.setResponseCommand(cmd);

            // 命中后立即移除, 避免重复处理
            responseTable.remove(opaque);

            // 异步回调存在时交给回调执行流程, 否则唤醒同步等待线程
            if (responseFuture.getInvokeCallback() != null) {
                // 执行回调 + 释放信号量
                executeInvokeCallback(responseFuture);
            } else {
                // 唤醒等待 + 释放信号量
                responseFuture.putResponse(cmd);
                responseFuture.release();
            }
        } else {
            log.warn("receive response, cmd={}, but not matched any request, address={}, channelId={}", cmd, RemotingHelper.parseChannelRemoteAddr(ctx.channel()), ctx.channel().id());
        }
    }

    /**
     * Execute callback in callback executor. If callback executor is null, run directly in current thread
     * <br>
     * 在回调线程池中执行回调, 若回调线程池为 null, 则在当前线程直接执行
     */
    private void executeInvokeCallback(final ResponseFuture responseFuture) {
        boolean runInThisThread = false;
        ExecutorService executor = this.getCallbackExecutor();
        // 优先在独立回调线程池中执行, 降低 I/O 线程负担
        if (executor != null && !executor.isShutdown()) {
            try {
                executor.submit(() -> {
                    try {
                        responseFuture.executeInvokeCallback();
                    } catch (Throwable e) {
                        log.warn("execute callback in executor exception, and callback throw", e);
                    } finally {
                        responseFuture.release();
                    }
                });
            } catch (Exception e) {
                runInThisThread = true;
                log.warn("execute callback in executor exception, maybe executor busy", e);
            }
        } else {
            runInThisThread = true;
        }

        // 线程池不可用或提交失败时回退到当前线程执行
        if (runInThisThread) {
            try {
                responseFuture.executeInvokeCallback();
            } catch (Throwable e) {
                log.warn("executeInvokeCallback Exception", e);
            } finally {
                responseFuture.release();
            }
        }
    }

    /**
     * Custom RPC hooks.
     * <br>
     * 获取自定义 RPC Hook 列表
     *
     * @return RPC hooks if specified; null otherwise.
     * <br>返回已注册的 RPC Hook 列表, 未注册时返回空列表
     */
    public List<RPCHook> getRPCHook() {
        return rpcHooks;
    }

    /**
     * 注册 RPC Hook
     *
     * @param rpcHook 待注册 Hook
     */
    public void registerRPCHook(RPCHook rpcHook) {
        if (rpcHook != null && !rpcHooks.contains(rpcHook)) {
            rpcHooks.add(rpcHook);
        }
    }

    /**
     * 设置请求处理流水线
     *
     * @param pipeline 请求流水线
     */
    public void setRequestPipeline(RequestPipeline pipeline) {
        this.requestPipeline = pipeline;
    }

    /**
     * 清空所有 RPC Hook
     */
    public void clearRPCHook() {
        rpcHooks.clear();
    }

    /**
     * This method specifies thread pool to use while invoking callback methods.
     * <br>
     * 指定执行回调方法时使用的线程池
     *
     * @return Dedicated thread pool instance if specified; or null if the callback is supposed to be executed in the
     * netty event-loop thread.
     * <br>若已指定则返回专用线程池, 否则返回 null 并在 Netty 事件循环线程中执行回调
     */
    public abstract ExecutorService getCallbackExecutor();

    /**
     * <p>
     * This method is periodically invoked to scan and expire deprecated request.
     * </p>
     * <p>
     * 该方法会被周期性调用, 用于扫描并清理已过期请求
     * </p>
     */
    public void scanResponseTable() {
        // 先收集过期请求, 再统一触发回调, 避免边遍历边回调带来的并发干扰
        final List<ResponseFuture> rfList = new LinkedList<>();
        Iterator<Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, ResponseFuture> next = it.next();
            ResponseFuture rep = next.getValue();

            // 超时请求释放资源并移出响应表
            if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000) <= System.currentTimeMillis()) {
                rep.release();
                it.remove();
                rfList.add(rep);
                log.warn("remove timeout request, " + rep);
            }
        }

        // 对过期请求触发回调, 通知调用方失败
        for (ResponseFuture rf : rfList) {
            try {
                executeInvokeCallback(rf); // 执行回调 + 释放信号量
            } catch (Throwable e) {
                log.warn("scanResponseTable, operationComplete Exception", e);
            }
        }
    }

    /**
     * 发起同步调用
     *
     * @param channel       目标通道
     * @param request       请求命令
     * @param timeoutMillis 超时时间, 单位为毫秒
     * @return 响应命令
     * @throws InterruptedException         等待响应期间线程中断
     * @throws RemotingSendRequestException 请求发送失败
     * @throws RemotingTimeoutException     请求超时
     */
    public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request,
        final long timeoutMillis)
        throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        // 基于异步实现封装同步等待, 超时后抛出超时异常
        try {
            return invokeImpl(channel, request, timeoutMillis).thenApply(ResponseFuture::getResponseCommand)
                .get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new RemotingSendRequestException(channel.remoteAddress().toString(), e.getCause());
        } catch (TimeoutException e) {
            throw new RemotingTimeoutException(channel.remoteAddress().toString(), timeoutMillis, e.getCause());
        }
    }

    /**
     * 异步调用统一入口
     *
     * @param channel       目标通道
     * @param request       请求命令
     * @param timeoutMillis 超时时间, 单位为毫秒
     * @return 异步响应 Future
     */
    public CompletableFuture<ResponseFuture> invokeImpl(final Channel channel, final RemotingCommand request,
        final long timeoutMillis) {
        return invoke0(channel, request, timeoutMillis);
    }

    /**
     * 执行异步请求发送与响应匹配
     *
     * @param channel       目标通道
     * @param request       请求命令
     * @param timeoutMillis 超时时间, 单位为毫秒
     * @return 包含 ResponseFuture 的 CompletableFuture
     */
    protected CompletableFuture<ResponseFuture> invoke0(final Channel channel, final RemotingCommand request,
        final long timeoutMillis) {
        // future 用于向上层传播发送结果与回调状态
        CompletableFuture<ResponseFuture> future = new CompletableFuture<>();
        long beginStartTime = System.currentTimeMillis();
        final int opaque = request.getOpaque();

        // 先尝试获取异步信号量, 控制并发请求上限
        boolean acquired;
        try {
            acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS); // 获取信号量
        } catch (Throwable t) {
            future.completeExceptionally(t);
            return future;
        }
        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);
            long costTime = System.currentTimeMillis() - beginStartTime;
            // 若获取许可已耗尽超时预算, 直接失败返回
            if (timeoutMillis < costTime) {
                once.release();
                future.completeExceptionally(new RemotingTimeoutException("invokeAsyncImpl call timeout"));
                return future;
            }

            // 将回调与 ResponseFuture 绑定, 回调触发时完成 future
            AtomicReference<ResponseFuture> responseFutureReference = new AtomicReference<>();
            final ResponseFuture responseFuture = new ResponseFuture(channel, opaque, request, timeoutMillis - costTime,
                new InvokeCallback() {
                    /**
                     * 异步完成通知, 当前实现无额外处理
                     *
                     * @param responseFuture 响应 Future
                     */
                    @Override
                    public void operationComplete(ResponseFuture responseFuture) {

                    }

                    /**
                     * 请求成功回调, 将上层 Future 标记完成
                     *
                     * @param response 响应命令
                     */
                    @Override
                    public void operationSucceed(RemotingCommand response) {
                        future.complete(responseFutureReference.get());
                    }

                    /**
                     * 请求失败回调, 将上层 Future 标记异常
                     *
                     * @param throwable 失败异常
                     */
                    @Override
                    public void operationFail(Throwable throwable) {
                        future.completeExceptionally(throwable);
                    }
                }, once);
            responseFutureReference.set(responseFuture);
            // 先放入响应表, 再发请求, 避免响应先到导致丢失
            this.responseTable.put(opaque, responseFuture);
            try {
                channel.writeAndFlush(request).addListener((ChannelFutureListener) f -> {
                    if (f.isSuccess()) {
                        responseFuture.setSendRequestOK(true);
                        return;
                    }
                    // 发送失败时触发失败回调并移除响应表
                    requestFail(opaque);
                    log.warn("send a request command to channel <{}>, channelId={}, failed.", RemotingHelper.parseChannelRemoteAddr(channel), channel.id());
                });
                return future;
            } catch (Exception e) {
                // 写请求异常时清理状态并透传异常
                responseTable.remove(opaque);
                responseFuture.release();
                log.warn("send a request command to channel <{}> channelId={} Exception", RemotingHelper.parseChannelRemoteAddr(channel), channel.id(), e);
                future.completeExceptionally(new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e));
                return future;
            }
        } else {
            // 未获取许可时区分快速失败与等待超时
            if (timeoutMillis <= 0) {
                future.completeExceptionally(new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fast"));
            } else {
                String info =
                    String.format("invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d",
                        timeoutMillis,
                        this.semaphoreAsync.getQueueLength(),
                        this.semaphoreAsync.availablePermits()
                    );
                log.warn(info);
                future.completeExceptionally(new RemotingTimeoutException(info));
            }
            return future;
        }
    }

    /**
     * 发起异步调用并执行回调
     *
     * @param channel        目标通道
     * @param request        请求命令
     * @param timeoutMillis  超时时间, 单位为毫秒
     * @param invokeCallback 调用回调
     */
    public void invokeAsyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis,
        final InvokeCallback invokeCallback) {
        // 先触发 operationComplete, 再根据结果触发 succeed 或 fail
        invokeImpl(channel, request, timeoutMillis)
            .whenComplete((v, t) -> {
                if (t == null) {
                    invokeCallback.operationComplete(v);
                } else {
                    ResponseFuture responseFuture = new ResponseFuture(channel, request.getOpaque(), request, timeoutMillis, null, null);
                    responseFuture.setCause(t);
                    invokeCallback.operationComplete(responseFuture);
                }
            })
            .thenAccept(responseFuture -> invokeCallback.operationSucceed(responseFuture.getResponseCommand()))
            .exceptionally(t -> {
                invokeCallback.operationFail(ExceptionUtils.getRealException(t));
                return null;
            });
    }

    /**
     * 处理请求发送失败逻辑
     *
     * @param opaque 请求唯一标识
     */
    private void requestFail(final int opaque) {
        // 从响应表移除并触发失败回调
        ResponseFuture responseFuture = responseTable.remove(opaque);
        if (responseFuture != null) {
            responseFuture.setSendRequestOK(false);
            responseFuture.putResponse(null);
            try {
                executeInvokeCallback(responseFuture);
            } catch (Throwable e) {
                log.warn("execute callback in requestFail, and callback throw", e);
            } finally {
                responseFuture.release();
            }
        }
    }

    /**
     * mark the request of the specified channel as fail and to invoke fail callback immediately
     * <br>
     * 将指定 Channel 上的请求标记为失败, 并立即触发失败回调
     *
     * @param channel the channel which is close already<br>已关闭的 Channel
     */
    protected void failFast(final Channel channel) {
        // 扫描同一通道上的请求, 逐个触发失败处理
        for (Entry<Integer, ResponseFuture> entry : responseTable.entrySet()) {
            if (entry.getValue().getChannel() == channel) {
                Integer opaque = entry.getKey();
                if (opaque != null) {
                    requestFail(opaque);
                }
            }
        }
    }

    /**
     * 发起单向调用
     *
     * @param channel       目标通道
     * @param request       请求命令
     * @param timeoutMillis 超时时间, 单位为毫秒
     * @throws InterruptedException            等待信号量时线程中断
     * @throws RemotingTooMuchRequestException 请求过快导致拒绝
     * @throws RemotingTimeoutException        获取信号量超时
     * @throws RemotingSendRequestException    请求发送失败
     */
    public void invokeOnewayImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        // 单向请求无需等待响应
        request.markOnewayRPC();
        boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);
            try {
                channel.writeAndFlush(request).addListener((ChannelFutureListener) f -> {
                    // 无论发送成功或失败都释放许可
                    once.release();
                    if (!f.isSuccess()) {
                        log.warn("send a request command to channel <" + channel.remoteAddress() + "> failed.");
                    }
                });
            } catch (Exception e) {
                once.release();
                log.warn("write send a request command to channel <" + channel.remoteAddress() + "> failed.");
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            // 未获取许可时区分过快调用与等待超时
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeOnewayImpl invoke too fast");
            } else {
                String info = String.format(
                    "invokeOnewayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreOnewayValue: %d",
                    timeoutMillis,
                    this.semaphoreOneway.getQueueLength(),
                    this.semaphoreOneway.availablePermits()
                );
                log.warn(info);
                throw new RemotingTimeoutException(info);
            }
        }
    }

    /**
     * 获取请求码到处理器映射表
     *
     * @return 处理器映射表
     */
    public HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>> getProcessorTable() {
        return processorTable;
    }

    /**
     * Netty 事件分发线程
     */
    class NettyEventExecutor extends ServiceThread {

        /**
         * 事件队列, 用于缓存待分发的 Netty 事件
         */
        private final LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<>();

        /**
         * 向事件队列投递 Netty 事件
         *
         * @param event Netty 事件
         */
        public void putNettyEvent(final NettyEvent event) {
            int currentSize = this.eventQueue.size();
            int maxSize = 10000;
            if (currentSize <= maxSize) {
                this.eventQueue.add(event);
            } else {
                log.warn("event queue size [{}] over the limit [{}], so drop this event {}", currentSize, maxSize, event.toString());
            }
        }

        /**
         * 事件分发主循环
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            final ChannelEventListener listener = NettyRemotingAbstract.this.getChannelEventListener();

            while (!this.isStopped()) {
                try {
                    // 轮询事件队列并按事件类型回调监听器
                    NettyEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                    if (event != null && listener != null) {
                        switch (event.getType()) {
                            case IDLE:
                                listener.onChannelIdle(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CLOSE:
                                listener.onChannelClose(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CONNECT:
                                listener.onChannelConnect(event.getRemoteAddr(), event.getChannel());
                                break;
                            case EXCEPTION:
                                listener.onChannelException(event.getRemoteAddr(), event.getChannel());
                                break;
                            case ACTIVE:
                                listener.onChannelActive(event.getRemoteAddr(), event.getChannel());
                                break;
                            default:
                                break;

                        }
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * 获取服务线程名称
         *
         * @return 线程名称
         */
        @Override
        public String getServiceName() {
            return NettyEventExecutor.class.getSimpleName();
        }
    }
}
