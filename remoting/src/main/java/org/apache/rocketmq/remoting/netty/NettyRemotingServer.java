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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.ProtocolDetectionResult;
import io.netty.handler.codec.ProtocolDetectionState;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyTLV;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.HAProxyConstants;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.BinaryUtil;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Netty Remoting 服务端实现
 */
public class NettyRemotingServer extends NettyRemotingAbstract implements RemotingServer {
    /**
     * Remoting 日志记录器
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);
    /**
     * 流量分布日志记录器
     */
    private static final Logger TRAFFIC_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_TRAFFIC_NAME);

    /**
     * 服务端 Bootstrap
     */
    private final ServerBootstrap serverBootstrap;
    /**
     * Selector 事件循环组 3
     */
    protected final EventLoopGroup eventLoopGroupSelector;
    /**
     * Boss 事件循环组 1
     */
    protected final EventLoopGroup eventLoopGroupBoss;
    /**
     * 服务端配置
     */
    protected final NettyServerConfig nettyServerConfig;

    /**
     * 公共执行线程池 4
     */
    private final ExecutorService publicExecutor;
    /**
     * 定时调度线程池 1
     */
    private final ScheduledExecutorService scheduledExecutorService;
    /**
     * Channel 事件监听器
     */
    private final ChannelEventListener channelEventListener;

    /**
     * 服务端定时器
     */
    private final HashedWheelTimer timer = new HashedWheelTimer(r -> new Thread(r, "ServerHouseKeepingService"));

    /**
     * 默认业务执行器组 8
     */
    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    /**
     * NettyRemotingServer may hold multiple SubRemotingServer, each server will be stored in this container with a
     * ListenPort key.
     * <br>
     * NettyRemotingServer 可能持有多个 SubRemotingServer, 每个服务会以 ListenPort 作为键存储在该容器中
     */
    private final ConcurrentMap<Integer/*Port*/, NettyRemotingAbstract> remotingServerTable = new ConcurrentHashMap<>();

    /**
     * 握手处理器名称
     */
    public static final String HANDSHAKE_HANDLER_NAME = "handshakeHandler";
    /**
     * HAProxy 解码器名称
     */
    public static final String HA_PROXY_DECODER = "HAProxyDecoder";
    /**
     * HAProxy 处理器名称
     */
    public static final String HA_PROXY_HANDLER = "HAProxyHandler";
    /**
     * TLS 模式处理器名称
     */
    public static final String TLS_MODE_HANDLER = "TlsModeHandler";
    /**
     * TLS 处理器名称
     */
    public static final String TLS_HANDLER_NAME = "sslHandler";
    /**
     * FileRegion 编码器名称
     */
    public static final String FILE_REGION_ENCODER_NAME = "fileRegionEncoder";

    // sharable handlers
    // 可共享处理器
    /**
     * TLS 模式处理器
     */
    protected final TlsModeHandler tlsModeHandler = new TlsModeHandler(TlsSystemConfig.tlsMode);
    /**
     * 编码处理器
     */
    protected final NettyEncoder encoder = new NettyEncoder();
    /**
     * 连接管理处理器
     */
    protected final NettyConnectManageHandler connectionManageHandler = new NettyConnectManageHandler();
    /**
     * 服务端消息处理器
     */
    protected final NettyServerHandler serverHandler = new NettyServerHandler();
    /**
     * 请求与响应码分布统计处理器
     */
    protected final RemotingCodeDistributionHandler distributionHandler = new RemotingCodeDistributionHandler();

    /**
     * 构造服务端实例
     *
     * @param nettyServerConfig 服务端配置
     */
    public NettyRemotingServer(final NettyServerConfig nettyServerConfig) {
        this(nettyServerConfig, null);
    }

    /**
     * 构造服务端实例
     *
     * @param nettyServerConfig    服务端配置
     * @param channelEventListener Channel 事件监听器
     */
    public NettyRemotingServer(final NettyServerConfig nettyServerConfig,
                               final ChannelEventListener channelEventListener) {
        super(nettyServerConfig.getServerOnewaySemaphoreValue(), nettyServerConfig.getServerAsyncSemaphoreValue());
        this.serverBootstrap = new ServerBootstrap();
        this.nettyServerConfig = nettyServerConfig;
        this.channelEventListener = channelEventListener;

        this.publicExecutor = buildPublicExecutor(nettyServerConfig);
        this.scheduledExecutorService = buildScheduleExecutor();

        this.eventLoopGroupBoss = buildEventLoopGroupBoss();
        this.eventLoopGroupSelector = buildEventLoopGroupSelector();

        loadSslContext();
    }

    /**
     * 构建 Selector 事件循环组
     *
     * @return EventLoopGroup
     */
    protected EventLoopGroup buildEventLoopGroupSelector() {
        if (useEpoll()) {
            return new EpollEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactoryImpl("NettyServerEPOLLSelector_"));
        } else {
            return new NioEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactoryImpl("NettyServerNIOSelector_"));
        }
    }

    /**
     * 构建 Boss 事件循环组
     *
     * @return EventLoopGroup
     */
    protected EventLoopGroup buildEventLoopGroupBoss() {
        if (useEpoll()) {
            return new EpollEventLoopGroup(1, new ThreadFactoryImpl("NettyEPOLLBoss_"));
        } else {
            return new NioEventLoopGroup(1, new ThreadFactoryImpl("NettyNIOBoss_"));
        }
    }

    /**
     * 构建公共线程池
     *
     * @param nettyServerConfig 服务端配置
     * @return ExecutorService
     */
    private ExecutorService buildPublicExecutor(NettyServerConfig nettyServerConfig) {
        int publicThreadNums = nettyServerConfig.getServerCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        return Executors.newFixedThreadPool(publicThreadNums, new ThreadFactoryImpl("NettyServerPublicExecutor_"));
    }

    /**
     * 构建定时调度线程池
     *
     * @return ScheduledExecutorService
     */
    private ScheduledExecutorService buildScheduleExecutor() {
        return ThreadUtils.newScheduledThreadPool(1,
            new ThreadFactoryImpl("NettyServerScheduler_", true),
            new ThreadPoolExecutor.DiscardOldestPolicy());
    }

    /**
     * 加载 TLS 上下文
     */
    public void loadSslContext() {
        TlsMode tlsMode = TlsSystemConfig.tlsMode;
        log.info("Server is running in TLS {} mode", tlsMode.getName());

        if (tlsMode != TlsMode.DISABLED) {
            try {
                sslContext = TlsHelper.buildSslContext(false);
                log.info("SSLContext created for server");
            } catch (CertificateException | IOException e) {
                log.error("Failed to create SSLContext for server", e);
            }
        }
    }

    /**
     * 判断是否使用 Epoll
     *
     * @return 可用且已启用返回 true, 否则返回 false
     */
    private boolean useEpoll() {
        return NetworkUtil.isLinuxPlatform()
            && nettyServerConfig.isUseEpollNativeSelector()
            && Epoll.isAvailable();
    }

    /**
     * 初始化服务端 Bootstrap
     *
     * @param serverBootstrap Bootstrap
     */
    protected void initServerBootstrap(ServerBootstrap serverBootstrap) {
        serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupSelector)
            .channel(useEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
            .option(ChannelOption.SO_BACKLOG, 1024)
            .option(ChannelOption.SO_REUSEADDR, true)
            .childOption(ChannelOption.SO_KEEPALIVE, false)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .localAddress(new InetSocketAddress(this.nettyServerConfig.getBindAddress(),
                this.nettyServerConfig.getListenPort()))
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) {
                    configChannel(ch); // 配置接收到的客户端 channel - 设置 pipeline
                }
            });

        addCustomConfig(serverBootstrap);
    }

    /**
     * 启动服务端
     */
    @Override
    public void start() {
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(nettyServerConfig.getServerWorkerThreads(),
            new ThreadFactoryImpl("NettyServerCodecThread_"));

        initServerBootstrap(serverBootstrap);

        try {
            ChannelFuture sync = serverBootstrap.bind().sync();
            InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
            if (0 == nettyServerConfig.getListenPort()) {
                this.nettyServerConfig.setListenPort(addr.getPort());
            }
            log.info("RemotingServer started, listening {}:{}", this.nettyServerConfig.getBindAddress(),
                this.nettyServerConfig.getListenPort());
            this.remotingServerTable.put(this.nettyServerConfig.getListenPort(), this);
        } catch (Exception e) {
            throw new IllegalStateException(String.format("Failed to bind to %s:%d", nettyServerConfig.getBindAddress(),
                nettyServerConfig.getListenPort()), e);
        }

        if (this.channelEventListener != null) {
            this.nettyEventExecutor.start();
        }

        TimerTask timerScanResponseTable = new TimerTask() {
            // 周期扫描响应表
            @Override
            public void run(Timeout timeout) {
                try {
                    NettyRemotingServer.this.scanResponseTable();
                } catch (Throwable e) {
                    log.error("scanResponseTable exception", e);
                } finally {
                    timer.newTimeout(this, 1000, TimeUnit.MILLISECONDS);
                }
            }
        };
        this.timer.newTimeout(timerScanResponseTable, 1000 * 3, TimeUnit.MILLISECONDS);

        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                NettyRemotingServer.this.printRemotingCodeDistribution();
            } catch (Throwable e) {
                TRAFFIC_LOGGER.error("NettyRemotingServer print remoting code distribution exception", e);
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    /**
     * config channel in ChannelInitializer
     * <br>
     * 在 ChannelInitializer 中配置 Channel
     *
     * @param ch the SocketChannel needed to init<br>需要初始化的 SocketChannel
     * @return the initialized ChannelPipeline, sub class can use it to extent in the future<br>
     * 已初始化的 ChannelPipeline, 子类未来可在此基础上扩展
     */
    protected ChannelPipeline configChannel(SocketChannel ch) {
        return ch.pipeline()
            .addLast(
                    nettyServerConfig.isServerNettyWorkerGroupEnable() ? defaultEventExecutorGroup : null, // 事件线程组执行 Pipeline#handler
                    HANDSHAKE_HANDLER_NAME,
                    new HandshakeHandler()
            )
            .addLast(
                    nettyServerConfig.isServerNettyWorkerGroupEnable() ? defaultEventExecutorGroup : null, // 事件线程组执行 Pipeline#handler
                    encoder,                 // 编码器 Object  -> Bytebuf    Out
                    new NettyDecoder(),      // 解码器 Bytebuf -> Object     In
                    distributionHandler,     // 统计请求响应码分布
                    new IdleStateHandler(0, 0, nettyServerConfig.getServerChannelMaxIdleTimeSeconds()), // 空闲检测
                    connectionManageHandler, // 连接管理器
                    serverHandler            // 处理器
            );
    }

    /**
     * 附加自定义网络参数配置
     *
     * @param childHandler ServerBootstrap
     */
    private void addCustomConfig(ServerBootstrap childHandler) {
        // 服务器发送缓冲区的默认大小
        if (nettyServerConfig.getServerSocketSndBufSize() > 0) {
            log.info("server set SO_SNDBUF to {}", nettyServerConfig.getServerSocketSndBufSize());
            childHandler.childOption(ChannelOption.SO_SNDBUF, nettyServerConfig.getServerSocketSndBufSize());
        }
        // 服务器接收缓冲区的默认大小
        if (nettyServerConfig.getServerSocketRcvBufSize() > 0) {
            log.info("server set SO_RCVBUF to {}", nettyServerConfig.getServerSocketRcvBufSize());
            childHandler.childOption(ChannelOption.SO_RCVBUF, nettyServerConfig.getServerSocketRcvBufSize());
        }
        // 写缓冲区默认高低水位线
        if (nettyServerConfig.getWriteBufferLowWaterMark() > 0 && nettyServerConfig.getWriteBufferHighWaterMark() > 0) {
            log.info("server set netty WRITE_BUFFER_WATER_MARK to {},{}",
                nettyServerConfig.getWriteBufferLowWaterMark(), nettyServerConfig.getWriteBufferHighWaterMark());
            childHandler.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(
                nettyServerConfig.getWriteBufferLowWaterMark(), nettyServerConfig.getWriteBufferHighWaterMark()));
        }

        // 开启 Netty 内存池化功能 (ByteBuf 池化)
        if (nettyServerConfig.isServerPooledByteBufAllocatorEnable()) {
            childHandler.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }
    }

    /**
     * 关闭服务端并释放资源
     */
    @Override
    public void shutdown() {
        try {
            if (nettyServerConfig.isEnableShutdownGracefully() && isShuttingDown.compareAndSet(false, true)) {
                Thread.sleep(Duration.ofSeconds(nettyServerConfig.getShutdownWaitTimeSeconds()).toMillis());
            }

            this.timer.stop();

            this.eventLoopGroupBoss.shutdownGracefully();

            this.eventLoopGroupSelector.shutdownGracefully();

            this.nettyEventExecutor.shutdown();

            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            log.error("NettyRemotingServer shutdown exception, ", e);
        }

        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                log.error("NettyRemotingServer shutdown exception, ", e);
            }
        }
    }

    /**
     * 注册请求处理器
     *
     * @param requestCode 请求码
     * @param processor   处理器
     * @param executor    执行器, 为 null 时使用公共线程池
     */
    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        ExecutorService executorThis = executor;
        if (null == executor) {
            executorThis = this.publicExecutor;
        }

        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<>(processor, executorThis);
        this.processorTable.put(requestCode, pair);
    }

    /**
     * 注册默认处理器
     *
     * @param processor 处理器
     * @param executor  执行器
     */
    @Override
    public void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executor) {
        this.defaultRequestProcessorPair = new Pair<>(processor, executor);
    }

    /**
     * 获取本地监听端口
     *
     * @return 监听端口
     */
    @Override
    public int localListenPort() {
        return this.nettyServerConfig.getListenPort();
    }

    @Override
    public Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(int requestCode) {
        return processorTable.get(requestCode);
    }

    @Override
    public Pair<NettyRequestProcessor, ExecutorService> getDefaultProcessorPair() {
        return defaultRequestProcessorPair;
    }

    /**
     * 创建子 Remoting 服务
     *
     * @param port 监听端口
     * @return 新建的 Remoting 服务
     */
    @Override
    public RemotingServer newRemotingServer(final int port) {
        SubRemotingServer remotingServer = new SubRemotingServer(port,
            this.nettyServerConfig.getServerOnewaySemaphoreValue(),
            this.nettyServerConfig.getServerAsyncSemaphoreValue());
        NettyRemotingAbstract existingServer = this.remotingServerTable.putIfAbsent(port, remotingServer);
        if (existingServer != null) {
            throw new RuntimeException("The port " + port + " already in use by another RemotingServer");
        }
        return remotingServer;
    }

    /**
     * 移除子 Remoting 服务
     *
     * @param port 监听端口
     */
    @Override
    public void removeRemotingServer(final int port) {
        this.remotingServerTable.remove(port);
    }

    /**
     * 同步调用
     *
     * @param channel       Channel
     * @param request       请求命令
     * @param timeoutMillis 超时时间, 单位为毫秒
     * @return 响应命令
     * @throws InterruptedException         等待期间线程中断
     * @throws RemotingSendRequestException 发送异常
     * @throws RemotingTimeoutException     超时异常
     */
    @Override
    public RemotingCommand invokeSync(final Channel channel, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        return this.invokeSyncImpl(channel, request, timeoutMillis);
    }

    /**
     * 异步调用
     *
     * @param channel        Channel
     * @param request        请求命令
     * @param timeoutMillis  超时时间, 单位为毫秒
     * @param invokeCallback 回调
     * @throws InterruptedException            等待期间线程中断
     * @throws RemotingTooMuchRequestException 请求过多
     * @throws RemotingTimeoutException        超时异常
     * @throws RemotingSendRequestException    发送异常
     */
    @Override
    public void invokeAsync(Channel channel, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
        throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
    }

    /**
     * 单向调用
     *
     * @param channel       Channel
     * @param request       请求命令
     * @param timeoutMillis 超时时间, 单位为毫秒
     * @throws InterruptedException            等待期间线程中断
     * @throws RemotingTooMuchRequestException 请求过多
     * @throws RemotingTimeoutException        超时异常
     * @throws RemotingSendRequestException    发送异常
     */
    @Override
    public void invokeOneway(Channel channel, RemotingCommand request, long timeoutMillis) throws InterruptedException,
        RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        this.invokeOnewayImpl(channel, request, timeoutMillis);
    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return channelEventListener;
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return this.publicExecutor;
    }

    /**
     * 打印请求码与响应码分布信息
     */
    private void printRemotingCodeDistribution() {
        if (distributionHandler != null) {
            String inBoundSnapshotString = distributionHandler.getInBoundSnapshotString();
            if (inBoundSnapshotString != null) {
                TRAFFIC_LOGGER.info("Port: {}, RequestCode Distribution: {}",
                    nettyServerConfig.getListenPort(), inBoundSnapshotString);
            }

            String outBoundSnapshotString = distributionHandler.getOutBoundSnapshotString();
            if (outBoundSnapshotString != null) {
                TRAFFIC_LOGGER.info("Port: {}, ResponseCode Distribution: {}",
                    nettyServerConfig.getListenPort(), outBoundSnapshotString);
            }
        }
    }

    public DefaultEventExecutorGroup getDefaultEventExecutorGroup() {
        return defaultEventExecutorGroup;
    }

    public NettyEncoder getEncoder() {
        return encoder;
    }

    public NettyConnectManageHandler getConnectionManageHandler() {
        return connectionManageHandler;
    }

    public NettyServerHandler getServerHandler() {
        return serverHandler;
    }

    public RemotingCodeDistributionHandler getDistributionHandler() {
        return distributionHandler;
    }

    /**
     * 握手探测处理器
     */
    public class HandshakeHandler extends ByteToMessageDecoder {

        /**
         * 构造握手处理器
         */
        public HandshakeHandler() {
        }

        /**
         * 检测 HAProxy 与 TLS 协议并动态安装后续处理器
         *
         * @param ctx     Channel 上下文
         * @param byteBuf 输入缓冲区
         * @param out     输出列表
         * @throws Exception 处理异常
         */
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> out) throws Exception {
            try {
                ProtocolDetectionResult<HAProxyProtocolVersion> detectionResult = HAProxyMessageDecoder.detectProtocol(byteBuf);
                if (detectionResult.state() == ProtocolDetectionState.NEEDS_MORE_DATA) {
                    return;
                }
                if (detectionResult.state() == ProtocolDetectionState.DETECTED) {
                    ctx.pipeline().addAfter(defaultEventExecutorGroup, ctx.name(), HA_PROXY_DECODER, new HAProxyMessageDecoder())
                        .addAfter(defaultEventExecutorGroup, HA_PROXY_DECODER, HA_PROXY_HANDLER, new HAProxyMessageHandler())
                        .addAfter(defaultEventExecutorGroup, HA_PROXY_HANDLER, TLS_MODE_HANDLER, tlsModeHandler);
                } else {
                    ctx.pipeline().addAfter(defaultEventExecutorGroup, ctx.name(), TLS_MODE_HANDLER, tlsModeHandler);
                }

                try {
                    // Remove this handler
                    // 移除当前处理器
                    ctx.pipeline().remove(this);
                } catch (NoSuchElementException e) {
                    log.error("Error while removing HandshakeHandler", e);
                }
            } catch (Exception e) {
                log.error("process proxy protocol negotiator failed.", e);
                throw e;
            }
        }
    }

    /**
     * TLS 模式协商处理器
     */
    @ChannelHandler.Sharable
    public class TlsModeHandler extends SimpleChannelInboundHandler<ByteBuf> {

        /**
         * 当前 TLS 模式
         */
        private final TlsMode tlsMode;

        /**
         * TLS 握手魔数
         */
        private static final byte HANDSHAKE_MAGIC_CODE = 0x16;

        /**
         * 构造 TLS 模式处理器
         *
         * @param tlsMode TLS 模式
         */
        TlsModeHandler(TlsMode tlsMode) {
            this.tlsMode = tlsMode;
        }

        /**
         * 根据首字节判断是否为 TLS 握手, 并按模式处理
         *
         * @param ctx Channel 上下文
         * @param msg 输入缓冲区
         */
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {

            // Peek the current read index byte to determine if the content is starting with TLS handshake
            // 读取当前读索引字节, 以判断内容是否以 TLS 握手开头
            byte b = msg.getByte(msg.readerIndex());

            if (b == HANDSHAKE_MAGIC_CODE) {
                switch (tlsMode) {
                    case DISABLED:
                        ctx.close();
                        log.warn("Clients intend to establish an SSL connection while this server is running in SSL disabled mode");
                        throw new UnsupportedOperationException("The NettyRemotingServer in SSL disabled mode doesn't support ssl client");
                    case PERMISSIVE:
                    case ENFORCING:
                        if (null != sslContext) {
                            ctx.pipeline()
                                .addAfter(defaultEventExecutorGroup, TLS_MODE_HANDLER, TLS_HANDLER_NAME, sslContext.newHandler(ctx.channel().alloc()))
                                .addAfter(defaultEventExecutorGroup, TLS_HANDLER_NAME, FILE_REGION_ENCODER_NAME, new FileRegionEncoder());
                            log.info("Handlers prepended to channel pipeline to establish SSL connection");
                        } else {
                            ctx.close();
                            log.error("Trying to establish an SSL connection but sslContext is null");
                        }
                        break;

                    default:
                        log.warn("Unknown TLS mode");
                        break;
                }
            } else if (tlsMode == TlsMode.ENFORCING) {
                ctx.close();
                log.warn("Clients intend to establish an insecure connection while this server is running in SSL enforcing mode");
            }

            try {
                // Remove this handler
                // 移除当前处理器
                ctx.pipeline().remove(this);
            } catch (NoSuchElementException e) {
                log.error("Error while removing TlsModeHandler", e);
            }

            // Hand over this message to the next .
            // 将消息移交给下一个处理器
            ctx.fireChannelRead(msg.retain());
        }
    }

    /**
     * 服务端命令处理器
     */
    @ChannelHandler.Sharable
    public class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        /**
         * 处理入站 RemotingCommand
         *
         * @param ctx Channel 上下文
         * @param msg 请求命令
         */
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) {
            int localPort = RemotingHelper.parseSocketAddressPort(ctx.channel().localAddress());
            NettyRemotingAbstract remotingAbstract = NettyRemotingServer.this.remotingServerTable.get(localPort);
            if (localPort != -1 && remotingAbstract != null) {
                remotingAbstract.processMessageReceived(ctx, msg);
                return;
            }
            // The related remoting server has been shutdown, so close the connected channel
            // 关联 Remoting 服务已关闭, 因此关闭当前连接 Channel
            RemotingHelper.closeChannel(ctx.channel());
        }

        /**
         * 处理 Channel 可写状态变化
         *
         * @param ctx Channel 上下文
         * @throws Exception 处理异常
         */
        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            Channel channel = ctx.channel();
            if (channel.isWritable()) {
                if (!channel.config().isAutoRead()) {
                    channel.config().setAutoRead(true);
                    log.info("Channel[{}] turns writable, bytes to buffer before changing channel to un-writable: {}",
                        RemotingHelper.parseChannelRemoteAddr(channel), channel.bytesBeforeUnwritable());
                }
            } else {
                channel.config().setAutoRead(false);
                log.warn("Channel[{}] auto-read is disabled, bytes to drain before it turns writable: {}",
                    RemotingHelper.parseChannelRemoteAddr(channel), channel.bytesBeforeWritable());
            }
            super.channelWritabilityChanged(ctx);
        }
    }

    /**
     * 连接状态管理处理器
     */
    @ChannelHandler.Sharable
    public class NettyConnectManageHandler extends ChannelDuplexHandler {

        /**
         * 处理 Channel 注册事件
         *
         * @param ctx Channel 上下文
         * @throws Exception 处理异常
         */
        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelRegistered {}", remoteAddress);
            super.channelRegistered(ctx);
        }

        /**
         * 处理 Channel 反注册事件
         *
         * @param ctx Channel 上下文
         * @throws Exception 处理异常
         */
        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelUnregistered, the channel[{}]", remoteAddress);
            super.channelUnregistered(ctx);
        }

        /**
         * 处理 Channel 激活事件
         *
         * @param ctx Channel 上下文
         * @throws Exception 处理异常
         */
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelActive, the channel[{}]", remoteAddress);
            super.channelActive(ctx);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remoteAddress, ctx.channel()));
            }
        }

        /**
         * 处理 Channel 非激活事件
         *
         * @param ctx Channel 上下文
         * @throws Exception 处理异常
         */
        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelInactive, the channel[{}]", remoteAddress);
            super.channelInactive(ctx);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        /**
         * 处理用户事件
         *
         * @param ctx Channel 上下文
         * @param evt 用户事件
         */
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    log.warn("NETTY SERVER PIPELINE: IDLE exception [{}]", remoteAddress);
                    RemotingHelper.closeChannel(ctx.channel());
                    if (NettyRemotingServer.this.channelEventListener != null) {
                        NettyRemotingServer.this
                            .putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
                    }
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        /**
         * 处理异常事件
         *
         * @param ctx Channel 上下文
         * @param cause 异常
         */
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.warn("NETTY SERVER PIPELINE: exceptionCaught {}", remoteAddress);
            log.warn("NETTY SERVER PIPELINE: exceptionCaught exception.", cause);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));
            }

            RemotingHelper.closeChannel(ctx.channel());
        }
    }

    /**
     * The NettyRemotingServer supports bind multiple ports, each port bound by a SubRemotingServer. The
     * SubRemotingServer will delegate all the functions to NettyRemotingServer, so the sub server can share all the
     * resources from its parent server.
     * <br>NettyRemotingServer 支持绑定多个端口, 每个端口由一个 SubRemotingServer 负责
     * <br>SubRemotingServer 会将所有功能委托给 NettyRemotingServer, 因此子服务可共享父服务资源
     */
    class SubRemotingServer extends NettyRemotingAbstract implements RemotingServer {

        /**
         * 监听端口
         */
        private volatile int listenPort;

        /**
         * 子服务绑定 Channel
         */
        private volatile Channel serverChannel;

        /**
         * 构造子 Remoting 服务
         *
         * @param port         监听端口
         * @param permitsOnway 单向信号量许可
         * @param permitsAsync 异步信号量许可
         */
        SubRemotingServer(final int port, final int permitsOnway, final int permitsAsync) {
            super(permitsOnway, permitsAsync);
            listenPort = port;
        }

        /**
         * 注册请求处理器
         *
         * @param requestCode 请求码
         * @param processor   处理器
         * @param executor    执行器, 为 null 时使用父服务公共执行器
         */
        @Override
        public void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
                                      final ExecutorService executor) {
            ExecutorService executorThis = executor;
            if (null == executor) {
                executorThis = NettyRemotingServer.this.publicExecutor;
            }

            Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<>(processor, executorThis);
            this.processorTable.put(requestCode, pair);
        }

        /**
         * 注册默认处理器
         *
         * @param processor 处理器
         * @param executor  执行器
         */
        @Override
        public void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executor) {
            this.defaultRequestProcessorPair = new Pair<>(processor, executor);
        }

        /**
         * 获取子服务监听端口
         *
         * @return 监听端口
         */
        @Override
        public int localListenPort() {
            return listenPort;
        }

        @Override
        public Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(final int requestCode) {
            return this.processorTable.get(requestCode);
        }

        @Override
        public Pair<NettyRequestProcessor, ExecutorService> getDefaultProcessorPair() {
            return this.defaultRequestProcessorPair;
        }

        /**
         * 子服务不支持继续创建嵌套 Remoting 服务
         *
         * @param port 监听端口
         * @return 不会返回
         */
        @Override
        public RemotingServer newRemotingServer(final int port) {
            throw new UnsupportedOperationException("The SubRemotingServer of NettyRemotingServer " +
                "doesn't support new nested RemotingServer");
        }

        /**
         * 子服务不支持移除嵌套 Remoting 服务
         *
         * @param port 监听端口
         */
        @Override
        public void removeRemotingServer(final int port) {
            throw new UnsupportedOperationException("The SubRemotingServer of NettyRemotingServer " +
                "doesn't support remove nested RemotingServer");
        }

        /**
         * 同步调用
         *
         * @param channel       Channel
         * @param request       请求命令
         * @param timeoutMillis 超时时间, 单位为毫秒
         * @return 响应命令
         * @throws InterruptedException         等待期间线程中断
         * @throws RemotingSendRequestException 发送异常
         * @throws RemotingTimeoutException     超时异常
         */
        @Override
        public RemotingCommand invokeSync(final Channel channel, final RemotingCommand request,
                                          final long timeoutMillis) throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
            return this.invokeSyncImpl(channel, request, timeoutMillis);
        }

        /**
         * 异步调用
         *
         * @param channel        Channel
         * @param request        请求命令
         * @param timeoutMillis  超时时间, 单位为毫秒
         * @param invokeCallback 回调
         * @throws InterruptedException            等待期间线程中断
         * @throws RemotingTooMuchRequestException 请求过多
         * @throws RemotingTimeoutException        超时异常
         * @throws RemotingSendRequestException    发送异常
         */
        @Override
        public void invokeAsync(final Channel channel, final RemotingCommand request, final long timeoutMillis,
                                final InvokeCallback invokeCallback) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
            this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
        }

        /**
         * 单向调用
         *
         * @param channel       Channel
         * @param request       请求命令
         * @param timeoutMillis 超时时间, 单位为毫秒
         * @throws InterruptedException            等待期间线程中断
         * @throws RemotingTooMuchRequestException 请求过多
         * @throws RemotingTimeoutException        超时异常
         * @throws RemotingSendRequestException    发送异常
         */
        @Override
        public void invokeOneway(final Channel channel, final RemotingCommand request,
                                 final long timeoutMillis) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
            this.invokeOnewayImpl(channel, request, timeoutMillis);
        }

        /**
         * 启动子服务
         */
        @Override
        public void start() {
            try {
                if (listenPort < 0) {
                    listenPort = 0;
                }
                this.serverChannel = NettyRemotingServer.this.serverBootstrap.bind(listenPort).sync().channel();
                if (0 == listenPort) {
                    InetSocketAddress addr = (InetSocketAddress) this.serverChannel.localAddress();
                    this.listenPort = addr.getPort();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("this.subRemotingServer.serverBootstrap.bind().sync() InterruptedException", e);
            }
        }

        /**
         * 关闭子服务
         */
        @Override
        public void shutdown() {
            isShuttingDown.set(true);
            if (this.serverChannel != null) {
                try {
                    this.serverChannel.close().await(5, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                }
            }
        }

        @Override
        public ChannelEventListener getChannelEventListener() {
            return NettyRemotingServer.this.getChannelEventListener();
        }

        @Override
        public ExecutorService getCallbackExecutor() {
            return NettyRemotingServer.this.getCallbackExecutor();
        }
    }

    /**
     * HAProxy 消息处理器
     */
    public class HAProxyMessageHandler extends ChannelInboundHandlerAdapter {

        /**
         * 处理入站 HAProxy 消息
         *
         * @param ctx Channel 上下文
         * @param msg 入站消息
         * @throws Exception 处理异常
         */
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof HAProxyMessage) {
                handleWithMessage((HAProxyMessage) msg, ctx.channel());
            } else {
                super.channelRead(ctx, msg);
            }
            ctx.pipeline().remove(this);
        }

        /**
         * The definition of key refers to the implementation of nginx
         * <a href="https://nginx.org/en/docs/http/ngx_http_core_module.html#var_proxy_protocol_addr">ngx_http_core_module</a>
         * @param msg HAProxyMessage
         * @param channel Channel
         */
        private void handleWithMessage(HAProxyMessage msg, Channel channel) {
            try {
                if (StringUtils.isNotBlank(msg.sourceAddress())) {
                    RemotingHelper.setPropertyToAttr(channel, AttributeKeys.PROXY_PROTOCOL_ADDR, msg.sourceAddress());
                }
                if (msg.sourcePort() > 0) {
                    RemotingHelper.setPropertyToAttr(channel, AttributeKeys.PROXY_PROTOCOL_PORT, String.valueOf(msg.sourcePort()));
                }
                if (StringUtils.isNotBlank(msg.destinationAddress())) {
                    RemotingHelper.setPropertyToAttr(channel, AttributeKeys.PROXY_PROTOCOL_SERVER_ADDR, msg.destinationAddress());
                }
                if (msg.destinationPort() > 0) {
                    RemotingHelper.setPropertyToAttr(channel, AttributeKeys.PROXY_PROTOCOL_SERVER_PORT, String.valueOf(msg.destinationPort()));
                }
                if (CollectionUtils.isNotEmpty(msg.tlvs())) {
                    msg.tlvs().forEach(tlv -> {
                        handleHAProxyTLV(tlv, channel);
                    });
                }
            } finally {
                msg.release();
            }
        }
    }

    /**
     * 处理 HAProxy TLV 扩展属性
     *
     * @param tlv     TLV 对象
     * @param channel Channel
     */
    protected void handleHAProxyTLV(HAProxyTLV tlv, Channel channel) {
        byte[] valueBytes = ByteBufUtil.getBytes(tlv.content());
        if (!BinaryUtil.isAscii(valueBytes)) {
            return;
        }
        AttributeKey<String> key = AttributeKeys.valueOf(
            HAProxyConstants.PROXY_PROTOCOL_TLV_PREFIX + String.format("%02x", tlv.typeByteValue()));
        RemotingHelper.setPropertyToAttr(channel, key, new String(valueBytes, CharsetUtil.UTF_8));
    }
}
