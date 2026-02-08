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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.google.common.base.Stopwatch;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.proxy.Socks5ProxyHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.resolver.NoopAddressResolverGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.FutureUtils;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.proxy.SocksProxyConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.cert.CertificateException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.rocketmq.remoting.common.RemotingHelper.convertChannelFutureToCompletableFuture;

/**
 * Netty Remoting 客户端实现
 */
public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {
    /**
     * Remoting 日志记录器
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);

    /**
     * 访问 Channel 表锁超时时间, 单位为毫秒
     */
    private static final long LOCK_TIMEOUT_MILLIS = 3000;
    /**
     * 最小关闭超时时间阈值, 单位为毫秒
     */
    private static final long MIN_CLOSE_TIMEOUT_MILLIS = 100;

    /**
     * 客户端配置对象
     */
    protected final NettyClientConfig nettyClientConfig;
    /**
     * 默认 Bootstrap
     */
    private final Bootstrap bootstrap = new Bootstrap();
    /**
     * Netty I/O 事件循环组 1
     */
    private final EventLoopGroup eventLoopGroupWorker;
    /**
     * Channel 表互斥锁
     */
    private final Lock lockChannelTables = new ReentrantLock();
    /**
     * 代理配置映射, key 为 CIDR, value 为代理配置
     */
    private final Map<String /* cidr */, SocksProxyConfig /* proxy */> proxyMap = new HashMap<>();
    /**
     * 按 CIDR 维度缓存带代理的 Bootstrap
     */
    private final ConcurrentHashMap<String /* cidr */, Bootstrap> bootstrapMap = new ConcurrentHashMap<>();
    /**
     * 地址到 ChannelWrapper 的映射
     */
    private final ConcurrentMap<String /* addr */, ChannelWrapper> channelTables = new ConcurrentHashMap<>();
    /**
     * Channel 到 ChannelWrapper 的映射
     */
    private final ConcurrentMap<Channel, ChannelWrapper> channelWrapperTables = new ConcurrentHashMap<>();

    /**
     * 客户端定时任务计时器
     */
    private final HashedWheelTimer timer = new HashedWheelTimer(r -> new Thread(r, "ClientHouseKeepingService"));

    /**
     * NameServer 地址列表
     */
    private final AtomicReference<List<String>> namesrvAddrList = new AtomicReference<>();
    /**
     * 可用 NameServer 地址集合映射
     */
    private final ConcurrentMap<String, Boolean> availableNamesrvAddrMap = new ConcurrentHashMap<>();
    /**
     * 当前选中的 NameServer 地址
     */
    private final AtomicReference<String> namesrvAddrChoosed = new AtomicReference<>();
    /**
     * NameServer 轮询索引
     */
    private final AtomicInteger namesrvIndex = new AtomicInteger(initValueIndex());
    /**
     * NameServer 通道创建锁
     */
    private final Lock namesrvChannelLock = new ReentrantLock();

    /**
     * 公共业务线程池 max(CPU, 4)
     */
    private final ExecutorService publicExecutor;
    /**
     * NameServer 可用性扫描线程池
     */
    private final ExecutorService scanExecutor;

    /**
     * Invoke the callback methods in this executor when process response.
     * <br>
     * 处理响应时, 在该执行器中执行回调方法
     */
    private ExecutorService callbackExecutor;
    /**
     * Channel 事件监听器
     */
    private final ChannelEventListener channelEventListener;
    /**
     * 默认业务执行器组 4
     */
    private EventExecutorGroup defaultEventExecutorGroup;

    /**
     * 构造 Netty Remoting 客户端
     *
     * @param nettyClientConfig 客户端配置
     */
    public NettyRemotingClient(final NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }

    /**
     * 构造 Netty Remoting 客户端
     *
     * @param nettyClientConfig    客户端配置
     * @param channelEventListener Channel 事件监听器
     */
    public NettyRemotingClient(final NettyClientConfig nettyClientConfig,
        final ChannelEventListener channelEventListener) {
        this(nettyClientConfig, channelEventListener, null, null);
    }

    /**
     * 构造 Netty Remoting 客户端
     *
     * @param nettyClientConfig    客户端配置
     * @param channelEventListener Channel 事件监听器
     * @param eventLoopGroup       外部传入 EventLoopGroup
     * @param eventExecutorGroup   外部传入 EventExecutorGroup
     */
    public NettyRemotingClient(final NettyClientConfig nettyClientConfig,
        final ChannelEventListener channelEventListener,
        final EventLoopGroup eventLoopGroup,
        final EventExecutorGroup eventExecutorGroup) {
        super(nettyClientConfig.getClientOnewaySemaphoreValue(), nettyClientConfig.getClientAsyncSemaphoreValue());
        this.nettyClientConfig = nettyClientConfig;
        this.channelEventListener = channelEventListener;

        this.loadSocksProxyJson();

        int publicThreadNums = nettyClientConfig.getClientCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactoryImpl("NettyClientPublicExecutor_"));

        this.scanExecutor = ThreadUtils.newThreadPoolExecutor(4, 10, 60, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(32), new ThreadFactoryImpl("NettyClientScan_thread_"));

        if (eventLoopGroup != null) {
            this.eventLoopGroupWorker = eventLoopGroup;
        } else {
            this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactoryImpl("NettyClientSelector_"));
        }
        this.defaultEventExecutorGroup = eventExecutorGroup;

        if (nettyClientConfig.isUseTLS()) {
            try {
                sslContext = TlsHelper.buildSslContext(true);
                LOGGER.info("SSL enabled for client");
            } catch (IOException e) {
                LOGGER.error("Failed to create SSLContext", e);
            } catch (CertificateException e) {
                LOGGER.error("Failed to create SSLContext", e);
                throw new RuntimeException("Failed to create SSLContext", e);
            }
        }
    }

    /**
     * 初始化 NameServer 轮询起始索引
     *
     * @return 随机初始索引值
     */
    private static int initValueIndex() {
        Random r = new Random();
        return r.nextInt(999);
    }

    /**
     * 读取并加载 Socks5 代理 JSON 配置
     */
    private void loadSocksProxyJson() {
        Map<String, SocksProxyConfig> sockProxyMap = JSON.parseObject(
            nettyClientConfig.getSocksProxyConfig(), new TypeReference<Map<String, SocksProxyConfig>>() {
            });
        if (sockProxyMap != null) {
            proxyMap.putAll(sockProxyMap);
        }
    }

    /**
     * 启动客户端网络组件与定时任务
     */
    @Override
    public void start() {
        if (this.defaultEventExecutorGroup == null) {
            this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
                nettyClientConfig.getClientWorkerThreads(),
                new ThreadFactoryImpl("NettyClientWorkerThread_"));
        }
        Bootstrap handler = this.bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, false)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis())
            .handler(new ChannelInitializer<SocketChannel>() {

                /**
                 * 初始化客户端 Channel Pipeline
                 *
                 * @param ch SocketChannel
                 * @throws Exception 初始化异常
                 */
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    if (nettyClientConfig.isUseTLS()) {
                        if (null != sslContext) {
                            pipeline.addFirst(defaultEventExecutorGroup, "sslHandler", sslContext.newHandler(ch.alloc()));
                            LOGGER.info("Prepend SSL handler");
                        } else {
                            LOGGER.warn("Connections are insecure as SSLContext is null!");
                        }
                    }
                    ch.pipeline().addLast(
                        nettyClientConfig.isDisableNettyWorkerGroup() ? null : defaultEventExecutorGroup,
                        new NettyEncoder(),              // 编码器 Object  -> Bytebuf    Out
                        new NettyDecoder(),              // 解码器 Bytebuf -> Object     In
                        new IdleStateHandler(0, 0, nettyClientConfig.getClientChannelMaxIdleTimeSeconds()), // 空闲检测
                        new NettyConnectManageHandler(), // 连接管理器
                        new NettyClientHandler());       // 处理器
                }
            });
        // 客户端发送缓冲区的默认大小
        if (nettyClientConfig.getClientSocketSndBufSize() > 0) {
            LOGGER.info("client set SO_SNDBUF to {}", nettyClientConfig.getClientSocketSndBufSize());
            handler.option(ChannelOption.SO_SNDBUF, nettyClientConfig.getClientSocketSndBufSize());
        }
        // 客户端接收缓冲区的默认大小
        if (nettyClientConfig.getClientSocketRcvBufSize() > 0) {
            LOGGER.info("client set SO_RCVBUF to {}", nettyClientConfig.getClientSocketRcvBufSize());
            handler.option(ChannelOption.SO_RCVBUF, nettyClientConfig.getClientSocketRcvBufSize());
        }
        // 写缓冲区默认高低水位线
        if (nettyClientConfig.getWriteBufferLowWaterMark() > 0 && nettyClientConfig.getWriteBufferHighWaterMark() > 0) {
            LOGGER.info("client set netty WRITE_BUFFER_WATER_MARK to {},{}",
                nettyClientConfig.getWriteBufferLowWaterMark(), nettyClientConfig.getWriteBufferHighWaterMark());
            handler.option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(
                nettyClientConfig.getWriteBufferLowWaterMark(), nettyClientConfig.getWriteBufferHighWaterMark()));
        }
        // 开启 Netty 内存池化功能 (ByteBuf 池化)
        if (nettyClientConfig.isClientPooledByteBufAllocatorEnable()) {
            handler.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }

        nettyEventExecutor.start();

        TimerTask timerTaskScanResponseTable = new TimerTask() {

            /**
             * 周期扫描响应表并清理超时请求
             *
             * @param timeout 定时任务上下文
             */
            @Override
            public void run(Timeout timeout) {
                try {
                    NettyRemotingClient.this.scanResponseTable();
                } catch (Throwable e) {
                    LOGGER.error("scanResponseTable exception", e);
                } finally {
                    timer.newTimeout(this, 1000, TimeUnit.MILLISECONDS);
                }
            }
        };
        this.timer.newTimeout(timerTaskScanResponseTable, 1000 * 3, TimeUnit.MILLISECONDS);

        if (nettyClientConfig.isScanAvailableNameSrv()) {
            int connectTimeoutMillis = this.nettyClientConfig.getConnectTimeoutMillis();
            TimerTask timerTaskScanAvailableNameSrv = new TimerTask() {

                /**
                 * 周期扫描可用 NameServer
                 *
                 * @param timeout 定时任务上下文
                 */
                @Override
                public void run(Timeout timeout) {
                    try {
                        NettyRemotingClient.this.scanAvailableNameSrv();
                    } catch (Exception e) {
                        LOGGER.error("scanAvailableNameSrv exception", e);
                    } finally {
                        timer.newTimeout(this, connectTimeoutMillis, TimeUnit.MILLISECONDS);
                    }
                }
            };
            this.timer.newTimeout(timerTaskScanAvailableNameSrv, 0, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 按地址匹配 Socks5 代理配置
     *
     * @param addr 目标地址
     * @return 匹配到的代理配置条目, 未匹配时返回 null
     */
    private Map.Entry<String, SocksProxyConfig> getProxy(String addr) {
        if (StringUtils.isBlank(addr) || !addr.contains(":")) {
            return null;
        }
        String[] hostAndPort = this.getHostAndPort(addr);
        for (Map.Entry<String, SocksProxyConfig> entry : proxyMap.entrySet()) {
            String cidr = entry.getKey();
            if (RemotingHelper.DEFAULT_CIDR_ALL.equals(cidr) || RemotingHelper.ipInCIDR(hostAndPort[0], cidr)) {
                return entry;
            }
        }
        return null;
    }

    /**
     * 发起异步连接
     *
     * @param addr 远端地址
     * @return ChannelFuture
     */
    protected ChannelFuture doConnect(String addr) {
        String[] hostAndPort = getHostAndPort(addr);
        String host = hostAndPort[0];
        int port = Integer.parseInt(hostAndPort[1]);
        return fetchBootstrap(addr).connect(host, port);
    }

    /**
     * 获取可用于目标地址的 Bootstrap
     *
     * @param addr 目标地址
     * @return 默认或带代理的 Bootstrap
     */
    private Bootstrap fetchBootstrap(String addr) {
        Map.Entry<String, SocksProxyConfig> proxyEntry = getProxy(addr);
        if (proxyEntry == null) {
            return bootstrap;
        }

        String cidr = proxyEntry.getKey();
        SocksProxyConfig socksProxyConfig = proxyEntry.getValue();

        LOGGER.info("Netty fetch bootstrap, addr: {}, cidr: {}, proxy: {}",
            addr, cidr, socksProxyConfig != null ? socksProxyConfig.getAddr() : "");

        Bootstrap bootstrapWithProxy = bootstrapMap.get(cidr);
        if (bootstrapWithProxy == null) {
            bootstrapWithProxy = createBootstrap(socksProxyConfig);
            Bootstrap old = bootstrapMap.putIfAbsent(cidr, bootstrapWithProxy);
            if (old != null) {
                bootstrapWithProxy = old;
            }
        }
        return bootstrapWithProxy;
    }

    /**
     * 创建带代理能力的 Bootstrap
     *
     * @param proxy Socks5 代理配置
     * @return Bootstrap 实例
     */
    private Bootstrap createBootstrap(final SocksProxyConfig proxy) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, false)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis())
            .option(ChannelOption.SO_SNDBUF, nettyClientConfig.getClientSocketSndBufSize())
            .option(ChannelOption.SO_RCVBUF, nettyClientConfig.getClientSocketRcvBufSize())
            .handler(new ChannelInitializer<SocketChannel>() {

                /**
                 * 初始化代理场景下的 Channel Pipeline
                 *
                 * @param ch SocketChannel
                 */
                @Override
                public void initChannel(SocketChannel ch) {
                    ChannelPipeline pipeline = ch.pipeline();
                    if (nettyClientConfig.isUseTLS()) {
                        if (null != sslContext) {
                            pipeline.addFirst(defaultEventExecutorGroup,
                                "sslHandler", sslContext.newHandler(ch.alloc()));
                            LOGGER.info("Prepend SSL handler");
                        } else {
                            LOGGER.warn("Connections are insecure as SSLContext is null!");
                        }
                    }

                    // Netty Socks5 Proxy
                    // Netty Socks5 代理
                    if (proxy != null) {
                        String[] hostAndPort = getHostAndPort(proxy.getAddr());
                        pipeline.addFirst(new Socks5ProxyHandler(
                            new InetSocketAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1])),
                            proxy.getUsername(), proxy.getPassword()));
                    }

                    pipeline.addLast(
                        nettyClientConfig.isDisableNettyWorkerGroup() ? null : defaultEventExecutorGroup,
                        new NettyEncoder(),
                        new NettyDecoder(),
                        new IdleStateHandler(0, 0, nettyClientConfig.getClientChannelMaxIdleTimeSeconds()),
                        new NettyConnectManageHandler(),
                        new NettyClientHandler());
                }
            });

        // Support Netty Socks5 Proxy
        // 支持 Netty Socks5 代理
        if (proxy != null) {
            bootstrap.resolver(NoopAddressResolverGroup.INSTANCE);
        }
        return bootstrap;
    }

    // Do not use RemotingHelper.string2SocketAddress(), it will directly resolve the domain
    // 不要使用 RemotingHelper.string2SocketAddress(), 它会直接解析域名
    /**
     * 解析地址中的 host 与 port
     *
     * @param address 地址字符串
     * @return host 与 port 数组
     */
    protected String[] getHostAndPort(String address) {
        int split = address.lastIndexOf(":");
        return split < 0 ? new String[]{address} : new String[]{address.substring(0, split), address.substring(split + 1)};
    }

    /**
     * 关闭客户端并释放资源
     */
    @Override
    public void shutdown() {
        try {
            this.timer.stop();

            for (Map.Entry<String, ChannelWrapper> channel : this.channelTables.entrySet()) {
                channel.getValue().close();
            }

            this.channelWrapperTables.clear();
            this.channelTables.clear();

            this.eventLoopGroupWorker.shutdownGracefully();

            if (this.nettyEventExecutor != null) {
                this.nettyEventExecutor.shutdown();
            }

            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            LOGGER.error("NettyRemotingClient shutdown exception, ", e);
        }

        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                LOGGER.error("NettyRemotingServer shutdown exception, ", e);
            }
        }

        if (this.scanExecutor != null) {
            try {
                this.scanExecutor.shutdown();
            } catch (Exception e) {
                LOGGER.error("NettyRemotingServer shutdown exception, ", e);
            }
        }
    }

    /**
     * 按地址关闭指定 Channel
     *
     * @param addr 远端地址
     * @param channel 待关闭 Channel
     */
    public void closeChannel(final String addr, final Channel channel) {
        if (null == channel) {
            return;
        }

        final String addrRemote = null == addr ? RemotingHelper.parseChannelRemoteAddr(channel) : addr;

        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    final ChannelWrapper prevCW = this.channelTables.get(addrRemote);

                    LOGGER.info("closeChannel: begin close the channel[addr={}, id={}] Found: {}", addrRemote, channel.id(), prevCW != null);

                    if (null == prevCW) {
                        LOGGER.info("closeChannel: the channel[addr={}, id={}] has been removed from the channel table before", addrRemote, channel.id());
                        removeItemFromTable = false;
                    } else if (prevCW.isWrapperOf(channel)) {
                        LOGGER.info("closeChannel: the channel[addr={}, id={}] has been closed before, and has been created again, nothing to do.",
                            addrRemote, channel.id());
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        ChannelWrapper channelWrapper = this.channelWrapperTables.remove(channel);
                        if (channelWrapper != null && channelWrapper.tryClose(channel)) {
                            this.channelTables.remove(addrRemote);
                        }
                        LOGGER.info("closeChannel: the channel[addr={}, id={}] was removed from channel table", addrRemote, channel.id());
                    }

                    RemotingHelper.closeChannel(channel);
                } catch (Exception e) {
                    LOGGER.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                LOGGER.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            LOGGER.error("closeChannel exception", e);
        }
    }

    /**
     * 关闭指定 Channel
     *
     * @param channel 待关闭 Channel
     */
    public void closeChannel(final Channel channel) {
        if (null == channel) {
            return;
        }

        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    ChannelWrapper prevCW = null;
                    String addrRemote = null;
                    for (Map.Entry<String, ChannelWrapper> entry : channelTables.entrySet()) {
                        String key = entry.getKey();
                        ChannelWrapper prev = entry.getValue();
                        if (prev.isWrapperOf(channel)) {
                            prevCW = prev;
                            addrRemote = key;
                            break;
                        }
                    }

                    if (null == prevCW) {
                        LOGGER.info("eventCloseChannel: the channel[addr={}, id={}] has been removed from the channel table before", RemotingHelper.parseChannelRemoteAddr(channel), channel.id());
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        ChannelWrapper channelWrapper = this.channelWrapperTables.remove(channel);
                        if (channelWrapper != null && channelWrapper.tryClose(channel)) {
                            this.channelTables.remove(addrRemote);
                        }
                        LOGGER.info("closeChannel: the channel[addr={}, id={}] was removed from channel table", addrRemote, channel.id());
                        RemotingHelper.closeChannel(channel);
                    }
                } catch (Exception e) {
                    LOGGER.error("closeChannel: close the channel[id={}] exception", channel.id(), e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                LOGGER.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            LOGGER.error("closeChannel exception", e);
        }
    }

    /**
     * 更新 NameServer 地址列表
     *
     * @param addrs 新地址列表
     */
    @Override
    public void updateNameServerAddressList(List<String> addrs) {
        List<String> old = this.namesrvAddrList.get();
        boolean update = false;

        if (!addrs.isEmpty()) {
            if (null == old) {
                update = true;
            } else if (addrs.size() != old.size()) {
                update = true;
            } else {
                for (String addr : addrs) {
                    if (!old.contains(addr)) {
                        update = true;
                        break;
                    }
                }
            }

            if (update) {
                Collections.shuffle(addrs);
                LOGGER.info("name server address updated. NEW : {} , OLD: {}", addrs, old);
                this.namesrvAddrList.set(addrs);

                // should close the channel if choosed addr is not exist.
                // 若已选择地址不存在于新列表中, 应关闭对应连接
                String chosenNameServerAddr = this.namesrvAddrChoosed.get();
                if (chosenNameServerAddr != null && !addrs.contains(chosenNameServerAddr)) {
                    namesrvAddrChoosed.compareAndSet(chosenNameServerAddr, null);
                    for (String addr : this.channelTables.keySet()) {
                        if (addr.contains(chosenNameServerAddr)) {
                            ChannelWrapper channelWrapper = this.channelTables.get(addr);
                            if (channelWrapper != null) {
                                channelWrapper.close();
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * 同步调用远端接口
     *
     * @param addr          目标地址
     * @param request       请求命令
     * @param timeoutMillis 超时时间, 单位为毫秒
     * @return 响应命令
     * @throws InterruptedException         等待期间线程中断
     * @throws RemotingConnectException     连接异常
     * @throws RemotingSendRequestException 发送异常
     * @throws RemotingTimeoutException     调用超时
     */
    @Override
    public RemotingCommand invokeSync(String addr, final RemotingCommand request, long timeoutMillis)
        throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException {
        long beginStartTime = System.currentTimeMillis();
        final Channel channel = this.getAndCreateChannel(addr);
        String channelRemoteAddr = RemotingHelper.parseChannelRemoteAddr(channel);
        if (channel != null && channel.isActive()) {
            long left = timeoutMillis;
            try {
                long costTime = System.currentTimeMillis() - beginStartTime;
                left -= costTime;
                if (left <= 0) {
                    throw new RemotingTimeoutException("invokeSync call the addr[" + channelRemoteAddr + "] timeout");
                }
                RemotingCommand response = this.invokeSyncImpl(channel, request, left);
                updateChannelLastResponseTime(addr);
                return response;
            } catch (RemotingSendRequestException e) {
                LOGGER.warn("invokeSync: send request exception, so close the channel[addr={}, id={}]", channelRemoteAddr, channel.id());
                this.closeChannel(addr, channel);
                throw e;
            } catch (RemotingTimeoutException e) {
                // avoid close the success channel if left timeout is small, since it may cost too much time in get the success channel, the left timeout for read is small
                // 当剩余超时较小时避免关闭成功连接, 因为获取可用连接可能耗时较长, 留给读响应的时间可能过小
                boolean shouldClose = left > MIN_CLOSE_TIMEOUT_MILLIS || left > timeoutMillis / 4;
                if (nettyClientConfig.isClientCloseSocketIfTimeout() && shouldClose) {
                    this.closeChannel(addr, channel);
                    LOGGER.warn("invokeSync: close socket because of timeout, {}ms, channel[addr={}, id={}]", timeoutMillis, channelRemoteAddr, channel.id());
                }
                LOGGER.warn("invokeSync: wait response timeout exception, the channel[addr={}, id={}]", channelRemoteAddr, channel.id());
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    /**
     * 批量关闭地址对应连接
     *
     * @param addrList 地址列表
     */
    @Override
    public void closeChannels(List<String> addrList) {
        for (String addr : addrList) {
            ChannelWrapper cw = this.channelTables.get(addr);
            if (cw == null) {
                continue;
            }
            this.closeChannel(addr, cw.getChannel());
        }
        interruptPullRequests(new HashSet<>(addrList));
    }

    /**
     * 中断指定 Broker 地址上的拉取请求
     *
     * @param brokerAddrSet Broker 地址集合
     */
    private void interruptPullRequests(Set<String> brokerAddrSet) {
        for (ResponseFuture responseFuture : responseTable.values()) {
            RemotingCommand cmd = responseFuture.getRequestCommand();
            if (cmd == null) {
                continue;
            }
            String remoteAddr = RemotingHelper.parseChannelRemoteAddr(responseFuture.getChannel());
            // interrupt only pull message request
            // 仅中断拉消息请求
            if (brokerAddrSet.contains(remoteAddr) && (cmd.getCode() == RequestCode.PULL_MESSAGE || cmd.getCode() == RequestCode.LITE_PULL_MESSAGE)) {
                LOGGER.info("interrupt {}", cmd);
                responseFuture.interrupt();
            }
        }
    }

    /**
     * 更新连接最近响应时间
     *
     * @param addr 地址, 为 null 时使用当前选中的 NameServer 地址
     */
    private void updateChannelLastResponseTime(final String addr) {
        String address = addr;
        if (address == null) {
            address = this.namesrvAddrChoosed.get();
        }
        if (address == null) {
            LOGGER.warn("[updateChannelLastResponseTime] could not find address!!");
            return;
        }
        ChannelWrapper channelWrapper = this.channelTables.get(address);
        if (channelWrapper != null && channelWrapper.isOK()) {
            channelWrapper.updateLastResponseTime();
        }
    }

    /**
     * 获取或创建指定地址的 ChannelFuture
     *
     * @param addr 地址
     * @return ChannelFuture
     * @throws InterruptedException 锁等待中断
     */
    private ChannelFuture getAndCreateChannelAsync(final String addr) throws InterruptedException {
        if (null == addr) {
            return getAndCreateNameserverChannelAsync();
        }

        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.getChannelFuture();
        }

        return this.createChannelAsync(addr);
    }

    /**
     * 获取或创建指定地址的 Channel
     *
     * @param addr 地址
     * @return Channel
     * @throws InterruptedException 等待中断
     */
    private Channel getAndCreateChannel(final String addr) throws InterruptedException {
        ChannelFuture channelFuture = getAndCreateChannelAsync(addr);
        if (channelFuture == null) {
            return null;
        }
        return channelFuture.awaitUninterruptibly().channel();
    }

    /**
     * 获取或创建 NameServer ChannelFuture
     *
     * @return NameServer ChannelFuture
     * @throws InterruptedException 锁等待中断
     */
    private ChannelFuture getAndCreateNameserverChannelAsync() throws InterruptedException {
        String addr = this.namesrvAddrChoosed.get();
        if (addr != null) {
            ChannelWrapper cw = this.channelTables.get(addr);
            if (cw != null && cw.isOK()) {
                return cw.getChannelFuture();
            }
        }

        final List<String> addrList = this.namesrvAddrList.get();
        if (this.namesrvChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                addr = this.namesrvAddrChoosed.get();
                if (addr != null) {
                    ChannelWrapper cw = this.channelTables.get(addr);
                    if (cw != null && cw.isOK()) {
                        return cw.getChannelFuture();
                    }
                }

                if (addrList != null && !addrList.isEmpty()) {
                    int index = this.namesrvIndex.incrementAndGet();
                    index = Math.abs(index);
                    index = index % addrList.size();
                    String newAddr = addrList.get(index);

                    this.namesrvAddrChoosed.set(newAddr);
                    LOGGER.info("new name server is chosen. OLD: {} , NEW: {}. namesrvIndex = {}", addr, newAddr, namesrvIndex);
                    return this.createChannelAsync(newAddr);
                }
            } catch (Exception e) {
                LOGGER.error("getAndCreateNameserverChannel: create name server channel exception", e);
            } finally {
                this.namesrvChannelLock.unlock();
            }
        } else {
            LOGGER.warn("getAndCreateNameserverChannel: try to lock name server, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        return null;
    }

    /**
     * 创建地址对应的 ChannelFuture
     *
     * @param addr 地址
     * @return ChannelFuture
     * @throws InterruptedException 锁等待中断
     */
    private ChannelFuture createChannelAsync(final String addr) throws InterruptedException {
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.getChannelFuture();
        }

        if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                cw = this.channelTables.get(addr);
                if (cw != null) {
                    if (cw.isOK() || !cw.getChannelFuture().isDone()) {
                        return cw.getChannelFuture();
                    } else {
                        this.channelTables.remove(addr);
                    }
                }
                return createChannel(addr).getChannelFuture();
            } catch (Exception e) {
                LOGGER.error("createChannel: create channel exception", e);
            } finally {
                this.lockChannelTables.unlock();
            }
        } else {
            LOGGER.warn("createChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        return null;
    }

    /**
     * 创建并缓存 ChannelWrapper
     *
     * @param addr 地址
     * @return ChannelWrapper
     */
    private ChannelWrapper createChannel(String addr) {
        ChannelFuture channelFuture = doConnect(addr);
        LOGGER.info("createChannel: begin to connect remote host[{}] asynchronously", addr);
        ChannelWrapper cw = new ChannelWrapper(addr, channelFuture);
        this.channelTables.put(addr, cw);
        this.channelWrapperTables.put(channelFuture.channel(), cw);
        return cw;
    }

    /**
     * 异步调用远端接口
     *
     * @param addr           目标地址
     * @param request        请求命令
     * @param timeoutMillis  超时时间, 单位为毫秒
     * @param invokeCallback 回调
     * @throws InterruptedException            等待期间线程中断
     * @throws RemotingConnectException        连接异常
     * @throws RemotingTooMuchRequestException 请求过多
     * @throws RemotingTimeoutException        调用超时
     * @throws RemotingSendRequestException    发送异常
     */
    @Override
    public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
        throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException,
        RemotingSendRequestException {
        long beginStartTime = System.currentTimeMillis();
        final ChannelFuture channelFuture = this.getAndCreateChannelAsync(addr);
        if (channelFuture == null) {
            invokeCallback.operationFail(new RemotingConnectException(addr));
            return;
        }
        channelFuture.addListener(future -> {
            if (future.isSuccess()) {
                Channel channel = channelFuture.channel();
                String channelRemoteAddr = RemotingHelper.parseChannelRemoteAddr(channel);
                if (channel != null && channel.isActive()) {
                    long costTime = System.currentTimeMillis() - beginStartTime;
                    if (timeoutMillis < costTime) {
                        invokeCallback.operationFail(new RemotingTooMuchRequestException("invokeAsync call the addr[" + channelRemoteAddr + "] timeout"));
                    }
                    this.invokeAsyncImpl(channel, request, timeoutMillis - costTime, new InvokeCallbackWrapper(invokeCallback, addr));
                } else {
                    this.closeChannel(addr, channel);
                    invokeCallback.operationFail(new RemotingConnectException(addr));
                }
            } else {
                invokeCallback.operationFail(new RemotingConnectException(addr));
            }
        });
    }

    /**
     * 单向调用远端接口
     *
     * @param addr          目标地址
     * @param request       请求命令
     * @param timeoutMillis 超时时间, 单位为毫秒
     * @throws InterruptedException            等待期间线程中断
     * @throws RemotingConnectException        连接异常
     * @throws RemotingTooMuchRequestException 请求过多
     * @throws RemotingTimeoutException        调用超时
     * @throws RemotingSendRequestException    发送异常
     */
    @Override
    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis) throws InterruptedException,
        RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        final ChannelFuture channelFuture = this.getAndCreateChannelAsync(addr);
        if (channelFuture == null) {
            throw new RemotingConnectException(addr);
        }
        channelFuture.addListener(future -> {
            if (future.isSuccess()) {
                Channel channel = channelFuture.channel();
                String channelRemoteAddr = RemotingHelper.parseChannelRemoteAddr(channel);
                if (channel != null && channel.isActive()) {
                    doBeforeRpcHooks(channelRemoteAddr, request);
                    this.invokeOnewayImpl(channel, request, timeoutMillis);
                } else {
                    this.closeChannel(addr, channel);
                }
            }
        });
    }

    /**
     * 以 CompletableFuture 方式发起请求
     *
     * @param addr          目标地址
     * @param request       请求命令
     * @param timeoutMillis 超时时间, 单位为毫秒
     * @return 响应 Future
     */
    @Override
    public CompletableFuture<RemotingCommand> invoke(String addr, RemotingCommand request,
        long timeoutMillis) {
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        try {
            final ChannelFuture channelFuture = this.getAndCreateChannelAsync(addr);
            if (channelFuture == null) {
                future.completeExceptionally(new RemotingConnectException(addr));
                return future;
            }
            channelFuture.addListener(f -> {
                if (f.isSuccess()) {
                    Channel channel = channelFuture.channel();
                    if (channel != null && channel.isActive()) {
                        invokeImpl(channel, request, timeoutMillis).whenComplete((v, t) -> {
                            if (t == null) {
                                updateChannelLastResponseTime(addr);
                            }
                        }).thenApply(ResponseFuture::getResponseCommand).whenComplete((v, t) -> {
                            if (t != null) {
                                future.completeExceptionally(t);
                            } else {
                                future.complete(v);
                            }
                        });
                    } else {
                        this.closeChannel(addr, channel);
                        future.completeExceptionally(new RemotingConnectException(addr));
                    }
                } else {
                    future.completeExceptionally(new RemotingConnectException(addr));
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    /**
     * 覆盖异步调用实现, 处理 GO_AWAY 重连逻辑
     *
     * @param channel       当前 Channel
     * @param request       请求命令
     * @param timeoutMillis 超时时间, 单位为毫秒
     * @return ResponseFuture 异步结果
     */
    @Override
    public CompletableFuture<ResponseFuture> invokeImpl(final Channel channel, final RemotingCommand request,
        final long timeoutMillis) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String channelRemoteAddr = RemotingHelper.parseChannelRemoteAddr(channel);
        doBeforeRpcHooks(channelRemoteAddr, request);

        return super.invokeImpl(channel, request, timeoutMillis).thenCompose(responseFuture -> {
            RemotingCommand response = responseFuture.getResponseCommand();
            if (response.getCode() == ResponseCode.GO_AWAY) {
                if (nettyClientConfig.isEnableReconnectForGoAway()) {
                    LOGGER.info("Receive go away from channelId={}, channel={}", channel.id(), channel);
                    ChannelWrapper channelWrapper = channelWrapperTables.computeIfPresent(channel, (channel0, channelWrapper0) -> {
                        try {
                            if (channelWrapper0.reconnect(channel0)) {
                                LOGGER.info("Receive go away from channelId={}, channel={}, recreate the channelId={}", channel0.id(), channel0, channelWrapper0.getChannel().id());
                                channelWrapperTables.put(channelWrapper0.getChannel(), channelWrapper0);
                            }
                        } catch (Throwable t) {
                            LOGGER.error("Channel {} reconnect error", channelWrapper0, t);
                        }
                        return channelWrapper0;
                    });
                    if (channelWrapper != null && !channelWrapper.isWrapperOf(channel)) {
                        RemotingCommand retryRequest = RemotingCommand.createRequestCommand(request.getCode(), request.readCustomHeader());
                        retryRequest.setBody(request.getBody());
                        retryRequest.setExtFields(request.getExtFields());
                        CompletableFuture<Void> future = convertChannelFutureToCompletableFuture(channelWrapper.getChannelFuture());
                        return future.thenCompose(v -> {
                            long duration = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                            stopwatch.stop();
                            return super.invokeImpl(channelWrapper.getChannel(), retryRequest, timeoutMillis - duration)
                                .thenCompose(r -> {
                                    if (r.getResponseCommand().getCode() == ResponseCode.GO_AWAY) {
                                        return FutureUtils.completeExceptionally(new RemotingSendRequestException(channelRemoteAddr,
                                            new Throwable("Receive GO_AWAY twice in request from channelId=" + channel.id())));
                                    }
                                    return CompletableFuture.completedFuture(r);
                                });
                        });
                    } else {
                        LOGGER.warn("invokeImpl receive GO_AWAY, channelWrapper is null or channel is the same in wrapper, channelId={}", channel.id());
                    }
                }
                return FutureUtils.completeExceptionally(new RemotingSendRequestException(channelRemoteAddr, new Throwable("Receive GO_AWAY from channelId=" + channel.id())));
            }
            return CompletableFuture.completedFuture(responseFuture);
        }).whenComplete((v, t) -> {
            if (t == null) {
                doAfterRpcHooks(channelRemoteAddr, request, v.getResponseCommand());
            }
        });
    }

    /**
     * 注册请求处理器
     *
     * @param requestCode 请求码
     * @param processor   处理器
     * @param executor    处理线程池, 为 null 时使用公共线程池
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
     * 判断地址对应 Channel 是否可写
     *
     * @param addr 地址
     * @return 可写返回 true, 否则返回 false
     */
    @Override
    public boolean isChannelWritable(String addr) {
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.isWritable();
        }
        return true;
    }

    /**
     * 判断地址是否可达
     *
     * @param addr 地址
     * @return 可达返回 true, 否则返回 false
     */
    @Override
    public boolean isAddressReachable(String addr) {
        if (addr == null || addr.isEmpty()) {
            return false;
        }
        try {
            Channel channel = getAndCreateChannel(addr);
            return channel != null && channel.isActive();
        } catch (Exception e) {
            LOGGER.warn("Get and create channel of {} failed", addr, e);
            return false;
        }
    }

    @Override
    public List<String> getNameServerAddressList() {
        return this.namesrvAddrList.get();
    }

    @Override
    public List<String> getAvailableNameSrvList() {
        return new ArrayList<>(this.availableNamesrvAddrMap.keySet());
    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return channelEventListener;
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        if (nettyClientConfig.isDisableCallbackExecutor()) {
            return null;
        }
        return callbackExecutor != null ? callbackExecutor : publicExecutor;
    }

    @Override
    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.callbackExecutor = callbackExecutor;
    }

    /**
     * 扫描 NameServer 连接表, 关闭长时间无响应连接
     */
    protected void scanChannelTablesOfNameServer() {
        List<String> nameServerList = this.namesrvAddrList.get();
        if (nameServerList == null) {
            LOGGER.warn("[SCAN] Addresses of name server is empty!");
            return;
        }

        for (Map.Entry<String, ChannelWrapper> entry : this.channelTables.entrySet()) {
            String addr = entry.getKey();
            ChannelWrapper channelWrapper = entry.getValue();
            if (channelWrapper == null) {
                continue;
            }

            if ((System.currentTimeMillis() - channelWrapper.getLastResponseTime()) > this.nettyClientConfig.getChannelNotActiveInterval()) {
                LOGGER.warn("[SCAN] No response after {} from name server {}, so close it!", channelWrapper.getLastResponseTime(),
                    addr);
                closeChannel(addr, channelWrapper.getChannel());
            }
        }
    }

    /**
     * 扫描并刷新可用 NameServer 列表
     */
    private void scanAvailableNameSrv() {
        List<String> nameServerList = this.namesrvAddrList.get();
        if (nameServerList == null) {
            LOGGER.debug("scanAvailableNameSrv addresses of name server is null!");
            return;
        }

        for (String address : NettyRemotingClient.this.availableNamesrvAddrMap.keySet()) {
            if (!nameServerList.contains(address)) {
                LOGGER.warn("scanAvailableNameSrv remove invalid address {}", address);
                NettyRemotingClient.this.availableNamesrvAddrMap.remove(address);
            }
        }

        for (final String namesrvAddr : nameServerList) {
            scanExecutor.execute(new Runnable() {

                /**
                 * 执行单个 NameServer 可用性检查
                 */
                @Override
                public void run() {
                    try {
                        Channel channel = NettyRemotingClient.this.getAndCreateChannel(namesrvAddr);
                        if (channel != null) {
                            NettyRemotingClient.this.availableNamesrvAddrMap.putIfAbsent(namesrvAddr, true);
                        } else {
                            Boolean value = NettyRemotingClient.this.availableNamesrvAddrMap.remove(namesrvAddr);
                            if (value != null) {
                                LOGGER.warn("scanAvailableNameSrv remove unconnected address {}", namesrvAddr);
                            }
                        }
                    } catch (Exception e) {
                        LOGGER.error("scanAvailableNameSrv get channel of {} failed, ", namesrvAddr, e);
                    }
                }
            });
        }
    }

    /**
     * Channel 包装类, 负责连接状态与重连管理
     */
    class ChannelWrapper {
        /**
         * 读写锁, 保护 ChannelFuture 切换
         */
        private final ReentrantReadWriteLock lock;
        /**
         * 当前生效 ChannelFuture
         */
        private ChannelFuture channelFuture;
        // only affected by sync or async request, oneway is not included.
        // 仅受 sync 或 async 请求影响, 不包含 oneway
        /**
         * 待关闭旧 ChannelFuture
         */
        private ChannelFuture channelToClose;
        /**
         * 最近响应时间戳, 单位为毫秒
         */
        private long lastResponseTime;
        /**
         * Channel 对应地址
         */
        private final String channelAddress;

        /**
         * 构造 Channel 包装对象
         *
         * @param address       地址
         * @param channelFuture ChannelFuture
         */
        public ChannelWrapper(String address, ChannelFuture channelFuture) {
            this.lock = new ReentrantReadWriteLock();
            this.channelFuture = channelFuture;
            this.lastResponseTime = System.currentTimeMillis();
            this.channelAddress = address;
        }

        /**
         * 判断当前连接是否可用
         *
         * @return 可用返回 true, 否则返回 false
         */
        public boolean isOK() {
            return getChannel() != null && getChannel().isActive();
        }

        /**
         * 判断当前连接是否可写
         *
         * @return 可写返回 true, 否则返回 false
         */
        public boolean isWritable() {
            return getChannel().isWritable();
        }

        /**
         * 判断包装对象是否持有指定 Channel
         *
         * @param channel Channel
         * @return 相同返回 true, 否则返回 false
         */
        public boolean isWrapperOf(Channel channel) {
            return this.channelFuture.channel() != null && this.channelFuture.channel() == channel;
        }

        /**
         * 获取当前 Channel
         *
         * @return Channel
         */
        private Channel getChannel() {
            return getChannelFuture().channel();
        }

        public ChannelFuture getChannelFuture() {
            lock.readLock().lock();
            try {
                return this.channelFuture;
            } finally {
                lock.readLock().unlock();
            }
        }

        public long getLastResponseTime() {
            return this.lastResponseTime;
        }

        /**
         * 更新最近响应时间
         */
        public void updateLastResponseTime() {
            this.lastResponseTime = System.currentTimeMillis();
        }

        public String getChannelAddress() {
            return channelAddress;
        }

        /**
         * 对指定 Channel 执行重连
         *
         * @param channel 原 Channel
         * @return 重连成功返回 true, 否则返回 false
         */
        public boolean reconnect(Channel channel) {
            if (!isWrapperOf(channel)) {
                LOGGER.warn("channelWrapper has reconnect, so do nothing, now channelId={}, input channelId={}",getChannel().id(), channel.id());
                return false;
            }
            if (lock.writeLock().tryLock()) {
                try {
                    if (isWrapperOf(channel)) {
                        channelToClose = channelFuture;
                        channelFuture = doConnect(channelAddress);
                        return true;
                    } else {
                        LOGGER.warn("channelWrapper has reconnect, so do nothing, now channelId={}, input channelId={}",getChannel().id(), channel.id());
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            } else {
                LOGGER.warn("channelWrapper reconnect try lock fail, now channelId={}", getChannel().id());
            }
            return false;
        }

        /**
         * 尝试确认是否可关闭指定 Channel
         *
         * @param channel Channel
         * @return 可关闭返回 true, 否则返回 false
         */
        public boolean tryClose(Channel channel) {
            try {
                lock.readLock().lock();
                if (channelFuture != null) {
                    if (channelFuture.channel().equals(channel)) {
                        return true;
                    }
                }
            } finally {
                lock.readLock().unlock();
            }
            return false;
        }

        /**
         * 关闭当前及待关闭 Channel
         */
        public void close() {
            try {
                lock.writeLock().lock();
                if (channelFuture != null) {
                    closeChannel(channelFuture.channel());
                }
                if (channelToClose != null) {
                    closeChannel(channelToClose.channel());
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    /**
     * 回调包装器, 用于附加连接响应时间更新逻辑
     */
    class InvokeCallbackWrapper implements InvokeCallback {

        /**
         * 原始回调
         */
        private final InvokeCallback invokeCallback;
        /**
         * 地址
         */
        private final String addr;

        /**
         * 构造回调包装器
         *
         * @param invokeCallback 原始回调
         * @param addr 地址
         */
        public InvokeCallbackWrapper(InvokeCallback invokeCallback, String addr) {
            this.invokeCallback = invokeCallback;
            this.addr = addr;
        }

        /**
         * 回调完成处理
         *
         * @param responseFuture 响应 Future
         */
        @Override
        public void operationComplete(ResponseFuture responseFuture) {
            this.invokeCallback.operationComplete(responseFuture);
        }

        /**
         * 回调成功处理
         *
         * @param response 响应命令
         */
        @Override
        public void operationSucceed(RemotingCommand response) {
            updateChannelLastResponseTime(addr);
            this.invokeCallback.operationSucceed(response);
        }

        /**
         * 回调失败处理
         *
         * @param throwable 异常
         */
        @Override
        public void operationFail(final Throwable throwable) {
            this.invokeCallback.operationFail(throwable);
        }
    }

    /**
     * 客户端入站消息处理器
     */
    public class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {
        /**
         * 处理入站 RemotingCommand
         *
         * @param ctx Channel 上下文
         * @param msg 入站消息
         * @throws Exception 处理异常
         */
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }

    /**
     * 客户端连接生命周期处理器
     */
    public class NettyConnectManageHandler extends ChannelDuplexHandler {
        /**
         * 处理连接建立事件
         *
         * @param ctx           Channel 上下文
         * @param remoteAddress 远端地址
         * @param localAddress  本地地址
         * @param promise       Promise
         * @throws Exception 处理异常
         */
        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
            ChannelPromise promise) throws Exception {
            final String local = localAddress == null ? NetworkUtil.getLocalAddress() : RemotingHelper.parseSocketAddressAddr(localAddress);
            final String remote = remoteAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(remoteAddress);
            LOGGER.info("NETTY CLIENT PIPELINE: CONNECT  {} => {}", local, remote);

            super.connect(ctx, remoteAddress, localAddress, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remote, ctx.channel()));
            }
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
            LOGGER.info("NETTY CLIENT PIPELINE: ACTIVE, {}, channelId={}", remoteAddress, ctx.channel().id());
            super.channelActive(ctx);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.ACTIVE, remoteAddress, ctx.channel()));
            }
        }

        /**
         * 处理断开连接事件
         *
         * @param ctx     Channel 上下文
         * @param promise Promise
         * @throws Exception 处理异常
         */
        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            LOGGER.info("NETTY CLIENT PIPELINE: DISCONNECT {}", remoteAddress);
            closeChannel(ctx.channel());
            super.disconnect(ctx, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        /**
         * 处理关闭事件
         *
         * @param ctx     Channel 上下文
         * @param promise Promise
         * @throws Exception 处理异常
         */
        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            LOGGER.info("NETTY CLIENT PIPELINE: CLOSE channel[addr={}, id={}]", remoteAddress, ctx.channel().id());
            closeChannel(ctx.channel());
            super.close(ctx, promise);
            NettyRemotingClient.this.failFast(ctx.channel());
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
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
            LOGGER.info("NETTY CLIENT PIPELINE: channelInactive, the channel[addr={}, id={}]", remoteAddress, ctx.channel().id());
            closeChannel(ctx.channel());
            super.channelInactive(ctx);
        }

        /**
         * 处理用户事件
         *
         * @param ctx Channel 上下文
         * @param evt 用户事件
         * @throws Exception 处理异常
         */
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    LOGGER.warn("NETTY CLIENT PIPELINE: IDLE exception channel[addr={}, id={}]", remoteAddress, ctx.channel().id());
                    closeChannel(ctx.channel());
                    if (NettyRemotingClient.this.channelEventListener != null) {
                        NettyRemotingClient.this
                            .putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
                    }
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        /**
         * 处理异常事件
         *
         * @param ctx   Channel 上下文
         * @param cause 异常原因
         * @throws Exception 处理异常
         */
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            LOGGER.warn("NETTY CLIENT PIPELINE: exceptionCaught channel[addr={}, id={}]", remoteAddress, ctx.channel().id(), cause);
            closeChannel(ctx.channel());
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));
            }
        }
    }
}
