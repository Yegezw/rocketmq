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
package org.apache.rocketmq.namesrv;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.future.FutureTaskExt;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.namesrv.kvconfig.KVConfigManager;
import org.apache.rocketmq.namesrv.processor.ClientRequestProcessor;
import org.apache.rocketmq.namesrv.processor.ClusterTestRequestProcessor;
import org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor;
import org.apache.rocketmq.namesrv.route.ZoneRouteRPCHook;
import org.apache.rocketmq.namesrv.routeinfo.BrokerHousekeepingService;
import org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager;
import org.apache.rocketmq.remoting.Configuration;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.*;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.srvutil.FileWatchService;

import java.util.Collections;
import java.util.concurrent.*;

/**
 * Broker 注册中心, 由 {@link NamesrvStartup} 启动
 * <p>
 * NamesrvController 是 Namesrv 模块的核心控制器, 负责协调和管理所有组件<br>
 * 1. 路由信息管理: 维护 Broker 和 Topic 的路由信息<br>
 * 2. KV 配置管理: 提供命名空间级别的配置存储<br>
 * 3. 网络通信: 处理来自 Broker、Producer、Consumer 的请求<br>
 * 4. 健康检测: 定期扫描非活跃 Broker, 清理过期连接
 */
public class NamesrvController {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    private static final Logger WATER_MARK_LOG = LoggerFactory.getLogger(LoggerName.NAMESRV_WATER_MARK_LOGGER_NAME);

    // ==================== 配置组件 ====================

    /**
     * Namesrv 配置: 包含扫描间隔、线程池大小等配置项
     */
    private final NamesrvConfig namesrvConfig;

    /**
     * Netty 服务端配置: 监听端口、线程数等网络服务端配置
     */
    private final NettyServerConfig nettyServerConfig;

    /**
     * Netty 客户端配置: 用于主动连接其它服务的客户端配置
     */
    private final NettyClientConfig nettyClientConfig;

    // ==================== 定时任务组件 ====================

    /**
     * 定时任务线程池: 用于执行周期性任务 (如打印 KV 配置、打印水位线)
     */
    private final ScheduledExecutorService scheduledExecutorService = ThreadUtils.newScheduledThreadPool(1,
            new BasicThreadFactory.Builder().namingPattern("NSScheduledThread").daemon(true).build());

    /**
     * 扫描任务线程池: 专门用于执行 Broker 心跳扫描任务
     */
    private final ScheduledExecutorService scanExecutorService = ThreadUtils.newScheduledThreadPool(1,
            new BasicThreadFactory.Builder().namingPattern("NSScanScheduledThread").daemon(true).build());

    // ==================== 核心管理组件 ====================

    /**
     * KV 配置管理器: 负责命名空间级别的 KV 配置存储、查询、持久化
     */
    private final KVConfigManager kvConfigManager;

    /**
     * 路由信息管理器: 核心组件, 维护 Broker 注册表、Topic 路由表、集群表等路由信息
     */
    private final RouteInfoManager routeInfoManager;

    // ==================== 网络通信组件 ====================

    /**
     * Remoting 客户端: 用于主动向其它服务发送请求 (如通知 Broker 状态变更)
     */
    private RemotingClient remotingClient;

    /**
     * Remoting 服务端: 接收来自 Broker、Producer、Consumer 的请求
     */
    private RemotingServer remotingServer;

    // ==================== Broker 连接管理组件 ====================

    /**
     * Broker 连接保活服务: 监听 Broker 连接事件 (连接、断开、异常), 触发路由清理
     */
    private final BrokerHousekeepingService brokerHousekeepingService;

    // ==================== 线程池组件 ====================

    /**
     * 默认请求处理线程池 16: 处理除路由查询外的所有请求 (Broker 注册、KV 配置等)
     */
    private ExecutorService defaultExecutor;

    /**
     * 默认线程池的任务队列: 用于监控线程池负载情况
     */
    private BlockingQueue<Runnable> defaultThreadPoolQueue;

    /**
     * 客户端请求处理线程池 8: 专门处理高频的路由查询请求 (GET_ROUTEINFO_BY_TOPIC)
     */
    private ExecutorService clientRequestExecutor;

    /**
     * 客户端请求线程池的任务队列: 用于监控路由查询请求的排队情况
     */
    private BlockingQueue<Runnable> clientRequestThreadPoolQueue;

    // ==================== 配置管理组件 ====================

    /**
     * 配置管理器: 统一管理 NamesrvConfig 和 NettyServerConfig, 支持动态更新
     */
    private final Configuration configuration;

    /**
     * 文件监听服务: 监听 SSL 证书文件变化, 支持动态加载证书 (TLS 场景)
     */
    private FileWatchService fileWatchService;

    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig) {
        this(namesrvConfig, nettyServerConfig, new NettyClientConfig());
    }

    /**
     * 构造函数: 初始化所有核心组件
     * <p>
     * 组件初始化顺序<br>
     * 1. 配置对象 (namesrvConfig, nettyServerConfig, nettyClientConfig)<br>
     * 2. KV 配置管理器 (kvConfigManager)<br>
     * 3. Broker连接保活服务 (brokerHousekeepingService)<br>
     * 4. 路由信息管理器 (routeInfoManager) - 核心组件<br>
     * 5. 配置管理器 (configuration)
     */
    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig, NettyClientConfig nettyClientConfig) {
        this.namesrvConfig = namesrvConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        this.kvConfigManager = new KVConfigManager(this);
        this.brokerHousekeepingService = new BrokerHousekeepingService(this);
        this.routeInfoManager = new RouteInfoManager(namesrvConfig, this);
        this.configuration = new Configuration(LOGGER, this.namesrvConfig, this.nettyServerConfig);
        this.configuration.setStorePathFromConfig(this.namesrvConfig, "configStorePath");
    }

    /**
     * 初始化 NamesrvController
     */
    public boolean initialize() {
        loadConfig();                // 加载 KV 配置
        initiateNetworkComponents(); // 初始化网络组件
        initiateThreadExecutors();   // 初始化线程池
        registerProcessor();         // 注册请求处理器
        startScheduleService();      // 启动定时任务
        initiateSslContext();        // 初始化 SSL 上下文 (如果启用)
        initiateRpcHooks();          // 注册 RPC 钩子
        return true;
    }

    /**
     * 加载 KV 配置: 从持久化文件加载配置到内存
     */
    private void loadConfig() {
        this.kvConfigManager.load();
    }

    /**
     * 启动定时任务服务
     * <p>
     * 定时任务包括<br>
     * 1. scanNotActiveBroker - 扫描非活跃 Broker (使用 scanExecutorService, 初始延迟 5 秒)<br>
     * 2. printAllPeriodically - 定期打印 KV 配置 (使用 scheduledExecutorService, 每 10 分钟)<br>
     * 3. printWaterMark - 打印线程池水位线 (使用 scheduledExecutorService, 每 1 秒)
     */
    private void startScheduleService() {
        // 扫描非活跃 Broker: 初始延迟 5 秒, 后续按配置的间隔执行
        this.scanExecutorService.scheduleAtFixedRate(NamesrvController.this.routeInfoManager::scanNotActiveBroker,
            5, this.namesrvConfig.getScanNotActiveBrokerInterval(), TimeUnit.MILLISECONDS);

        // 定期打印 KV 配置: 初始延迟 1 分钟, 每 10 分钟执行一次
        this.scheduledExecutorService.scheduleAtFixedRate(NamesrvController.this.kvConfigManager::printAllPeriodically,
            1, 10, TimeUnit.MINUTES);

        // 打印线程池水位线: 初始延迟 10 秒, 每 1 秒执行一次 (用于监控线程池负载)
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                NamesrvController.this.printWaterMark();
            } catch (Throwable e) {
                LOGGER.error("printWaterMark error.", e);
            }
        }, 10, 1, TimeUnit.SECONDS);
    }

    /**
     * 初始化网络组件
     * <p>
     * 1. RemotingServer - 接收外部请求 (Broker 注册、路由查询等)<br>
     * 2. RemotingClient - 主动发送请求 (通知 Broker 状态变更等)
     * <p>
     * 注意: RemotingServer 会使用 brokerHousekeepingService 监听连接事件
     */
    private void initiateNetworkComponents() {
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.brokerHousekeepingService);
        this.remotingClient = new NettyRemotingClient(this.nettyClientConfig);
    }

    /**
     * 初始化线程执行器
     * <p>
     * 创建两个线程池<br>
     * 1. defaultExecutor - 处理默认请求 (Broker 注册、KV 配置等)<br>
     * 2. clientRequestExecutor - 专门处理高频路由查询请求
     * <p>
     * 分离线程池的目的: 路由查询请求频率高, 单独线程池可以避免影响其它请求处理
     */
    private void initiateThreadExecutors() {
        this.defaultThreadPoolQueue = new LinkedBlockingQueue<>(this.namesrvConfig.getDefaultThreadPoolQueueCapacity());
        this.defaultExecutor = ThreadUtils.newThreadPoolExecutor(this.namesrvConfig.getDefaultThreadPoolNums(), this.namesrvConfig.getDefaultThreadPoolNums(), 1000 * 60, TimeUnit.MILLISECONDS, this.defaultThreadPoolQueue, new ThreadFactoryImpl("RemotingExecutorThread_"));

        this.clientRequestThreadPoolQueue = new LinkedBlockingQueue<>(this.namesrvConfig.getClientRequestThreadPoolQueueCapacity());
        this.clientRequestExecutor = ThreadUtils.newThreadPoolExecutor(this.namesrvConfig.getClientRequestThreadPoolNums(), this.namesrvConfig.getClientRequestThreadPoolNums(), 1000 * 60, TimeUnit.MILLISECONDS, this.clientRequestThreadPoolQueue, new ThreadFactoryImpl("ClientRequestExecutorThread_"));
    }

    /**
     * 初始化 SSL 上下文 (如果启用 TLS)
     * <p>
     * 功能<br>
     * 1. 如果 TLS 未启用, 直接返回<br>
     * 2. 如果启用 TLS, 创建文件监听服务, 监听证书文件变化<br>
     * 3. 当证书文件变化时, 自动重新加载 SSL 上下文 (支持热更新)
     * <p>
     * 监听的文件<br>
     * - tlsServerCertPath: 服务器证书<br>
     * - tlsServerKeyPath: 服务器私钥<br>
     * - tlsServerTrustCertPath: 信任证书
     */
    private void initiateSslContext() {
        if (TlsSystemConfig.tlsMode == TlsMode.DISABLED) {
            return;
        }

        String[] watchFiles = {TlsSystemConfig.tlsServerCertPath, TlsSystemConfig.tlsServerKeyPath, TlsSystemConfig.tlsServerTrustCertPath};

        FileWatchService.Listener listener = new FileWatchService.Listener() {
            boolean certChanged, keyChanged = false;

            @Override
            public void onChanged(String path) {
                // 信任证书变化, 立即重新加载
                if (path.equals(TlsSystemConfig.tlsServerTrustCertPath)) {
                    LOGGER.info("The trust certificate changed, reload the ssl context");
                    ((NettyRemotingServer) remotingServer).loadSslContext();
                }
                // 记录证书和私钥的变化状态
                if (path.equals(TlsSystemConfig.tlsServerCertPath)) {
                    certChanged = true;
                }
                if (path.equals(TlsSystemConfig.tlsServerKeyPath)) {
                    keyChanged = true;
                }
                // 证书和私钥都变化后, 重新加载 SSL 上下文
                if (certChanged && keyChanged) {
                    LOGGER.info("The certificate and private key changed, reload the ssl context");
                    certChanged = keyChanged = false;
                    ((NettyRemotingServer) remotingServer).loadSslContext();
                }
            }
        };

        try {
            fileWatchService = new FileWatchService(watchFiles, listener);
        } catch (Exception e) {
            LOGGER.warn("FileWatchService created error, can't load the certificate dynamically");
        }
    }

    /**
     * 打印线程池水位线
     * <p>
     * 用于监控线程池的负载情况<br>
     * - ClientQueueSize: 客户端请求线程池队列大小<br>
     * - ClientQueueSlowTime: 客户端请求队列中最老任务的等待时间<br>
     * - DefaultQueueSize: 默认线程池队列大小<br>
     * - DefaultQueueSlowTime: 默认线程池队列中最老任务的等待时间
     * <p>
     * 用途: 帮助发现性能瓶颈, 当队列积压或等待时间过长时, 需要调整线程池配置
     */
    private void printWaterMark() {
        WATER_MARK_LOG.info("[WATERMARK] ClientQueueSize:{} ClientQueueSlowTime:{} " + "DefaultQueueSize:{} DefaultQueueSlowTime:{}", this.clientRequestThreadPoolQueue.size(), headSlowTimeMills(this.clientRequestThreadPoolQueue), this.defaultThreadPoolQueue.size(), headSlowTimeMills(this.defaultThreadPoolQueue));
    }

    /**
     * 计算队列头部任务的等待时间 (毫秒)
     * <p>
     * 通过检查队列头部任务的创建时间戳, 计算该任务已等待的时间<br>
     * 用于监控线程池的响应延迟
     *
     * @param q 任务队列
     * @return 队列头部任务的等待时间 (毫秒), 如果无法计算则返回 0
     */
    private long headSlowTimeMills(BlockingQueue<Runnable> q) {
        long slowTimeMills = 0;
        final Runnable firstRunnable = q.peek();

        if (firstRunnable instanceof FutureTaskExt) {
            final Runnable inner = ((FutureTaskExt<?>) firstRunnable).getRunnable();
            if (inner instanceof RequestTask) {
                slowTimeMills = System.currentTimeMillis() - ((RequestTask) inner).getCreateTimestamp();
            }
        }

        if (slowTimeMills < 0) {
            slowTimeMills = 0;
        }

        return slowTimeMills;
    }

    /**
     * 注册请求处理器 {@link NettyRequestProcessor} 到 {@link #remotingServer}
     * <p>
     * 处理器注册策略<br>
     * 1. 集群测试模式: 使用 ClusterTestRequestProcessor 处理所有请求<br>
     * 2. 正常模式<br>
     * - ClientRequestProcessor: 专门处理 GET_ROUTEINFO_BY_TOPIC (路由查询), 使用 clientRequestExecutor<br>
     * - DefaultRequestProcessor: 处理其它所有请求 (Broker 注册、KV 配置等), 使用 defaultExecutor
     * <p>
     * 分离处理器的目的: 路由查询请求频率高, 单独处理器和线程池可以优化性能
     */
    private void registerProcessor() {
        if (namesrvConfig.isClusterTest()) {
            // 集群测试模式: 使用测试处理器
            this.remotingServer.registerDefaultProcessor(new ClusterTestRequestProcessor(this, namesrvConfig.getProductEnvName()), this.defaultExecutor);
        } else {
            // 正常模式: 分离路由查询和其它请求
            // 注册路由查询处理器 (高频请求, 使用专用线程池)
            ClientRequestProcessor clientRequestProcessor = new ClientRequestProcessor(this);
            this.remotingServer.registerProcessor(RequestCode.GET_ROUTEINFO_BY_TOPIC, clientRequestProcessor, this.clientRequestExecutor);

            // 注册默认处理器 (处理其它所有请求)
            this.remotingServer.registerDefaultProcessor(new DefaultRequestProcessor(this), this.defaultExecutor);
        }
    }

    /**
     * 初始化 RPC 钩子
     * <p>
     * 注册 ZoneRouteRPCHook: 用于处理跨区域路由的 RPC 钩子
     */
    private void initiateRpcHooks() {
        this.remotingServer.registerRPCHook(new ZoneRouteRPCHook());
    }

    /**
     * 启动 Broker 注册中心
     * <p>
     * 启动顺序<br>
     * 1. 启动 RemotingServer (开始接收请求)<br>
     * 2. 更新 RemotingClient 的 Nameserver 地址列表<br>
     * 3. 启动 RemotingClient (可以主动发送请求)<br>
     * 4. 启动文件监听服务 (如果启用 SSL)<br>
     * 5. 启动 RouteInfoManager (启动批量注销服务)
     */
    public void start() throws Exception {
        // 启动 RemotingServer: 开始监听端口, 接收请求
        this.remotingServer.start();   // 服务端

        // 如果端口为 0 (由系统自动分配), 将实际分配的端口写回配置
        if (0 == nettyServerConfig.getListenPort()) {
            nettyServerConfig.setListenPort(this.remotingServer.localListenPort());
        }

        // 更新 RemotingClient 的 Nameserver 地址列表 (用于客户端主动连接)
        this.remotingClient.updateNameServerAddressList(Collections.singletonList(NetworkUtil.getLocalAddress()
            + ":" + nettyServerConfig.getListenPort()));
        this.remotingClient.start();   // 客户端

        // 启动文件监听服务 (如果启用 SSL, 用于动态加载证书)
        if (this.fileWatchService != null) {
            this.fileWatchService.start();
        }

        // 启动 RouteInfoManager (启动批量注销服务)
        this.routeInfoManager.start(); // 路由管理器
    }

    /**
     * 停止 Broker 注册中心
     * <p>
     * 关闭顺序 (与启动顺序相反)<br>
     * 1. 关闭 RemotingClient<br>
     * 2. 关闭 RemotingServer<br>
     * 3. 关闭线程池 (defaultExecutor, clientRequestExecutor)<br>
     * 4. 关闭定时任务线程池 (scheduledExecutorService, scanExecutorService)<br>
     * 5. 关闭 RouteInfoManager<br>
     * 6. 关闭文件监听服务 (如果存在)
     */
    public void shutdown() {
        this.remotingClient.shutdown();
        this.remotingServer.shutdown();
        this.defaultExecutor.shutdown();
        this.clientRequestExecutor.shutdown();
        this.scheduledExecutorService.shutdown();
        this.scanExecutorService.shutdown();
        this.routeInfoManager.shutdown();

        if (this.fileWatchService != null) {
            this.fileWatchService.shutdown();
        }
    }

    public NamesrvConfig getNamesrvConfig() {
        return namesrvConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public KVConfigManager getKvConfigManager() {
        return kvConfigManager;
    }

    public RouteInfoManager getRouteInfoManager() {
        return routeInfoManager;
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public RemotingClient getRemotingClient() {
        return remotingClient;
    }

    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}
