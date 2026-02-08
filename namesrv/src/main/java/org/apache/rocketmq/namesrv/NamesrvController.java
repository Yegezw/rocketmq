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
 * NameServer 控制器, 负责初始化组件、注册处理器并管理 NameServer 生命周期
 */
public class NamesrvController {
    /**
     * NameServer 主日志对象
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    /**
     * NameServer 水位日志对象, 记录线程池堆积信息
     */
    private static final Logger WATER_MARK_LOG = LoggerFactory.getLogger(LoggerName.NAMESRV_WATER_MARK_LOGGER_NAME);

    /**
     * NameServer 运行配置
     */
    private final NamesrvConfig namesrvConfig;

    /**
     * NameServer Netty 服务端配置
     */
    private final NettyServerConfig nettyServerConfig;
    /**
     * NameServer Netty 客户端配置
     */
    private final NettyClientConfig nettyClientConfig;

    /**
     * NameServer 定时任务线程池, 负责常规周期任务
     */
    private final ScheduledExecutorService scheduledExecutorService = ThreadUtils.newScheduledThreadPool(1,
            new BasicThreadFactory.Builder().namingPattern("NSScheduledThread").daemon(true).build());

    /**
     * Broker 扫描线程池, 负责扫描失活 Broker
     */
    private final ScheduledExecutorService scanExecutorService = ThreadUtils.newScheduledThreadPool(1,
            new BasicThreadFactory.Builder().namingPattern("NSScanScheduledThread").daemon(true).build());

    /**
     * KV 配置管理器
     */
    private final KVConfigManager kvConfigManager;

    /**
     * 路由信息管理器
     */
    private final RouteInfoManager routeInfoManager;

    /**
     * Remoting 客户端实例
     */
    private RemotingClient remotingClient;
    /**
     * Remoting 服务端实例
     */
    private RemotingServer remotingServer;

    /**
     * Broker 连接事件处理服务
     */
    private final BrokerHousekeepingService brokerHousekeepingService;

    /**
     * 默认请求线程池 16
     */
    private ExecutorService defaultExecutor;
    /**
     * 客户端路由请求线程池 8
     */
    private ExecutorService clientRequestExecutor;

    /**
     * 默认请求线程池队列 10000
     */
    private BlockingQueue<Runnable> defaultThreadPoolQueue;
    /**
     * 客户端请求线程池队列 50000
     */
    private BlockingQueue<Runnable> clientRequestThreadPoolQueue;

    /**
     * 动态配置管理对象
     */
    private final Configuration configuration;
    /**
     * TLS 文件监听服务, 用于证书热加载
     */
    private FileWatchService fileWatchService;

    /**
     * 创建 NamesrvController 实例, 使用默认 NettyClientConfig
     *
     * @param namesrvConfig     NameServer 配置
     * @param nettyServerConfig Netty 服务端配置
     */
    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig) {
        this(namesrvConfig, nettyServerConfig, new NettyClientConfig());
    }

    /**
     * 创建 NamesrvController 实例
     *
     * @param namesrvConfig     NameServer 配置
     * @param nettyServerConfig Netty 服务端配置
     * @param nettyClientConfig Netty 客户端配置
     */
    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig, NettyClientConfig nettyClientConfig) {
        // 保存基础配置并初始化核心组件
        this.namesrvConfig = namesrvConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        this.kvConfigManager = new KVConfigManager(this);
        this.brokerHousekeepingService = new BrokerHousekeepingService(this);
        this.routeInfoManager = new RouteInfoManager(namesrvConfig, this);
        this.configuration = new Configuration(LOGGER, this.namesrvConfig, this.nettyServerConfig);

        // 指定配置持久化路径字段
        this.configuration.setStorePathFromConfig(this.namesrvConfig, "configStorePath");
    }

    /**
     * 初始化 NameServer 控制器
     *
     * @return true 表示初始化完成
     */
    public boolean initialize() {
        // 按依赖顺序完成配置加载、网络初始化、线程池初始化和处理器注册
        loadConfig();                // 加载 KV 配置
        initiateNetworkComponents(); // 核心 - 初始化网络组件
        initiateThreadExecutors();   // 核心 - 初始化业务线程池与任务队列
        registerProcessor();         // 核心 - 注册请求处理器
        startScheduleService();      // 启动周期任务服务
        initiateSslContext();        // 初始化 TLS 上下文动态刷新能力
        initiateRpcHooks();          // 注册 RPC Hook
        return true;
    }

    /**
     * 加载 KV 配置
     */
    private void loadConfig() {
        this.kvConfigManager.load();
    }

    /**
     * 启动周期任务服务
     */
    private void startScheduleService() {
        // 周期扫描失活 Broker
        this.scanExecutorService.scheduleAtFixedRate(NamesrvController.this.routeInfoManager::scanNotActiveBroker,
            5, this.namesrvConfig.getScanNotActiveBrokerInterval(), TimeUnit.MILLISECONDS);

        // 周期打印 KV 配置快照
        this.scheduledExecutorService.scheduleAtFixedRate(NamesrvController.this.kvConfigManager::printAllPeriodically,
            1, 10, TimeUnit.MINUTES);

        // 周期打印线程池水位信息
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
     */
    private void initiateNetworkComponents() {
        // 创建 Remoting 服务端与客户端
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.brokerHousekeepingService);
        this.remotingClient = new NettyRemotingClient(this.nettyClientConfig);
    }

    /**
     * 初始化业务线程池与任务队列
     */
    private void initiateThreadExecutors() {
        // 初始化默认请求处理线程池
        this.defaultThreadPoolQueue = new LinkedBlockingQueue<>(this.namesrvConfig.getDefaultThreadPoolQueueCapacity());
        this.defaultExecutor = ThreadUtils.newThreadPoolExecutor(this.namesrvConfig.getDefaultThreadPoolNums(), this.namesrvConfig.getDefaultThreadPoolNums(),
            1000 * 60, TimeUnit.MILLISECONDS, this.defaultThreadPoolQueue, new ThreadFactoryImpl("RemotingExecutorThread_"));

        // 初始化客户端路由请求处理线程池
        this.clientRequestThreadPoolQueue = new LinkedBlockingQueue<>(this.namesrvConfig.getClientRequestThreadPoolQueueCapacity());
        this.clientRequestExecutor = ThreadUtils.newThreadPoolExecutor(this.namesrvConfig.getClientRequestThreadPoolNums(), this.namesrvConfig.getClientRequestThreadPoolNums(),
            1000 * 60, TimeUnit.MILLISECONDS, this.clientRequestThreadPoolQueue, new ThreadFactoryImpl("ClientRequestExecutorThread_"));
    }

    /**
     * 初始化 TLS 上下文动态刷新能力
     */
    private void initiateSslContext() {
        // TLS 关闭时无需监听证书文件
        if (TlsSystemConfig.tlsMode == TlsMode.DISABLED) {
            return;
        }

        // 监听服务端证书、私钥和信任证书文件
        String[] watchFiles = {TlsSystemConfig.tlsServerCertPath, TlsSystemConfig.tlsServerKeyPath, TlsSystemConfig.tlsServerTrustCertPath};

        FileWatchService.Listener listener = new FileWatchService.Listener() {
            // 服务端证书是否发生变化
            boolean certChanged = false;

            // 服务端私钥是否发生变化
            boolean keyChanged = false;

            // 处理证书文件变化事件, path = 发生变化的文件路径
            @Override
            public void onChanged(String path) {
                // 信任证书变化时立即重载 SSL 上下文
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

                // 证书与私钥同时变化时重载 SSL 上下文
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
     * 打印线程池水位信息
     */
    private void printWaterMark() {
        WATER_MARK_LOG.info("[WATERMARK] ClientQueueSize:{} ClientQueueSlowTime:{} " + "DefaultQueueSize:{} DefaultQueueSlowTime:{}",
            this.clientRequestThreadPoolQueue.size(), headSlowTimeMills(this.clientRequestThreadPoolQueue), this.defaultThreadPoolQueue.size(),
            headSlowTimeMills(this.defaultThreadPoolQueue));
    }

    /**
     * 计算队列头任务等待耗时
     *
     * @param q 目标队列
     * @return 头部任务等待毫秒数
     */
    private long headSlowTimeMills(BlockingQueue<Runnable> q) {
        // 默认慢耗时为 0
        long slowTimeMills = 0;
        final Runnable firstRunnable = q.peek();

        // 仅对 RequestTask 类型计算排队时间
        if (firstRunnable instanceof FutureTaskExt) {
            final Runnable inner = ((FutureTaskExt<?>) firstRunnable).getRunnable();
            if (inner instanceof RequestTask) {
                slowTimeMills = System.currentTimeMillis() - ((RequestTask) inner).getCreateTimestamp();
            }
        }

        // 负值视为 0, 防止系统时间回拨导致异常值
        if (slowTimeMills < 0) {
            slowTimeMills = 0;
        }

        return slowTimeMills;
    }

    /**
     * 注册请求处理器
     */
    private void registerProcessor() {
        // 集群测试模式下使用专用处理器
        if (namesrvConfig.isClusterTest()) {
            this.remotingServer.registerDefaultProcessor(new ClusterTestRequestProcessor(this, namesrvConfig.getProductEnvName()), this.defaultExecutor);
        } else {
            // 临时将获取路由请求分离到独立线程池
            ClientRequestProcessor clientRequestProcessor = new ClientRequestProcessor(this);
            this.remotingServer.registerProcessor(RequestCode.GET_ROUTEINFO_BY_TOPIC, clientRequestProcessor, this.clientRequestExecutor);

            // 其余请求走默认处理器
            this.remotingServer.registerDefaultProcessor(new DefaultRequestProcessor(this), this.defaultExecutor);
        }
    }

    /**
     * 注册 RPC Hook
     */
    private void initiateRpcHooks() {
        this.remotingServer.registerRPCHook(new ZoneRouteRPCHook());
    }

    /**
     * 启动 NameServer 控制器
     *
     * @throws Exception 启动异常
     */
    public void start() throws Exception {
        // 启动 Remoting 服务端
        this.remotingServer.start();

        // In test scenarios where it is up to OS to pick up an available port, set the listening port back to config
        // 测试场景下端口由操作系统分配时, 回写实际监听端口
        if (0 == nettyServerConfig.getListenPort()) {
            nettyServerConfig.setListenPort(this.remotingServer.localListenPort());
        }

        // 更新 NameServer 地址列表并启动客户端
        this.remotingClient.updateNameServerAddressList(Collections.singletonList(NetworkUtil.getLocalAddress()
            + ":" + nettyServerConfig.getListenPort()));
        this.remotingClient.start();

        // 启动证书监听服务
        if (this.fileWatchService != null) {
            this.fileWatchService.start();
        }

        // 启动路由信息管理服务
        this.routeInfoManager.start();
    }

    /**
     * 关闭 NameServer 控制器
     */
    public void shutdown() {
        // 依次关闭网络、线程池与路由服务
        this.remotingClient.shutdown();
        this.remotingServer.shutdown();
        this.defaultExecutor.shutdown();
        this.clientRequestExecutor.shutdown();
        this.scheduledExecutorService.shutdown();
        this.scanExecutorService.shutdown();
        this.routeInfoManager.shutdown();

        // 关闭证书监听服务
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
