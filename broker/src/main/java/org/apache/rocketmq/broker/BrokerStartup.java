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
package org.apache.rocketmq.broker;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Broker 启动入口<br>
 * 负责构建 启动与关闭 BrokerController
 */
public class BrokerStartup {

    /**
     * Broker 日志记录器
     */
    public static Logger log;
    /**
     * 系统配置文件辅助类
     */
    public static final SystemConfigFileHelper CONFIG_FILE_HELPER = new SystemConfigFileHelper();

    /**
     * Broker 进程主入口
     *
     * @param args 启动参数
     */
    public static void main(String[] args) {
        start(createBrokerController(args));
    }

    /**
     * 启动 Broker 控制器
     *
     * @param controller Broker 控制器
     * @return 启动后的 Broker 控制器
     */
    public static BrokerController start(BrokerController controller) {
        try {
            controller.start();

            String tip = String.format("The broker[%s, %s] boot success. serializeType=%s",
                controller.getBrokerConfig().getBrokerName(), controller.getBrokerAddr(),
                RemotingCommand.getSerializeTypeConfigInThisServer());

            if (null != controller.getBrokerConfig().getNamesrvAddr()) {
                tip += " and name server is " + controller.getBrokerConfig().getNamesrvAddr();
            }

            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    /**
     * 关闭 Broker 控制器
     *
     * @param controller Broker 控制器
     */
    public static void shutdown(final BrokerController controller) {
        if (null != controller) {
            controller.shutdown();
        }
    }

    /**
     * 构建 Broker 控制器
     *
     * @param args 启动参数
     * @return Broker 控制器
     * @throws Exception 构建过程异常
     */
    public static BrokerController buildBrokerController(String[] args) throws Exception {
        // 固定 remoting 协议版本, 确保客户端与服务端序列化行为一致
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        // 1. 加载配置

        // 初始化核心配置对象, 后续按配置文件与命令行顺序覆盖
        final BrokerConfig brokerConfig = new BrokerConfig();
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        final NettyClientConfig nettyClientConfig = new NettyClientConfig();
        final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        final AuthConfig authConfig = new AuthConfig();
        // Broker 默认监听端口
        nettyServerConfig.setListenPort(10911);
        // HA 端口先置为 0, 后续会根据业务端口自动推导
        messageStoreConfig.setHaListenPort(0);

        // 解析命令行参数, 参数非法时直接退出
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine(
            "mqbroker", args, buildCommandlineOptions(options), new DefaultParser());
        if (null == commandLine) {
            System.exit(-1);
        }

        // 先加载 -c 指定的配置文件, 作为基础配置来源
        Properties properties = null;
        if (commandLine.hasOption('c')) {
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                CONFIG_FILE_HELPER.setFile(file);
                // 保存配置文件路径, 供后续动态配置模块使用
                BrokerPathConfigHelper.setBrokerConfigPath(file);
                properties = CONFIG_FILE_HELPER.loadConfig();
            }
        }

        // 将文件配置映射到运行对象, 顺序保持与组件一致
        if (properties != null) {
            // 同步地址服务域名配置到系统属性, 供 NameServer 寻址模块读取
            properties2SystemEnv(properties);
            MixAll.properties2Object(properties, brokerConfig);
            MixAll.properties2Object(properties, nettyServerConfig);
            MixAll.properties2Object(properties, nettyClientConfig);
            MixAll.properties2Object(properties, messageStoreConfig);
            MixAll.properties2Object(properties, authConfig);
        }

        // 命令行参数覆盖同名配置项, 使启动参数优先级高于配置文件
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);
        if (null == brokerConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment " +
                "to match the location of the RocketMQ installation", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }

        // Validate namesrvAddr
        // 2. 校验 namesrvAddr
        String namesrvAddr = brokerConfig.getNamesrvAddr();
        if (StringUtils.isNotBlank(namesrvAddr)) {
            try {
                // 多个 namesrv 地址使用分号分隔, 逐个校验 host:port 格式
                String[] addrArray = namesrvAddr.split(";");
                for (String addr : addrArray) {
                    NetworkUtil.string2SocketAddress(addr);
                }
            } catch (Exception e) {
                System.out.printf("The Name Server Address[%s] illegal, please set it as follows, " +
                        "\"127.0.0.1:9876;192.168.0.1:9876\"%n", namesrvAddr);
                System.exit(-3);
            }
        }

        // 3. 从节点默认下调内存访问阈值, 避免读流量挤压复制链路
        if (BrokerRole.SLAVE == messageStoreConfig.getBrokerRole()) {
            int ratio = messageStoreConfig.getAccessMessageInMemoryMaxRatio() - 10;
            messageStoreConfig.setAccessMessageInMemoryMaxRatio(ratio);
        }

        // Set broker role according to ha config
        // 4. 根据 HA 配置设置 broker 角色
        if (!brokerConfig.isEnableControllerMode()) {
            switch (messageStoreConfig.getBrokerRole()) {
                case ASYNC_MASTER:
                case SYNC_MASTER:
                    // 主节点角色固定 brokerId 为 0
                    brokerConfig.setBrokerId(MixAll.MASTER_ID);
                    break;
                case SLAVE:
                    // 从节点 brokerId 必须大于 0, 避免与主节点冲突
                    if (brokerConfig.getBrokerId() <= MixAll.MASTER_ID) {
                        System.out.printf("Slave's brokerId must be > 0%n");
                        System.exit(-3);
                    }
                    break;
                default:
                    break;
            }
        }

        // DLedger 启动阶段先将 brokerId 置为 -1 作为占位, 角色切换后再按 dLegerSelfId 与选举结果回写为 0 或正数
        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            brokerConfig.setBrokerId(-1);
        }

        // controller 与 DLedger 是两套主从选举体系, 不能同时启用
        if (brokerConfig.isEnableControllerMode() && messageStoreConfig.isEnableDLegerCommitLog()) {
            System.out.printf("The config enableControllerMode and enableDLegerCommitLog cannot both be true.%n");
            System.exit(-4);
        }

        // 5. 若未显式配置 HA 端口, 默认使用业务端口 + 1 = 10912
        if (messageStoreConfig.getHaListenPort() <= 0) {
            messageStoreConfig.setHaListenPort(nettyServerConfig.getListenPort() + 1);
        }

        // 6. Broker 单进程启动场景固定为非容器模式
        brokerConfig.setInBrokerContainer(false);

        // 7. 日志目录隔离策略: 默认不隔离, 按配置切换到 brokerName + brokerId
        System.setProperty("brokerLogDir", "");
        if (brokerConfig.isIsolateLogEnable()) {
            System.setProperty("brokerLogDir", brokerConfig.getBrokerName() + "_" + brokerConfig.getBrokerId());
        }
        // DLedger 场景使用 dLegerSelfId 作为日志目录后缀, 便于节点定位
        if (brokerConfig.isIsolateLogEnable() && messageStoreConfig.isEnableDLegerCommitLog()) {
            System.setProperty("brokerLogDir", brokerConfig.getBrokerName() + "_" + messageStoreConfig.getdLegerSelfId());
        }

        // 仅打印配置并退出, 用于诊断启动参数是否生效
        if (commandLine.hasOption('p')) {
            Logger console = LoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
            MixAll.printObjectProperties(console, brokerConfig);
            MixAll.printObjectProperties(console, nettyServerConfig);
            MixAll.printObjectProperties(console, nettyClientConfig);
            MixAll.printObjectProperties(console, messageStoreConfig);
            System.exit(0);
        } else if (commandLine.hasOption('m')) {
            Logger console = LoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
            MixAll.printObjectProperties(console, brokerConfig, true);
            MixAll.printObjectProperties(console, nettyServerConfig, true);
            MixAll.printObjectProperties(console, nettyClientConfig, true);
            MixAll.printObjectProperties(console, messageStoreConfig, true);
            System.exit(0);
        }

        // 初始化 Broker 主日志并打印最终生效配置
        log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
        MixAll.printObjectProperties(log, brokerConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);
        MixAll.printObjectProperties(log, nettyClientConfig);
        MixAll.printObjectProperties(log, messageStoreConfig);

        // 鉴权配置补齐运行时上下文, 认证模块依赖这三个关键字段
        authConfig.setConfigName(brokerConfig.getBrokerName());
        authConfig.setClusterName(brokerConfig.getBrokerClusterName());
        authConfig.setAuthConfigPath(messageStoreConfig.getStorePathRootDir() + File.separator + "config");

        // 8. 组装 BrokerController, 后续由 createBrokerController 调用 initialize 与 start
        final BrokerController controller = new BrokerController(
            brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig, authConfig);

        // Remember all configs to prevent discard
        // 保留全部配置项避免丢弃
        controller.getConfiguration().registerConfig(properties);

        return controller;
    }

    /**
     * 构建关闭钩子
     *
     * @param brokerController Broker 控制器
     * @return 关闭钩子任务
     */
    public static Runnable buildShutdownHook(BrokerController brokerController) {
        return new Runnable() {
            // 是否已执行关闭
            private volatile boolean hasShutdown = false;
            // 关闭触发次数
            private final AtomicInteger shutdownTimes = new AtomicInteger(0);

            // 执行 Broker 关闭逻辑
            @Override
            public void run() {
                synchronized (this) {
                    log.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                    if (!this.hasShutdown) {
                        this.hasShutdown = true;
                        long beginTime = System.currentTimeMillis();
                        brokerController.shutdown();
                        long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                        log.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                    }
                }
            }
        };
    }

    /**
     * 创建并初始化 Broker 控制器
     *
     * @param args 启动参数
     * @return Broker 控制器
     */
    public static BrokerController createBrokerController(String[] args) {
        try {
            BrokerController controller = buildBrokerController(args);
            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }
            Runtime.getRuntime().addShutdownHook(new Thread(buildShutdownHook(controller)));
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }

    /**
     * 将配置项同步到系统属性
     *
     * @param properties 配置属性
     */
    private static void properties2SystemEnv(Properties properties) {
        if (properties == null) {
            return;
        }
        String rmqAddressServerDomain = properties.getProperty("rmqAddressServerDomain", MixAll.WS_DOMAIN_NAME);
        String rmqAddressServerSubGroup = properties.getProperty("rmqAddressServerSubGroup", MixAll.WS_DOMAIN_SUBGROUP);
        System.setProperty("rocketmq.namesrv.domain", rmqAddressServerDomain);
        System.setProperty("rocketmq.namesrv.domain.subgroup", rmqAddressServerSubGroup);
    }

    /**
     * 构建命令行参数选项
     *
     * @param options 原始参数集合
     * @return 增强后的参数集合
     */
    private static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Broker config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "printImportantConfig", false, "Print important config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    /**
     * 系统配置文件辅助类
     */
    public static class SystemConfigFileHelper {
        /**
         * 配置辅助日志记录器
         */
        private static final Logger LOGGER = LoggerFactory.getLogger(SystemConfigFileHelper.class);

        /**
         * 配置文件路径
         */
        private String file;

        /**
         * 构造配置文件辅助类
         */
        public SystemConfigFileHelper() {
        }

        /**
         * 加载配置文件
         *
         * @return 配置属性
         * @throws Exception 加载过程异常
         */
        public Properties loadConfig() throws Exception {
            InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(file)));
            Properties properties = new Properties();
            properties.load(in);
            in.close();
            return properties;
        }

        /**
         * 更新配置文件
         *
         * @param properties 配置属性
         * @throws Exception 更新过程异常
         */
        public void update(Properties properties) throws Exception {
            LOGGER.error("[SystemConfigFileHelper] update no thing.");
        }

        public void setFile(String file) {
            this.file = file;
        }

        public String getFile() {
            return file;
        }
    }
}
