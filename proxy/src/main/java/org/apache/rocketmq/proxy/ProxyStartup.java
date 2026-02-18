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

package org.apache.rocketmq.proxy;

import com.google.common.collect.Lists;
import io.grpc.protobuf.services.ChannelzService;
import io.grpc.protobuf.services.ProtoReflectionService;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerStartup;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.common.utils.AbstractStartAndShutdown;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.Configuration;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.GrpcServer;
import org.apache.rocketmq.proxy.grpc.GrpcServerBuilder;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingApplication;
import org.apache.rocketmq.proxy.metrics.ProxyMetricsManager;
import org.apache.rocketmq.proxy.processor.DefaultMessagingProcessor;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.RemotingProtocolServer;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ProxyStartup {
    /**
     * Proxy 启动流程日志记录器
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    /**
     * Proxy 生命周期管理器
     */
    private static final ProxyStartAndShutdown PROXY_START_AND_SHUTDOWN = new ProxyStartAndShutdown();

    /**
     * 启动与关闭组件聚合器, 用于统一管理 Proxy 生命周期
     */
    private static class ProxyStartAndShutdown extends AbstractStartAndShutdown {
        /**
         * 追加生命周期组件, 当前实现直接复用父类行为
         *
         * @param startAndShutdown 生命周期组件
         */
        @Override
        public void appendStartAndShutdown(StartAndShutdown startAndShutdown) {
            super.appendStartAndShutdown(startAndShutdown);
        }
    }

    /**
     * Proxy 启动入口, 负责初始化配置与协议服务
     *
     * @param args 启动参数
     */
    public static void main(String[] args) {
        try {
            // parse argument from command line
            // 解析命令行参数
            CommandLineArgument commandLineArgument = parseCommandLineArgument(args);
            initConfiguration(commandLineArgument);

            // init thread pool monitor for proxy.
            // 初始化 Proxy 线程池监控
            initThreadPoolMonitor();

            ThreadPoolExecutor executor = createServerExecutor();

            MessagingProcessor messagingProcessor = createMessagingProcessor();

            // create grpcServer
            // 创建 gRPC 服务, 与生产者和消费者进行通信
            // GrpcServer -> GrpcMessagingApplication -> DefaultGrpcMessingActivity (容器) -> DefaultMessagingProcessor
            GrpcServer grpcServer = GrpcServerBuilder.newBuilder(executor, ConfigurationManager.getProxyConfig().getGrpcServerPort())
                .addService(createServiceProcessor(messagingProcessor))   // 消息处理器
                .addService(ChannelzService.newInstance(100))  // 连接与流量监控服务
                .addService(ProtoReflectionService.newInstance())         // 反射服务, 支持客户端动态查询服务接口
                .configInterceptor() // 全局拦截器链
                .shutdownTime(ConfigurationManager.getProxyConfig().getGrpcShutdownTimeSeconds(), TimeUnit.SECONDS) // 优雅关闭配置
                .build();
            PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(grpcServer);

            // 创建 Netty 服务, 与 Nameserver 和 Broker 进行通信
            // RemotingProtocolServer -> NettyRemotingServer -> NettyServerHandler -> NettyRemotingAbstract (容器) -> AbstractRemotingActivity -> DefaultMessagingProcessor
            RemotingProtocolServer remotingServer = new RemotingProtocolServer(messagingProcessor);
            PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(remotingServer);

            // start servers one by one.
            // 顺序启动所有服务
            PROXY_START_AND_SHUTDOWN.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("try to shutdown server");
                try {
                    PROXY_START_AND_SHUTDOWN.preShutdown();
                    PROXY_START_AND_SHUTDOWN.shutdown();
                } catch (Exception e) {
                    log.error("err when shutdown rocketmq-proxy", e);
                }
            }));
        } catch (Exception e) {
            e.printStackTrace();
            log.error("find an unexpect err.", e);
            System.exit(1);
        }

        System.out.printf("%s%n", new Date() + " rocketmq-proxy startup successfully");
        log.info(new Date() + " rocketmq-proxy startup successfully");
    }

    /**
     * 初始化 Proxy 配置, 并应用命令行覆盖项
     *
     * @param commandLineArgument 命令行参数对象
     * @throws Exception 初始化异常
     */
    protected static void initConfiguration(CommandLineArgument commandLineArgument) throws Exception {
        if (StringUtils.isNotBlank(commandLineArgument.getProxyConfigPath())) {
            System.setProperty(Configuration.CONFIG_PATH_PROPERTY, commandLineArgument.getProxyConfigPath());
        }
        ConfigurationManager.initEnv();
        ConfigurationManager.intConfig();
        setConfigFromCommandLineArgument(commandLineArgument);
        log.info("Current configuration: " + ConfigurationManager.formatProxyConfig());

    }

    /**
     * 解析命令行参数并映射为启动参数对象
     *
     * @param args 原始命令行参数
     * @return 启动参数对象
     */
    protected static CommandLineArgument parseCommandLineArgument(String[] args) {
        CommandLine commandLine = ServerUtil.parseCmdLine("mqproxy", args,
            buildCommandlineOptions(), new DefaultParser());
        if (commandLine == null) {
            throw new RuntimeException("parse command line argument failed");
        }

        CommandLineArgument commandLineArgument = new CommandLineArgument();
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), commandLineArgument);
        return commandLineArgument;
    }

    /**
     * 构建 Proxy 启动命令行选项
     *
     * @return 命令行选项定义
     */
    private static Options buildCommandlineOptions() {
        Options options = ServerUtil.buildCommandlineOptions(new Options());

        Option opt = new Option("bc", "brokerConfigPath", true, "Broker config file path for local mode");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("pc", "proxyConfigPath", true, "Proxy config file path");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("pm", "proxyMode", true, "Proxy run in local or cluster mode");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    /**
     * 将命令行参数同步到 Proxy 配置对象
     *
     * @param commandLineArgument 启动参数对象
     */
    private static void setConfigFromCommandLineArgument(CommandLineArgument commandLineArgument) {
        if (StringUtils.isNotBlank(commandLineArgument.getNamesrvAddr())) {
            ConfigurationManager.getProxyConfig().setNamesrvAddr(commandLineArgument.getNamesrvAddr());
        }
        if (StringUtils.isNotBlank(commandLineArgument.getBrokerConfigPath())) {
            ConfigurationManager.getProxyConfig().setBrokerConfigPath(commandLineArgument.getBrokerConfigPath());
        }
        if (StringUtils.isNotBlank(commandLineArgument.getProxyMode())) {
            ConfigurationManager.getProxyConfig().setProxyMode(commandLineArgument.getProxyMode());
        }
    }

    /**
     * 根据运行模式创建消息处理器并注册生命周期
     *
     * @return 消息处理器实例
     */
    protected static MessagingProcessor createMessagingProcessor() {
        String proxyModeStr = ConfigurationManager.getProxyConfig().getProxyMode();
        MessagingProcessor messagingProcessor;

        if (ProxyMode.isClusterMode(proxyModeStr)) {
            // 集群模式
            messagingProcessor = DefaultMessagingProcessor.createForClusterMode();
            ProxyMetricsManager proxyMetricsManager = ProxyMetricsManager.initClusterMode(ConfigurationManager.getProxyConfig());
            PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(proxyMetricsManager);
        } else if (ProxyMode.isLocalMode(proxyModeStr)) {
            // 本地模式
            BrokerController brokerController = createBrokerController();
            ProxyMetricsManager.initLocalMode(brokerController.getBrokerMetricsManager(), ConfigurationManager.getProxyConfig());
            StartAndShutdown brokerControllerWrapper = new StartAndShutdown() {
                // 启动本地 Broker, 并输出关键启动信息
                @Override
                public void start() throws Exception {
                    brokerController.start();
                    String tip = "The broker[" + brokerController.getBrokerConfig().getBrokerName() + ", "
                        + brokerController.getBrokerAddr() + "] boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
                    if (null != brokerController.getBrokerConfig().getNamesrvAddr()) {
                        tip += " and name server is " + brokerController.getBrokerConfig().getNamesrvAddr();
                    }
                    log.info(tip);
                }

                // 关闭本地 Broker
                @Override
                public void shutdown() throws Exception {
                    brokerController.shutdown();
                }
            };
            PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(brokerControllerWrapper);
            messagingProcessor = DefaultMessagingProcessor.createForLocalMode(brokerController);
        } else {
            throw new IllegalArgumentException("try to start grpc server with wrong mode, use 'local' or 'cluster'");
        }
        PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(messagingProcessor);
        return messagingProcessor;
    }

    /**
     * 创建 gRPC 应用处理器并注册生命周期
     *
     * @param messagingProcessor 消息处理器
     * @return gRPC 消息应用实例
     */
    private static GrpcMessagingApplication createServiceProcessor(MessagingProcessor messagingProcessor) {
        GrpcMessagingApplication application = GrpcMessagingApplication.create(messagingProcessor);
        PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(application);
        return application;
    }

    /**
     * 根据 Proxy 配置构建本地 Broker 控制器
     *
     * @return Broker 控制器
     */
    protected static BrokerController createBrokerController() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        List<String> brokerStartupArgList = Lists.newArrayList("-c", config.getBrokerConfigPath());
        if (StringUtils.isNotBlank(config.getNamesrvAddr())) {
            brokerStartupArgList.add("-n");
            brokerStartupArgList.add(config.getNamesrvAddr());
        }
        String[] brokerStartupArgs = brokerStartupArgList.toArray(new String[0]);
        return BrokerStartup.createBrokerController(brokerStartupArgs);
    }

    /**
     * 创建 gRPC 请求执行线程池, 并注册关闭回调
     *
     * @return gRPC 线程池执行器
     */
    public static ThreadPoolExecutor createServerExecutor() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        int threadPoolNums = config.getGrpcThreadPoolNums();                   // 16 + CPU * 2
        int threadPoolQueueCapacity = config.getGrpcThreadPoolQueueCapacity(); // 100000
        ThreadPoolExecutor executor = ThreadPoolMonitor.createAndMonitor(
            threadPoolNums,
            threadPoolNums,
            1, TimeUnit.MINUTES,
            "GrpcRequestExecutorThread",
            threadPoolQueueCapacity
        );
        PROXY_START_AND_SHUTDOWN.appendShutdown(executor::shutdown);
        return executor;
    }

    /**
     * 初始化线程池监控配置
     */
    public static void initThreadPoolMonitor() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        ThreadPoolMonitor.config(
            LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME),
            LoggerFactory.getLogger(LoggerName.PROXY_WATER_MARK_LOGGER_NAME),
            config.isEnablePrintJstack(), config.getPrintJstackInMillis(),
            config.getPrintThreadPoolStatusInMillis());
        ThreadPoolMonitor.init();
    }
}
