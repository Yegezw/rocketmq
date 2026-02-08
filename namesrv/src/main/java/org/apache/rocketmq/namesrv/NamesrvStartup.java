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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.JraftConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.controller.ControllerManager;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.srvutil.ShutdownHookThread;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * NameServer 启动入口, 负责解析参数、加载配置并启动 NameServer 与 Controller
 */
public class NamesrvStartup {

    /**
     * NameServer 主日志对象
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    /**
     * NameServer 控制台日志对象
     */
    private static final Logger logConsole = LoggerFactory.getLogger(LoggerName.NAMESRV_CONSOLE_LOGGER_NAME);

    /**
     * 配置文件与命令行合并后的属性集合
     */
    private static Properties properties = null;

    /**
     * NameServer 配置对象
     */
    private static NamesrvConfig namesrvConfig = null;
    /**
     * NameServer Netty 服务端配置
     */
    private static NettyServerConfig nettyServerConfig = null;
    /**
     * NameServer Netty 客户端配置
     */
    private static NettyClientConfig nettyClientConfig = null;

    /**
     * Controller 配置对象, 仅在启用 Controller 模式时初始化
     */
    private static ControllerConfig controllerConfig = null;

    /**
     * 程序主入口, 先启动 NameServer, 再尝试启动 Controller
     *
     * @param args 启动参数
     */
    public static void main(String[] args) {
        main0(args);
        controllerManagerMain();
    }

    /**
     * 启动 NameServer 主流程
     *
     * @param args 启动参数
     * @return 启动后的 NameServer 控制器
     */
    public static NamesrvController main0(String[] args) {
        try {
            // 解析命令行与配置文件, 初始化静态配置对象
            parseCommandlineAndConfigFile(args);

            // 创建并启动 NameServer 控制器
            NamesrvController controller = createAndStartNamesrvController();
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    /**
     * 启动 Controller 管理器流程
     *
     * @return 启动后的 Controller 管理器, 未启用时返回 null
     */
    public static ControllerManager controllerManagerMain() {
        try {
            // 仅在配置开启时创建并启动 Controller
            if (namesrvConfig.isEnableControllerInNamesrv()) {
                return createAndStartControllerManager();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }

    /**
     * 解析命令行参数与配置文件并初始化运行配置
     *
     * @param args 启动参数
     * @throws Exception 参数解析或文件读取异常
     */
    public static void parseCommandlineAndConfigFile(String[] args) throws Exception {
        // 设置远程通信协议版本
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        // 构建命令行选项并解析启动参数
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options), new DefaultParser());
        if (null == commandLine) {
            System.exit(-1);
            return;
        }

        // 初始化 NameServer 与 Netty 默认配置
        namesrvConfig = new NamesrvConfig();
        nettyServerConfig = new NettyServerConfig();
        nettyClientConfig = new NettyClientConfig();
        nettyServerConfig.setListenPort(9876);

        // 读取 -c 指定的配置文件并覆盖默认配置
        if (commandLine.hasOption('c')) {
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(file)));
                properties = new Properties();
                properties.load(in);
                MixAll.properties2Object(properties, namesrvConfig);
                MixAll.properties2Object(properties, nettyServerConfig);
                MixAll.properties2Object(properties, nettyClientConfig);

                // 启用 Controller 时继续加载 Controller 与 JRaft 配置
                if (namesrvConfig.isEnableControllerInNamesrv()) {
                    controllerConfig = new ControllerConfig();
                    JraftConfig jraftConfig = new JraftConfig();
                    controllerConfig.setJraftConfig(jraftConfig);
                    MixAll.properties2Object(properties, controllerConfig);
                    MixAll.properties2Object(properties, jraftConfig);
                }
                namesrvConfig.setConfigStorePath(file);

                System.out.printf("load config properties file OK, %s%n", file);
                in.close();
            }
        }

        // 命令行参数优先级高于配置文件
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);

        // 指定 -p 时打印配置并退出
        if (commandLine.hasOption('p')) {
            MixAll.printObjectProperties(logConsole, namesrvConfig);
            MixAll.printObjectProperties(logConsole, nettyServerConfig);
            MixAll.printObjectProperties(logConsole, nettyClientConfig);
            if (namesrvConfig.isEnableControllerInNamesrv()) {
                MixAll.printObjectProperties(logConsole, controllerConfig);
            }
            System.exit(0);
        }

        // RocketMQ Home 未配置时直接退出
        if (null == namesrvConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation%n", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }

        // 输出最终生效配置
        MixAll.printObjectProperties(log, namesrvConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);

    }

    /**
     * 创建并启动 NameServer 控制器
     *
     * @return 启动后的 NameServer 控制器
     * @throws Exception 启动异常
     */
    public static NamesrvController createAndStartNamesrvController() throws Exception {

        // 创建 NameServer 控制器并执行启动流程
        NamesrvController controller = createNamesrvController();
        start(controller);

        // 启动成功后输出提示信息
        NettyServerConfig serverConfig = controller.getNettyServerConfig();
        String tip = String.format("The Name Server boot success. serializeType=%s, address %s:%d", RemotingCommand.getSerializeTypeConfigInThisServer(), serverConfig.getBindAddress(), serverConfig.getListenPort());
        log.info(tip);
        System.out.printf("%s%n", tip);
        return controller;
    }

    /**
     * 创建 NameServer 控制器实例
     *
     * @return NameServer 控制器实例
     */
    public static NamesrvController createNamesrvController() {

        final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig, nettyClientConfig);

        // 注册全部配置, 防止配置项被丢弃
        controller.getConfiguration().registerConfig(properties);
        return controller;
    }

    /**
     * 启动 NameServer 控制器
     *
     * @param controller NameServer 控制器
     * @return 启动后的 NameServer 控制器
     * @throws Exception 启动异常
     */
    public static NamesrvController start(final NamesrvController controller) throws Exception {

        // 参数为空时直接抛错
        if (null == controller) {
            throw new IllegalArgumentException("NamesrvController is null");
        }

        // 初始化失败时主动关闭并退出进程
        boolean initResult = controller.initialize();
        if (!initResult) {
            controller.shutdown();
            System.exit(-3);
        }

        // 注册 JVM 关闭钩子, 保证进程退出时完成资源释放
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, (Callable<Void>) () -> {
            controller.shutdown();
            return null;
        }));

        // 正式启动 NameServer 控制器
        controller.start();

        return controller;
    }

    /**
     * 创建并启动 Controller 管理器
     *
     * @return 启动后的 Controller 管理器
     * @throws Exception 启动异常
     */
    public static ControllerManager createAndStartControllerManager() throws Exception {
        // 创建 Controller 管理器并执行启动流程
        ControllerManager controllerManager = createControllerManager();
        start(controllerManager);

        // 启动成功后输出提示信息
        String tip = "The ControllerManager boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
        log.info(tip);
        System.out.printf("%s%n", tip);
        return controllerManager;
    }

    /**
     * 创建 Controller 管理器实例
     *
     * @return Controller 管理器实例
     * @throws Exception 配置克隆异常
     */
    public static ControllerManager createControllerManager() throws Exception {
        // 使用 NameServer 端口配置克隆一个独立的 Controller Netty 配置
        NettyServerConfig controllerNettyServerConfig = (NettyServerConfig) nettyServerConfig.clone();
        ControllerManager controllerManager = new ControllerManager(controllerConfig, controllerNettyServerConfig, nettyClientConfig);

        // 注册全部配置, 防止配置项被丢弃
        controllerManager.getConfiguration().registerConfig(properties);
        return controllerManager;
    }

    /**
     * 启动 Controller 管理器
     *
     * @param controllerManager Controller 管理器
     * @return 启动后的 Controller 管理器
     * @throws Exception 启动异常
     */
    public static ControllerManager start(final ControllerManager controllerManager) throws Exception {

        // 参数为空时直接抛错
        if (null == controllerManager) {
            throw new IllegalArgumentException("ControllerManager is null");
        }

        // 初始化失败时主动关闭并退出进程
        boolean initResult = controllerManager.initialize();
        if (!initResult) {
            controllerManager.shutdown();
            System.exit(-3);
        }

        // 注册 JVM 关闭钩子, 保证进程退出时完成资源释放
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, (Callable<Void>) () -> {
            controllerManager.shutdown();
            return null;
        }));

        // 正式启动 Controller 管理器
        controllerManager.start();

        return controllerManager;
    }

    /**
     * 关闭 NameServer 控制器
     *
     * @param controller NameServer 控制器
     */
    public static void shutdown(final NamesrvController controller) {
        controller.shutdown();
    }

    /**
     * 关闭 Controller 管理器
     *
     * @param controllerManager Controller 管理器
     */
    public static void shutdown(final ControllerManager controllerManager) {
        controllerManager.shutdown();
    }

    /**
     * 构建 NameServer 启动命令行选项
     *
     * @param options 命令行选项对象
     * @return 补充后的命令行选项对象
     */
    public static Options buildCommandlineOptions(final Options options) {
        // -c 指定配置文件路径
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        // -p 打印配置后退出
        opt = new Option("p", "printConfigItem", false, "Print all config items");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    public static Properties getProperties() {
        return properties;
    }
}
