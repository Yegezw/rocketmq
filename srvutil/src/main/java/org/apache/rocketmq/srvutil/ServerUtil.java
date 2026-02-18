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
package org.apache.rocketmq.srvutil;

import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * 服务端命令行工具方法集合
 */
public class ServerUtil {

    /**
     * 构建服务端通用命令行参数, 包含帮助参数与 NameServer 地址参数
     *
     * @param options 命令行选项对象
     * @return 补充后的命令行选项对象
     */
    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("h", "help", false, "Print help");
        opt.setRequired(false);
        options.addOption(opt);

        opt =
            new Option("n", "namesrvAddr", true,
                "Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876'");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    /**
     * 解析命令行参数, 在帮助或异常场景输出使用说明
     *
     * @param appName 应用名
     * @param args 命令行参数
     * @param options 命令行选项定义
     * @param parser 命令行解析器
     * @return 解析后的命令行对象
     */
    public static CommandLine parseCmdLine(final String appName, String[] args, Options options,
        CommandLineParser parser) {
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
            if (commandLine.hasOption('h')) {
                hf.printHelp(appName, options, true);
                System.exit(0);
            }
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            hf.printHelp(appName, options, true);
            System.exit(1);
        }

        return commandLine;
    }

    /**
     * 打印命令行帮助信息
     *
     * @param appName 应用名
     * @param options 命令行选项定义
     */
    public static void printCommandLineHelp(final String appName, final Options options) {
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        hf.printHelp(appName, options, true);
    }

    /**
     * 将命令行参数转换为 Properties 对象
     *
     * @param commandLine 已解析命令行对象
     * @return 参数键值属性集合
     */
    public static Properties commandLine2Properties(final CommandLine commandLine) {
        Properties properties = new Properties();
        Option[] opts = commandLine.getOptions();

        if (opts != null) {
            for (Option opt : opts) {
                String name = opt.getLongOpt();
                String value = commandLine.getOptionValue(name);
                if (value != null) {
                    properties.setProperty(name, value);
                }
            }
        }

        return properties;
    }

}
