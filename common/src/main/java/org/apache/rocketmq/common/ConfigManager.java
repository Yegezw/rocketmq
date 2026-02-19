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
package org.apache.rocketmq.common;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * 配置管理抽象基类<br>
 * 统一封装配置加载, 备份回退与持久化流程
 */
public abstract class ConfigManager {
    /**
     * 公共模块日志记录器, 用于输出配置读写与异常信息
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    /**
     * 加载配置文件<br>
     * 优先读取主配置文件, 失败时回退读取备份文件
     *
     * @return true 表示加载成功或配置为空可接受
     */
    public boolean load() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName);

            if (null == jsonString || jsonString.length() == 0) {
                return this.loadBak(); // 加载 bak 备份文件
            } else {
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " failed, and try to load backup file", e);
            return this.loadBak();
        }
    }

    /**
     * 加载备份配置文件
     *
     * @return true 表示备份加载成功或备份不存在, false 表示读取备份异常
     */
    private boolean loadBak() {
        String fileName = null;
        try {
            fileName = this.configFilePath() + ".bak";
            String jsonString = MixAll.file2String(fileName);
            if (jsonString != null && jsonString.length() > 0) {
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " Failed", e);
            return false;
        }

        return true;
    }

    /**
     * 按主题维度持久化配置<br>
     * 当前默认实现仅复用全量持久化逻辑
     *
     * @param topicName 主题名称
     * @param t 主题配置对象
     * @param <T> 配置对象类型
     */
    public synchronized <T> void persist(String topicName, T t) {
        // stub for future
        // 为未来扩展保留, 当前统一走全量持久化
        this.persist();
    }

    /**
     * 按映射维度持久化配置<br>
     * 当前默认实现仅复用全量持久化逻辑
     *
     * @param m 配置映射
     * @param <T> 配置对象类型
     */
    public synchronized <T> void persist(Map<String, T> m) {
        // stub for future
        // 为未来扩展保留, 当前统一走全量持久化
        this.persist();
    }

    /**
     * 全量持久化配置<br>
     * 将编码后的配置内容写入 configFilePath 指定路径
     */
    public synchronized void persist() {
        String jsonString = this.encode(true);
        if (jsonString != null) {
            String fileName = this.configFilePath();
            try {
                MixAll.string2File(jsonString, fileName);
            } catch (IOException e) {
                log.error("persist file " + fileName + " exception", e);
            }
        }
    }

    /**
     * 停止配置管理器<br>
     * 默认实现不执行额外动作
     *
     * @return true 表示停止成功
     */
    public boolean stop() {
        return true;
    }

    /**
     * 返回配置文件路径
     *
     * @return 配置文件绝对路径或相对路径
     */
    public abstract String configFilePath();

    /**
     * 编码配置对象
     *
     * @return JSON 字符串
     */
    public abstract String encode();

    /**
     * 编码配置对象
     *
     * @param prettyFormat true 表示输出格式化 JSON
     * @return JSON 字符串
     */
    public abstract String encode(final boolean prettyFormat);

    /**
     * 解码配置内容并回填内存对象
     *
     * @param jsonString JSON 字符串
     */
    public abstract void decode(final String jsonString);
}
