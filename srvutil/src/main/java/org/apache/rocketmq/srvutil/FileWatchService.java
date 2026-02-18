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

import com.google.common.base.Strings;
import org.apache.rocketmq.common.LifecycleAwareServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;

/**
 * 文件变更监听服务, 通过周期计算 MD5 检测目标文件变化
 */
public class FileWatchService extends LifecycleAwareServiceThread {
    /**
     * 文件监听服务日志记录器
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    /**
     * 当前文件路径与哈希值映射
     */
    private final Map<String, String> currentHash = new HashMap<>();
    /**
     * 文件变化回调监听器
     */
    private final Listener listener;
    /**
     * 扫描间隔, 单位毫秒
     */
    private static final int WATCH_INTERVAL = 500;
    /**
     * MD5 摘要计算器
     */
    private final MessageDigest md = MessageDigest.getInstance("MD5");

    /**
     * 创建文件监听服务并初始化哈希缓存
     *
     * @param watchFiles 待监听文件路径数组
     * @param listener 文件变化回调监听器
     * @throws Exception 初始化摘要计算或哈希缓存时异常
     */
    public FileWatchService(final String[] watchFiles,
        final Listener listener) throws Exception {
        this.listener = listener;
        for (String file : watchFiles) {
            if (!Strings.isNullOrEmpty(file) && new File(file).exists()) {
                currentHash.put(file, md5Digest(file));
            }
        }
    }

    @Override
    public String getServiceName() {
        return "FileWatchService";
    }

    /**
     * 服务主循环, 周期扫描文件并在内容变化时触发回调
     */
    @Override
    public void run0() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                this.waitForRunning(WATCH_INTERVAL);
                for (Map.Entry<String, String> entry : currentHash.entrySet()) {
                    String newHash = md5Digest(entry.getKey());
                    if (!newHash.equals(entry.getValue())) {
                        entry.setValue(newHash);
                        listener.onChanged(entry.getKey());
                    }
                }
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service raised an unexpected exception.", e);
            }
        }
        log.info(this.getServiceName() + " service end");
    }

    /**
     * Note: we ignore DELETE event on purpose. This is useful when application renew CA file.
     * When the operator delete/rename the old CA file and copy a new one, this ensures the old CA file is used during
     * the operation.
     * <p>
     * As we know exactly what to do when file does not exist or when IO exception is raised, there is no need to
     * propagate the exception up.
     * <br>
     * 说明: 当前实现有意忽略 DELETE 事件, 以便证书轮换期间继续使用旧证书内容<br>
     * 当文件不存在或读取异常时复用缓存哈希, 无需向上抛出异常
     *
     * @param filePath Absolute path of the file to calculate its MD5 digest.<br>待计算 MD5 的文件绝对路径
     * @return Hash of the file content if exists; empty string otherwise.<br>文件内容哈希, 文件缺失或读取失败时返回缓存值或空字符串
     */
    private String md5Digest(String filePath) {
        Path path = Paths.get(filePath);
        if (!path.toFile().exists()) {
            // Reuse previous hash result
            // 复用上次哈希结果
            return currentHash.getOrDefault(filePath, "");
        }
        byte[] raw;
        try {
            raw = Files.readAllBytes(path);
        } catch (IOException e) {
            log.info("Failed to read content of {}", filePath);
            // Reuse previous hash result
            // 复用上次哈希结果
            return currentHash.getOrDefault(filePath, "");
        }
        md.update(raw);
        byte[] hash = md.digest();
        return UtilAll.bytes2string(hash);
    }

    /**
     * 文件变化回调监听器
     */
    public interface Listener {
        /**
         * Will be called when the target files are changed
         * <br>
         * 目标文件发生变化时触发回调
         *
         * @param path the changed file path, 变化文件路径
         */
        void onChanged(String path);
    }
}
