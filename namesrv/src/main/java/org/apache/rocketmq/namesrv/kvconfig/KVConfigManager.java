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
package org.apache.rocketmq.namesrv.kvconfig;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.protocol.body.KVTable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * KV 配置管理器, 负责加载、查询、更新和持久化 NameServer 的 KV 数据
 */
public class KVConfigManager {
    /**
     * NameServer 日志对象, 记录 KV 配置管理相关日志
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    /**
     * NameServer 控制器引用, 用于访问 NameServer 配置
     */
    private final NamesrvController namesrvController;

    /**
     * 读写锁, 控制配置表并发读写
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * KV 配置内存表, 第一层键为命名空间, 第二层键值为配置项
     */
    private final HashMap<String/* 命名空间 */, HashMap<String/* 键 */, String/* 值 */>> configTable =
        new HashMap<>();

    /**
     * 创建 KVConfigManager 实例, 绑定 NameServer 控制器
     *
     * @param namesrvController NameServer 控制器
     */
    public KVConfigManager(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }

    /**
     * 从磁盘加载 KV 配置到内存表
     */
    public void load() {
        String content = null;
        try {
            // 读取配置文件内容
            content = MixAll.file2String(this.namesrvController.getNamesrvConfig().getKvConfigPath());
        } catch (IOException e) {
            log.warn("Load KV config table exception", e);
        }
        if (content != null) {
            // 将 JSON 反序列化并合并到内存表
            KVConfigSerializeWrapper kvConfigSerializeWrapper =
                KVConfigSerializeWrapper.fromJson(content, KVConfigSerializeWrapper.class);
            if (null != kvConfigSerializeWrapper) {
                this.configTable.putAll(kvConfigSerializeWrapper.getConfigTable());
                log.info("load KV config table OK");
            }
        }
    }

    /**
     * 写入或更新指定命名空间下的 KV 配置
     *
     * @param namespace 命名空间
     * @param key       配置键
     * @param value     配置值
     */
    public void putKVConfig(final String namespace, final String key, final String value) {
        try {
            // 获取写锁, 保证并发写安全
            this.lock.writeLock().lockInterruptibly();
            try {
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                if (null == kvTable) {
                    // 不存在命名空间时先创建容器
                    kvTable = new HashMap<>();
                    this.configTable.put(namespace, kvTable);
                    log.info("putKVConfig create new Namespace {}", namespace);
                }

                // 写入新值并记录是新增还是更新
                final String prev = kvTable.put(key, value);
                if (null != prev) {
                    log.info("putKVConfig update config item, Namespace: {} Key: {} Value: {}",
                        namespace, key, value);
                } else {
                    log.info("putKVConfig create new config item, Namespace: {} Key: {} Value: {}",
                        namespace, key, value);
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("putKVConfig InterruptedException", e);
        }

        this.persist();
    }

    /**
     * 将内存中的 KV 配置持久化到磁盘
     */
    public void persist() {
        try {
            // 获取读锁, 确保持久化期间数据视图一致
            this.lock.readLock().lockInterruptibly();
            try {
                KVConfigSerializeWrapper kvConfigSerializeWrapper = new KVConfigSerializeWrapper();
                kvConfigSerializeWrapper.setConfigTable(this.configTable);

                // 序列化内存表为 JSON
                String content = kvConfigSerializeWrapper.toJson();

                if (null != content) {
                    // 将序列化结果写入配置文件
                    MixAll.string2File(content, this.namesrvController.getNamesrvConfig().getKvConfigPath());
                }
            } catch (IOException e) {
                log.error("persist kvconfig Exception, "
                    + this.namesrvController.getNamesrvConfig().getKvConfigPath(), e);
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("persist InterruptedException", e);
        }

    }

    /**
     * 删除指定命名空间下的 KV 配置项
     *
     * @param namespace 命名空间
     * @param key       配置键
     */
    public void deleteKVConfig(final String namespace, final String key) {
        try {
            this.lock.writeLock().lockInterruptibly();
            try {
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                if (null != kvTable) {
                    // 仅在命名空间存在时删除配置项
                    String value = kvTable.remove(key);
                    log.info("deleteKVConfig delete a config item, Namespace: {} Key: {} Value: {}",
                        namespace, key, value);
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("deleteKVConfig InterruptedException", e);
        }

        this.persist();
    }

    /**
     * 获取指定命名空间的 KV 列表并编码为字节数组
     *
     * @param namespace 命名空间
     * @return 编码后的 KV 列表字节数组, 未命中时返回 null
     */
    public byte[] getKVListByNamespace(final String namespace) {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                if (null != kvTable) {
                    KVTable table = new KVTable();
                    table.setTable(kvTable);
                    return table.encode();
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getKVListByNamespace InterruptedException", e);
        }

        return null;
    }

    /**
     * 获取指定命名空间下某个键对应的值
     *
     * @param namespace 命名空间
     * @param key 配置键
     * @return 配置值, 未命中时返回 null
     */
    public String getKVConfig(final String namespace, final String key) {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                if (null != kvTable) {
                    return kvTable.get(key);
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getKVConfig InterruptedException", e);
        }

        return null;
    }

    /**
     * 定时打印所有命名空间及其 KV 配置
     */
    public void printAllPeriodically() {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                log.info("--------------------------------------------------------");

                {
                    // 逐层遍历命名空间和键值对并输出日志
                    log.info("configTable SIZE: {}", this.configTable.size());
                    for (Entry<String, HashMap<String, String>> next : this.configTable.entrySet()) {
                        for (Entry<String, String> nextSub : next.getValue().entrySet()) {
                            log.info("configTable NS: {} Key: {} Value: {}", next.getKey(), nextSub.getKey(),
                                    nextSub.getValue());
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("printAllPeriodically InterruptedException", e);
        }
    }
}
