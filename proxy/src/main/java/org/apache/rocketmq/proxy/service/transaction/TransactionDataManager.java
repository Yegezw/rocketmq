/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.service.transaction;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.proxy.config.ConfigurationManager;

/**
 * 事务数据管理器, 负责缓存维护与过期清理
 */
public class TransactionDataManager implements StartAndShutdown {
    /**
     * Proxy 日志记录器
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    /**
     * 当前缓存中事务数据的最大过期时间
     */
    protected final AtomicLong maxTransactionDataExpireTime = new AtomicLong(System.currentTimeMillis());
    /**
     * 事务数据缓存, key 由 producerGroup 与 transactionId 组成
     */
    protected final Map<String /* producerGroup@transactionId */, NavigableSet<TransactionData>> transactionIdDataMap = new ConcurrentHashMap<>();
    /**
     * 事务数据清理线程
     */
    protected final TransactionDataCleaner transactionDataCleaner = new TransactionDataCleaner();

    /**
     * 构造事务数据缓存 key
     *
     * @param producerGroup 生产者组
     * @param transactionId 事务标识
     * @return 组合 key
     */
    protected String buildKey(String producerGroup, String transactionId) {
        return producerGroup + "@" + transactionId;
    }

    /**
     * 添加事务数据到缓存
     *
     * @param producerGroup 生产者组
     * @param transactionId 事务标识
     * @param transactionData 事务数据
     */
    public void addTransactionData(String producerGroup, String transactionId, TransactionData transactionData) {
        this.transactionIdDataMap.compute(buildKey(producerGroup, transactionId), (key, dataSet) -> {
            if (dataSet == null) {
                dataSet = new ConcurrentSkipListSet<>();
            }
            dataSet.add(transactionData);
            if (dataSet.size() > ConfigurationManager.getProxyConfig().getTransactionDataMaxNum()) {
                dataSet.pollFirst();
            }
            return dataSet;
        });
    }

    /**
     * 获取未过期的最新事务数据并从缓存移除
     *
     * @param producerGroup 生产者组
     * @param transactionId 事务标识
     * @return 未过期事务数据, 不存在时返回 null
     */
    public TransactionData pollNoExpireTransactionData(String producerGroup, String transactionId) {
        AtomicReference<TransactionData> res = new AtomicReference<>();
        long currTimestamp = System.currentTimeMillis();
        this.transactionIdDataMap.computeIfPresent(buildKey(producerGroup, transactionId), (key, dataSet) -> {
            TransactionData data = dataSet.pollLast();
            while (data != null && data.getExpireTime() < currTimestamp) {
                data = dataSet.pollLast();
            }
            if (data != null) {
                res.set(data);
            }
            if (dataSet.isEmpty()) {
                return null;
            }
            return dataSet;
        });
        return res.get();
    }

    /**
     * 从缓存移除指定事务数据
     *
     * @param producerGroup 生产者组
     * @param transactionId 事务标识
     * @param transactionData 事务数据
     */
    public void removeTransactionData(String producerGroup, String transactionId, TransactionData transactionData) {
        this.transactionIdDataMap.computeIfPresent(buildKey(producerGroup, transactionId), (key, dataSet) -> {
            dataSet.remove(transactionData);
            if (dataSet.isEmpty()) {
                return null;
            }
            return dataSet;
        });
    }

    /**
     * 清理全部过期事务数据并刷新最大过期时间
     */
    protected void cleanExpireTransactionData() {
        long currTimestamp = System.currentTimeMillis();
        Set<String> transactionIdSet = this.transactionIdDataMap.keySet();
        for (String transactionId : transactionIdSet) {
            this.transactionIdDataMap.computeIfPresent(transactionId, (transactionIdKey, dataSet) -> {
                Iterator<TransactionData> iterator = dataSet.iterator();
                while (iterator.hasNext()) {
                    try {
                        TransactionData data = iterator.next();
                        if (data.getExpireTime() < currTimestamp) {
                            iterator.remove();
                        } else {
                            break;
                        }
                    } catch (NoSuchElementException ignore) {
                        break;
                    }
                }
                if (dataSet.isEmpty()) {
                    return null;
                }
                try {
                    TransactionData maxData = dataSet.last();
                    maxTransactionDataExpireTime.set(Math.max(maxTransactionDataExpireTime.get(), maxData.getExpireTime()));
                } catch (NoSuchElementException ignore) {
                }
                return dataSet;
            });
        }
    }

    /**
     * 事务数据后台清理线程
     */
    protected class TransactionDataCleaner extends ServiceThread {

        @Override
        public String getServiceName() {
            return "TransactionDataCleaner";
        }

        /**
         * 线程主循环, 按配置周期触发过期清理
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                this.waitForRunning(ConfigurationManager.getProxyConfig().getTransactionDataExpireScanPeriodMillis());
            }
            log.info(this.getServiceName() + " service stopped");
        }

        /**
         * 每次等待结束后执行清理逻辑
         */
        @Override
        protected void onWaitEnd() {
            cleanExpireTransactionData();
        }
    }

    /**
     * 等待事务数据清理完成, 最长等待配置上限
     *
     * @throws InterruptedException 线程中断异常
     */
    protected void waitTransactionDataClear() throws InterruptedException {
        this.cleanExpireTransactionData();
        long waitMs = Math.max(this.maxTransactionDataExpireTime.get() - System.currentTimeMillis(), 0);
        waitMs = Math.min(waitMs, ConfigurationManager.getProxyConfig().getTransactionDataMaxWaitClearMillis());

        if (waitMs > 0) {
            TimeUnit.MILLISECONDS.sleep(waitMs);
        }
    }

    /**
     * 停止清理线程并等待缓存数据清空
     */
    @Override
    public void shutdown() throws Exception {
        this.transactionDataCleaner.shutdown();
        this.waitTransactionDataClear();
    }

    /**
     * 启动事务数据清理线程
     */
    @Override
    public void start() throws Exception {
        this.transactionDataCleaner.start();
    }
}
