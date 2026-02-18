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

package org.apache.rocketmq.proxy.common;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * 回执句柄分组容器<br>
 * 按消息维度维护回执句柄并提供并发安全读写能力
 */
public class ReceiptHandleGroup {
    /**
     * Proxy 日志记录器
     */
    protected final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    // The messages having the same messageId will be deduplicated based on the parameters of broker, queueId, and offset
    // 相同 messageId 的消息按 broker queueId offset 维度去重
    /**
     * 消息 ID 到句柄映射的存储结构
     */
    protected final Map<String /* msgID */, Map<HandleKey, HandleData>> receiptHandleMap = new ConcurrentHashMap<>();

    /**
     * 句柄键<br>
     * 用于唯一标识同一消息下的回执句柄
     */
    public static class HandleKey {
        /**
         * 初始句柄字符串
         */
        private final String originalHandle;
        /**
         * broker 名称
         */
        private final String broker;
        /**
         * 队列 ID
         */
        private final int queueId;
        /**
         * 消息偏移量
         */
        private final long offset;

        /**
         * 根据句柄字符串构造键
         *
         * @param handle 句柄字符串
         */
        public HandleKey(String handle) {
            this(ReceiptHandle.decode(handle));
        }

        /**
         * 根据句柄对象构造键
         *
         * @param receiptHandle 句柄对象
         */
        public HandleKey(ReceiptHandle receiptHandle) {
            this.originalHandle = receiptHandle.getReceiptHandle();
            this.broker = receiptHandle.getBrokerName();
            this.queueId = receiptHandle.getQueueId();
            this.offset = receiptHandle.getOffset();
        }

        /**
         * 比较两个键对象是否相等
         *
         * @param o 对比对象
         * @return true 表示相等
         */
        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            HandleKey key = (HandleKey) o;
            return queueId == key.queueId && offset == key.offset && Objects.equal(broker, key.broker);
        }

        /**
         * 计算键对象哈希值
         *
         * @return 哈希值
         */
        @Override
        public int hashCode() {
            return Objects.hashCode(broker, queueId, offset);
        }

        /**
         * 输出键对象可读字符串
         *
         * @return 字符串描述
         */
        @Override
        public String toString() {
            return new ToStringBuilder(this)
                .append("originalHandle", originalHandle)
                .append("broker", broker)
                .append("queueId", queueId)
                .append("offset", offset)
                .toString();
        }

        public String getOriginalHandle() {
            return originalHandle;
        }

        public String getBroker() {
            return broker;
        }

        public int getQueueId() {
            return queueId;
        }

        public long getOffset() {
            return offset;
        }
    }

    /**
     * 句柄数据<br>
     * 包含句柄实体与并发控制状态
     */
    public static class HandleData {
        /**
         * 并发互斥信号量
         */
        private final Semaphore semaphore = new Semaphore(1);
        /**
         * 最近一次加锁时间戳
         */
        private final AtomicLong lastLockTimeMs = new AtomicLong(-1L);
        /**
         * 是否需要删除
         */
        private volatile boolean needRemove = false;
        /**
         * 当前回执句柄对象
         */
        private volatile MessageReceiptHandle messageReceiptHandle;

        /**
         * 构造句柄数据对象
         *
         * @param messageReceiptHandle 回执句柄对象
         */
        public HandleData(MessageReceiptHandle messageReceiptHandle) {
            this.messageReceiptHandle = messageReceiptHandle;
        }

        /**
         * 尝试加锁并返回加锁时间
         *
         * @param timeoutMs 获取锁超时时间 毫秒
         * @return 加锁成功时间戳, 返回 null 表示失败
         */
        public Long lock(long timeoutMs) {
            try {
                boolean result = this.semaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);
                long currentTimeMs = System.currentTimeMillis();
                if (result) {
                    this.lastLockTimeMs.set(currentTimeMs);
                    return currentTimeMs;
                } else {
                    // if the lock is expired, can be acquired again
                    // 若锁已过期, 可重新获取
                    long expiredTimeMs = ConfigurationManager.getProxyConfig().getLockTimeoutMsInHandleGroup() * 3;
                    if (currentTimeMs - this.lastLockTimeMs.get() > expiredTimeMs) {
                        synchronized (this) {
                            if (currentTimeMs - this.lastLockTimeMs.get() > expiredTimeMs) {
                                log.warn("HandleData lock expired, acquire lock success and reset lock time. " +
                                    "MessageReceiptHandle={}, lockTime={}", messageReceiptHandle, currentTimeMs);
                                this.lastLockTimeMs.set(currentTimeMs);
                                return currentTimeMs;
                            }
                        }
                    }
                }
                return null;
            } catch (InterruptedException e) {
                return null;
            }
        }

        /**
         * 释放锁
         *
         * @param lockTimeMs 加锁时间戳
         */
        public void unlock(long lockTimeMs) {
            // if the lock is expired, we don't need to unlock it
            // 若锁已过期, 无需释放
            if (System.currentTimeMillis() - lockTimeMs > ConfigurationManager.getProxyConfig().getLockTimeoutMsInHandleGroup() * 2) {
                log.warn("HandleData lock expired, unlock fail. MessageReceiptHandle={}, lockTime={}, now={}",
                    messageReceiptHandle, lockTimeMs, System.currentTimeMillis());
                return;
            }
            this.semaphore.release();
        }

        public MessageReceiptHandle getMessageReceiptHandle() {
            return messageReceiptHandle;
        }

        /**
         * 判断是否为同一对象实例
         *
         * @param o 对比对象
         * @return true 表示同一实例
         */
        @Override
        public boolean equals(Object o) {
            return this == o;
        }

        /**
         * 计算对象哈希值
         *
         * @return 哈希值
         */
        @Override
        public int hashCode() {
            return Objects.hashCode(semaphore, needRemove, messageReceiptHandle);
        }

        /**
         * 输出对象可读字符串
         *
         * @return 字符串描述
         */
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("semaphore", semaphore)
                .add("needRemove", needRemove)
                .add("messageReceiptHandle", messageReceiptHandle)
                .toString();
        }
    }

    /**
     * 写入或更新消息回执句柄
     *
     * @param msgID 消息 ID
     * @param value 回执句柄对象
     */
    public void put(String msgID, MessageReceiptHandle value) {
        long timeout = ConfigurationManager.getProxyConfig().getLockTimeoutMsInHandleGroup();
        Map<HandleKey, HandleData> handleMap = ConcurrentHashMapUtils.computeIfAbsent((ConcurrentHashMap<String, Map<HandleKey, HandleData>>) this.receiptHandleMap,
            msgID, msgIDKey -> new ConcurrentHashMap<>());
        handleMap.compute(new HandleKey(value.getOriginalReceiptHandle()), (handleKey, handleData) -> {
            if (handleData == null || handleData.needRemove) {
                return new HandleData(value);
            }
            Long lockTimeMs = handleData.lock(timeout);
            if (lockTimeMs == null) {
                throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, "try to put handle failed");
            }
            try {
                if (handleData.needRemove) {
                    return new HandleData(value);
                }
                handleData.messageReceiptHandle = value;
            } finally {
                handleData.unlock(lockTimeMs);
            }
            return handleData;
        });
    }

    public boolean isEmpty() {
        return this.receiptHandleMap.isEmpty();
    }

    public MessageReceiptHandle get(String msgID, String handle) {
        Map<HandleKey, HandleData> handleMap = this.receiptHandleMap.get(msgID);
        if (handleMap == null) {
            return null;
        }
        long timeout = ConfigurationManager.getProxyConfig().getLockTimeoutMsInHandleGroup();
        AtomicReference<MessageReceiptHandle> res = new AtomicReference<>();
        handleMap.computeIfPresent(new HandleKey(handle), (handleKey, handleData) -> {
            Long lockTimeMs = handleData.lock(timeout);
            if (lockTimeMs == null) {
                throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, "try to get handle failed");
            }
            try {
                if (handleData.needRemove) {
                    return null;
                }
                res.set(handleData.messageReceiptHandle);
            } finally {
                handleData.unlock(lockTimeMs);
            }
            return handleData;
        });
        return res.get();
    }

    /**
     * 删除并返回指定回执句柄
     *
     * @param msgID 消息 ID
     * @param handle 回执句柄字符串
     * @return 被删除的回执句柄, 不存在时返回 null
     */
    public MessageReceiptHandle remove(String msgID, String handle) {
        Map<HandleKey, HandleData> handleMap = this.receiptHandleMap.get(msgID);
        if (handleMap == null) {
            return null;
        }
        long timeout = ConfigurationManager.getProxyConfig().getLockTimeoutMsInHandleGroup();
        AtomicReference<MessageReceiptHandle> res = new AtomicReference<>();
        handleMap.computeIfPresent(new HandleKey(handle), (handleKey, handleData) -> {
            Long lockTimeMs = handleData.lock(timeout);
            if (lockTimeMs == null) {
                throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, "try to remove and get handle failed");
            }
            try {
                if (!handleData.needRemove) {
                    handleData.needRemove = true;
                    res.set(handleData.messageReceiptHandle);
                }
                return null;
            } finally {
                handleData.unlock(lockTimeMs);
            }
        });
        removeHandleMapKeyIfNeed(msgID);
        return res.get();
    }

    /**
     * 删除并返回某消息下任意一个回执句柄
     *
     * @param msgID 消息 ID
     * @return 被删除的回执句柄, 不存在时返回 null
     */
    public MessageReceiptHandle removeOne(String msgID) {
        Map<HandleKey, HandleData> handleMap = this.receiptHandleMap.get(msgID);
        if (handleMap == null) {
            return null;
        }
        Set<HandleKey> keys = handleMap.keySet();
        for (HandleKey key : keys) {
            MessageReceiptHandle res = this.remove(msgID, key.originalHandle);
            if (res != null) {
                return res;
            }
        }
        return null;
    }

    /**
     * 对指定回执句柄执行异步更新
     *
     * @param msgID 消息 ID
     * @param handle 回执句柄字符串
     * @param function 异步更新函数
     */
    public void computeIfPresent(String msgID, String handle,
        Function<MessageReceiptHandle, CompletableFuture<MessageReceiptHandle>> function) {
        Map<HandleKey, HandleData> handleMap = this.receiptHandleMap.get(msgID);
        if (handleMap == null) {
            return;
        }
        long timeout = ConfigurationManager.getProxyConfig().getLockTimeoutMsInHandleGroup();
        handleMap.computeIfPresent(new HandleKey(handle), (handleKey, handleData) -> {
            Long lockTimeMs = handleData.lock(timeout);
            if (lockTimeMs == null) {
                throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, "try to compute failed");
            }
            CompletableFuture<MessageReceiptHandle> future = function.apply(handleData.messageReceiptHandle);
            future.whenComplete((messageReceiptHandle, throwable) -> {
                try {
                    if (throwable != null) {
                        return;
                    }
                    if (messageReceiptHandle == null) {
                        handleData.needRemove = true;
                    } else {
                        handleData.messageReceiptHandle = messageReceiptHandle;
                    }
                } finally {
                    handleData.unlock(lockTimeMs);
                }
                if (handleData.needRemove) {
                    handleMap.remove(handleKey, handleData);
                }
                removeHandleMapKeyIfNeed(msgID);
            });
            return handleData;
        });
    }

    /**
     * 当消息下无句柄时清理消息键
     *
     * @param msgID 消息 ID
     */
    protected void removeHandleMapKeyIfNeed(String msgID) {
        this.receiptHandleMap.computeIfPresent(msgID, (msgIDKey, handleMap) -> {
            if (handleMap.isEmpty()) {
                return null;
            }
            return handleMap;
        });
    }

    /**
     * 数据扫描回调接口
     */
    public interface DataScanner {
        /**
         * 处理扫描到的数据
         *
         * @param msgID 消息 ID
         * @param handle 回执句柄字符串
         * @param receiptHandle 回执句柄对象
         */
        void onData(String msgID, String handle, MessageReceiptHandle receiptHandle);
    }

    /**
     * 扫描全部回执句柄数据
     *
     * @param scanner 扫描回调
     */
    public void scan(DataScanner scanner) {
        this.receiptHandleMap.forEach((msgID, handleMap) -> {
            handleMap.forEach((handleKey, v) -> {
                scanner.onData(msgID, handleKey.originalHandle, v.messageReceiptHandle);
            });
        });
    }

    /**
     * 输出分组对象可读字符串
     *
     * @return 字符串描述
     */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("receiptHandleMap", receiptHandleMap)
            .toString();
    }
}
