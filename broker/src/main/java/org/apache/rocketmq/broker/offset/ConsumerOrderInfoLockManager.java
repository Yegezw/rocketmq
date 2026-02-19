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
package org.apache.rocketmq.broker.offset;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 顺序消费锁释放通知管理器, 通过时间轮在锁到期时唤醒长轮询请求
 */
public class ConsumerOrderInfoLockManager {
    /**
     * POP 模块日志记录器
     */
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    /**
     * broker 控制器引用, 用于访问配置与长轮询处理器
     */
    private final BrokerController brokerController;
    /**
     * 锁释放通知任务索引, key 为 topic/group/queue 组合
     */
    private final Map<Key, Timeout> timeoutMap = new ConcurrentHashMap<>();
    /**
     * 基于哈希时间轮的延迟任务调度器
     */
    private final Timer timer;
    /**
     * 时间轮 tick 间隔, 单位毫秒
     */
    private static final int TIMER_TICK_MS = 100;

    /**
     * 创建顺序消费锁释放通知管理器
     *
     * @param brokerController broker 控制器实例
     */
    public ConsumerOrderInfoLockManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.timer = new HashedWheelTimer(
            new ThreadFactoryImpl("ConsumerOrderInfoLockManager_"),
            TIMER_TICK_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * when ConsumerOrderInfoManager load from disk, recover data
     * <br>
     * 当 ConsumerOrderInfoManager 从磁盘加载后, 恢复未来时间点的解锁通知任务
     */
    public void recover(Map<String/* topic@group, 主题与消费组组合键 */, ConcurrentHashMap<Integer/* queueId, 队列 ID */, ConsumerOrderInfoManager.OrderInfo>> table) {
        if (!this.brokerController.getBrokerConfig().isEnableNotifyAfterPopOrderLockRelease()) {
            return;
        }
        for (Map.Entry<String, ConcurrentHashMap<Integer, ConsumerOrderInfoManager.OrderInfo>> entry : table.entrySet()) {
            String topicAtGroup = entry.getKey();
            ConcurrentHashMap<Integer/* queueId, 队列 ID */, ConsumerOrderInfoManager.OrderInfo> qs = entry.getValue();
            String[] arrays = ConsumerOrderInfoManager.decodeKey(topicAtGroup);
            if (arrays.length != 2) {
                continue;
            }
            String topic = arrays[0];
            String group = arrays[1];
            for (Map.Entry<Integer, ConsumerOrderInfoManager.OrderInfo> qsEntry : qs.entrySet()) {
                Long lockFreeTimestamp = qsEntry.getValue().getLockFreeTimestamp();
                if (lockFreeTimestamp == null || lockFreeTimestamp <= System.currentTimeMillis()) {
                    continue;
                }
                this.updateLockFreeTimestamp(topic, group, qsEntry.getKey(), lockFreeTimestamp);
            }
        }
    }

    /**
     * 根据 OrderInfo 计算锁可释放时间, 并刷新对应通知任务
     *
     * @param topic 主题名
     * @param group 消费组名
     * @param queueId 队列 ID
     * @param orderInfo 顺序消费状态
     */
    public void updateLockFreeTimestamp(String topic, String group, int queueId, ConsumerOrderInfoManager.OrderInfo orderInfo) {
        this.updateLockFreeTimestamp(topic, group, queueId, orderInfo.getLockFreeTimestamp());
    }

    /**
     * 按锁可释放时间刷新通知任务, 如存在旧任务则先取消
     *
     * @param topic 主题名
     * @param group 消费组名
     * @param queueId 队列 ID
     * @param lockFreeTimestamp 锁可释放时间戳
     */
    public void updateLockFreeTimestamp(String topic, String group, int queueId, Long lockFreeTimestamp) {
        if (!this.brokerController.getBrokerConfig().isEnableNotifyAfterPopOrderLockRelease()) {
            return;
        }
        if (lockFreeTimestamp == null) {
            return;
        }
        try {
            this.timeoutMap.compute(new Key(topic, group, queueId), (key, oldTimeout) -> {
                try {
                    long delay = lockFreeTimestamp - System.currentTimeMillis();
                    Timeout newTimeout = this.timer.newTimeout(new NotifyLockFreeTimerTask(key), delay, TimeUnit.MILLISECONDS);
                    if (oldTimeout != null) {
                        // 取消旧的 timerTask, 避免重复通知
                        // cancel prev timerTask
                        oldTimeout.cancel();
                    }
                    return newTimeout;
                } catch (Exception e) {
                    POP_LOGGER.warn("add timeout task failed. key:{}, lockFreeTimestamp:{}", key, lockFreeTimestamp, e);
                    return oldTimeout;
                }
            });
        } catch (Exception e) {
            POP_LOGGER.error("unexpect error when updateLockFreeTimestamp. topic:{}, group:{}, queueId:{}, lockFreeTimestamp:{}",
                topic, group, queueId, lockFreeTimestamp, e);
        }
    }

    /**
     * 通知 POP 长轮询流程当前队列锁已可释放
     *
     * @param key topic/group/queue 组合键
     */
    protected void notifyLockIsFree(Key key) {
        try {
            this.brokerController.getPopMessageProcessor().notifyLongPollingRequestIfNeed(key.topic, key.group, key.queueId);
        } catch (Exception e) {
            POP_LOGGER.error("unexpect error when notifyLockIsFree. key:{}", key, e);
        }
    }

    /**
     * 关闭时间轮调度器并停止后续通知任务
     */
    public void shutdown() {
        this.timer.stop();
    }

    @VisibleForTesting
    protected Map<Key, Timeout> getTimeoutMap() {
        return timeoutMap;
    }

    /**
     * 锁释放通知任务, 到期后触发长轮询唤醒
     */
    private class NotifyLockFreeTimerTask implements TimerTask {

        /**
         * 任务关联的 topic/group/queue 键
         */
        private final Key key;

        /**
         * 创建锁释放通知任务
         *
         * @param key topic/group/queue 组合键
         */
        private NotifyLockFreeTimerTask(Key key) {
            this.key = key;
        }

        /**
         * 定时任务回调, 若任务仍有效则执行锁释放通知
         *
         * @param timeout 当前触发的超时任务句柄
         * @throws Exception 回调执行异常
         */
        @Override
        public void run(Timeout timeout) throws Exception {
            if (timeout.isCancelled() || !brokerController.getBrokerConfig().isEnableNotifyAfterPopOrderLockRelease()) {
                return;
            }
            notifyLockIsFree(key);
            timeoutMap.computeIfPresent(key, (key1, curTimeout) -> {
                if (curTimeout == timeout) {
                    // 当前 timeout 仍为最新任务时才移除映射
                    // remove from map
                    return null;
                }
                return curTimeout;
            });
        }
    }

    /**
     * topic/group/queue 唯一键, 用于定位锁释放通知任务
     */
    private static class Key {
        /**
         * 主题名
         */
        private final String topic;
        /**
         * 消费组名
         */
        private final String group;
        /**
         * 队列 ID
         */
        private final int queueId;

        /**
         * 创建组合键实例
         *
         * @param topic 主题名
         * @param group 消费组名
         * @param queueId 队列 ID
         */
        public Key(String topic, String group, int queueId) {
            this.topic = topic;
            this.group = group;
            this.queueId = queueId;
        }

        /**
         * 比较两个键是否表示同一 topic/group/queue
         *
         * @param o 待比较对象
         * @return true 表示完全相同
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key key = (Key) o;
            return queueId == key.queueId && Objects.equal(topic, key.topic) && Objects.equal(group, key.group);
        }

        /**
         * 生成键的哈希值, 与 equals 保持一致
         *
         * @return 哈希值
         */
        @Override
        public int hashCode() {
            return Objects.hashCode(topic, group, queueId);
        }

        /**
         * 输出便于日志排障的键描述
         *
         * @return 字符串化后的键信息
         */
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("topic", topic)
                .add("group", group)
                .add("queueId", queueId)
                .toString();
        }
    }
}
