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
package org.apache.rocketmq.broker.processor;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.pop.PopCheckPoint;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * POP 在途消息计数器<br>
 * 维护 topic + group + queue 维度的未确认消息数量
 */
public class PopInflightMessageCounter {
    /**
     * Broker 日志记录器
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    /**
     * topic 与 group 复合键分隔符
     */
    private static final String TOPIC_GROUP_SEPARATOR = "@";
    /**
     * 在途消息计数表<br>
     * key: topic@group, value: queueId 到计数器映射
     */
    private final Map<String /* topic@group: 主题与消费组复合键 */, Map<Integer /* queueId: 队列 ID */, AtomicLong>> topicInFlightMessageNum =
        new ConcurrentHashMap<>(512);
    /**
     * Broker 控制器引用, 用于获取启动保护时间等上下文信息
     */
    private final BrokerController brokerController;

    /**
     * 构造在途消息计数器
     *
     * @param brokerController Broker 控制器
     */
    public PopInflightMessageCounter(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 增加在途消息数量
     *
     * @param topic 主题名
     * @param group 消费组
     * @param queueId 队列 ID
     * @param num 增量值, 小于等于 0 时忽略
     */
    public void incrementInFlightMessageNum(String topic, String group, int queueId, int num) {
        if (num <= 0) {
            return;
        }
        topicInFlightMessageNum.compute(buildKey(topic, group), (key, queueNum) -> {
            if (queueNum == null) {
                queueNum = new ConcurrentHashMap<>(8);
            }
            queueNum.compute(queueId, (queueIdKey, counter) -> {
                if (counter == null) {
                    return new AtomicLong(num);
                }
                if (counter.addAndGet(num) <= 0) {
                    return null;
                }
                return counter;
            });
            return queueNum;
        });
    }

    /**
     * 减少在途消息数量<br>
     * 启动保护时间之前产生的记录会被忽略, 避免冷启动期间误减计数
     *
     * @param topic 主题名
     * @param group 消费组
     * @param popTime POP 时间
     * @param qId 队列 ID
     * @param delta 减量值
     */
    public void decrementInFlightMessageNum(String topic, String group, long popTime, int qId, int delta) {
        if (popTime < this.brokerController.getShouldStartTime()) {
            return;
        }
        decrementInFlightMessageNum(topic, group, qId, delta);
    }

    /**
     * 根据检查点减少在途消息数量
     *
     * @param checkPoint POP 检查点
     */
    public void decrementInFlightMessageNum(PopCheckPoint checkPoint) {
        if (checkPoint.getPopTime() < this.brokerController.getShouldStartTime()) {
            return;
        }
        decrementInFlightMessageNum(checkPoint.getTopic(), checkPoint.getCId(), checkPoint.getQueueId(), 1);
    }

    /**
     * 在指定 topic + group + queue 维度减少计数
     *
     * @param topic 主题名
     * @param group 消费组
     * @param queueId 队列 ID
     * @param delta 减量值
     */
    private void decrementInFlightMessageNum(String topic, String group, int queueId, int delta) {
        topicInFlightMessageNum.computeIfPresent(buildKey(topic, group), (key, queueNum) -> {
            queueNum.computeIfPresent(queueId, (queueIdKey, counter) -> {
                if (counter.addAndGet(-delta) <= 0) {
                    return null;
                }
                return counter;
            });
            if (queueNum.isEmpty()) {
                return null;
            }
            return queueNum;
        });
    }

    /**
     * 按消费组清理全部在途计数
     *
     * @param group 消费组
     */
    public void clearInFlightMessageNumByGroupName(String group) {
        Set<String> topicGroupKey = this.topicInFlightMessageNum.keySet();
        for (String key : topicGroupKey) {
            if (key.contains(group)) {
                Pair<String, String> topicAndGroup = splitKey(key);
                if (topicAndGroup != null && topicAndGroup.getObject2().equals(group)) {
                    this.topicInFlightMessageNum.remove(key);
                    log.info("PopInflightMessageCounter#clearInFlightMessageNumByGroupName: clean by group, topic={}, group={}",
                        topicAndGroup.getObject1(), topicAndGroup.getObject2());
                }
            }
        }
    }

    /**
     * 按主题清理全部在途计数
     *
     * @param topic 主题名
     */
    public void clearInFlightMessageNumByTopicName(String topic) {
        Set<String> topicGroupKey = this.topicInFlightMessageNum.keySet();
        for (String key : topicGroupKey) {
            if (key.contains(topic)) {
                Pair<String, String> topicAndGroup = splitKey(key);
                if (topicAndGroup != null && topicAndGroup.getObject1().equals(topic)) {
                    this.topicInFlightMessageNum.remove(key);
                    log.info("PopInflightMessageCounter#clearInFlightMessageNumByTopicName: clean by topic, topic={}, group={}",
                        topicAndGroup.getObject1(), topicAndGroup.getObject2());
                }
            }
        }
    }

    /**
     * 清理指定队列在途计数
     *
     * @param topic 主题名
     * @param group 消费组
     * @param queueId 队列 ID
     */
    public void clearInFlightMessageNum(String topic, String group, int queueId) {
        topicInFlightMessageNum.computeIfPresent(buildKey(topic, group), (key, queueNum) -> {
            queueNum.computeIfPresent(queueId, (queueIdKey, counter) -> null);
            if (queueNum.isEmpty()) {
                return null;
            }
            return queueNum;
        });
    }

    /**
     * 查询指定队列在途消息数量
     *
     * @param topic 主题名
     * @param group 消费组
     * @param queueId 队列 ID
     * @return 在途消息数量, 不小于 0
     */
    public long getGroupPopInFlightMessageNum(String topic, String group, int queueId) {
        Map<Integer /* queueId: 队列 ID */, AtomicLong> queueCounter = topicInFlightMessageNum.get(buildKey(topic, group));
        if (queueCounter == null) {
            return 0;
        }
        AtomicLong counter = queueCounter.get(queueId);
        if (counter == null) {
            return 0;
        }
        return Math.max(0, counter.get());
    }

    /**
     * 解析复合键为主题与消费组
     *
     * @param key topic@group 复合键
     * @return 主题与消费组二元组, 解析失败返回 null
     */
    private static Pair<String /* topic: 主题名 */, String /* group: 消费组 */> splitKey(String key) {
        String[] strings = key.split(TOPIC_GROUP_SEPARATOR);
        if (strings.length != 2) {
            return null;
        }
        return new Pair<>(strings[0], strings[1]);
    }

    /**
     * 构建 topic 与 group 的复合键
     *
     * @param topic 主题名
     * @param group 消费组
     * @return topic@group 形式复合键
     */
    private static String buildKey(String topic, String group) {
        return topic + TOPIC_GROUP_SEPARATOR + group;
    }
}
