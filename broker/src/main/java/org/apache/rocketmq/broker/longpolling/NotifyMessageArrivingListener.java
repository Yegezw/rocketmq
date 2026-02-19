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

package org.apache.rocketmq.broker.longpolling;

import java.util.Map;
import org.apache.rocketmq.broker.processor.NotificationProcessor;
import org.apache.rocketmq.broker.processor.PopMessageProcessor;
import org.apache.rocketmq.store.MessageArrivingListener;

/**
 * 消息到达监听器, 将到达事件广播给 Pull, POP 与 Notification 处理链路
 */
public class NotifyMessageArrivingListener implements MessageArrivingListener {
    /**
     * Pull 长轮询挂起服务, 用于唤醒 pull 请求
     */
    private final PullRequestHoldService pullRequestHoldService;
    /**
     * POP 消息处理器, 用于唤醒 POP 长轮询请求
     */
    private final PopMessageProcessor popMessageProcessor;
    /**
     * Notification 请求处理器, 用于唤醒通知型长轮询请求
     */
    private final NotificationProcessor notificationProcessor;

    /**
     * 构建统一消息到达监听器
     *
     * @param pullRequestHoldService Pull 请求挂起服务
     * @param popMessageProcessor POP 消息处理器
     * @param notificationProcessor Notification 请求处理器
     */
    public NotifyMessageArrivingListener(final PullRequestHoldService pullRequestHoldService, final PopMessageProcessor popMessageProcessor, final NotificationProcessor notificationProcessor) {
        this.pullRequestHoldService = pullRequestHoldService;
        this.popMessageProcessor = popMessageProcessor;
        this.notificationProcessor = notificationProcessor;
    }

    /**
     * 在消息写入存储后触发, 依次通知各类长轮询服务尝试唤醒请求
     *
     * @param topic 消息所属主题
     * @param queueId 消息所属队列
     * @param logicOffset 消费队列逻辑偏移量
     * @param tagsCode 标签哈希值
     * @param msgStoreTime 消息存储时间戳
     * @param filterBitMap 过滤位图
     * @param properties 消息属性集合
     */
    @Override
    public void arriving(String topic, int queueId, long logicOffset, long tagsCode,
                         long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {

        this.pullRequestHoldService.notifyMessageArriving(
            topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties);
        this.popMessageProcessor.notifyMessageArriving(
            topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties);
        this.notificationProcessor.notifyMessageArriving(
            topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties);
    }
}
