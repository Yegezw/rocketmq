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

import org.apache.rocketmq.remoting.protocol.header.NotificationRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;

/**
 * 统一封装 POP 与 Notification 长轮询请求头, 便于复用轮询流程
 */
public class PollingHeader {
    /**
     * 消费组标识, 用于隔离不同订阅者的轮询队列
     */
    private final String consumerGroup;
    /**
     * 轮询目标主题
     */
    private final String topic;
    /**
     * 轮询目标队列编号, -1 表示全队列广播键
     */
    private final int queueId;
    /**
     * 客户端请求创建时间戳
     */
    private final long bornTime;
    /**
     * 客户端允许的轮询时长, 单位毫秒
     */
    private final long pollTime;

    /**
     * 从 POP 请求头构造统一轮询头
     *
     * @param requestHeader POP 请求头
     */
    public PollingHeader(PopMessageRequestHeader requestHeader) {
        this.consumerGroup = requestHeader.getConsumerGroup();
        this.topic = requestHeader.getTopic();
        this.queueId = requestHeader.getQueueId();
        this.bornTime = requestHeader.getBornTime();
        this.pollTime = requestHeader.getPollTime();
    }

    /**
     * 从 Notification 请求头构造统一轮询头
     *
     * @param requestHeader Notification 请求头
     */
    public PollingHeader(NotificationRequestHeader requestHeader) {
        this.consumerGroup = requestHeader.getConsumerGroup();
        this.topic = requestHeader.getTopic();
        this.queueId = requestHeader.getQueueId();
        this.bornTime = requestHeader.getBornTime();
        this.pollTime = requestHeader.getPollTime();
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getBornTime() {
        return bornTime;
    }

    public long getPollTime() {
        return pollTime;
    }
}
