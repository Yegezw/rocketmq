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

import com.google.common.base.Objects;

public class TopicQueueId {
    /**
     * 主题名称
     */
    private final String topic;
    /**
     * 队列 ID
     */
    private final int queueId;

    /**
     * 预计算哈希值
     */
    private final int hash;

    /**
     * 创建主题队列标识对象并预计算哈希值
     *
     * @param topic 主题名称
     * @param queueId 队列 ID
     */
    public TopicQueueId(String topic, int queueId) {
        this.topic = topic;
        this.queueId = queueId;

        this.hash = Objects.hashCode(topic, queueId);
    }

    /**
     * 判断两个主题队列标识是否相等
     *
     * @param o 待比较对象
     * @return 主题名称与队列 ID 均相等时返回 true
     */
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TopicQueueId broker = (TopicQueueId) o;
        return queueId == broker.queueId && Objects.equal(topic, broker.topic);
    }

    /**
     * 返回预计算哈希值, 避免重复计算
     *
     * @return 对象哈希值
     */
    @Override
    public int hashCode() {
        return hash;
    }

    /**
     * 输出可读字符串表示, 用于日志与调试定位
     *
     * @return 主题队列字符串表示
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MessageQueueInBroker{");
        sb.append("topic='").append(topic).append('\'');
        sb.append(", queueId=").append(queueId);
        sb.append('}');
        return sb.toString();
    }
}
