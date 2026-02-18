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
import org.apache.rocketmq.common.consumer.ReceiptHandle;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 消息回执句柄模型<br>
 * 用于记录消息消费与续期过程中的句柄状态
 */
public class MessageReceiptHandle {
    /**
     * 消费组
     */
    private final String group;
    /**
     * 主题
     */
    private final String topic;
    /**
     * 队列 ID
     */
    private final int queueId;
    /**
     * 消息 ID
     */
    private final String messageId;
    /**
     * 队列偏移量
     */
    private final long queueOffset;
    /**
     * 初始回执句柄字符串
     */
    private final String originalReceiptHandleStr;
    /**
     * 初始回执句柄对象
     */
    private final ReceiptHandle originalReceiptHandle;
    /**
     * 当前重试次数
     */
    private final int reconsumeTimes;

    /**
     * 续期重试次数
     */
    private final AtomicInteger renewRetryTimes = new AtomicInteger(0);
    /**
     * 已续期次数
     */
    private final AtomicInteger renewTimes = new AtomicInteger(0);
    /**
     * 首次消费时间戳
     */
    private final long consumeTimestamp;
    /**
     * 当前可用回执句柄字符串
     */
    private volatile String receiptHandleStr;

    /**
     * 构造消息回执句柄对象
     *
     * @param group 消费组
     * @param topic 主题
     * @param queueId 队列 ID
     * @param receiptHandleStr 回执句柄字符串
     * @param messageId 消息 ID
     * @param queueOffset 队列偏移量
     * @param reconsumeTimes 当前重试次数
     */
    public MessageReceiptHandle(String group, String topic, int queueId, String receiptHandleStr, String messageId,
        long queueOffset, int reconsumeTimes) {
        this.originalReceiptHandle = ReceiptHandle.decode(receiptHandleStr);
        this.group = group;
        this.topic = topic;
        this.queueId = queueId;
        this.receiptHandleStr = receiptHandleStr;
        this.originalReceiptHandleStr = receiptHandleStr;
        this.messageId = messageId;
        this.queueOffset = queueOffset;
        this.reconsumeTimes = reconsumeTimes;
        this.consumeTimestamp = originalReceiptHandle.getRetrieveTime();
    }

    /**
     * 比较两个消息回执句柄对象是否相等
     *
     * @param o 对比对象
     * @return true 表示相等
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MessageReceiptHandle handle = (MessageReceiptHandle) o;
        return queueId == handle.queueId && queueOffset == handle.queueOffset && consumeTimestamp == handle.consumeTimestamp
            && reconsumeTimes == handle.reconsumeTimes
            && Objects.equal(group, handle.group) && Objects.equal(topic, handle.topic)
            && Objects.equal(messageId, handle.messageId) && Objects.equal(originalReceiptHandleStr, handle.originalReceiptHandleStr)
            && Objects.equal(receiptHandleStr, handle.receiptHandleStr);
    }

    /**
     * 计算对象哈希值
     *
     * @return 哈希值
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(group, topic, queueId, messageId, queueOffset, originalReceiptHandleStr, consumeTimestamp,
            reconsumeTimes, receiptHandleStr);
    }

    /**
     * 输出对象可读字符串
     *
     * @return 字符串描述
     */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("group", group)
            .add("topic", topic)
            .add("queueId", queueId)
            .add("messageId", messageId)
            .add("queueOffset", queueOffset)
            .add("originalReceiptHandleStr", originalReceiptHandleStr)
            .add("reconsumeTimes", reconsumeTimes)
            .add("renewRetryTimes", renewRetryTimes)
            .add("firstConsumeTimestamp", consumeTimestamp)
            .add("receiptHandleStr", receiptHandleStr)
            .toString();
    }

    public String getGroup() {
        return group;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public String getReceiptHandleStr() {
        return receiptHandleStr;
    }

    public String getOriginalReceiptHandleStr() {
        return originalReceiptHandleStr;
    }

    public String getMessageId() {
        return messageId;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public int getReconsumeTimes() {
        return reconsumeTimes;
    }

    public long getConsumeTimestamp() {
        return consumeTimestamp;
    }

    /**
     * 更新当前回执句柄
     *
     * @param receiptHandleStr 新回执句柄
     */
    public void updateReceiptHandle(String receiptHandleStr) {
        this.receiptHandleStr = receiptHandleStr;
    }

    /**
     * 递增续期重试次数并返回新值
     *
     * @return 新的续期重试次数
     */
    public int incrementAndGetRenewRetryTimes() {
        return this.renewRetryTimes.incrementAndGet();
    }

    /**
     * 递增续期次数并返回新值
     *
     * @return 新的续期次数
     */
    public int incrementRenewTimes() {
        return this.renewTimes.incrementAndGet();
    }

    public int getRenewTimes() {
        return this.renewTimes.get();
    }

    /**
     * 重置续期重试次数
     */
    public void resetRenewRetryTimes() {
        this.renewRetryTimes.set(0);
    }

    public int getRenewRetryTimes() {
        return this.renewRetryTimes.get();
    }

    public ReceiptHandle getOriginalReceiptHandle() {
        return originalReceiptHandle;
    }
}
