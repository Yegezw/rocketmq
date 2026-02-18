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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

/**
 * 事务回查所需的事务元数据
 */
public class TransactionData implements Comparable<TransactionData> {
    /**
     * 事务所属 Broker 名称
     */
    private final String brokerName;
    /**
     * 事务消息主题
     */
    private final String topic;
    /**
     * 事务状态表偏移量
     */
    private final long tranStateTableOffset;
    /**
     * CommitLog 偏移量
     */
    private final long commitLogOffset;
    /**
     * 事务标识
     */
    private final String transactionId;
    /**
     * 记录时间戳
     */
    private final long checkTimestamp;
    /**
     * 事务数据有效期, 单位毫秒
     */
    private final long expireMs;

    /**
     * 初始化事务数据
     *
     * @param brokerName Broker 名称
     * @param topic 主题
     * @param tranStateTableOffset 事务状态表偏移量
     * @param commitLogOffset CommitLog 偏移量
     * @param transactionId 事务标识
     * @param checkTimestamp 记录时间戳
     * @param expireMs 有效期毫秒值
     */
    public TransactionData(String brokerName, String topic, long tranStateTableOffset, long commitLogOffset, String transactionId,
        long checkTimestamp, long expireMs) {
        this.brokerName = brokerName;
        this.topic = topic;
        this.tranStateTableOffset = tranStateTableOffset;
        this.commitLogOffset = commitLogOffset;
        this.transactionId = transactionId;
        this.checkTimestamp = checkTimestamp;
        this.expireMs = expireMs;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public String getTopic() {
        return topic;
    }

    public long getTranStateTableOffset() {
        return tranStateTableOffset;
    }

    public long getCommitLogOffset() {
        return commitLogOffset;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public long getCheckTimestamp() {
        return checkTimestamp;
    }

    public long getExpireMs() {
        return expireMs;
    }

    public long getExpireTime() {
        return checkTimestamp + expireMs;
    }

    /**
     * 基于关键事务字段判断对象相等
     *
     * @param o 比较对象
     * @return 是否相等
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TransactionData data = (TransactionData) o;
        return tranStateTableOffset == data.tranStateTableOffset && commitLogOffset == data.commitLogOffset &&
            getExpireTime() == data.getExpireTime() && Objects.equal(brokerName, data.brokerName) &&
            Objects.equal(transactionId, data.transactionId);
    }

    /**
     * 返回事务对象哈希值
     *
     * @return 哈希值
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(brokerName, transactionId, tranStateTableOffset, commitLogOffset, getExpireTime());
    }

    /**
     * 按过期时间与关键字段排序事务数据
     *
     * @param o 另一个事务数据
     * @return 排序比较结果
     */
    @Override
    public int compareTo(TransactionData o) {
        return ComparisonChain.start()
            .compare(getExpireTime(), o.getExpireTime())
            .compare(brokerName, o.brokerName)
            .compare(commitLogOffset, o.commitLogOffset)
            .compare(tranStateTableOffset, o.tranStateTableOffset)
            .compare(transactionId, o.transactionId)
            .result();
    }

    /**
     * 构造事务数据字符串表示
     *
     * @return 字符串表示
     */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("brokerName", brokerName)
            .add("tranStateTableOffset", tranStateTableOffset)
            .add("commitLogOffset", commitLogOffset)
            .add("transactionId", transactionId)
            .add("checkTimestamp", checkTimestamp)
            .add("expireMs", expireMs)
            .toString();
    }
}
