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
package org.apache.rocketmq.broker.mqtrace;

import java.util.Map;

import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

public class ConsumeMessageContext {
    /**
     * 消费者组名, 用于标识消费端逻辑集群
     */
    private String consumerGroup;
    /**
     * 当前消费主题, 可能包含命名空间前缀
     */
    private String topic;
    /**
     * 本次消费队列编号
     */
    private Integer queueId;
    /**
     * 消费客户端地址, 用于定位消费来源
     */
    private String clientHost;
    /**
     * Broker 存储节点地址, 用于追踪消息存储位置
     */
    private String storeHost;
    /**
     * 消费消息 ID 到队列偏移量的映射
     */
    private Map<String, Long> messageIds;
    /**
     * 本次消费消息体总长度, 单位为字节
     */
    private int bodyLength;
    /**
     * 消费结果标记, true 表示消费成功
     */
    private boolean success;
    /**
     * 消费状态字符串, 用于记录详细业务结果
     */
    private String status;
    /**
     * 钩子私有上下文对象, 在消费前后阶段透传
     */
    private Object mqTraceContext;
    /**
     * 主题配置快照, 供钩子读取消费相关配置
     */
    private TopicConfig topicConfig;

    /**
     * 认证方式, 用于账户维度统计
     */
    private String accountAuthType;
    /**
     * 账户父级归属, 用于汇总上级主体指标
     */
    private String accountOwnerParent;
    /**
     * 账户自身归属, 用于主体级别计费与审计
     */
    private String accountOwnerSelf;
    /**
     * 本次接收消息条数
     */
    private int rcvMsgNum;
    /**
     * 本次接收消息总大小, 单位为字节
     */
    private int rcvMsgSize;
    /**
     * 接收统计类型, 用于选择统计维度
     */
    private BrokerStatsManager.StatsType rcvStat;
    /**
     * 商业计费视角下的接收消息条数
     */
    private int commercialRcvMsgNum;

    /**
     * 商业归属主体, 用于计费口径隔离
     */
    private String commercialOwner;
    /**
     * 商业接收统计类型, 对应计费指标分类
     */
    private BrokerStatsManager.StatsType commercialRcvStats;
    /**
     * 商业计费视角下的接收次数
     */
    private int commercialRcvTimes;
    /**
     * 商业计费视角下的接收总大小, 单位为字节
     */
    private int commercialRcvSize;
    /**
     * 被过滤消息条数, 反映过滤表达式命中情况
     */
    private int filterMessageCount;

    /**
     * 命名空间, 用于隔离同名主题与消费组
     */
    private String namespace;

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public String getClientHost() {
        return clientHost;
    }

    public void setClientHost(String clientHost) {
        this.clientHost = clientHost;
    }

    public String getStoreHost() {
        return storeHost;
    }

    public void setStoreHost(String storeHost) {
        this.storeHost = storeHost;
    }

    public Map<String, Long> getMessageIds() {
        return messageIds;
    }

    public void setMessageIds(Map<String, Long> messageIds) {
        this.messageIds = messageIds;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Object getMqTraceContext() {
        return mqTraceContext;
    }

    public void setMqTraceContext(Object mqTraceContext) {
        this.mqTraceContext = mqTraceContext;
    }

    public TopicConfig getTopicConfig() {
        return topicConfig;
    }

    public void setTopicConfig(TopicConfig topicConfig) {
        this.topicConfig = topicConfig;
    }

    public int getBodyLength() {
        return bodyLength;
    }

    public void setBodyLength(int bodyLength) {
        this.bodyLength = bodyLength;
    }

    public String getAccountAuthType() {
        return accountAuthType;
    }

    public void setAccountAuthType(String accountAuthType) {
        this.accountAuthType = accountAuthType;
    }

    public String getAccountOwnerParent() {
        return accountOwnerParent;
    }

    public void setAccountOwnerParent(String accountOwnerParent) {
        this.accountOwnerParent = accountOwnerParent;
    }

    public String getAccountOwnerSelf() {
        return accountOwnerSelf;
    }

    public void setAccountOwnerSelf(String accountOwnerSelf) {
        this.accountOwnerSelf = accountOwnerSelf;
    }

    public int getRcvMsgNum() {
        return rcvMsgNum;
    }

    public void setRcvMsgNum(int rcvMsgNum) {
        this.rcvMsgNum = rcvMsgNum;
    }

    public int getRcvMsgSize() {
        return rcvMsgSize;
    }

    public void setRcvMsgSize(int rcvMsgSize) {
        this.rcvMsgSize = rcvMsgSize;
    }

    public BrokerStatsManager.StatsType getRcvStat() {
        return rcvStat;
    }

    public void setRcvStat(BrokerStatsManager.StatsType rcvStat) {
        this.rcvStat = rcvStat;
    }

    public int getCommercialRcvMsgNum() {
        return commercialRcvMsgNum;
    }

    public void setCommercialRcvMsgNum(int commercialRcvMsgNum) {
        this.commercialRcvMsgNum = commercialRcvMsgNum;
    }

    public String getCommercialOwner() {
        return commercialOwner;
    }

    public void setCommercialOwner(final String commercialOwner) {
        this.commercialOwner = commercialOwner;
    }

    public BrokerStatsManager.StatsType getCommercialRcvStats() {
        return commercialRcvStats;
    }

    public void setCommercialRcvStats(final BrokerStatsManager.StatsType commercialRcvStats) {
        this.commercialRcvStats = commercialRcvStats;
    }

    public int getCommercialRcvTimes() {
        return commercialRcvTimes;
    }

    public void setCommercialRcvTimes(final int commercialRcvTimes) {
        this.commercialRcvTimes = commercialRcvTimes;
    }

    public int getCommercialRcvSize() {
        return commercialRcvSize;
    }

    public void setCommercialRcvSize(final int commercialRcvSize) {
        this.commercialRcvSize = commercialRcvSize;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public int getFilterMessageCount() {
        return filterMessageCount;
    }

    public void setFilterMessageCount(int filterMessageCount) {
        this.filterMessageCount = filterMessageCount;
    }
}
