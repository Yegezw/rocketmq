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

import org.apache.rocketmq.common.message.MessageType;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.util.Properties;

public class SendMessageContext {
    /**
     * namespace
     * <br>
     * 命名空间, 用于隔离多租户资源
     */
    private String namespace;
    /**
     * producer group without namespace.
     * <br>
     * 去除命名空间后的生产者组名, 便于统计与审计保持稳定维度
     */
    private String producerGroup;
    /**
     * topic without namespace.
     * <br>
     * 去除命名空间后的主题名, 用于回调与监控统一标识
     */
    private String topic;
    /**
     * Broker 返回的消息 ID, 用于客户端确认发送结果
     */
    private String msgId;
    /**
     * 原始消息 ID, 在重试或消息转发链路中用于关联源消息
     */
    private String originMsgId;
    /**
     * 消息写入的队列编号
     */
    private Integer queueId;
    /**
     * 消息写入后的逻辑偏移量
     */
    private Long queueOffset;
    /**
     * 处理本次请求的 Broker 地址
     */
    private String brokerAddr;
    /**
     * 生产者客户端地址, 用于追踪消息来源
     */
    private String bornHost;
    /**
     * 消息体长度, 单位为字节
     */
    private int bodyLength;
    /**
     * 响应码, 表示发送处理结果
     */
    private int code;
    /**
     * 错误描述, 仅在发送失败时写入
     */
    private String errorMsg;
    /**
     * 序列化后的消息属性字符串, 用于钩子扩展处理
     */
    private String msgProps;
    /**
     * 钩子私有上下文对象, 在 before 与 after 阶段透传
     */
    private Object mqTraceContext;
    /**
     * 额外扩展属性, 供钩子实现读写附加信息
     */
    private Properties extProps;
    /**
     * Broker 所属地域标识, 用于跨地域链路追踪
     */
    private String brokerRegionId;
    /**
     * 消息唯一键, 用于业务去重与检索
     */
    private String msgUniqueKey;
    /**
     * 客户端创建消息的时间戳
     */
    private long bornTimeStamp;
    /**
     * Broker 接收请求的时间戳
     */
    private long requestTimeStamp;
    /**
     * 消息类型, 默认使用事务提交类型以兼容历史链路
     */
    private MessageType msgType = MessageType.Trans_msg_Commit;

    /**
     * 发送是否成功, 由后置钩子根据结果回填
     */
    private boolean isSuccess = false;

    /**
     * Account Statistics
     * <br>
     * 账户维度统计字段, 用于计费与审计
     * <br>
     * 认证方式, 用于区分不同鉴权来源
     */
    private String accountAuthType;
    /**
     * 账户父级归属, 用于聚合统计上级主体
     */
    private String accountOwnerParent;
    /**
     * 账户自身归属, 用于主体级别统计
     */
    private String accountOwnerSelf;
    /**
     * 本次发送消息条数
     */
    private int sendMsgNum;
    /**
     * 本次发送消息总大小, 单位为字节
     */
    private int sendMsgSize;
    /**
     * 发送统计类型, 用于选择统计项
     */
    private BrokerStatsManager.StatsType sendStat;
    /**
     * 商业计费视角下的发送消息条数
     */
    private int commercialSendMsgNum;

    /**
     * For Commercial
     * <br>
     * 商业计费维度字段, 用于计量与结算
     * <br>
     * 商业归属主体, 用于计费口径隔离
     */
    private String commercialOwner;
    /**
     * 商业发送统计类型, 对应计费指标分类
     */
    private BrokerStatsManager.StatsType commercialSendStats;
    /**
     * 商业计费视角下的发送总大小, 单位为字节
     */
    private int commercialSendSize;
    /**
     * 商业计费视角下的发送次数
     */
    private int commercialSendTimes;

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public void setSuccess(final boolean success) {
        isSuccess = success;
    }

    public MessageType getMsgType() {
        return msgType;
    }

    public void setMsgType(final MessageType msgType) {
        this.msgType = msgType;
    }

    public String getMsgUniqueKey() {
        return msgUniqueKey;
    }

    public void setMsgUniqueKey(final String msgUniqueKey) {
        this.msgUniqueKey = msgUniqueKey;
    }

    public long getBornTimeStamp() {
        return bornTimeStamp;
    }

    public void setBornTimeStamp(final long bornTimeStamp) {
        this.bornTimeStamp = bornTimeStamp;
    }

    public long getRequestTimeStamp() {
        return requestTimeStamp;
    }

    public void setRequestTimeStamp(long requestTimeStamp) {
        this.requestTimeStamp = requestTimeStamp;
    }

    public String getBrokerRegionId() {
        return brokerRegionId;
    }

    public void setBrokerRegionId(final String brokerRegionId) {
        this.brokerRegionId = brokerRegionId;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public String getOriginMsgId() {
        return originMsgId;
    }

    public void setOriginMsgId(String originMsgId) {
        this.originMsgId = originMsgId;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public Long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(Long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }

    public String getBornHost() {
        return bornHost;
    }

    public void setBornHost(String bornHost) {
        this.bornHost = bornHost;
    }

    public int getBodyLength() {
        return bodyLength;
    }

    public void setBodyLength(int bodyLength) {
        this.bodyLength = bodyLength;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public String getMsgProps() {
        return msgProps;
    }

    public void setMsgProps(String msgProps) {
        this.msgProps = msgProps;
    }

    public Object getMqTraceContext() {
        return mqTraceContext;
    }

    public void setMqTraceContext(Object mqTraceContext) {
        this.mqTraceContext = mqTraceContext;
    }

    public Properties getExtProps() {
        return extProps;
    }

    public void setExtProps(Properties extProps) {
        this.extProps = extProps;
    }

    public String getCommercialOwner() {
        return commercialOwner;
    }

    public void setCommercialOwner(final String commercialOwner) {
        this.commercialOwner = commercialOwner;
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

    public int getSendMsgNum() {
        return sendMsgNum;
    }

    public void setSendMsgNum(int sendMsgNum) {
        this.sendMsgNum = sendMsgNum;
    }

    public int getSendMsgSize() {
        return sendMsgSize;
    }

    public void setSendMsgSize(int sendMsgSize) {
        this.sendMsgSize = sendMsgSize;
    }

    public BrokerStatsManager.StatsType getSendStat() {
        return sendStat;
    }

    public void setSendStat(BrokerStatsManager.StatsType sendStat) {
        this.sendStat = sendStat;
    }

    public BrokerStatsManager.StatsType getCommercialSendStats() {
        return commercialSendStats;
    }

    public int getCommercialSendMsgNum() {
        return commercialSendMsgNum;
    }

    public void setCommercialSendMsgNum(int commercialSendMsgNum) {
        this.commercialSendMsgNum = commercialSendMsgNum;
    }

    public void setCommercialSendStats(final BrokerStatsManager.StatsType commercialSendStats) {
        this.commercialSendStats = commercialSendStats;
    }

    public int getCommercialSendSize() {
        return commercialSendSize;
    }

    public void setCommercialSendSize(final int commercialSendSize) {
        this.commercialSendSize = commercialSendSize;
    }

    public int getCommercialSendTimes() {
        return commercialSendTimes;
    }

    public void setCommercialSendTimes(final int commercialSendTimes) {
        this.commercialSendTimes = commercialSendTimes;
    }
}
