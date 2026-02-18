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

package org.apache.rocketmq.proxy.service.sysmessage;

import com.google.common.base.MoreObjects;
import java.util.Set;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

/**
 * 心跳同步消息数据模型
 */
public class HeartbeatSyncerData {
    /**
     * 心跳类型
     */
    private HeartbeatType heartbeatType;
    /**
     * 客户端标识
     */
    private String clientId;
    /**
     * 客户端语言
     */
    private LanguageCode language;
    /**
     * 客户端协议版本
     */
    private int version;
    /**
     * 最近更新时间戳
     */
    private long lastUpdateTimestamp = System.currentTimeMillis();
    /**
     * 订阅数据集合
     */
    private Set<SubscriptionData> subscriptionDataSet;
    /**
     * 消费者组
     */
    private String group;
    /**
     * 消费类型
     */
    private ConsumeType consumeType;
    /**
     * 消息模型
     */
    private MessageModel messageModel;
    /**
     * 消费起始位置
     */
    private ConsumeFromWhere consumeFromWhere;
    /**
     * 本地 Proxy 标识
     */
    private String localProxyId;
    /**
     * 通道序列化数据
     */
    private String channelData;

    /**
     * 默认构造方法
     */
    public HeartbeatSyncerData() {
    }

    /**
     * 初始化心跳同步数据
     *
     * @param heartbeatType 心跳类型
     * @param clientId 客户端标识
     * @param language 客户端语言
     * @param version 客户端协议版本
     * @param group 消费者组
     * @param consumeType 消费类型
     * @param messageModel 消息模型
     * @param consumeFromWhere 消费起始位置
     * @param localProxyId 本地 Proxy 标识
     * @param channelData 通道序列化数据
     */
    public HeartbeatSyncerData(HeartbeatType heartbeatType, String clientId,
        LanguageCode language, int version, String group,
        ConsumeType consumeType, MessageModel messageModel,
        ConsumeFromWhere consumeFromWhere, String localProxyId,
        String channelData) {
        this.heartbeatType = heartbeatType;
        this.clientId = clientId;
        this.language = language;
        this.version = version;
        this.group = group;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        this.consumeFromWhere = consumeFromWhere;
        this.localProxyId = localProxyId;
        this.channelData = channelData;
    }

    public HeartbeatType getHeartbeatType() {
        return heartbeatType;
    }

    public void setHeartbeatType(HeartbeatType heartbeatType) {
        this.heartbeatType = heartbeatType;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public void setLanguage(LanguageCode language) {
        this.language = language;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public Set<SubscriptionData> getSubscriptionDataSet() {
        return subscriptionDataSet;
    }

    public void setSubscriptionDataSet(
        Set<SubscriptionData> subscriptionDataSet) {
        this.subscriptionDataSet = subscriptionDataSet;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public ConsumeType getConsumeType() {
        return consumeType;
    }

    public void setConsumeType(ConsumeType consumeType) {
        this.consumeType = consumeType;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }

    public String getLocalProxyId() {
        return localProxyId;
    }

    public void setLocalProxyId(String localProxyId) {
        this.localProxyId = localProxyId;
    }

    public String getChannelData() {
        return channelData;
    }

    public void setChannelData(String channelData) {
        this.channelData = channelData;
    }

    /**
     * 构造心跳同步数据字符串表示
     *
     * @return 字符串表示
     */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("heartbeatType", heartbeatType)
            .add("clientId", clientId)
            .add("language", language)
            .add("version", version)
            .add("lastUpdateTimestamp", lastUpdateTimestamp)
            .add("subscriptionDataSet", subscriptionDataSet)
            .add("group", group)
            .add("consumeType", consumeType)
            .add("messageModel", messageModel)
            .add("consumeFromWhere", consumeFromWhere)
            .add("connectProxyIp", localProxyId)
            .add("channelData", channelData)
            .toString();
    }
}
