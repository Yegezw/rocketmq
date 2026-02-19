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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * 消费者管理器<br>
 * 负责维护消费组连接, 订阅关系与消费组变更事件通知
 */
public class ConsumerManager {
    /**
     * Broker 日志记录器
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    /**
     * 消费组信息表<br>
     * key: 消费组, value: 组内连接与订阅数据
     */
    private final ConcurrentMap<String, ConsumerGroupInfo> consumerTable =
        new ConcurrentHashMap<>(1024);
    /**
     * 主题到消费组映射表<br>
     * key: 主题, value: 订阅该主题的消费组集合
     */
    private final ConcurrentMap<String, Set<String>> topicGroupTable =
            new ConcurrentHashMap<>(1024);
    /**
     * 补偿消费组信息表<br>
     * 用于心跳缺失场景下补偿保存订阅信息
     */
    private final ConcurrentMap<String, ConsumerGroupInfo> consumerCompensationTable =
        new ConcurrentHashMap<>(1024);
    /**
     * 消费组变更监听器列表
     */
    private final List<ConsumerIdsChangeListener> consumerIdsChangeListenerList = new CopyOnWriteArrayList<>();
    /**
     * Broker 统计管理器, 为空时表示不记录统计
     */
    protected final BrokerStatsManager brokerStatsManager;
    /**
     * Channel 过期阈值, 单位毫秒
     */
    private final long channelExpiredTimeout;
    /**
     * 补偿订阅过期阈值, 单位毫秒
     */
    private final long subscriptionExpiredTimeout;
    /**
     * Broker 配置对象, 测试场景可为空
     */
    private final BrokerConfig brokerConfig;

    /**
     * 构造消费者管理器<br>
     * 主要用于测试或简化场景, 使用统一过期阈值
     *
     * @param consumerIdsChangeListener 消费组变更监听器
     * @param expiredTimeout 连接与订阅统一过期阈值
     */
    public ConsumerManager(final ConsumerIdsChangeListener consumerIdsChangeListener, long expiredTimeout) {
        this.consumerIdsChangeListenerList.add(consumerIdsChangeListener);
        this.brokerStatsManager = null;
        this.channelExpiredTimeout = expiredTimeout;
        this.subscriptionExpiredTimeout = expiredTimeout;
        this.brokerConfig = null;
    }

    /**
     * 构造消费者管理器<br>
     * 使用 BrokerConfig 中独立的连接与订阅过期阈值
     *
     * @param consumerIdsChangeListener 消费组变更监听器
     * @param brokerStatsManager 统计管理器
     * @param brokerConfig Broker 配置
     */
    public ConsumerManager(final ConsumerIdsChangeListener consumerIdsChangeListener,
        final BrokerStatsManager brokerStatsManager, BrokerConfig brokerConfig) {
        this.consumerIdsChangeListenerList.add(consumerIdsChangeListener);
        this.brokerStatsManager = brokerStatsManager;
        this.channelExpiredTimeout = brokerConfig.getChannelExpiredTimeout();
        this.subscriptionExpiredTimeout = brokerConfig.getSubscriptionExpiredTimeout();
        this.brokerConfig = brokerConfig;
    }

    /**
     * 根据消费组与客户端 ID 查询连接信息
     *
     * @param group 消费组
     * @param clientId 客户端 ID
     * @return 连接信息, 不存在时返回 null
     */
    public ClientChannelInfo findChannel(final String group, final String clientId) {
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.findChannel(clientId);
        }
        return null;
    }

    /**
     * 根据消费组与网络通道查询连接信息
     *
     * @param group 消费组
     * @param channel 网络通道
     * @return 连接信息, 不存在时返回 null
     */
    public ClientChannelInfo findChannel(final String group, final Channel channel) {
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.findChannel(channel);
        }
        return null;
    }

    /**
     * 查询订阅数据<br>
     * 默认允许从补偿表回退查询
     *
     * @param group 消费组
     * @param topic 主题
     * @return 订阅数据, 不存在时返回 null
     */
    public SubscriptionData findSubscriptionData(final String group, final String topic) {
        return findSubscriptionData(group, topic, true);
    }

    /**
     * 查询订阅数据
     *
     * @param group 消费组
     * @param topic 主题
     * @param fromCompensationTable 是否允许从补偿表回退查询
     * @return 订阅数据, 不存在时返回 null
     */
    public SubscriptionData findSubscriptionData(final String group, final String topic,
        boolean fromCompensationTable) {
        ConsumerGroupInfo consumerGroupInfo = getConsumerGroupInfo(group, false);
        if (consumerGroupInfo != null) {
            SubscriptionData subscriptionData = consumerGroupInfo.findSubscriptionData(topic);
            if (subscriptionData != null) {
                return subscriptionData;
            }
        }

        if (fromCompensationTable) {
            ConsumerGroupInfo consumerGroupCompensationInfo = consumerCompensationTable.get(group);
            if (consumerGroupCompensationInfo != null) {
                return consumerGroupCompensationInfo.findSubscriptionData(topic);
            }
        }
        return null;
    }

    public ConcurrentMap<String, ConsumerGroupInfo> getConsumerTable() {
        return this.consumerTable;
    }

    public ConsumerGroupInfo getConsumerGroupInfo(final String group) {
        return getConsumerGroupInfo(group, false);
    }

    public ConsumerGroupInfo getConsumerGroupInfo(String group, boolean fromCompensationTable) {
        ConsumerGroupInfo consumerGroupInfo = consumerTable.get(group);
        if (consumerGroupInfo == null && fromCompensationTable) {
            consumerGroupInfo = consumerCompensationTable.get(group);
        }
        return consumerGroupInfo;
    }

    /**
     * 统计消费组订阅主题数量
     *
     * @param group 消费组
     * @return 订阅主题数量
     */
    public int findSubscriptionDataCount(final String group) {
        ConsumerGroupInfo consumerGroupInfo = this.getConsumerGroupInfo(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.getSubscriptionTable().size();
        }

        return 0;
    }

    /**
     * 处理通道关闭事件<br>
     * 会回收失效连接, 清理空消费组并触发对应监听事件
     *
     * @param remoteAddr 远端地址
     * @param channel 关闭的通道
     * @return true 表示至少有一个连接被移除
     */
    public boolean doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        boolean removed = false;
        if (this.brokerConfig != null && this.brokerConfig.isEnableFastChannelEventProcess()) {
            List<String> groups = ClientChannelAttributeHelper.getConsumerGroups(channel);
            if (this.brokerConfig.isPrintChannelGroups() && groups.size() >= 5 && groups.size() >= this.brokerConfig.getPrintChannelGroupsMinNum()) {
                LOGGER.warn("channel close event, too many consumer groups one channel, {}, {}, {}", groups.size(), remoteAddr, groups);
            }
            for (String group : groups) {
                if (null == group || group.length() == 0) {
                    continue;
                }
                ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
                if (null == consumerGroupInfo) {
                    continue;
                }
                ClientChannelInfo clientChannelInfo = consumerGroupInfo.doChannelCloseEvent(remoteAddr, channel);
                if (clientChannelInfo != null) {
                    removed = true;
                    callConsumerIdsChangeListener(ConsumerGroupEvent.CLIENT_UNREGISTER, group, clientChannelInfo, consumerGroupInfo.getSubscribeTopics());
                    if (consumerGroupInfo.getChannelInfoTable().isEmpty()) {
                        ConsumerGroupInfo remove = this.consumerTable.remove(group);
                        if (remove != null) {
                            LOGGER.info("unregister consumer ok, no any connection, and remove consumer group, {}",
                                    group);
                            callConsumerIdsChangeListener(ConsumerGroupEvent.UNREGISTER, group);
                            clearTopicGroupTable(remove);
                        }
                    }
                    callConsumerIdsChangeListener(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
                }
            }
            return removed;
        }
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> next = it.next();
            ConsumerGroupInfo info = next.getValue();
            ClientChannelInfo clientChannelInfo = info.doChannelCloseEvent(remoteAddr, channel);
            if (clientChannelInfo != null) {
                removed = true;
                callConsumerIdsChangeListener(ConsumerGroupEvent.CLIENT_UNREGISTER, next.getKey(), clientChannelInfo, info.getSubscribeTopics());
                if (info.getChannelInfoTable().isEmpty()) {
                    ConsumerGroupInfo remove = this.consumerTable.remove(next.getKey());
                    if (remove != null) {
                        LOGGER.info("unregister consumer ok, no any connection, and remove consumer group, {}",
                            next.getKey());
                        callConsumerIdsChangeListener(ConsumerGroupEvent.UNREGISTER, next.getKey());
                        clearTopicGroupTable(remove);
                    }
                }
                if (!isBroadcastMode(info.getMessageModel())) {
                    callConsumerIdsChangeListener(ConsumerGroupEvent.CHANGE, next.getKey(), info.getAllChannel());
                }
            }
        }
        return removed;
    }

    /**
     * 清理主题到消费组映射表中的指定消费组
     *
     * @param groupInfo 消费组信息
     */
    private void clearTopicGroupTable(final ConsumerGroupInfo groupInfo) {
        for (String subscribeTopic : groupInfo.getSubscribeTopics()) {
            Set<String> groups = this.topicGroupTable.get(subscribeTopic);
            if (groups != null) {
                groups.remove(groupInfo.getGroupName());
            }
            if (groups != null && groups.isEmpty()) {
                this.topicGroupTable.remove(subscribeTopic);
            }
        }
    }

    // compensate consumer info for consumer without heartbeat
    // 为无心跳消费者补偿基础信息
    /**
     * 补偿消费组基础信息
     *
     * @param group 消费组
     * @param consumeType 消费类型
     * @param messageModel 消费模型
     */
    public void compensateBasicConsumerInfo(String group, ConsumeType consumeType, MessageModel messageModel) {
        ConsumerGroupInfo consumerGroupInfo = consumerCompensationTable.computeIfAbsent(group, ConsumerGroupInfo::new);
        consumerGroupInfo.setConsumeType(consumeType);
        consumerGroupInfo.setMessageModel(messageModel);
    }

    // compensate subscription for pull consumer and consumer via proxy
    // 为拉模式消费者和代理消费者补偿订阅信息
    /**
     * 补偿消费组订阅数据
     *
     * @param group 消费组
     * @param topic 主题
     * @param subscriptionData 订阅数据
     */
    public void compensateSubscribeData(String group, String topic, SubscriptionData subscriptionData) {
        ConsumerGroupInfo consumerGroupInfo = consumerCompensationTable.computeIfAbsent(group, ConsumerGroupInfo::new);
        consumerGroupInfo.getSubscriptionTable().put(topic, subscriptionData);
    }

    /**
     * 注册消费者<br>
     * 默认会更新订阅数据
     *
     * @param group 消费组
     * @param clientChannelInfo 客户端通道信息
     * @param consumeType 消费类型
     * @param messageModel 消费模型
     * @param consumeFromWhere 初始消费位点策略
     * @param subList 订阅列表
     * @param isNotifyConsumerIdsChangedEnable 是否通知变更事件
     * @return true 表示连接或订阅有变化
     */
    public boolean registerConsumer(final String group, final ClientChannelInfo clientChannelInfo,
        ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere,
        final Set<SubscriptionData> subList, boolean isNotifyConsumerIdsChangedEnable) {
        return registerConsumer(group, clientChannelInfo, consumeType, messageModel, consumeFromWhere, subList,
            isNotifyConsumerIdsChangedEnable, true);
    }

    /**
     * 注册消费者
     *
     * @param group 消费组
     * @param clientChannelInfo 客户端通道信息
     * @param consumeType 消费类型
     * @param messageModel 消费模型
     * @param consumeFromWhere 初始消费位点策略
     * @param subList 订阅列表
     * @param isNotifyConsumerIdsChangedEnable 是否通知变更事件
     * @param updateSubscription 是否更新订阅信息
     * @return true 表示连接或订阅有变化
     */
    public boolean registerConsumer(final String group, final ClientChannelInfo clientChannelInfo,
        ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere,
        final Set<SubscriptionData> subList, boolean isNotifyConsumerIdsChangedEnable, boolean updateSubscription) {
        long start = System.currentTimeMillis();
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null == consumerGroupInfo) {
            ConsumerGroupInfo tmp = new ConsumerGroupInfo(group, consumeType, messageModel, consumeFromWhere);
            ConsumerGroupInfo prev = this.consumerTable.putIfAbsent(group, tmp);
            consumerGroupInfo = prev != null ? prev : tmp;
        }

        for (SubscriptionData subscriptionData : subList) {
            Set<String> groups = this.topicGroupTable.get(subscriptionData.getTopic());
            if (groups == null) {
                Set<String> tmp = new HashSet<>();
                Set<String> prev = this.topicGroupTable.putIfAbsent(subscriptionData.getTopic(), tmp);
                groups = prev != null ? prev : tmp;
            }
            groups.add(subscriptionData.getTopic());
        }

        boolean r1 =
            consumerGroupInfo.updateChannel(clientChannelInfo, consumeType, messageModel,
                consumeFromWhere);
        if (r1) {
            callConsumerIdsChangeListener(ConsumerGroupEvent.CLIENT_REGISTER, group, clientChannelInfo,
                subList.stream().map(SubscriptionData::getTopic).collect(Collectors.toSet()));
        }
        boolean r2 = false;
        if (updateSubscription) {
            r2 = consumerGroupInfo.updateSubscription(subList);
        }

        if (r1 || r2) {
            if (isNotifyConsumerIdsChangedEnable && !isBroadcastMode(consumerGroupInfo.getMessageModel())) {
                callConsumerIdsChangeListener(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
            }
        }

        if (this.brokerConfig != null && this.brokerConfig.isEnableFastChannelEventProcess() && r1) {
            ClientChannelAttributeHelper.addConsumerGroup(clientChannelInfo.getChannel(), group);
        }

        if (null != this.brokerStatsManager) {
            this.brokerStatsManager.incConsumerRegisterTime((int) (System.currentTimeMillis() - start));
        }

        callConsumerIdsChangeListener(ConsumerGroupEvent.REGISTER, group, subList, clientChannelInfo);

        return r1 || r2;
    }

    /**
     * 注册消费者但不更新订阅<br>
     * 主要用于仅刷新通道场景
     *
     * @param group 消费组
     * @param clientChannelInfo 客户端通道信息
     * @param consumeType 消费类型
     * @param messageModel 消费模型
     * @param consumeFromWhere 初始消费位点策略
     * @param isNotifyConsumerIdsChangedEnable 是否通知变更事件
     * @return true 表示通道信息发生变化
     */
    public boolean registerConsumerWithoutSub(final String group, final ClientChannelInfo clientChannelInfo,
        ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere, boolean isNotifyConsumerIdsChangedEnable) {
        long start = System.currentTimeMillis();
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null == consumerGroupInfo) {
            ConsumerGroupInfo tmp = new ConsumerGroupInfo(group, consumeType, messageModel, consumeFromWhere);
            ConsumerGroupInfo prev = this.consumerTable.putIfAbsent(group, tmp);
            consumerGroupInfo = prev != null ? prev : tmp;
        }

        for (SubscriptionData subscriptionData : consumerGroupInfo.getSubscriptionTable().values()) {
            Set<String> groups = this.topicGroupTable.get(subscriptionData.getTopic());
            if (groups == null) {
                Set<String> tmp = new HashSet<>();
                Set<String> prev = this.topicGroupTable.putIfAbsent(subscriptionData.getTopic(), tmp);
                groups = prev != null ? prev : tmp;
            }
            groups.add(subscriptionData.getTopic());
        }

        boolean updateChannelRst = consumerGroupInfo.updateChannel(clientChannelInfo, consumeType, messageModel, consumeFromWhere);
        if (updateChannelRst && isNotifyConsumerIdsChangedEnable && !isBroadcastMode(consumerGroupInfo.getMessageModel())) {
            callConsumerIdsChangeListener(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
        }
        if (null != this.brokerStatsManager) {
            this.brokerStatsManager.incConsumerRegisterTime((int) (System.currentTimeMillis() - start));
        }
        return updateChannelRst;
    }

    /**
     * 注销消费者连接<br>
     * 当消费组连接清空时会删除整个消费组并触发注销事件
     *
     * @param group 消费组
     * @param clientChannelInfo 客户端通道信息
     * @param isNotifyConsumerIdsChangedEnable 是否通知变更事件
     */
    public void unregisterConsumer(final String group, final ClientChannelInfo clientChannelInfo,
        boolean isNotifyConsumerIdsChangedEnable) {
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null != consumerGroupInfo) {
            boolean removed = consumerGroupInfo.unregisterChannel(clientChannelInfo);
            if (removed) {
                callConsumerIdsChangeListener(ConsumerGroupEvent.CLIENT_UNREGISTER, group, clientChannelInfo, consumerGroupInfo.getSubscribeTopics());
            }
            if (consumerGroupInfo.getChannelInfoTable().isEmpty()) {
                ConsumerGroupInfo remove = this.consumerTable.remove(group);
                if (remove != null) {
                    LOGGER.info("unregister consumer ok, no any connection, and remove consumer group, {}", group);

                    callConsumerIdsChangeListener(ConsumerGroupEvent.UNREGISTER, group);
                    clearTopicGroupTable(remove);
                }
            }
            if (isNotifyConsumerIdsChangedEnable && !isBroadcastMode(consumerGroupInfo.getMessageModel())) {
                callConsumerIdsChangeListener(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
            }
        }
    }

    /**
     * 清理补偿表中过期消费组数据<br>
     * 超时订阅会被移除, 订阅为空的消费组会被删除
     */
    public void removeExpireConsumerGroupInfo() {
        List<String> removeList = new ArrayList<>();
        consumerCompensationTable.forEach((group, consumerGroupInfo) -> {
            List<String> removeTopicList = new ArrayList<>();
            ConcurrentMap<String, SubscriptionData> subscriptionTable = consumerGroupInfo.getSubscriptionTable();
            subscriptionTable.forEach((topic, subscriptionData) -> {
                long diff = System.currentTimeMillis() - subscriptionData.getSubVersion();
                if (diff > subscriptionExpiredTimeout) {
                    removeTopicList.add(topic);
                }
            });
            for (String topic : removeTopicList) {
                subscriptionTable.remove(topic);
                if (subscriptionTable.isEmpty()) {
                    removeList.add(group);
                }
            }
        });
        for (String group : removeList) {
            consumerCompensationTable.remove(group);
        }
    }

    /**
     * 扫描并移除长时间未活跃的通道<br>
     * 同时清理补偿消费组过期数据
     */
    public void scanNotActiveChannel() {
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> next = it.next();
            String group = next.getKey();
            ConsumerGroupInfo consumerGroupInfo = next.getValue();
            ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable =
                consumerGroupInfo.getChannelInfoTable();

            Iterator<Entry<Channel, ClientChannelInfo>> itChannel = channelInfoTable.entrySet().iterator();
            while (itChannel.hasNext()) {
                Entry<Channel, ClientChannelInfo> nextChannel = itChannel.next();
                ClientChannelInfo clientChannelInfo = nextChannel.getValue();
                long diff = System.currentTimeMillis() - clientChannelInfo.getLastUpdateTimestamp();
                if (diff > channelExpiredTimeout) {
                    LOGGER.warn(
                        "SCAN: remove expired channel from ConsumerManager consumerTable. channel={}, consumerGroup={}",
                        RemotingHelper.parseChannelRemoteAddr(clientChannelInfo.getChannel()), group);
                    callConsumerIdsChangeListener(ConsumerGroupEvent.CLIENT_UNREGISTER, group, clientChannelInfo, consumerGroupInfo.getSubscribeTopics());
                    RemotingHelper.closeChannel(clientChannelInfo.getChannel());
                    itChannel.remove();
                }
            }

            if (channelInfoTable.isEmpty()) {
                LOGGER.warn(
                    "SCAN: remove expired channel from ConsumerManager consumerTable, all clear, consumerGroup={}",
                    group);
                it.remove();
            }
        }
        removeExpireConsumerGroupInfo();
    }

    /**
     * 查询订阅指定主题的消费组集合
     *
     * @param topic 主题
     * @return 消费组集合副本
     */
    public HashSet<String> queryTopicConsumeByWho(final String topic) {
        HashSet<String> groups = new HashSet<>();
        if (this.topicGroupTable.get(topic) != null) {
            groups.addAll(this.topicGroupTable.get(topic));
        }
        return groups;
    }

    /**
     * 追加消费组变更监听器
     *
     * @param listener 监听器
     */
    public void appendConsumerIdsChangeListener(ConsumerIdsChangeListener listener) {
        consumerIdsChangeListenerList.add(listener);
    }

    /**
     * 广播消费组变更事件
     *
     * @param event 事件类型
     * @param group 消费组
     * @param args 事件附加参数
     */
    protected void callConsumerIdsChangeListener(ConsumerGroupEvent event, String group, Object... args) {
        for (ConsumerIdsChangeListener listener : consumerIdsChangeListenerList) {
            try {
                listener.handle(event, group, args);
            } catch (Throwable t) {
                LOGGER.error("err when call consumerIdsChangeListener", t);
            }
        }
    }

    /**
     * 判断是否为广播消费模型
     *
     * @param messageModel 消费模型
     * @return true 表示广播模式
     */
    private boolean isBroadcastMode(final MessageModel messageModel) {
        return MessageModel.BROADCASTING.equals(messageModel);
    }
}
