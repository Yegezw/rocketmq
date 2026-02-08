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
package org.apache.rocketmq.namesrv.routeinfo;

import com.google.common.collect.Sets;
import io.netty.channel.Channel;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.body.*;
import org.apache.rocketmq.remoting.protocol.header.NotifyMinBrokerIdChangeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.UnRegisterBrokerRequestHeader;
import org.apache.rocketmq.remoting.protocol.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 路由信息管理器, 负责 Broker 注册、路由维护、Topic 元数据管理和失活清理
 */
public class RouteInfoManager {
    /**
     * NameServer 日志对象
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    /**
     * Broker 通道默认过期时间, 单位为毫秒
     */
    private static final long DEFAULT_BROKER_CHANNEL_EXPIRED_TIME = 1000 * 60 * 2;

    /**
     * 路由表读写锁
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    /**
     * Topic 队列路由表, 按 Topic 和 brokerName 索引 QueueData
     */
    private final Map<String/* topic */, Map<String, QueueData>> topicQueueTable;
    /**
     * Broker 地址表, 按 brokerName 索引 BrokerData
     */
    private final Map<String/* brokerName */, BrokerData> brokerAddrTable;
    /**
     * 集群地址表, 维护 clusterName 到 brokerName 集合映射
     */
    private final Map<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable;
    /**
     * Broker 存活信息表, 按 Broker 地址索引运行时状态
     */
    private final Map<BrokerAddrInfo/* brokerAddr */, BrokerLiveInfo> brokerLiveTable;
    /**
     * FilterServer 映射表, 按 Broker 地址索引 FilterServer 列表
     */
    private final Map<BrokerAddrInfo/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;
    /**
     * 静态 Topic 队列映射信息表
     */
    private final Map<String/* topic */, Map<String/*brokerName*/, TopicQueueMappingInfo>> topicQueueMappingInfoTable;

    /**
     * 批量注销服务
     */
    private final BatchUnregistrationService unRegisterService;

    /**
     * NameServer 控制器引用
     */
    private final NamesrvController namesrvController;
    /**
     * NameServer 配置
     */
    private final NamesrvConfig namesrvConfig;

    /**
     * 创建 RouteInfoManager 实例
     *
     * @param namesrvConfig     NameServer 配置
     * @param namesrvController NameServer 控制器
     */
    public RouteInfoManager(final NamesrvConfig namesrvConfig, NamesrvController namesrvController) {
        this.topicQueueTable = new ConcurrentHashMap<>(1024);
        this.brokerAddrTable = new ConcurrentHashMap<>(128);
        this.clusterAddrTable = new ConcurrentHashMap<>(32);
        this.brokerLiveTable = new ConcurrentHashMap<>(256);
        this.filterServerTable = new ConcurrentHashMap<>(256);
        this.topicQueueMappingInfoTable = new ConcurrentHashMap<>(1024);
        this.unRegisterService = new BatchUnregistrationService(this, namesrvConfig);
        this.namesrvConfig = namesrvConfig;
        this.namesrvController = namesrvController;
    }

    /**
     * 启动路由信息管理服务
     */
    public void start() {
        this.unRegisterService.start();
    }

    /**
     * 关闭路由信息管理服务
     */
    public void shutdown() {
        this.unRegisterService.shutdown(true);
    }

    /**
     * 提交 Broker 注销请求
     *
     * @param unRegisterRequest 注销请求
     * @return true 表示提交成功
     */
    public boolean submitUnRegisterBrokerRequest(UnRegisterBrokerRequestHeader unRegisterRequest) {
        return this.unRegisterService.submit(unRegisterRequest);
    }

    // For test only
    // 仅用于测试
    /**
     * 获取当前阻塞的注销请求数量
     *
     * @return 阻塞请求数量
     */
    int blockedUnRegisterRequests() {
        return this.unRegisterService.queueLength();
    }

    /**
     * 获取全部集群信息
     *
     * @return 集群信息快照
     */
    public ClusterInfo getAllClusterInfo() {
        ClusterInfo clusterInfoSerializeWrapper = new ClusterInfo();
        clusterInfoSerializeWrapper.setBrokerAddrTable(this.brokerAddrTable);
        clusterInfoSerializeWrapper.setClusterAddrTable(this.clusterAddrTable);
        return clusterInfoSerializeWrapper;
    }

    /**
     * 注册 Topic 路由
     *
     * @param topic      Topic 名称
     * @param queueDatas 队列数据列表
     */
    public void registerTopic(final String topic, List<QueueData> queueDatas) {
        if (queueDatas == null || queueDatas.isEmpty()) {
            return;
        }

        try {
            this.lock.writeLock().lockInterruptibly();
            if (this.topicQueueTable.containsKey(topic)) {
                Map<String, QueueData> queueDataMap  = this.topicQueueTable.get(topic);
                for (QueueData queueData : queueDatas) {
                    if (!this.brokerAddrTable.containsKey(queueData.getBrokerName())) {
                        log.warn("Register topic contains illegal broker, {}, {}", topic, queueData);
                        return;
                    }
                    queueDataMap.put(queueData.getBrokerName(), queueData);
                }
                log.info("Topic route already exist.{}, {}", topic, this.topicQueueTable.get(topic));
            } else {
                // check and construct queue data map
                // 检查并构建队列数据映射
                Map<String, QueueData> queueDataMap = new HashMap<>();
                for (QueueData queueData : queueDatas) {
                    if (!this.brokerAddrTable.containsKey(queueData.getBrokerName())) {
                        log.warn("Register topic contains illegal broker, {}, {}", topic, queueData);
                        return;
                    }
                    queueDataMap.put(queueData.getBrokerName(), queueData);
                }

                this.topicQueueTable.put(topic, queueDataMap);
                log.info("Register topic route:{}, {}", topic, queueDatas);
            }
        } catch (Exception e) {
            log.error("registerTopic Exception", e);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    /**
     * 删除指定 Topic 的全部路由
     *
     * @param topic Topic 名称
     */
    public void deleteTopic(final String topic) {
        try {
            this.lock.writeLock().lockInterruptibly();
            this.topicQueueTable.remove(topic);
        } catch (Exception e) {
            log.error("deleteTopic Exception", e);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    /**
     * 在指定集群范围内删除 Topic 路由
     *
     * @param topic       Topic 名称
     * @param clusterName 集群名称
     */
    public void deleteTopic(final String topic, final String clusterName) {
        try {
            this.lock.writeLock().lockInterruptibly();
            //get all the brokerNames fot the specified cluster
            // 获取指定集群下的所有 brokerName
            Set<String> brokerNames = this.clusterAddrTable.get(clusterName);
            if (brokerNames == null || brokerNames.isEmpty()) {
                return;
            }
            //get the store information for single topic
            // 获取单个 Topic 的存储信息
            Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topic);
            if (queueDataMap != null) {
                for (String brokerName : brokerNames) {
                    final QueueData removedQD = queueDataMap.remove(brokerName);
                    if (removedQD != null) {
                        log.info("deleteTopic, remove one broker's topic {} {} {}", brokerName, topic, removedQD);
                    }
                }
                if (queueDataMap.isEmpty()) {
                    log.info("deleteTopic, remove the topic all queue {} {}", clusterName, topic);
                    this.topicQueueTable.remove(topic);
                }
            }
        } catch (Exception e) {
            log.error("deleteTopic Exception", e);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    /**
     * 获取全部 Topic 列表
     *
     * @return Topic 列表
     */
    public TopicList getAllTopicList() {
        TopicList topicList = new TopicList();
        try {
            this.lock.readLock().lockInterruptibly();
            topicList.getTopicList().addAll(this.topicQueueTable.keySet());
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }

        return topicList;
    }

    /**
     * 注册 Broker, 使用默认不启用 Acting Master 的参数
     *
     * @param clusterName        集群名称
     * @param brokerAddr         Broker 地址
     * @param brokerName         Broker 名称
     * @param brokerId           Broker ID
     * @param haServerAddr       HA 地址
     * @param zoneName           Zone 名称
     * @param timeoutMillis      心跳超时
     * @param topicConfigWrapper Topic 配置包装
     * @param filterServerList   FilterServer 列表
     * @param channel            连接通道
     * @return Broker 注册结果
     */
    public RegisterBrokerResult registerBroker(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId,
        final String haServerAddr,
        final String zoneName,
        final Long timeoutMillis,
        final TopicConfigSerializeWrapper topicConfigWrapper,
        final List<String> filterServerList,
        final Channel channel) {
        return registerBroker(clusterName, brokerAddr, brokerName, brokerId, haServerAddr, zoneName, timeoutMillis, false, topicConfigWrapper, filterServerList, channel);
    }

    /**
     * 注册 Broker 并更新路由、Topic 配置和存活状态
     *
     * @param clusterName        集群名称
     * @param brokerAddr         Broker 地址
     * @param brokerName         Broker 名称
     * @param brokerId           Broker ID
     * @param haServerAddr       HA 地址
     * @param zoneName           Zone 名称
     * @param timeoutMillis      心跳超时
     * @param enableActingMaster 是否启用 Acting Master
     * @param topicConfigWrapper Topic 配置包装
     * @param filterServerList   FilterServer 列表
     * @param channel            连接通道
     * @return Broker 注册结果
     */
    public RegisterBrokerResult registerBroker(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId,
        final String haServerAddr,
        final String zoneName,
        final Long timeoutMillis,
        final Boolean enableActingMaster,
        final TopicConfigSerializeWrapper topicConfigWrapper,
        final List<String> filterServerList,
        final Channel channel) {
        // 创建注册结果对象并开始执行注册流程
        RegisterBrokerResult result = new RegisterBrokerResult();
        try {
            this.lock.writeLock().lockInterruptibly();

            //init or update the cluster info
            // 初始化或更新集群信息
            Set<String> brokerNames = ConcurrentHashMapUtils.computeIfAbsent((ConcurrentHashMap<String, Set<String>>) this.clusterAddrTable, clusterName, k -> new HashSet<>());
            brokerNames.add(brokerName);

            boolean registerFirst = false;

            BrokerData brokerData = this.brokerAddrTable.get(brokerName);
            if (null == brokerData) {
                registerFirst = true;
                brokerData = new BrokerData(clusterName, brokerName, new HashMap<>());
                this.brokerAddrTable.put(brokerName, brokerData);
            }

            boolean isOldVersionBroker = enableActingMaster == null;
            brokerData.setEnableActingMaster(!isOldVersionBroker && enableActingMaster);
            brokerData.setZoneName(zoneName);

            Map<Long, String> brokerAddrsMap = brokerData.getBrokerAddrs();

            boolean isMinBrokerIdChanged = false;
            long prevMinBrokerId = 0;
            if (!brokerAddrsMap.isEmpty()) {
                prevMinBrokerId = Collections.min(brokerAddrsMap.keySet());
            }

            if (brokerId < prevMinBrokerId) {
                isMinBrokerIdChanged = true;
            }

            //Switch slave to master: first remove <1, IP:PORT> in namesrv, then add <0, IP:PORT>
            // Slave 切换为 Master 时, 先移除 <1, IP:PORT>, 再添加 <0, IP:PORT>
            //The same IP:PORT must only have one record in brokerAddrTable
            // 相同 IP:PORT 在 brokerAddrTable 中只能存在一条记录
            brokerAddrsMap.entrySet().removeIf(item -> null != brokerAddr && brokerAddr.equals(item.getValue()) && brokerId != item.getKey());

            //If Local brokerId stateVersion bigger than the registering one,
            // 如果本地 brokerId 的 stateVersion 大于注册请求版本,
            String oldBrokerAddr = brokerAddrsMap.get(brokerId);
            if (null != oldBrokerAddr && !oldBrokerAddr.equals(brokerAddr)) {
                BrokerLiveInfo oldBrokerInfo = brokerLiveTable.get(new BrokerAddrInfo(clusterName, oldBrokerAddr));

                if (null != oldBrokerInfo) {
                    long oldStateVersion = oldBrokerInfo.getDataVersion().getStateVersion();
                    long newStateVersion = topicConfigWrapper.getDataVersion().getStateVersion();
                    if (oldStateVersion > newStateVersion) {
                        log.warn("Registering Broker conflicts with the existed one, just ignore.: Cluster:{}, BrokerName:{}, BrokerId:{}, " +
                                "Old BrokerAddr:{}, Old Version:{}, New BrokerAddr:{}, New Version:{}.",
                            clusterName, brokerName, brokerId, oldBrokerAddr, oldStateVersion, brokerAddr, newStateVersion);
                        //Remove the rejected brokerAddr from brokerLiveTable.
                        // 从 brokerLiveTable 移除被拒绝的 brokerAddr
                        brokerLiveTable.remove(new BrokerAddrInfo(clusterName, brokerAddr));
                        return result;
                    }
                }
            }

            if (!brokerAddrsMap.containsKey(brokerId) && topicConfigWrapper.getTopicConfigTable().size() == 1) {
                log.warn("Can't register topicConfigWrapper={} because broker[{}]={} has not registered.",
                    topicConfigWrapper.getTopicConfigTable(), brokerId, brokerAddr);
                return null;
            }

            String oldAddr = brokerAddrsMap.put(brokerId, brokerAddr);
            registerFirst = registerFirst || (StringUtils.isEmpty(oldAddr));

            boolean isMaster = MixAll.MASTER_ID == brokerId;

            boolean isPrimeSlave = !isOldVersionBroker && !isMaster
                && brokerId == Collections.min(brokerAddrsMap.keySet());

            if (null != topicConfigWrapper && (isMaster || isPrimeSlave)) {

                ConcurrentMap<String, TopicConfig> tcTable =
                    topicConfigWrapper.getTopicConfigTable();

                if (tcTable != null) {

                    TopicConfigAndMappingSerializeWrapper mappingSerializeWrapper = TopicConfigAndMappingSerializeWrapper.from(topicConfigWrapper);
                    Map<String, TopicQueueMappingInfo> topicQueueMappingInfoMap = mappingSerializeWrapper.getTopicQueueMappingInfoMap();

                    // Delete the topics that don't exist in tcTable from the current broker
                    // 删除当前 Broker 上 tcTable 中不存在的 Topic
                    // Static topic is not supported currently
                    // 当前不支持静态 Topic
                    if (namesrvConfig.isDeleteTopicWithBrokerRegistration() && topicQueueMappingInfoMap.isEmpty()) {
                        final Set<String> oldTopicSet = topicSetOfBrokerName(brokerName);
                        final Set<String> newTopicSet = tcTable.keySet();
                        final Sets.SetView<String> toDeleteTopics = Sets.difference(oldTopicSet, newTopicSet);
                        for (final String toDeleteTopic : toDeleteTopics) {
                            Map<String, QueueData> queueDataMap = topicQueueTable.get(toDeleteTopic);
                            final QueueData removedQD = queueDataMap.remove(brokerName);
                            if (removedQD != null) {
                                log.info("deleteTopic, remove one broker's topic {} {} {}", brokerName, toDeleteTopic, removedQD);
                            }

                            if (queueDataMap.isEmpty()) {
                                log.info("deleteTopic, remove the topic all queue {}", toDeleteTopic);
                                topicQueueTable.remove(toDeleteTopic);
                            }
                        }
                    }

                    for (Map.Entry<String, TopicConfig> entry : tcTable.entrySet()) {
                        if (registerFirst || this.isTopicConfigChanged(clusterName, brokerAddr,
                            topicConfigWrapper.getDataVersion(), brokerName,
                            entry.getValue().getTopicName())) {
                            final TopicConfig topicConfig = entry.getValue();
                            // In Slave Acting Master mode, Namesrv will regard the surviving Slave with the smallest brokerId as the "agent" Master, and modify the brokerPermission to read-only.
                            // 在 Slave Acting Master 模式下, Namesrv 会将最小 brokerId 的存活 Slave 视为代理 Master, 并将权限改为只读
                            if (isPrimeSlave && brokerData.isEnableActingMaster()) {
                                // Wipe write perm for prime slave
                                // 清除首要 Slave 的写权限
                                topicConfig.setPerm(topicConfig.getPerm() & (~PermName.PERM_WRITE));
                            }
                            this.createAndUpdateQueueData(brokerName, topicConfig);
                        }
                    }

                    if (this.isBrokerTopicConfigChanged(clusterName, brokerAddr, topicConfigWrapper.getDataVersion()) || registerFirst) {
                        //the topicQueueMappingInfoMap should never be null, but can be empty
                        // topicQueueMappingInfoMap 不应为 null, 但可以为空
                        for (Map.Entry<String, TopicQueueMappingInfo> entry : topicQueueMappingInfoMap.entrySet()) {
                            if (!topicQueueMappingInfoTable.containsKey(entry.getKey())) {
                                topicQueueMappingInfoTable.put(entry.getKey(), new HashMap<>());
                            }
                            //Note asset brokerName equal entry.getValue().getBname()
                            // 约定 brokerName 等于 entry.getValue().getBname()
                            //here use the mappingDetail.bname
                            // 这里使用 mappingDetail.bname
                            topicQueueMappingInfoTable.get(entry.getKey()).put(entry.getValue().getBname(), entry.getValue());
                        }
                    }
                }
            }

            BrokerAddrInfo brokerAddrInfo = new BrokerAddrInfo(clusterName, brokerAddr);
            BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.put(brokerAddrInfo,
                new BrokerLiveInfo(
                    System.currentTimeMillis(),
                    timeoutMillis == null ? DEFAULT_BROKER_CHANNEL_EXPIRED_TIME : timeoutMillis,
                    topicConfigWrapper == null ? new DataVersion() : topicConfigWrapper.getDataVersion(),
                    channel,
                    haServerAddr));
            if (null == prevBrokerLiveInfo) {
                log.info("new broker registered, {} HAService: {}", brokerAddrInfo, haServerAddr);
            }

            if (filterServerList != null) {
                if (filterServerList.isEmpty()) {
                    this.filterServerTable.remove(brokerAddrInfo);
                } else {
                    this.filterServerTable.put(brokerAddrInfo, filterServerList);
                }
            }

            if (MixAll.MASTER_ID != brokerId) {
                String masterAddr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                if (masterAddr != null) {
                    BrokerAddrInfo masterAddrInfo = new BrokerAddrInfo(clusterName, masterAddr);
                    BrokerLiveInfo masterLiveInfo = this.brokerLiveTable.get(masterAddrInfo);
                    if (masterLiveInfo != null) {
                        result.setHaServerAddr(masterLiveInfo.getHaServerAddr());
                        result.setMasterAddr(masterAddr);
                    }
                }
            }

            if (isMinBrokerIdChanged && namesrvConfig.isNotifyMinBrokerIdChanged()) {
                notifyMinBrokerIdChanged(brokerAddrsMap, null,
                    this.brokerLiveTable.get(brokerAddrInfo).getHaServerAddr());
            }
        } catch (Exception e) {
            log.error("registerBroker Exception", e);
        } finally {
            this.lock.writeLock().unlock();
        }

        return result;
    }

    /**
     * 获取指定 Broker 名称关联的 Topic 集合
     *
     * @param brokerName Broker 名称
     * @return Topic 集合
     */
    private Set<String> topicSetOfBrokerName(final String brokerName) {
        Set<String> topicOfBroker = new HashSet<>();
        for (final Entry<String, Map<String, QueueData>> entry : this.topicQueueTable.entrySet()) {
            if (entry.getValue().containsKey(brokerName)) {
                topicOfBroker.add(entry.getKey());
            }
        }
        return topicOfBroker;
    }

    /**
     * 获取 Broker 成员组信息
     *
     * @param clusterName 集群名称
     * @param brokerName  Broker 名称
     * @return Broker 成员组
     */
    public BrokerMemberGroup getBrokerMemberGroup(String clusterName, String brokerName) {
        BrokerMemberGroup groupMember = new BrokerMemberGroup(clusterName, brokerName);
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                final BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (brokerData != null) {
                    groupMember.getBrokerAddrs().putAll(brokerData.getBrokerAddrs());
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("Get broker member group exception", e);
        }
        return groupMember;
    }

    /**
     * 判断 Broker 的 Topic 配置是否发生变化
     *
     * @param clusterName 集群名称
     * @param brokerAddr  Broker 地址
     * @param dataVersion 数据版本
     * @return true 表示已变化
     */
    public boolean isBrokerTopicConfigChanged(final String clusterName, final String brokerAddr,
        final DataVersion dataVersion) {
        DataVersion prev = queryBrokerTopicConfig(clusterName, brokerAddr);
        return null == prev || !prev.equals(dataVersion);
    }

    /**
     * 判断指定 Topic 在指定 Broker 上的配置是否变化
     *
     * @param clusterName 集群名称
     * @param brokerAddr  Broker 地址
     * @param dataVersion 数据版本
     * @param brokerName  Broker 名称
     * @param topic       Topic 名称
     * @return true 表示已变化
     */
    public boolean isTopicConfigChanged(final String clusterName, final String brokerAddr,
        final DataVersion dataVersion, String brokerName, String topic) {
        boolean isChange = isBrokerTopicConfigChanged(clusterName, brokerAddr, dataVersion);
        if (isChange) {
            return true;
        }
        final Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topic);
        if (queueDataMap == null || queueDataMap.isEmpty()) {
            return true;
        }

        // The topicQueueTable already contains the broker
        // topicQueueTable 中已经包含该 Broker
        return !queueDataMap.containsKey(brokerName);
    }

    /**
     * 查询 Broker 的 Topic 配置版本
     *
     * @param clusterName 集群名称
     * @param brokerAddr  Broker 地址
     * @return 数据版本
     */
    public DataVersion queryBrokerTopicConfig(final String clusterName, final String brokerAddr) {
        BrokerAddrInfo addrInfo = new BrokerAddrInfo(clusterName, brokerAddr);
        BrokerLiveInfo prev = this.brokerLiveTable.get(addrInfo);
        if (prev != null) {
            return prev.getDataVersion();
        }
        return null;
    }

    /**
     * 更新 Broker 最近活跃时间
     *
     * @param clusterName 集群名称
     * @param brokerAddr  Broker 地址
     */
    public void updateBrokerInfoUpdateTimestamp(final String clusterName, final String brokerAddr) {
        BrokerAddrInfo addrInfo = new BrokerAddrInfo(clusterName, brokerAddr);
        BrokerLiveInfo prev = this.brokerLiveTable.get(addrInfo);
        if (prev != null) {
            prev.setLastUpdateTimestamp(System.currentTimeMillis());
        }
    }

    /**
     * 创建或更新队列路由数据
     *
     * @param brokerName  Broker 名称
     * @param topicConfig Topic 配置
     */
    private void createAndUpdateQueueData(final String brokerName, final TopicConfig topicConfig) {
        QueueData queueData = new QueueData();
        queueData.setBrokerName(brokerName);
        queueData.setWriteQueueNums(topicConfig.getWriteQueueNums());
        queueData.setReadQueueNums(topicConfig.getReadQueueNums());
        queueData.setPerm(topicConfig.getPerm());
        queueData.setTopicSysFlag(topicConfig.getTopicSysFlag());

        Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topicConfig.getTopicName());
        if (null == queueDataMap) {
            queueDataMap = new HashMap<>();
            queueDataMap.put(brokerName, queueData);
            this.topicQueueTable.put(topicConfig.getTopicName(), queueDataMap);
            log.info("new topic registered, {} {}", topicConfig.getTopicName(), queueData);
        } else {
            final QueueData existedQD = queueDataMap.get(brokerName);
            if (existedQD == null) {
                queueDataMap.put(brokerName, queueData);
            } else if (!existedQD.equals(queueData)) {
                log.info("topic changed, {} OLD: {} NEW: {}", topicConfig.getTopicName(), existedQD,
                    queueData);
                queueDataMap.put(brokerName, queueData);
            }
        }
    }

    /**
     * 通过加锁方式清除 Broker 写权限
     *
     * @param brokerName Broker 名称
     * @return 影响的 Topic 数量
     */
    public int wipeWritePermOfBrokerByLock(final String brokerName) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                return operateWritePermOfBroker(brokerName, RequestCode.WIPE_WRITE_PERM_OF_BROKER);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("wipeWritePermOfBrokerByLock Exception", e);
        }

        return 0;
    }

    /**
     * 通过加锁方式恢复 Broker 写权限
     *
     * @param brokerName Broker 名称
     * @return 影响的 Topic 数量
     */
    public int addWritePermOfBrokerByLock(final String brokerName) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                return operateWritePermOfBroker(brokerName, RequestCode.ADD_WRITE_PERM_OF_BROKER);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("addWritePermOfBrokerByLock Exception", e);
        }
        return 0;
    }

    /**
     * 执行 Broker 写权限变更
     *
     * @param brokerName  Broker 名称
     * @param requestCode 请求码
     * @return 影响的 Topic 数量
     */
    private int operateWritePermOfBroker(final String brokerName, final int requestCode) {
        int topicCnt = 0;

        for (Entry<String, Map<String, QueueData>> entry : this.topicQueueTable.entrySet()) {
            Map<String, QueueData> qdMap = entry.getValue();

            final QueueData qd = qdMap.get(brokerName);
            if (qd == null) {
                continue;
            }
            int perm = qd.getPerm();
            switch (requestCode) {
                case RequestCode.WIPE_WRITE_PERM_OF_BROKER:
                    perm &= ~PermName.PERM_WRITE;
                    break;
                case RequestCode.ADD_WRITE_PERM_OF_BROKER:
                    perm = PermName.PERM_READ | PermName.PERM_WRITE;
                    break;
            }
            qd.setPerm(perm);
            topicCnt++;
        }
        return topicCnt;
    }

    /**
     * 构造单 Broker 注销请求并执行注销
     *
     * @param clusterName 集群名称
     * @param brokerAddr  Broker 地址
     * @param brokerName  Broker 名称
     * @param brokerId    Broker ID
     */
    public void unregisterBroker(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId) {
        UnRegisterBrokerRequestHeader unRegisterBrokerRequest = new UnRegisterBrokerRequestHeader();
        unRegisterBrokerRequest.setClusterName(clusterName);
        unRegisterBrokerRequest.setBrokerAddr(brokerAddr);
        unRegisterBrokerRequest.setBrokerName(brokerName);
        unRegisterBrokerRequest.setBrokerId(brokerId);

        unRegisterBroker(Sets.newHashSet(unRegisterBrokerRequest));
    }

    /**
     * 批量注销 Broker
     *
     * @param unRegisterRequests 注销请求集合
     */
    public void unRegisterBroker(Set<UnRegisterBrokerRequestHeader> unRegisterRequests) {
        // 汇总本次注销请求涉及的 Broker 变化信息
        try {
            Set<String> removedBroker = new HashSet<>();
            Set<String> reducedBroker = new HashSet<>();
            Map<String, BrokerStatusChangeInfo> needNotifyBrokerMap = new HashMap<>();

            this.lock.writeLock().lockInterruptibly();
            for (final UnRegisterBrokerRequestHeader unRegisterRequest : unRegisterRequests) {
                final String brokerName = unRegisterRequest.getBrokerName();
                final String clusterName = unRegisterRequest.getClusterName();
                final String brokerAddr = unRegisterRequest.getBrokerAddr();

                BrokerAddrInfo brokerAddrInfo = new BrokerAddrInfo(clusterName, brokerAddr);

                BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.remove(brokerAddrInfo);
                log.info("unregisterBroker, remove from brokerLiveTable {}, {}",
                    brokerLiveInfo != null ? "OK" : "Failed",
                    brokerAddrInfo
                );

                this.filterServerTable.remove(brokerAddrInfo);

                boolean removeBrokerName = false;
                boolean isMinBrokerIdChanged = false;
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (null != brokerData) {
                    if (!brokerData.getBrokerAddrs().isEmpty() &&
                        unRegisterRequest.getBrokerId().equals(Collections.min(brokerData.getBrokerAddrs().keySet()))) {
                        isMinBrokerIdChanged = true;
                    }
                    boolean removed = brokerData.getBrokerAddrs().entrySet().removeIf(item -> item.getValue().equals(brokerAddr));
                    log.info("unregisterBroker, remove addr from brokerAddrTable {}, {}",
                        removed ? "OK" : "Failed",
                        brokerAddrInfo
                    );
                    if (brokerData.getBrokerAddrs().isEmpty()) {
                        this.brokerAddrTable.remove(brokerName);
                        log.info("unregisterBroker, remove name from brokerAddrTable OK, {}",
                            brokerName
                        );

                        removeBrokerName = true;
                    } else if (isMinBrokerIdChanged) {
                        needNotifyBrokerMap.put(brokerName, new BrokerStatusChangeInfo(
                            brokerData.getBrokerAddrs(), brokerAddr, null));
                    }
                }

                if (removeBrokerName) {
                    Set<String> nameSet = this.clusterAddrTable.get(clusterName);
                    if (nameSet != null) {
                        boolean removed = nameSet.remove(brokerName);
                        log.info("unregisterBroker, remove name from clusterAddrTable {}, {}",
                            removed ? "OK" : "Failed",
                            brokerName);

                        if (nameSet.isEmpty()) {
                            this.clusterAddrTable.remove(clusterName);
                            log.info("unregisterBroker, remove cluster from clusterAddrTable {}",
                                clusterName
                            );
                        }
                    }
                    removedBroker.add(brokerName);
                } else {
                    reducedBroker.add(brokerName);
                }
            }

            cleanTopicByUnRegisterRequests(removedBroker, reducedBroker);

            if (!needNotifyBrokerMap.isEmpty() && namesrvConfig.isNotifyMinBrokerIdChanged()) {
                notifyMinBrokerIdChanged(needNotifyBrokerMap);
            }
        } catch (Exception e) {
            log.error("unregisterBroker Exception", e);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    /**
     * 根据注销结果清理 Topic 路由
     *
     * @param removedBroker 已完全移除的 Broker 集合
     * @param reducedBroker 仅地址减少的 Broker 集合
     */
    private void cleanTopicByUnRegisterRequests(Set<String> removedBroker, Set<String> reducedBroker) {
        Iterator<Entry<String, Map<String, QueueData>>> itMap = this.topicQueueTable.entrySet().iterator();
        while (itMap.hasNext()) {
            Entry<String, Map<String, QueueData>> entry = itMap.next();

            String topic = entry.getKey();
            Map<String, QueueData> queueDataMap = entry.getValue();

            for (final String brokerName : removedBroker) {
                final QueueData removedQD = queueDataMap.remove(brokerName);
                if (removedQD != null) {
                    log.debug("removeTopicByBrokerName, remove one broker's topic {} {}", topic, removedQD);
                }
            }

            if (queueDataMap.isEmpty()) {
                log.debug("removeTopicByBrokerName, remove the topic all queue {}", topic);
                itMap.remove();
            }

            for (final String brokerName : reducedBroker) {
                final QueueData queueData = queueDataMap.get(brokerName);

                if (queueData != null) {
                    if (this.brokerAddrTable.get(brokerName).isEnableActingMaster()) {
                        // Master has been unregistered, wipe the write perm
                        // Master 已注销时, 清除写权限
                        if (isNoMasterExists(brokerName)) {
                            queueData.setPerm(queueData.getPerm() & (~PermName.PERM_WRITE));
                        }
                    }
                }
            }
        }
    }

    /**
     * 判断 Broker 是否不存在 Master
     *
     * @param brokerName Broker 名称
     * @return true 表示不存在 Master
     */
    private boolean isNoMasterExists(String brokerName) {
        final BrokerData brokerData = this.brokerAddrTable.get(brokerName);
        if (brokerData == null) {
            return true;
        }

        if (brokerData.getBrokerAddrs().size() == 0) {
            return true;
        }

        return Collections.min(brokerData.getBrokerAddrs().keySet()) > 0;
    }

    /**
     * 按 Topic 提取路由数据
     *
     * @param topic Topic 名称
     * @return Topic 路由数据
     */
    public TopicRouteData pickupTopicRouteData(final String topic) {
        // 构建返回给客户端的 Topic 路由快照
        TopicRouteData topicRouteData = new TopicRouteData();
        boolean foundQueueData = false;
        boolean foundBrokerData = false;
        List<BrokerData> brokerDataList = new LinkedList<>();
        topicRouteData.setBrokerDatas(brokerDataList);

        HashMap<String, List<String>> filterServerMap = new HashMap<>();
        topicRouteData.setFilterServerTable(filterServerMap);

        try {
            this.lock.readLock().lockInterruptibly();
            Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topic);
            if (queueDataMap != null) {
                topicRouteData.setQueueDatas(new ArrayList<>(queueDataMap.values()));
                foundQueueData = true;

                Set<String> brokerNameSet = new HashSet<>(queueDataMap.keySet());

                for (String brokerName : brokerNameSet) {
                    BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                    if (null == brokerData) {
                        continue;
                    }
                    BrokerData brokerDataClone = new BrokerData(brokerData);

                    brokerDataList.add(brokerDataClone);
                    foundBrokerData = true;
                    if (filterServerTable.isEmpty()) {
                        continue;
                    }
                    for (final String brokerAddr : brokerDataClone.getBrokerAddrs().values()) {
                        BrokerAddrInfo brokerAddrInfo = new BrokerAddrInfo(brokerDataClone.getCluster(), brokerAddr);
                        List<String> filterServerList = this.filterServerTable.get(brokerAddrInfo);
                        filterServerMap.put(brokerAddr, filterServerList);
                    }

                }
            }
        } catch (Exception e) {
            log.error("pickupTopicRouteData Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }

        log.debug("pickupTopicRouteData {} {}", topic, topicRouteData);

        if (foundBrokerData && foundQueueData) {

            topicRouteData.setTopicQueueMappingByBroker(this.topicQueueMappingInfoTable.get(topic));

            if (!namesrvConfig.isSupportActingMaster()) {
                return topicRouteData;
            }

            if (topic.startsWith(TopicValidator.SYNC_BROKER_MEMBER_GROUP_PREFIX)) {
                return topicRouteData;
            }

            if (topicRouteData.getBrokerDatas().size() == 0 || topicRouteData.getQueueDatas().size() == 0) {
                return topicRouteData;
            }

            boolean needActingMaster = false;

            for (final BrokerData brokerData : topicRouteData.getBrokerDatas()) {
                if (brokerData.getBrokerAddrs().size() != 0
                    && !brokerData.getBrokerAddrs().containsKey(MixAll.MASTER_ID)) {
                    needActingMaster = true;
                    break;
                }
            }

            if (!needActingMaster) {
                return topicRouteData;
            }

            for (final BrokerData brokerData : topicRouteData.getBrokerDatas()) {
                final HashMap<Long, String> brokerAddrs = brokerData.getBrokerAddrs();
                if (brokerAddrs.size() == 0 || brokerAddrs.containsKey(MixAll.MASTER_ID) || !brokerData.isEnableActingMaster()) {
                    continue;
                }

                // No master
                // 不存在 Master
                for (final QueueData queueData : topicRouteData.getQueueDatas()) {
                    if (queueData.getBrokerName().equals(brokerData.getBrokerName())) {
                        if (!PermName.isWriteable(queueData.getPerm())) {
                            final Long minBrokerId = Collections.min(brokerAddrs.keySet());
                            final String actingMasterAddr = brokerAddrs.remove(minBrokerId);
                            brokerAddrs.put(MixAll.MASTER_ID, actingMasterAddr);
                        }
                        break;
                    }
                }

            }

            return topicRouteData;
        }

        return null;
    }

    /**
     * 扫描并清理失活 Broker
     */
    public void scanNotActiveBroker() {
        // 扫描超时未心跳的 Broker 并触发下线流程
        try {
            log.info("start scanNotActiveBroker");
            for (Entry<BrokerAddrInfo, BrokerLiveInfo> next : this.brokerLiveTable.entrySet()) {
                long last = next.getValue().getLastUpdateTimestamp();
                long timeoutMillis = next.getValue().getHeartbeatTimeoutMillis();
                if ((last + timeoutMillis) < System.currentTimeMillis()) {
                    RemotingHelper.closeChannel(next.getValue().getChannel());
                    log.warn("The broker channel expired, {} {}ms", next.getKey(), timeoutMillis);
                    this.onChannelDestroy(next.getKey());
                }
            }
        } catch (Exception e) {
            log.error("scanNotActiveBroker exception", e);
        }
    }

    /**
     * 按 Broker 地址信息处理通道销毁
     *
     * @param brokerAddrInfo Broker 地址信息
     */
    public void onChannelDestroy(BrokerAddrInfo brokerAddrInfo) {
        UnRegisterBrokerRequestHeader unRegisterRequest = new UnRegisterBrokerRequestHeader();
        boolean needUnRegister = false;
        if (brokerAddrInfo != null) {
            try {
                try {
                    this.lock.readLock().lockInterruptibly();
                    needUnRegister = setupUnRegisterRequest(unRegisterRequest, brokerAddrInfo);
                } finally {
                    this.lock.readLock().unlock();
                }
            } catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }

        if (needUnRegister) {
            boolean result = this.submitUnRegisterBrokerRequest(unRegisterRequest);
            log.info("the broker's channel destroyed, submit the unregister request at once, " +
                "broker info: {}, submit result: {}", unRegisterRequest, result);
        }
    }

    /**
     * 按通道对象处理通道销毁
     *
     * @param channel 通道对象
     */
    public void onChannelDestroy(Channel channel) {
        UnRegisterBrokerRequestHeader unRegisterRequest = new UnRegisterBrokerRequestHeader();
        BrokerAddrInfo brokerAddrFound = null;
        boolean needUnRegister = false;
        if (channel != null) {
            try {
                try {
                    this.lock.readLock().lockInterruptibly();
                    for (Entry<BrokerAddrInfo, BrokerLiveInfo> entry : this.brokerLiveTable.entrySet()) {
                        if (entry.getValue().getChannel() == channel) {
                            brokerAddrFound = entry.getKey();
                            break;
                        }
                    }

                    if (brokerAddrFound != null) {
                        needUnRegister = setupUnRegisterRequest(unRegisterRequest, brokerAddrFound);
                    }
                } finally {
                    this.lock.readLock().unlock();
                }
            } catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }

        if (needUnRegister) {
            boolean result = this.submitUnRegisterBrokerRequest(unRegisterRequest);
            log.info("the broker's channel destroyed, submit the unregister request at once, " +
                "broker info: {}, submit result: {}", unRegisterRequest, result);
        }
    }

    /**
     * 根据 Broker 地址填充注销请求
     *
     * @param unRegisterRequest 注销请求
     * @param brokerAddrInfo    Broker 地址信息
     * @return true 表示填充成功
     */
    private boolean setupUnRegisterRequest(UnRegisterBrokerRequestHeader unRegisterRequest,
        BrokerAddrInfo brokerAddrInfo) {
        unRegisterRequest.setClusterName(brokerAddrInfo.getClusterName());
        unRegisterRequest.setBrokerAddr(brokerAddrInfo.getBrokerAddr());

        for (Entry<String, BrokerData> stringBrokerDataEntry : this.brokerAddrTable.entrySet()) {
            BrokerData brokerData = stringBrokerDataEntry.getValue();
            if (!brokerAddrInfo.getClusterName().equals(brokerData.getCluster())) {
                continue;
            }

            for (Entry<Long, String> entry : brokerData.getBrokerAddrs().entrySet()) {
                Long brokerId = entry.getKey();
                String brokerAddr = entry.getValue();
                if (brokerAddr.equals(brokerAddrInfo.getBrokerAddr())) {
                    unRegisterRequest.setBrokerName(brokerData.getBrokerName());
                    unRegisterRequest.setBrokerId(brokerId);
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * 通知最小 Broker ID 变化
     *
     * @param needNotifyBrokerMap 需要通知的 Broker 变化信息
     */
    private void notifyMinBrokerIdChanged(Map<String, BrokerStatusChangeInfo> needNotifyBrokerMap)
        throws InterruptedException, RemotingConnectException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingTooMuchRequestException {
        for (String brokerName : needNotifyBrokerMap.keySet()) {
            BrokerStatusChangeInfo brokerStatusChangeInfo = needNotifyBrokerMap.get(brokerName);
            BrokerData brokerData = brokerAddrTable.get(brokerName);
            if (brokerData != null && brokerData.isEnableActingMaster()) {
                notifyMinBrokerIdChanged(brokerStatusChangeInfo.getBrokerAddrs(),
                    brokerStatusChangeInfo.getOfflineBrokerAddr(), brokerStatusChangeInfo.getHaBrokerAddr());
            }
        }
    }

    /**
     * 发送最小 Broker ID 变化通知
     *
     * @param brokerAddrMap     Broker 地址映射
     * @param offlineBrokerAddr 下线 Broker 地址
     * @param haBrokerAddr      HA Broker 地址
     * @throws InterruptedException            中断异常
     * @throws RemotingSendRequestException    发送请求异常
     * @throws RemotingTimeoutException        请求超时异常
     * @throws RemotingTooMuchRequestException 请求过多异常
     * @throws RemotingConnectException        连接异常
     */
    private void notifyMinBrokerIdChanged(Map<Long, String> brokerAddrMap, String offlineBrokerAddr,
        String haBrokerAddr)
        throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException,
        RemotingTooMuchRequestException, RemotingConnectException {
        if (brokerAddrMap == null || brokerAddrMap.isEmpty() || this.namesrvController == null) {
            return;
        }

        NotifyMinBrokerIdChangeRequestHeader requestHeader = new NotifyMinBrokerIdChangeRequestHeader();
        long minBrokerId = Collections.min(brokerAddrMap.keySet());
        requestHeader.setMinBrokerId(minBrokerId);
        requestHeader.setMinBrokerAddr(brokerAddrMap.get(minBrokerId));
        requestHeader.setOfflineBrokerAddr(offlineBrokerAddr);
        requestHeader.setHaBrokerAddr(haBrokerAddr);

        List<String> brokerAddrsNotify = chooseBrokerAddrsToNotify(brokerAddrMap, offlineBrokerAddr);
        log.info("min broker id changed to {}, notify {}, offline broker addr {}", minBrokerId, brokerAddrsNotify, offlineBrokerAddr);
        RemotingCommand request =
            RemotingCommand.createRequestCommand(RequestCode.NOTIFY_MIN_BROKER_ID_CHANGE, requestHeader);
        for (String brokerAddr : brokerAddrsNotify) {
            this.namesrvController.getRemotingClient().invokeOneway(brokerAddr, request, 300);
        }
    }

    /**
     * 选择需要通知的 Broker 地址列表
     *
     * @param brokerAddrMap     Broker 地址映射
     * @param offlineBrokerAddr 下线 Broker 地址
     * @return 需要通知的 Broker 地址列表
     */
    private List<String> chooseBrokerAddrsToNotify(Map<Long, String> brokerAddrMap, String offlineBrokerAddr) {
        if (offlineBrokerAddr != null || brokerAddrMap.size() == 1) {
            // notify the reset brokers.
            // 通知需要重置的 Broker
            return new ArrayList<>(brokerAddrMap.values());
        }

        // new broker registered, notify previous brokers.
        // 新 Broker 注册后, 通知之前的 Broker
        long minBrokerId = Collections.min(brokerAddrMap.keySet());
        List<String> brokerAddrList = new ArrayList<>();
        for (Long brokerId : brokerAddrMap.keySet()) {
            if (brokerId != minBrokerId) {
                brokerAddrList.add(brokerAddrMap.get(brokerId));
            }
        }
        return brokerAddrList;
    }

    // For test only
    // 仅用于测试
    /**
     * 周期打印全部路由元数据
     */
    public void printAllPeriodically() {
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                log.info("--------------------------------------------------------");
                {
                    log.info("topicQueueTable SIZE: {}", this.topicQueueTable.size());
                    for (Entry<String, Map<String, QueueData>> next : this.topicQueueTable.entrySet()) {
                        log.info("topicQueueTable Topic: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("brokerAddrTable SIZE: {}", this.brokerAddrTable.size());
                    for (Entry<String, BrokerData> next : this.brokerAddrTable.entrySet()) {
                        log.info("brokerAddrTable brokerName: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("brokerLiveTable SIZE: {}", this.brokerLiveTable.size());
                    for (Entry<BrokerAddrInfo, BrokerLiveInfo> next : this.brokerLiveTable.entrySet()) {
                        log.info("brokerLiveTable brokerAddr: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("clusterAddrTable SIZE: {}", this.clusterAddrTable.size());
                    for (Entry<String, Set<String>> next : this.clusterAddrTable.entrySet()) {
                        log.info("clusterAddrTable clusterName: {} {}", next.getKey(), next.getValue());
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("printAllPeriodically Exception", e);
        }
    }

    /**
     * 获取系统 Topic 列表
     *
     * @return 系统 Topic 列表
     */
    public TopicList getSystemTopicList() {
        TopicList topicList = new TopicList();
        try {
            this.lock.readLock().lockInterruptibly();
            for (Map.Entry<String, Set<String>> entry : clusterAddrTable.entrySet()) {
                topicList.getTopicList().add(entry.getKey());
                topicList.getTopicList().addAll(entry.getValue());
            }

            if (!brokerAddrTable.isEmpty()) {
                for (String s : brokerAddrTable.keySet()) {
                    BrokerData bd = brokerAddrTable.get(s);
                    HashMap<Long, String> brokerAddrs = bd.getBrokerAddrs();
                    if (brokerAddrs != null && !brokerAddrs.isEmpty()) {
                        Iterator<Long> it2 = brokerAddrs.keySet().iterator();
                        topicList.setBrokerAddr(brokerAddrs.get(it2.next()));
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("getSystemTopicList Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }

        return topicList;
    }

    /**
     * 按集群获取 Topic 列表
     *
     * @param cluster 集群名称
     * @return Topic 列表
     */
    public TopicList getTopicsByCluster(String cluster) {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Set<String> brokerNameSet = this.clusterAddrTable.get(cluster);
                for (String brokerName : brokerNameSet) {
                    for (Entry<String, Map<String, QueueData>> topicEntry : this.topicQueueTable.entrySet()) {
                        String topic = topicEntry.getKey();
                        Map<String, QueueData> queueDataMap = topicEntry.getValue();
                        final QueueData qd = queueDataMap.get(brokerName);
                        if (qd != null) {
                            topicList.getTopicList().add(topic);
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getTopicsByCluster Exception", e);
        }

        return topicList;
    }

    /**
     * 获取单元化 Topic 列表
     *
     * @return Topic 列表
     */
    public TopicList getUnitTopics() {
        TopicList topicList = new TopicList();
        try {
            this.lock.readLock().lockInterruptibly();
            for (Entry<String, Map<String, QueueData>> topicEntry : this.topicQueueTable.entrySet()) {
                String topic = topicEntry.getKey();
                Map<String, QueueData> queueDatas = topicEntry.getValue();
                if (queueDatas != null && queueDatas.size() > 0
                    && TopicSysFlag.hasUnitFlag(queueDatas.values().iterator().next().getTopicSysFlag())) {
                    topicList.getTopicList().add(topic);
                }
            }
        } catch (Exception e) {
            log.error("getUnitTopics Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }

        return topicList;
    }

    /**
     * 获取包含单元化订阅标记的 Topic 列表
     *
     * @return Topic 列表
     */
    public TopicList getHasUnitSubTopicList() {
        TopicList topicList = new TopicList();
        try {
            this.lock.readLock().lockInterruptibly();
            for (Entry<String, Map<String, QueueData>> topicEntry : this.topicQueueTable.entrySet()) {
                String topic = topicEntry.getKey();
                Map<String, QueueData> queueDatas = topicEntry.getValue();
                if (queueDatas != null && queueDatas.size() > 0
                    && TopicSysFlag.hasUnitSubFlag(queueDatas.values().iterator().next().getTopicSysFlag())) {
                    topicList.getTopicList().add(topic);
                }
            }
        } catch (Exception e) {
            log.error("getHasUnitSubTopicList Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }

        return topicList;
    }

    /**
     * 获取包含单元化订阅但不包含单元化标记的 Topic 列表
     *
     * @return Topic 列表
     */
    public TopicList getHasUnitSubUnUnitTopicList() {
        TopicList topicList = new TopicList();
        try {
            this.lock.readLock().lockInterruptibly();
            for (Entry<String, Map<String, QueueData>> topicEntry : this.topicQueueTable.entrySet()) {
                String topic = topicEntry.getKey();
                Map<String, QueueData> queueDatas = topicEntry.getValue();
                if (queueDatas != null && queueDatas.size() > 0
                    && !TopicSysFlag.hasUnitFlag(queueDatas.values().iterator().next().getTopicSysFlag())
                    && TopicSysFlag.hasUnitSubFlag(queueDatas.values().iterator().next().getTopicSysFlag())) {
                    topicList.getTopicList().add(topic);
                }
            }
        } catch (Exception e) {
            log.error("getHasUnitSubUnUnitTopicList Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }

        return topicList;
    }
}

/**
 * broker address information
 * <br>
 * Broker 地址信息
 */
class BrokerAddrInfo {
    /**
     * 集群名称
     */
    private String clusterName;
    /**
     * Broker 地址
     */
    private String brokerAddr;

    /**
     * 缓存哈希值
     */
    private int hash;

    /**
     * 创建 BrokerAddrInfo 实例
     *
     * @param clusterName 集群名称
     * @param brokerAddr  Broker 地址
     */
    public BrokerAddrInfo(String clusterName, String brokerAddr) {
        this.clusterName = clusterName;
        this.brokerAddr = brokerAddr;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    /**
     * 判断地址信息是否为空
     *
     * @return true 表示集群名和地址均为空
     */
    public boolean isEmpty() {
        return clusterName.isEmpty() && brokerAddr.isEmpty();
    }

    /**
     * 判断两个 Broker 地址信息是否相等
     *
     * @param obj 对比对象
     * @return true 表示相等
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }

        if (obj instanceof BrokerAddrInfo) {
            BrokerAddrInfo addr = (BrokerAddrInfo) obj;
            return clusterName.equals(addr.clusterName) && brokerAddr.equals(addr.brokerAddr);
        }
        return false;
    }

    /**
     * 计算并返回哈希值
     *
     * @return 哈希值
     */
    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0 && clusterName.length() + brokerAddr.length() > 0) {
            for (int i = 0; i < clusterName.length(); i++) {
                h = 31 * h + clusterName.charAt(i);
            }
            h = 31 * h + '_';
            for (int i = 0; i < brokerAddr.length(); i++) {
                h = 31 * h + brokerAddr.charAt(i);
            }
            hash = h;
        }
        return h;
    }

    /**
     * 返回对象字符串表示
     *
     * @return 字符串表示
     */
    @Override
    public String toString() {
        return "BrokerIdentityInfo [clusterName=" + clusterName + ", brokerAddr=" + brokerAddr + "]";
    }
}

/**
 * Broker 存活信息实体
 */
class BrokerLiveInfo {
    /**
     * 最近更新时间戳
     */
    private long lastUpdateTimestamp;
    /**
     * 心跳超时时间, 单位为毫秒
     */
    private long heartbeatTimeoutMillis;
    /**
     * Topic 配置数据版本
     */
    private DataVersion dataVersion;
    /**
     * Broker 连接通道
     */
    private Channel channel;
    /**
     * HA 服务地址
     */
    private String haServerAddr;

    /**
     * 创建 BrokerLiveInfo 实例
     *
     * @param lastUpdateTimestamp    最近更新时间戳
     * @param heartbeatTimeoutMillis 心跳超时时间
     * @param dataVersion            数据版本
     * @param channel                连接通道
     * @param haServerAddr           HA 地址
     */
    public BrokerLiveInfo(long lastUpdateTimestamp, long heartbeatTimeoutMillis, DataVersion dataVersion,
        Channel channel,
        String haServerAddr) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
        this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
        this.dataVersion = dataVersion;
        this.channel = channel;
        this.haServerAddr = haServerAddr;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public long getHeartbeatTimeoutMillis() {
        return heartbeatTimeoutMillis;
    }

    public void setHeartbeatTimeoutMillis(long heartbeatTimeoutMillis) {
        this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public String getHaServerAddr() {
        return haServerAddr;
    }

    public void setHaServerAddr(String haServerAddr) {
        this.haServerAddr = haServerAddr;
    }

    /**
     * 返回对象字符串表示
     *
     * @return 字符串表示
     */
    @Override
    public String toString() {
        return "BrokerLiveInfo [lastUpdateTimestamp=" + lastUpdateTimestamp + ", dataVersion=" + dataVersion
            + ", channel=" + channel + ", haServerAddr=" + haServerAddr + "]";
    }
}

/**
 * Broker 状态变化信息实体
 */
class BrokerStatusChangeInfo {
    /**
     * Broker 地址映射
     */
    Map<Long, String> brokerAddrs;
    /**
     * 下线 Broker 地址
     */
    String offlineBrokerAddr;
    /**
     * HA Broker 地址
     */
    String haBrokerAddr;

    /**
     * 创建 BrokerStatusChangeInfo 实例
     *
     * @param brokerAddrs       Broker 地址映射
     * @param offlineBrokerAddr 下线 Broker 地址
     * @param haBrokerAddr      HA Broker 地址
     */
    public BrokerStatusChangeInfo(Map<Long, String> brokerAddrs, String offlineBrokerAddr, String haBrokerAddr) {
        this.brokerAddrs = brokerAddrs;
        this.offlineBrokerAddr = offlineBrokerAddr;
        this.haBrokerAddr = haBrokerAddr;
    }

    public Map<Long, String> getBrokerAddrs() {
        return brokerAddrs;
    }

    public void setBrokerAddrs(Map<Long, String> brokerAddrs) {
        this.brokerAddrs = brokerAddrs;
    }

    public String getOfflineBrokerAddr() {
        return offlineBrokerAddr;
    }

    public void setOfflineBrokerAddr(String offlineBrokerAddr) {
        this.offlineBrokerAddr = offlineBrokerAddr;
    }

    public String getHaBrokerAddr() {
        return haBrokerAddr;
    }

    public void setHaBrokerAddr(String haBrokerAddr) {
        this.haBrokerAddr = haBrokerAddr;
    }
}
