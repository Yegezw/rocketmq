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
package org.apache.rocketmq.proxy.service.transaction;

import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;

/**
 * 集群模式事务服务, 负责事务订阅维护与心跳上报
 */
public class ClusterTransactionService extends AbstractTransactionService {
    /**
     * Proxy 日志记录器
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    /**
     * 事务心跳使用的客户端标识
     */
    private static final String TRANS_HEARTBEAT_CLIENT_ID = "rmq-proxy-producer-client";

    /**
     * MQ 客户端工厂
     */
    private final MQClientAPIFactory mqClientAPIFactory;
    /**
     * 主题路由服务
     */
    private final TopicRouteService topicRouteService;
    /**
     * Producer 管理器
     */
    private final ProducerManager producerManager;

    /**
     * 事务心跳发送线程池
     */
    private ThreadPoolExecutor heartbeatExecutors;
    /**
     * 生产者组到集群集合的订阅关系
     */
    private final Map<String /* group */, Set<ClusterData>/* cluster list */> groupClusterData = new ConcurrentHashMap<>();
    /**
     * Broker 地址到名称映射缓存
     */
    private final AtomicReference<Map<String /* brokerAddr */, String /* brokerName */>> brokerAddrNameMapRef = new AtomicReference<>();
    /**
     * 事务心跳后台线程
     */
    private TxHeartbeatServiceThread txHeartbeatServiceThread;

    /**
     * 初始化集群事务服务
     *
     * @param topicRouteService 主题路由服务
     * @param producerManager Producer 管理器
     * @param mqClientAPIFactory MQ 客户端工厂
     */
    public ClusterTransactionService(TopicRouteService topicRouteService, ProducerManager producerManager,
        MQClientAPIFactory mqClientAPIFactory) {
        this.topicRouteService = topicRouteService;
        this.producerManager = producerManager;
        this.mqClientAPIFactory = mqClientAPIFactory;
    }

    /**
     * 为生产者组追加多个事务主题订阅
     *
     * @param ctx 请求上下文
     * @param group 生产者组
     * @param topicList 主题列表
     */
    @Override
    public void addTransactionSubscription(ProxyContext ctx, String group, List<String> topicList) {
        for (String topic : topicList) {
            addTransactionSubscription(ctx, group, topic);
        }
    }

    /**
     * 为生产者组追加单个事务主题订阅
     *
     * @param ctx 请求上下文
     * @param group 生产者组
     * @param topic 主题
     */
    @Override
    public void addTransactionSubscription(ProxyContext ctx, String group, String topic) {
        try {
            groupClusterData.compute(group, (groupName, clusterDataSet) -> {
                if (clusterDataSet == null) {
                    clusterDataSet = Sets.newHashSet();
                }
                clusterDataSet.addAll(getClusterDataFromTopic(ctx, topic));
                return clusterDataSet;
            });
        } catch (Exception e) {
            log.error("add producer group err in txHeartBeat. groupId: {}, err: {}", group, e);
        }
    }

    /**
     * 使用新主题集合替换事务订阅
     *
     * @param ctx 请求上下文
     * @param group 生产者组
     * @param topicList 主题列表
     */
    @Override
    public void replaceTransactionSubscription(ProxyContext ctx, String group, List<String> topicList) {
        Set<ClusterData> clusterDataSet = new HashSet<>();
        for (String topic : topicList) {
            clusterDataSet.addAll(getClusterDataFromTopic(ctx, topic));
        }
        groupClusterData.put(group, clusterDataSet);
    }

    private Set<ClusterData> getClusterDataFromTopic(ProxyContext ctx, String topic) {
        try {
            MessageQueueView messageQueue = this.topicRouteService.getAllMessageQueueView(ctx, topic);
            List<BrokerData> brokerDataList = messageQueue.getTopicRouteData().getBrokerDatas();

            if (brokerDataList == null) {
                return Collections.emptySet();
            }
            Set<ClusterData> res = Sets.newHashSet();
            for (BrokerData brokerData : brokerDataList) {
                res.add(new ClusterData(brokerData.getCluster()));
            }
            return res;
        } catch (Throwable t) {
            log.error("get cluster data failed in txHeartBeat. topic: {}, err: {}", topic, t);
        }
        return Collections.emptySet();
    }

    /**
     * 取消生产者组全部事务主题订阅
     *
     * @param ctx 请求上下文
     * @param group 生产者组
     */
    @Override
    public void unSubscribeAllTransactionTopic(ProxyContext ctx, String group) {
        groupClusterData.remove(group);
    }

    /**
     * 扫描在线生产者并向目标集群发送事务心跳
     */
    public void scanProducerHeartBeat() {
        Set<String> groupSet = groupClusterData.keySet();

        Map<String /* cluster */, List<HeartbeatData>> clusterHeartbeatData = new HashMap<>();
        for (String group : groupSet) {
            groupClusterData.computeIfPresent(group, (groupName, clusterDataSet) -> {
                if (clusterDataSet.isEmpty()) {
                    return null;
                }
                if (!this.producerManager.groupOnline(groupName)) {
                    return null;
                }

                ProducerData producerData = new ProducerData();
                producerData.setGroupName(groupName);

                for (ClusterData clusterData : clusterDataSet) {
                    List<HeartbeatData> heartbeatDataList = clusterHeartbeatData.get(clusterData.cluster);
                    if (heartbeatDataList == null) {
                        heartbeatDataList = new ArrayList<>();
                    }

                    HeartbeatData heartbeatData;
                    if (heartbeatDataList.isEmpty()) {
                        heartbeatData = new HeartbeatData();
                        heartbeatData.setClientID(TRANS_HEARTBEAT_CLIENT_ID);
                        heartbeatDataList.add(heartbeatData);
                    } else {
                        heartbeatData = heartbeatDataList.get(heartbeatDataList.size() - 1);
                        if (heartbeatData.getProducerDataSet().size() >= ConfigurationManager.getProxyConfig().getTransactionHeartbeatBatchNum()) {
                            heartbeatData = new HeartbeatData();
                            heartbeatData.setClientID(TRANS_HEARTBEAT_CLIENT_ID);
                            heartbeatDataList.add(heartbeatData);
                        }
                    }

                    heartbeatData.getProducerDataSet().add(producerData);
                    clusterHeartbeatData.put(clusterData.cluster, heartbeatDataList);
                }

                if (clusterDataSet.isEmpty()) {
                    return null;
                }
                return clusterDataSet;
            });
        }

        if (clusterHeartbeatData.isEmpty()) {
            return;
        }
        Map<String, String> brokerAddrNameMap = new ConcurrentHashMap<>();
        Set<Map.Entry<String, List<HeartbeatData>>> clusterEntry = clusterHeartbeatData.entrySet();
        for (Map.Entry<String, List<HeartbeatData>> entry : clusterEntry) {
            sendHeartBeatToCluster(entry.getKey(), entry.getValue(), brokerAddrNameMap);
        }
        this.brokerAddrNameMapRef.set(brokerAddrNameMap);
    }

    public Map<String, Set<ClusterData>> getGroupClusterData() {
        return groupClusterData;
    }

    /**
     * 向指定集群批量发送事务心跳
     *
     * @param clusterName 集群名称
     * @param heartbeatDataList 心跳数据列表
     * @param brokerAddrNameMap Broker 地址到名称映射
     */
    protected void sendHeartBeatToCluster(String clusterName, List<HeartbeatData> heartbeatDataList, Map<String, String> brokerAddrNameMap) {
        if (heartbeatDataList == null) {
            return;
        }
        for (HeartbeatData heartbeatData : heartbeatDataList) {
            sendHeartBeatToCluster(clusterName, heartbeatData, brokerAddrNameMap);
        }
        this.brokerAddrNameMapRef.set(brokerAddrNameMap);
    }

    /**
     * 向指定集群发送单条事务心跳
     *
     * @param clusterName 集群名称
     * @param heartbeatData 心跳数据
     * @param brokerAddrNameMap Broker 地址到名称映射
     */
    protected void sendHeartBeatToCluster(String clusterName, HeartbeatData heartbeatData, Map<String, String> brokerAddrNameMap) {
        try {
            MessageQueueView messageQueue = this.topicRouteService.getAllMessageQueueView(ProxyContext.createForInner(this.getClass()), clusterName);
            List<BrokerData> brokerDataList = messageQueue.getTopicRouteData().getBrokerDatas();
            if (brokerDataList == null) {
                return;
            }
            for (BrokerData brokerData : brokerDataList) {
                brokerAddrNameMap.put(brokerData.selectBrokerAddr(), brokerData.getBrokerName());
                heartbeatExecutors.submit(() -> {
                    String brokerAddr = brokerData.selectBrokerAddr();
                    this.mqClientAPIFactory.getClient()
                        .sendHeartbeatOneway(brokerAddr, heartbeatData, Duration.ofSeconds(3).toMillis())
                        .exceptionally(t -> {
                            log.error("Send transactionHeartbeat to broker err. brokerAddr: {}", brokerAddr, t);
                            return null;
                        });
                });
            }
        } catch (Exception e) {
            log.error("get broker add in cluster failed in tx. clusterName: {}", clusterName, e);
        }
    }

    @Override
    protected String getBrokerNameByAddr(String brokerAddr) {
        if (StringUtils.isBlank(brokerAddr)) {
            return null;
        }
        return brokerAddrNameMapRef.get().get(brokerAddr);
    }

    /**
     * 集群维度订阅数据
     */
    static class ClusterData {
        /**
         * 集群名称
         */
        private final String cluster;

        /**
         * 初始化集群订阅数据
         *
         * @param cluster 集群名称
         */
        public ClusterData(String cluster) {
            this.cluster = cluster;
        }

        public String getCluster() {
            return cluster;
        }

        /**
         * 基于集群名称判断对象相等
         *
         * @param obj 比较对象
         * @return 是否相等
         */
        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof ClusterData)) {
                return super.equals(obj);
            }

            ClusterData other = (ClusterData) obj;
            return cluster.equals(other.cluster);
        }

        /**
         * 返回基于集群名称的哈希值
         *
         * @return 哈希值
         */
        @Override
        public int hashCode() {
            return cluster.hashCode();
        }
    }

    /**
     * 定时触发事务心跳扫描的后台线程
     */
    class TxHeartbeatServiceThread extends ServiceThread {

        @Override
        public String getServiceName() {
            return TxHeartbeatServiceThread.class.getName();
        }

        /**
         * 线程主循环, 按配置周期触发扫描
         */
        @Override
        public void run() {
            while (!this.isStopped()) {
                this.waitForRunning(TimeUnit.SECONDS.toMillis(ConfigurationManager.getProxyConfig().getTransactionHeartbeatPeriodSecond()));
            }
        }

        /**
         * 每次等待结束后执行心跳扫描
         */
        @Override
        protected void onWaitEnd() {
            scanProducerHeartBeat();
        }
    }

    /**
     * 启动事务服务与心跳线程
     */
    @Override
    public void start() throws Exception {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        txHeartbeatServiceThread = new TxHeartbeatServiceThread();

        super.start();
        txHeartbeatServiceThread.start();
        heartbeatExecutors = ThreadPoolMonitor.createAndMonitor(
            proxyConfig.getTransactionHeartbeatThreadPoolNums(),
            proxyConfig.getTransactionHeartbeatThreadPoolNums(),
            0L, TimeUnit.MILLISECONDS,
            "TransactionHeartbeatRegisterThread",
            proxyConfig.getTransactionHeartbeatThreadPoolQueueCapacity()
        );
    }

    /**
     * 停止事务服务并释放线程资源
     */
    @Override
    public void shutdown() throws Exception {
        txHeartbeatServiceThread.shutdown();
        heartbeatExecutors.shutdown();
        super.shutdown();
    }
}
