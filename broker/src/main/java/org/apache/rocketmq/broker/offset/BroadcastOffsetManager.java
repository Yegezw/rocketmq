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
package org.apache.rocketmq.broker.offset;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.store.exception.ConsumeQueueException;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * manage the offset of broadcast.
 * now, use this to support switch remoting client between proxy and broker
 * <br>
 * 管理广播消费位点, 兼容客户端接入链路切换场景<br>
 * 当前用于支持 remoting 客户端在 proxy 与 broker 间切换
 */
public class BroadcastOffsetManager extends ServiceThread {
    /**
     * topic 与 group 拼接分隔符, 用于构建内部键
     */
    private static final String TOPIC_GROUP_SEPARATOR = "@";
    /**
     * broker 运行时上下文, 用于访问消费进度与存储能力
     */
    private final BrokerController brokerController;
    /**
     * broker 广播位点相关配置快照, 用于过期判定
     */
    private final BrokerConfig brokerConfig;

    /**
     * k: topic@groupId<br>
     * v: the pull offset of all client of all queue<br>
     * 键格式: topic@groupId<br>
     * 值为该消费组下所有客户端的拉取位点快照
     */
    protected final ConcurrentHashMap<String /* topic@groupId, 主题与消费组组合键 */, BroadcastOffsetData> offsetStoreMap =
        new ConcurrentHashMap<>();

    /**
     * 创建广播位点管理器, 绑定 broker 控制器与配置
     *
     * @param brokerController broker 控制器实例
     */
    public BroadcastOffsetManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.brokerConfig = brokerController.getBrokerConfig();
    }

    /**
     * 更新指定客户端在指定队列上的广播拉取位点
     *
     * @param topic 主题名
     * @param group 消费组名
     * @param queueId 队列 ID
     * @param offset 客户端上报的拉取位点
     * @param clientId 客户端唯一标识
     * @param fromProxy 位点是否由 proxy 链路上报
     */
    public void updateOffset(String topic, String group, int queueId, long offset, String clientId, boolean fromProxy) {
        BroadcastOffsetData broadcastOffsetData = offsetStoreMap.computeIfAbsent(
            buildKey(topic, group), key -> new BroadcastOffsetData(topic, group));

        broadcastOffsetData.clientOffsetStore.compute(clientId, (clientIdKey, broadcastTimedOffsetStore) -> {
            if (broadcastTimedOffsetStore == null) {
                broadcastTimedOffsetStore = new BroadcastTimedOffsetStore(fromProxy);
            }

            broadcastTimedOffsetStore.timestamp = System.currentTimeMillis();
            broadcastTimedOffsetStore.fromProxy = fromProxy;
            broadcastTimedOffsetStore.offsetStore.updateOffset(queueId, offset, true);
            return broadcastTimedOffsetStore;
        });
    }

    /**
     * the time need init offset<br>
     * 1. client connect to proxy -> client connect to broker<br>
     * 2. client connect to broker -> client connect to proxy<br>
     * 3. client connect to proxy at the first time
     * <br>
     * 判断是否需要初始化广播拉取位点, 主要覆盖接入链路切换场景<br>
     * 1. 客户端从 proxy 切换到 broker<br>
     * 2. 客户端从 broker 切换到 proxy<br>
     * 3. 客户端首次通过 proxy 接入
     *
     * @return -1 means no init offset, use the queueOffset in pullRequestHeader
     * <br>
     * -1 表示无需初始化, 此时沿用 pullRequestHeader 中的 queueOffset
     */
    public Long queryInitOffset(String topic, String groupId, int queueId, String clientId, long requestOffset,
        boolean fromProxy) throws ConsumeQueueException {

        BroadcastOffsetData broadcastOffsetData = offsetStoreMap.get(buildKey(topic, groupId));
        if (broadcastOffsetData == null) {
            if (fromProxy && requestOffset < 0) {
                return getOffset(null, topic, groupId, queueId);
            } else {
                return -1L;
            }
        }

        final AtomicLong offset = new AtomicLong(-1L);
        BroadcastTimedOffsetStore offsetStore = broadcastOffsetData.clientOffsetStore.get(clientId);
        if (offsetStore == null) {
            offsetStore = new BroadcastTimedOffsetStore(fromProxy);
            broadcastOffsetData.clientOffsetStore.put(clientId, offsetStore);
        }

        if (offsetStore.fromProxy && requestOffset < 0) {
            // when from proxy and requestOffset is -1
            // means proxy need a init offset to pull message
            // 来自 proxy 且 requestOffset 为 -1 时, 需回补初始化位点
            // 表示 proxy 需要初始化位点后再开始拉取
            offset.set(getOffset(offsetStore, topic, groupId, queueId));
        } else {
            if (offsetStore.fromProxy != fromProxy) {
                offset.set(getOffset(offsetStore, topic, groupId, queueId));
            }
        }
        return offset.get();
    }

    private long getOffset(BroadcastTimedOffsetStore offsetStore, String topic, String groupId, int queueId)
        throws ConsumeQueueException {
        long storeOffset = -1;
        if (offsetStore != null) {
            storeOffset = offsetStore.offsetStore.readOffset(queueId);
        }
        if (storeOffset < 0) {
            storeOffset =
                brokerController.getConsumerOffsetManager().queryOffset(broadcastGroupId(groupId), topic, queueId);
        }
        if (storeOffset < 0) {
            if (this.brokerController.getMessageStore().checkInMemByConsumeOffset(topic, queueId, 0, 1)) {
                storeOffset = 0;
            } else {
                storeOffset = brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId, true);
            }
        }
        return storeOffset;
    }

    /**
     * 扫描并裁剪过期客户端位点, 计算每个队列最小广播位点并提交<br>
     * 1. scan expire offset<br>
     * 2. calculate the min offset of all client of one topic@group,
     * and then commit consumer offset by group@broadcast
     */
    protected void scanOffsetData() {
        for (String k : offsetStoreMap.keySet()) {
            BroadcastOffsetData broadcastOffsetData = offsetStoreMap.get(k);
            if (broadcastOffsetData == null) {
                continue;
            }

            Map<Integer, Long> queueMinOffset = new HashMap<>();

            for (String clientId : broadcastOffsetData.clientOffsetStore.keySet()) {
                broadcastOffsetData.clientOffsetStore
                    .computeIfPresent(clientId, (clientIdKey, broadcastTimedOffsetStore) -> {
                        long interval = System.currentTimeMillis() - broadcastTimedOffsetStore.timestamp;
                        boolean clientIsOnline = brokerController.getConsumerManager().findChannel(broadcastOffsetData.group, clientId) != null;
                        if (clientIsOnline || interval < Duration.ofSeconds(brokerConfig.getBroadcastOffsetExpireSecond()).toMillis()) {
                            Set<Integer> queueSet = broadcastTimedOffsetStore.offsetStore.queueList();
                            for (Integer queue : queueSet) {
                                long offset = broadcastTimedOffsetStore.offsetStore.readOffset(queue);
                                offset = Math.min(queueMinOffset.getOrDefault(queue, offset), offset);
                                queueMinOffset.put(queue, offset);
                            }
                        }
                        if (clientIsOnline && interval >= Duration.ofSeconds(brokerConfig.getBroadcastOffsetExpireMaxSecond()).toMillis()) {
                            return null;
                        }
                        if (!clientIsOnline && interval >= Duration.ofSeconds(brokerConfig.getBroadcastOffsetExpireSecond()).toMillis()) {
                            return null;
                        }
                        return broadcastTimedOffsetStore;
                    });
            }

            offsetStoreMap.computeIfPresent(k, (key, broadcastOffsetDataVal) -> {
                if (broadcastOffsetDataVal.clientOffsetStore.isEmpty()) {
                    return null;
                }
                return broadcastOffsetDataVal;
            });

            queueMinOffset.forEach((queueId, offset) ->
                this.brokerController.getConsumerOffsetManager().commitOffset("BroadcastOffset",
                broadcastGroupId(broadcastOffsetData.group), broadcastOffsetData.topic, queueId, offset));
        }
    }

    /**
     * 构建 topic 与 group 组合键
     *
     * @param topic 主题名
     * @param group 消费组名
     * @return topic@group 形式的内部键
     */
    private String buildKey(String topic, String group) {
        return topic + TOPIC_GROUP_SEPARATOR + group;
    }

    /**
     * 将普通消费组转换为广播位点提交专用组名
     *
     * @param group group of users
     * 消费组名
     * @return the groupId used to commit offset
     */
    private static String broadcastGroupId(String group) {
        return group + TOPIC_GROUP_SEPARATOR + "broadcast";
    }

    @Override
    public String getServiceName() {
        return "BroadcastOffsetManager";
    }

    /**
     * 线程主循环, 周期等待并在唤醒后执行位点扫描
     */
    @Override
    public void run() {
        while (!this.isStopped()) {
            this.waitForRunning(Duration.ofSeconds(5).toMillis());
        }
    }

    /**
     * 等待周期结束后的回调, 触发广播位点清理与提交
     */
    @Override
    protected void onWaitEnd() {
        this.scanOffsetData();
    }

    /**
     * 单个 topic@group 的广播位点容器
     */
    public static class BroadcastOffsetData {
        /**
         * 广播位点所属主题
         */
        private final String topic;
        /**
         * 广播位点所属消费组
         */
        private final String group;
        /**
         * 客户端维度位点快照表, key 为 clientId
         */
        private final ConcurrentHashMap<String /* clientId, 客户端唯一标识 */, BroadcastTimedOffsetStore> clientOffsetStore;

        /**
         * 初始化 topic@group 维度位点容器
         *
         * @param topic 主题名
         * @param group 消费组名
         */
        public BroadcastOffsetData(String topic, String group) {
            this.topic = topic;
            this.group = group;
            this.clientOffsetStore = new ConcurrentHashMap<>();
        }
    }

    /**
     * 带更新时间与来源标记的客户端广播位点快照
     */
    public static class BroadcastTimedOffsetStore {

        /**
         * 最近一次位点更新发生时间戳
         * the timeStamp of last update occurred
         */
        private volatile long timestamp;

        /**
         * 标记位点是否由 proxy 链路上报
         * mark the offset of this client is updated by proxy or not
         */
        private volatile boolean fromProxy;

        /**
         * 按队列保存的拉取位点表
         * the pulled offset of each queue
         */
        private final BroadcastOffsetStore offsetStore;

        /**
         * 创建客户端位点快照, 默认记录当前时间
         *
         * @param fromProxy 位点来源是否为 proxy 链路
         */
        public BroadcastTimedOffsetStore(boolean fromProxy) {
            this.timestamp = System.currentTimeMillis();
            this.fromProxy = fromProxy;
            this.offsetStore = new BroadcastOffsetStore();
        }
    }
}
