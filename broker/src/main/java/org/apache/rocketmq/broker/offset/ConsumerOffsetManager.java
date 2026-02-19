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

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * 消费进度管理器, 负责维护普通消费位点, 重置位点与拉取位点
 */
public class ConsumerOffsetManager extends ConfigManager {
    /**
     * broker 主日志记录器
     */
    protected static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    /**
     * topic 与 group 组合键分隔符
     */
    public static final String TOPIC_GROUP_SEPARATOR = "@";

    /**
     * 消费位点数据版本, 用于跨节点同步判定
     */
    protected DataVersion dataVersion = new DataVersion();

    /**
     * 持久化消费位点表, 键为 topic@group
     */
    protected ConcurrentMap<String/* topic@group, 主题与消费组组合键 */, ConcurrentMap<Integer, Long>> offsetTable =
        new ConcurrentHashMap<>(512);

    /**
     * 临时重置位点表, 服务端重置流程优先读取该表
     */
    private final ConcurrentMap<String, ConcurrentMap<Integer, Long>> resetOffsetTable =
        new ConcurrentHashMap<>(512);

    /**
     * 最近一次拉取位点表, 用于 POP 等场景快速查询
     */
    private final ConcurrentMap<String/* topic@group, 主题与消费组组合键 */, ConcurrentMap<Integer, Long>> pullOffsetTable =
        new ConcurrentHashMap<>(512);

    /**
     * broker 控制器引用, 用于访问消息存储与订阅信息
     */
    protected transient BrokerController brokerController;

    /**
     * 位点变更计数器, 达到阈值后推进 dataVersion
     */
    private final transient AtomicLong versionChangeCounter = new AtomicLong(0);

    /**
     * 默认构造, 主要供序列化框架反射使用
     */
    public ConsumerOffsetManager() {
    }

    /**
     * 使用 broker 控制器构造消费位点管理器
     *
     * @param brokerController broker 控制器实例
     */
    public ConsumerOffsetManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 删除底层扩展实现中的消费位点, 子类按需覆写
     *
     * @param topicAtGroup 目标 topic@group 键
     */
    protected void removeConsumerOffset(String topicAtGroup) {

    }

    /**
     * 按消费组清理全部主题位点
     *
     * @param group 消费组名
     */
    public void cleanOffset(String group) {
        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            if (topicAtGroup.contains(group)) {
                String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
                if (arrays.length == 2 && group.equals(arrays[1])) {
                    it.remove();
                    removeConsumerOffset(topicAtGroup);
                    LOG.warn("Clean group's offset, {}, {}", topicAtGroup, next.getValue());
                }
            }
        }
    }

    /**
     * 按主题清理全部消费组位点
     *
     * @param topic 主题名
     */
    public void cleanOffsetByTopic(String topic) {
        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            if (topicAtGroup.contains(topic)) {
                String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
                if (arrays.length == 2 && topic.equals(arrays[0])) {
                    it.remove();
                    removeConsumerOffset(topicAtGroup);
                    LOG.warn("Clean topic's offset, {}, {}", topicAtGroup, next.getValue());
                }
            }
        }
    }

    /**
     * 扫描并移除已无订阅且位点落后于最小存储位点的数据
     */
    public void scanUnsubscribedTopic() {
        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (arrays.length == 2) {
                String topic = arrays[0];
                String group = arrays[1];

                if (null == brokerController.getConsumerManager().findSubscriptionData(group, topic)
                    && this.offsetBehindMuchThanData(topic, next.getValue())) {
                    it.remove();
                    removeConsumerOffset(topicAtGroup);
                    LOG.warn("remove topic offset, {}", topicAtGroup);
                }
            }
        }
    }

    /**
     * 判断位点是否完全落后于存储最小位点, 用于识别可回收数据
     *
     * @param topic 主题名
     * @param table 队列位点映射
     * @return true 表示所有队列位点都不大于最小存储位点
     */
    private boolean offsetBehindMuchThanData(final String topic, ConcurrentMap<Integer, Long> table) {
        Iterator<Entry<Integer, Long>> it = table.entrySet().iterator();
        boolean result = !table.isEmpty();

        while (it.hasNext() && result) {
            Entry<Integer, Long> next = it.next();
            long minOffsetInStore = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, next.getKey());
            long offsetInPersist = next.getValue();
            result = offsetInPersist <= minOffsetInStore;
        }

        return result;
    }

    /**
     * 查询指定消费组正在消费的主题集合
     *
     * @param group 消费组名
     * @return 该消费组对应的主题集合
     */
    public Set<String> whichTopicByConsumer(final String group) {
        Set<String> topics = new HashSet<>();

        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (arrays.length == 2) {
                if (group.equals(arrays[1])) {
                    topics.add(arrays[0]);
                }
            }
        }

        return topics;
    }

    /**
     * 查询指定主题被哪些消费组消费
     *
     * @param topic 主题名
     * @return 消费该主题的消费组集合
     */
    public Set<String> whichGroupByTopic(final String topic) {
        Set<String> groups = new HashSet<>();

        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (arrays.length == 2) {
                if (topic.equals(arrays[0])) {
                    groups.add(arrays[1]);
                }
            }
        }

        return groups;
    }

    public Map<String, Set<String>> getGroupTopicMap() {
        Map<String, Set<String>> retMap = new HashMap<>(128);

        for (String key : this.offsetTable.keySet()) {
            String[] arr = key.split(TOPIC_GROUP_SEPARATOR);
            if (arr.length == 2) {
                String topic = arr[0];
                String group = arr[1];

                Set<String> topics = retMap.get(group);
                if (topics == null) {
                    topics = new HashSet<>(8);
                    retMap.put(group, topics);
                }

                topics.add(topic);
            }
        }

        return retMap;
    }

    /**
     * 提交消费位点到 offsetTable
     *
     * @param clientHost 客户端来源地址
     * @param group 消费组名
     * @param topic 主题名
     * @param queueId 队列 ID
     * @param offset 待提交位点
     */
    public void commitOffset(final String clientHost, final String group, final String topic, final int queueId,
        final long offset) {
        // topic@group 组合键
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        this.commitOffset(clientHost, key, queueId, offset);
    }

    /**
     * 使用 topic@group 键提交消费位点, 并按步长推进数据版本
     *
     * @param clientHost 客户端来源地址
     * @param key topic@group 组合键
     * @param queueId 队列 ID
     * @param offset 待提交位点
     */
    private void commitOffset(final String clientHost, final String key, final int queueId, final long offset) {
        ConcurrentMap<Integer, Long> map = this.offsetTable.get(key);
        if (null == map) {
            map = new ConcurrentHashMap<>(32);
            map.put(queueId, offset);
            this.offsetTable.put(key, map);
        } else {
            Long storeOffset = map.put(queueId, offset);
            if (storeOffset != null && offset < storeOffset) {
                LOG.warn("[NOTIFYME]update consumer offset less than store. clientHost={}, key={}, queueId={}, requestOffset={}, storeOffset={}", clientHost, key, queueId, offset, storeOffset);
            }
        }
        if (versionChangeCounter.incrementAndGet() % brokerController.getBrokerConfig().getConsumerOffsetUpdateVersionStep() == 0) {
            long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
            dataVersion.nextVersion(stateMachineVersion);
        }
    }

    /**
     * 记录最近拉取位点, 用于拉取链路位点追踪
     *
     * @param clientHost 客户端来源地址
     * @param group 消费组名
     * @param topic 主题名
     * @param queueId 队列 ID
     * @param offset 拉取位点
     */
    public void commitPullOffset(final String clientHost, final String group, final String topic, final int queueId,
        final long offset) {
        // topic@group 组合键
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        ConcurrentMap<Integer, Long> map = this.pullOffsetTable.computeIfAbsent(
            key, k -> new ConcurrentHashMap<>(32));
        map.put(queueId, offset);
    }

    /**
     *
     * If the target queue has temporary reset offset, return the reset-offset.
     * Otherwise, return the current consume offset in the offset store.
     * <br>
     * 若目标队列存在临时重置位点, 则优先返回重置位点<br>
     * 否则返回位点存储中的当前消费位点
     *
     * @param group   Consumer group
     * @param topic   Topic
     * @param queueId Queue ID
     * @return current consume offset or reset offset if there were one.<br>当前消费位点, 若存在重置位点则返回重置值
     */
    public long queryOffset(final String group, final String topic, final int queueId) {
        // topic@group 组合键
        String key = topic + TOPIC_GROUP_SEPARATOR + group;

        if (this.brokerController.getBrokerConfig().isUseServerSideResetOffset()) {
            Map<Integer, Long> reset = resetOffsetTable.get(key);
            if (null != reset && reset.containsKey(queueId)) {
                return reset.get(queueId);
            }
        }

        ConcurrentMap<Integer, Long> map = this.offsetTable.get(key);
        if (null != map) {
            Long offset = map.get(queueId);
            if (offset != null) {
                return offset;
            }
        }

        return -1L;
    }

    /**
     *
     * Query pull offset in pullOffsetTable
     * <br>
     * 在 pullOffsetTable 中查询拉取位点
     *
     * @param group   Consumer group
     * @param topic   Topic
     * @param queueId Queue ID
     * @return latest pull offset of consumer group 消费组当前最新拉取位点
     */
    public long queryPullOffset(final String group, final String topic, final int queueId) {
        // topic@group 组合键
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        Long offset = null;

        ConcurrentMap<Integer, Long> map = this.pullOffsetTable.get(key);
        if (null != map) {
            offset = map.get(queueId);
        }

        if (offset == null) {
            offset = queryOffset(group, topic, queueId);
        }

        return offset;
    }

    /**
     * 清理指定消费组在指定主题上的拉取位点记录
     *
     * @param group 消费组名
     * @param topic 主题名
     */
    public void clearPullOffset(final String group, final String topic) {
        this.pullOffsetTable.remove(topic + TOPIC_GROUP_SEPARATOR + group);
    }

    /**
     * 序列化消费位点管理数据
     *
     * @return JSON 字符串
     */
    @Override
    public String encode() {
        return this.encode(false);
    }

    /**
     * 返回消费位点配置文件路径
     *
     * @return 位点文件绝对路径
     */
    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getConsumerOffsetPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
    }

    /**
     * 反序列化并加载消费位点数据
     *
     * @param jsonString 消费位点 JSON 字符串
     */
    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            ConsumerOffsetManager obj = RemotingSerializable.fromJson(jsonString, ConsumerOffsetManager.class);
            if (obj != null) {
                this.setOffsetTable(obj.getOffsetTable());
                this.dataVersion = obj.dataVersion;
            }
        }
    }

    /**
     * 按指定格式序列化消费位点管理数据
     *
     * @param prettyFormat 是否使用格式化输出
     * @return JSON 字符串
     */
    @Override
    public String encode(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, Long>> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable) {
        this.offsetTable = offsetTable;
    }

    /**
     * 查询主题在全部消费组中的最小可提交位点
     *
     * @param topic 主题名
     * @param filterGroups 逗号分隔的过滤消费组, 匹配到的组会先移除位点
     * @return 队列维度最小位点
     */
    public Map<Integer, Long> queryMinOffsetInAllGroup(final String topic, final String filterGroups) {

        Map<Integer, Long> queueMinOffset = new HashMap<>();
        Set<String> topicGroups = this.offsetTable.keySet();
        if (!UtilAll.isBlank(filterGroups)) {
            for (String group : filterGroups.split(",")) {
                Iterator<String> it = topicGroups.iterator();
                while (it.hasNext()) {
                    String topicAtGroup = it.next();
                    if (group.equals(topicAtGroup.split(TOPIC_GROUP_SEPARATOR)[1])) {
                        it.remove();
                        removeConsumerOffset(topicAtGroup);
                    }
                }
            }
        }

        for (Map.Entry<String, ConcurrentMap<Integer, Long>> offSetEntry : this.offsetTable.entrySet()) {
            String topicGroup = offSetEntry.getKey();
            String[] topicGroupArr = topicGroup.split(TOPIC_GROUP_SEPARATOR);
            if (topic.equals(topicGroupArr[0])) {
                for (Entry<Integer, Long> entry : offSetEntry.getValue().entrySet()) {
                    long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, entry.getKey());
                    if (entry.getValue() >= minOffset) {
                        Long offset = queueMinOffset.get(entry.getKey());
                        if (offset == null) {
                            queueMinOffset.put(entry.getKey(), Math.min(Long.MAX_VALUE, entry.getValue()));
                        } else {
                            queueMinOffset.put(entry.getKey(), Math.min(entry.getValue(), offset));
                        }
                    }
                }
            }

        }
        return queueMinOffset;
    }

    /**
     * 查询指定 topic@group 在各队列上的消费位点
     *
     * @param group 消费组名
     * @param topic 主题名
     * @return 队列到位点的映射, 不存在时返回 null
     */
    public Map<Integer, Long> queryOffset(final String group, final String topic) {
        // topic@group 组合键
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        return this.offsetTable.get(key);
    }

    /**
     * 复制源消费组位点到目标消费组
     *
     * @param srcGroup 源消费组
     * @param destGroup 目标消费组
     * @param topic 主题名
     */
    public void cloneOffset(final String srcGroup, final String destGroup, final String topic) {
        ConcurrentMap<Integer, Long> offsets = this.offsetTable.get(topic + TOPIC_GROUP_SEPARATOR + srcGroup);
        if (offsets != null) {
            this.offsetTable.put(topic + TOPIC_GROUP_SEPARATOR + destGroup, new ConcurrentHashMap<>(offsets));
        }
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }

    /**
     * 仅加载消费位点数据版本, 用于 broker 启动恢复
     *
     * @return true 表示加载成功, false 表示加载失败
     */
    public boolean loadDataVersion() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName);
            if (jsonString != null) {
                ConsumerOffsetManager obj = RemotingSerializable.fromJson(jsonString, ConsumerOffsetManager.class);
                if (obj != null) {
                    this.dataVersion = obj.dataVersion;
                }
                LOG.info("load consumer offset dataVersion success,{},{} ", fileName, jsonString);
            }
            return true;
        } catch (Exception e) {
            LOG.error("load consumer offset dataVersion failed " + fileName, e);
            return false;
        }
    }

    /**
     * 删除指定消费组在主位点表, 重置表, 拉取表中的全部记录
     *
     * @param group 消费组名
     */
    public void removeOffset(final String group) {
        Function<Iterator<Entry<String, ConcurrentMap<Integer, Long>>>, Boolean> deleteFunction = it -> {
            boolean removed = false;
            while (it.hasNext()) {
                Entry<String, ConcurrentMap<Integer, Long>> entry = it.next();
                String topicAtGroup = entry.getKey();
                if (topicAtGroup.contains(group)) {
                    String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
                    if (arrays.length == 2 && group.equals(arrays[1])) {
                        it.remove();
                        removeConsumerOffset(topicAtGroup);
                        removed = true;
                    }
                }
            }
            return removed;
        };

        boolean clearOffset = deleteFunction.apply(this.offsetTable.entrySet().iterator());
        boolean clearReset = deleteFunction.apply(this.resetOffsetTable.entrySet().iterator());
        boolean clearPull = deleteFunction.apply(this.pullOffsetTable.entrySet().iterator());

        LOG.info("Consumer offset manager clean group offset, groupName={}, " +
            "offsetTable={}, resetOffsetTable={}, pullOffsetTable={}", group, clearOffset, clearReset, clearPull);
    }

    /**
     * 为指定队列设置服务端重置位点, 并覆盖当前消费位点
     *
     * @param topic 主题名
     * @param group 消费组名
     * @param queueId 队列 ID
     * @param offset 重置位点
     */
    public void assignResetOffset(String topic, String group, int queueId, long offset) {
        if (Strings.isNullOrEmpty(topic) || Strings.isNullOrEmpty(group) || queueId < 0 || offset < 0) {
            LOG.warn("Illegal arguments when assigning reset offset. Topic={}, group={}, queueId={}, offset={}",
                topic, group, queueId, offset);
            return;
        }

        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        resetOffsetTable.computeIfAbsent(key, k -> Maps.newConcurrentMap()).put(queueId, offset);
        LOG.debug("Reset offset OK. Topic={}, group={}, queueId={}, resetOffset={}", topic, group, queueId, offset);

        // Two things are important here:
        // 1, currentOffsetMap might be null if there is no previous records;
        // 2, Our overriding here may get overridden by the client instantly in concurrent cases; But it still makes
        // sense in cases like clients are offline.
        // 这里有两个关键点
        // 1, 如果此前没有记录, currentOffsetMap 可能为 null
        // 2, 并发场景下可能被客户端瞬间覆盖, 但客户端离线时依旧有意义
        // 例如客户端离线场景, 覆盖写仍可生效
        offsetTable.computeIfAbsent(key, k -> Maps.newConcurrentMap()).put(queueId, offset);
    }

    /**
     * 判断指定队列是否存在待生效的重置位点
     *
     * @param topic 主题名
     * @param group 消费组名
     * @param queueId 队列 ID
     * @return true 表示存在重置位点
     */
    public boolean hasOffsetReset(String topic, String group, int queueId) {
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        ConcurrentMap<Integer, Long> map = resetOffsetTable.get(key);
        if (null == map) {
            return false;
        }
        return map.containsKey(queueId);
    }

    /**
     * 查询并删除指定队列的重置位点, 用于一次性下发
     *
     * @param topic 主题名
     * @param group 消费组名
     * @param queueId 队列 ID
     * @return 重置位点, 若不存在返回 null
     */
    public Long queryThenEraseResetOffset(String topic, String group, Integer queueId) {
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        ConcurrentMap<Integer, Long> map = resetOffsetTable.get(key);
        if (null == map) {
            return null;
        } else {
            return map.remove(queueId);
        }
    }
}
