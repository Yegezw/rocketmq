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

import com.alibaba.fastjson.annotation.JSONField;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * POP 顺序消费状态管理器, 维护 topic@group 维度的 OrderInfo
 */
public class ConsumerOrderInfoManager extends ConfigManager {

    /**
     * broker 主日志记录器
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    /**
     * topic 与 group 的组合键分隔符
     */
    private static final String TOPIC_GROUP_SEPARATOR = "@";
    /**
     * 自动清理阈值, 超过该时间未消费的顺序信息会被回收
     */
    private static final long CLEAN_SPAN_FROM_LAST = 24 * 3600 * 1000;

    /**
     * 顺序消费信息表, 键为 topic@group, 值为 queueId 到 OrderInfo 的映射
     */
    private ConcurrentHashMap<String/* topic@group, 主题与消费组组合键 */, ConcurrentHashMap<Integer/* queueId, 队列 ID */, OrderInfo>> table =
        new ConcurrentHashMap<>(128);

    /**
     * 锁释放通知管理器, 用于按可见时间唤醒长轮询
     */
    private transient ConsumerOrderInfoLockManager consumerOrderInfoLockManager;
    /**
     * broker 控制器引用, 用于查询主题与订阅组元数据
     */
    private transient BrokerController brokerController;

    /**
     * 默认构造, 主要供反序列化使用
     */
    public ConsumerOrderInfoManager() {
    }

    /**
     * 使用 broker 控制器构建顺序消费信息管理器
     *
     * @param brokerController broker 控制器实例
     */
    public ConsumerOrderInfoManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.consumerOrderInfoLockManager = new ConsumerOrderInfoLockManager(brokerController);
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, OrderInfo>> getTable() {
        return table;
    }

    public void setTable(ConcurrentHashMap<String, ConcurrentHashMap<Integer, OrderInfo>> table) {
        this.table = table;
    }

    /**
     * 构建 topic@group 组合键
     *
     * @param topic 主题名
     * @param group 消费组名
     * @return topic@group 形式键
     */
    protected static String buildKey(String topic, String group) {
        return topic + TOPIC_GROUP_SEPARATOR + group;
    }

    /**
     * 解析 topic@group 组合键
     *
     * @param key topic@group 键
     * @return 下标 0 为 topic, 下标 1 为 group
     */
    protected static String[] decodeKey(String key) {
        return key.split(TOPIC_GROUP_SEPARATOR);
    }

    /**
     * 根据 OrderInfo 更新队列锁释放时间
     *
     * @param topic 主题名
     * @param group 消费组名
     * @param queueId 队列 ID
     * @param orderInfo 顺序消费状态
     */
    private void updateLockFreeTimestamp(String topic, String group, int queueId, OrderInfo orderInfo) {
        if (consumerOrderInfoLockManager != null) {
            consumerOrderInfoLockManager.updateLockFreeTimestamp(topic, group, queueId, orderInfo);
        }
    }

    /**
     * update the message list received
     * <br>
     * 更新本次 POP 返回消息的顺序消费状态
     *
     * @param attemptId 本次拉取尝试 ID
     * @param isRetry is retry topic or not
     * @param topic topic
     * @param group group
     * @param queueId queue id of message
     * @param popTime the time of pop message
     * @param invisibleTime invisible time
     * @param msgQueueOffsetList the queue offsets of messages
     * @param orderInfoBuilder will append order info to this builder
     */
    public void update(String attemptId, boolean isRetry, String topic, String group, int queueId, long popTime, long invisibleTime,
        List<Long> msgQueueOffsetList, StringBuilder orderInfoBuilder) {
        String key = buildKey(topic, group);
        ConcurrentHashMap<Integer/* queueId, 队列 ID */, OrderInfo> qs = table.get(key);
        if (qs == null) {
            qs = new ConcurrentHashMap<>(16);
            ConcurrentHashMap<Integer/* queueId, 队列 ID */, OrderInfo> old = table.putIfAbsent(key, qs);
            if (old != null) {
                qs = old;
            }
        }

        OrderInfo orderInfo = qs.get(queueId);

        if (orderInfo != null) {
            OrderInfo newOrderInfo = new OrderInfo(attemptId, popTime, invisibleTime, msgQueueOffsetList, System.currentTimeMillis(), 0);
            newOrderInfo.mergeOffsetConsumedCount(orderInfo.attemptId, orderInfo.offsetList, orderInfo.offsetConsumedCount);

            orderInfo = newOrderInfo;
        } else {
            orderInfo = new OrderInfo(attemptId, popTime, invisibleTime, msgQueueOffsetList, System.currentTimeMillis(), 0);
        }
        qs.put(queueId, orderInfo);

        Map<Long, Integer> offsetConsumedCount = orderInfo.offsetConsumedCount;
        int minConsumedTimes = Integer.MAX_VALUE;
        if (offsetConsumedCount != null) {
            Set<Long> offsetSet = offsetConsumedCount.keySet();
            for (Long offset : offsetSet) {
                Integer consumedTimes = offsetConsumedCount.getOrDefault(offset, 0);
                ExtraInfoUtil.buildQueueOffsetOrderCountInfo(orderInfoBuilder, topic, queueId, offset, consumedTimes);
                minConsumedTimes = Math.min(minConsumedTimes, consumedTimes);
            }

            if (offsetConsumedCount.size() != orderInfo.offsetList.size()) {
                // offsetConsumedCount only save messages which consumed count is greater than 0
                // if size not equal, means there are some new messages
                // offsetConsumedCount 只保存消费次数大于 0 的消息
                // 尺寸不相等表示本次存在新的未消费消息
                minConsumedTimes = 0;
            }
        } else {
            minConsumedTimes = 0;
        }

        // for compatibility
        // the old pop sdk use queueId to get consumedTimes from orderCountInfo
        // 兼容旧版本 POP SDK, 仍按 queueId 回填最小消费次数
        // 旧 POP SDK 会通过 queueId 从 orderCountInfo 读取 consumedTimes
        ExtraInfoUtil.buildQueueIdOrderCountInfo(orderInfoBuilder, topic, queueId, minConsumedTimes);
        updateLockFreeTimestamp(topic, group, queueId, orderInfo);
    }

    /**
     * 判断指定队列当前是否仍处于顺序阻塞状态
     *
     * @param attemptId 本次拉取尝试 ID
     * @param topic 主题名
     * @param group 消费组名
     * @param queueId 队列 ID
     * @param invisibleTime 不可见时长
     * @return true 表示仍需阻塞后续拉取
     */
    public boolean checkBlock(String attemptId, String topic, String group, int queueId, long invisibleTime) {
        String key = buildKey(topic, group);
        ConcurrentHashMap<Integer/* queueId, 队列 ID */, OrderInfo> qs = table.get(key);
        if (qs == null) {
            qs = new ConcurrentHashMap<>(16);
            ConcurrentHashMap<Integer/* queueId, 队列 ID */, OrderInfo> old = table.putIfAbsent(key, qs);
            if (old != null) {
                qs = old;
            }
        }

        OrderInfo orderInfo = qs.get(queueId);

        if (orderInfo == null) {
            return false;
        }
        return orderInfo.needBlock(attemptId, invisibleTime);
    }

    /**
     * 清理指定队列的顺序阻塞信息
     *
     * @param topic 主题名
     * @param group 消费组名
     * @param queueId 队列 ID
     */
    public void clearBlock(String topic, String group, int queueId) {
        table.computeIfPresent(buildKey(topic, group), (key, val) -> {
            val.remove(queueId);
            return val;
        });
    }

    /**
     * mark message is consumed finished. return the consumer offset
     * <br>
     * 标记消息已消费完成, 并计算下一次可提交的消费位点
     *
     * @param topic topic
     * @param group group
     * @param queueId queue id of message
     * @param queueOffset queue offset of message
     * @param popTime 本次 POP 请求时间戳
     * @return -1 : illegal, -2 : no need commit, >= 0 : commit
     */
    public long commitAndNext(String topic, String group, int queueId, long queueOffset, long popTime) {
        String key = buildKey(topic, group);
        ConcurrentHashMap<Integer/* queueId, 队列 ID */, OrderInfo> qs = table.get(key);

        if (qs == null) {
            return queueOffset + 1;
        }
        OrderInfo orderInfo = qs.get(queueId);
        if (orderInfo == null) {
            log.warn("OrderInfo is null, {}, {}, {}", key, queueOffset, orderInfo);
            return queueOffset + 1;
        }

        List<Long> o = orderInfo.offsetList;
        if (o == null || o.isEmpty()) {
            log.warn("OrderInfo is empty, {}, {}, {}", key, queueOffset, orderInfo);
            return -1;
        }

        if (popTime != orderInfo.popTime) {
            log.warn("popTime is not equal to orderInfo saved. key: {}, offset: {}, orderInfo: {}, popTime: {}", key, queueOffset, orderInfo, popTime);
            return -2;
        }

        Long first = o.get(0);
        int i = 0, size = o.size();
        for (; i < size; i++) {
            long temp;
            if (i == 0) {
                temp = first;
            } else {
                temp = first + o.get(i);
            }
            if (queueOffset == temp) {
                break;
            }
        }
        // not found
        // 未找到对应位点, 说明提交请求非法
        if (i >= size) {
            log.warn("OrderInfo not found commit offset, {}, {}, {}", key, queueOffset, orderInfo);
            return -1;
        }
        //set bit
        // 将命中位点标记为已确认
        orderInfo.setCommitOffsetBit(orderInfo.commitOffsetBit | (1L << i));
        long nextOffset = orderInfo.getNextOffset();

        updateLockFreeTimestamp(topic, group, queueId, orderInfo);
        return nextOffset;
    }

    /**
     * update next visible time of this message
     * <br>
     * 更新指定消息的下一次可见时间, 以延后顺序队列解锁
     *
     * @param topic topic
     * @param group group
     * @param queueId queue id of message
     * @param queueOffset queue offset of message
     * @param popTime 本次 POP 请求时间戳
     * @param nextVisibleTime nex visible time
     */
    public void updateNextVisibleTime(String topic, String group, int queueId, long queueOffset, long popTime, long nextVisibleTime) {
        String key = buildKey(topic, group);
        ConcurrentHashMap<Integer/* queueId, 队列 ID */, OrderInfo> qs = table.get(key);

        if (qs == null) {
            log.warn("orderInfo of queueId is null. key: {}, queueOffset: {}, queueId: {}", key, queueOffset, queueId);
            return;
        }
        OrderInfo orderInfo = qs.get(queueId);
        if (orderInfo == null) {
            log.warn("orderInfo is null, key: {}, queueOffset: {}, queueId: {}", key, queueOffset, queueId);
            return;
        }
        if (popTime != orderInfo.popTime) {
            log.warn("popTime is not equal to orderInfo saved. key: {}, queueOffset: {}, orderInfo: {}, popTime: {}", key, queueOffset, orderInfo, popTime);
            return;
        }

        orderInfo.updateOffsetNextVisibleTime(queueOffset, nextVisibleTime);
        updateLockFreeTimestamp(topic, group, queueId, orderInfo);
    }

    /**
     * 自动清理无效顺序消费数据, 包含主题删除, 订阅组删除, 队列越界与长期不消费场景
     */
    protected void autoClean() {
        if (brokerController == null) {
            return;
        }
        Iterator<Map.Entry<String/* topic@group, 主题与消费组组合键 */, ConcurrentHashMap<Integer/* queueId, 队列 ID */, OrderInfo>>> iterator =
            this.table.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String/* topic@group, 主题与消费组组合键 */, ConcurrentHashMap<Integer/* queueId, 队列 ID */, OrderInfo>> entry =
                iterator.next();
            String topicAtGroup = entry.getKey();
            ConcurrentHashMap<Integer/* queueId, 队列 ID */, OrderInfo> qs = entry.getValue();
            String[] arrays = decodeKey(topicAtGroup);
            if (arrays.length != 2) {
                continue;
            }
            String topic = arrays[0];
            String group = arrays[1];

            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
            if (topicConfig == null) {
                iterator.remove();
                log.info("Topic not exist, Clean order info, {}:{}", topicAtGroup, qs);
                continue;
            }

            if (!this.brokerController.getSubscriptionGroupManager().containsSubscriptionGroup(group)) {
                iterator.remove();
                log.info("Group not exist, Clean order info, {}:{}", topicAtGroup, qs);
                continue;
            }

            if (qs.isEmpty()) {
                iterator.remove();
                log.info("Order table is empty, Clean order info, {}:{}", topicAtGroup, qs);
                continue;
            }

            Iterator<Map.Entry<Integer/* queueId, 队列 ID */, OrderInfo>> qsIterator = qs.entrySet().iterator();
            while (qsIterator.hasNext()) {
                Map.Entry<Integer/* queueId, 队列 ID */, OrderInfo> qsEntry = qsIterator.next();

                if (qsEntry.getKey() >= topicConfig.getReadQueueNums()) {
                    qsIterator.remove();
                    log.info("Queue not exist, Clean order info, {}:{}, {}", topicAtGroup, entry.getValue(), topicConfig);
                    continue;
                }

                if (System.currentTimeMillis() - qsEntry.getValue().getLastConsumeTimestamp() > CLEAN_SPAN_FROM_LAST) {
                    qsIterator.remove();
                    log.info("Not consume long time, Clean order info, {}:{}, {}", topicAtGroup, entry.getValue(), topicConfig);
                }
            }
        }
    }

    /**
     * 序列化顺序消费信息
     *
     * @return JSON 字符串
     */
    @Override
    public String encode() {
        return this.encode(false);
    }

    /**
     * 返回顺序消费信息配置文件路径
     *
     * @return 配置文件绝对路径
     */
    @Override
    public String configFilePath() {
        if (brokerController != null) {
            return BrokerPathConfigHelper.getConsumerOrderInfoPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        } else {
            return BrokerPathConfigHelper.getConsumerOrderInfoPath("~");
        }
    }

    /**
     * 反序列化并恢复顺序消费信息, 同步恢复锁释放通知任务
     *
     * @param jsonString 顺序消费信息 JSON 字符串
     */
    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            ConsumerOrderInfoManager obj = RemotingSerializable.fromJson(jsonString, ConsumerOrderInfoManager.class);
            if (obj != null) {
                this.table = obj.table;
                if (this.consumerOrderInfoLockManager != null) {
                    this.consumerOrderInfoLockManager.recover(this.table);
                }
            }
        }
    }

    /**
     * 按指定格式序列化顺序消费信息, 编码前先执行自动清理
     *
     * @param prettyFormat 是否格式化输出
     * @return JSON 字符串
     */
    @Override
    public String encode(boolean prettyFormat) {
        this.autoClean();
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    /**
     * 关闭顺序消费锁通知管理器
     */
    public void shutdown() {
        if (this.consumerOrderInfoLockManager != null) {
            this.consumerOrderInfoLockManager.shutdown();
        }
    }

    @VisibleForTesting
    protected ConsumerOrderInfoLockManager getConsumerOrderInfoLockManager() {
        return consumerOrderInfoLockManager;
    }

    /**
     * 单队列顺序消费状态快照
     */
    public static class OrderInfo {
        /**
         * 本批消息的 POP 时间戳
         */
        private long popTime;
        /**
         * the invisibleTime when pop message
         * <br>
         * POP 时设置的不可见时长
         */
        @JSONField(name = "i")
        private Long invisibleTime;
        /**
         * offset<br>
         * offsetList[0] is the queue offset of message<br>
         * offsetList[i] (i > 0) is the distance between current message and offsetList[0]<br>
         * 压缩后的位点列表<br>
         * offsetList[0] 保存首条消息的真实队列位点<br>
         * i > 0 时保存相对首条消息的距离
         */
        @JSONField(name = "o")
        private List<Long> offsetList;
        /**
         * next visible timestamp for message<br>
         * key: message queue offset
         * 每条消息的下一次可见时间<br>
         * key 为消息真实队列位点
         */
        @JSONField(name = "ot")
        private Map<Long, Long> offsetNextVisibleTime;
        /**
         * message consumed count for offset<br>
         * key: message queue offset<br>
         * 每条消息累计消费次数<br>
         * key 为消息真实队列位点
         */
        @JSONField(name = "oc")
        private Map<Long, Integer> offsetConsumedCount;
        /**
         * last consume timestamp<br>
         * 最近一次消费时间戳
         */
        @JSONField(name = "l")
        private long lastConsumeTimestamp;
        /**
         * commit offset bit<br>
         * 已确认位点位图, 第 i 位表示 offsetList 第 i 个元素是否已确认
         */
        @JSONField(name = "cm")
        private long commitOffsetBit;
        /**
         * 当前顺序消费状态对应的尝试 ID
         */
        @JSONField(name = "a")
        private String attemptId;

        /**
         * 默认构造, 主要供反序列化使用
         */
        public OrderInfo() {
        }

        /**
         * 构建顺序消费状态快照
         *
         * @param attemptId 本次拉取尝试 ID
         * @param popTime POP 时间戳
         * @param invisibleTime 不可见时长
         * @param queueOffsetList 原始队列位点列表
         * @param lastConsumeTimestamp 最近消费时间戳
         * @param commitOffsetBit 已确认位图
         */
        public OrderInfo(String attemptId, long popTime, long invisibleTime, List<Long> queueOffsetList, long lastConsumeTimestamp,
            long commitOffsetBit) {
            this.popTime = popTime;
            this.invisibleTime = invisibleTime;
            this.offsetList = buildOffsetList(queueOffsetList);
            this.lastConsumeTimestamp = lastConsumeTimestamp;
            this.commitOffsetBit = commitOffsetBit;
            this.attemptId = attemptId;
        }

        public List<Long> getOffsetList() {
            return offsetList;
        }

        public void setOffsetList(List<Long> offsetList) {
            this.offsetList = offsetList;
        }

        public long getLastConsumeTimestamp() {
            return lastConsumeTimestamp;
        }

        public void setLastConsumeTimestamp(long lastConsumeTimestamp) {
            this.lastConsumeTimestamp = lastConsumeTimestamp;
        }

        public long getCommitOffsetBit() {
            return commitOffsetBit;
        }

        public void setCommitOffsetBit(long commitOffsetBit) {
            this.commitOffsetBit = commitOffsetBit;
        }

        public long getPopTime() {
            return popTime;
        }

        public void setPopTime(long popTime) {
            this.popTime = popTime;
        }

        public Long getInvisibleTime() {
            return invisibleTime;
        }

        public void setInvisibleTime(Long invisibleTime) {
            this.invisibleTime = invisibleTime;
        }

        public Map<Long, Long> getOffsetNextVisibleTime() {
            return offsetNextVisibleTime;
        }

        public void setOffsetNextVisibleTime(Map<Long, Long> offsetNextVisibleTime) {
            this.offsetNextVisibleTime = offsetNextVisibleTime;
        }

        public Map<Long, Integer> getOffsetConsumedCount() {
            return offsetConsumedCount;
        }

        public void setOffsetConsumedCount(Map<Long, Integer> offsetConsumedCount) {
            this.offsetConsumedCount = offsetConsumedCount;
        }

        public String getAttemptId() {
            return attemptId;
        }

        public void setAttemptId(String attemptId) {
            this.attemptId = attemptId;
        }

        /**
         * 将原始队列位点压缩为首位点加差值列表, 降低序列化体积
         *
         * @param queueOffsetList 原始队列位点列表
         * @return 压缩后的位点列表
         */
        public static List<Long> buildOffsetList(List<Long> queueOffsetList) {
            List<Long> simple = new ArrayList<>();
            if (queueOffsetList.size() == 1) {
                simple.addAll(queueOffsetList);
                return simple;
            }
            Long first = queueOffsetList.get(0);
            simple.add(first);
            for (int i = 1; i < queueOffsetList.size(); i++) {
                simple.add(queueOffsetList.get(i) - first);
            }
            return simple;
        }

        /**
         * 判断当前队列是否仍需阻塞, 仅有未确认且未到可见时间的消息时返回 true
         *
         * @param attemptId            本次拉取尝试 ID
         * @param currentInvisibleTime 当前请求不可见时长
         * @return true 表示仍需阻塞
         */
        @JSONField(serialize = false, deserialize = false)
        public boolean needBlock(String attemptId, long currentInvisibleTime) {
            if (offsetList == null || offsetList.isEmpty()) {
                return false;
            }
            if (this.attemptId != null && this.attemptId.equals(attemptId)) {
                return false;
            }
            int num = offsetList.size();
            int i = 0;
            if (this.invisibleTime == null || this.invisibleTime <= 0) {
                this.invisibleTime = currentInvisibleTime;
            }
            long currentTime = System.currentTimeMillis();
            for (; i < num; i++) {
                if (isNotAck(i)) {
                    long nextVisibleTime = popTime + invisibleTime;
                    if (offsetNextVisibleTime != null) {
                        Long time = offsetNextVisibleTime.get(this.getQueueOffset(i));
                        if (time != null) {
                            nextVisibleTime = time;
                        }
                    }
                    if (currentTime < nextVisibleTime) {
                        return true;
                    }
                }
            }
            return false;
        }

        @JSONField(serialize = false, deserialize = false)
        public Long getLockFreeTimestamp() {
            if (offsetList == null || offsetList.isEmpty()) {
                return null;
            }
            int num = offsetList.size();
            int i = 0;
            long currentTime = System.currentTimeMillis();
            for (; i < num; i++) {
                if (isNotAck(i)) {
                    if (invisibleTime == null || invisibleTime <= 0) {
                        return null;
                    }
                    long nextVisibleTime = popTime + invisibleTime;
                    if (offsetNextVisibleTime != null) {
                        Long time = offsetNextVisibleTime.get(this.getQueueOffset(i));
                        if (time != null) {
                            nextVisibleTime = time;
                        }
                    }
                    if (currentTime < nextVisibleTime) {
                        return nextVisibleTime;
                    }
                }
            }
            return currentTime;
        }

        /**
         * 更新单条消息下一次可见时间
         *
         * @param queueOffset 消息真实队列位点
         * @param nextVisibleTime 下一次可见时间戳
         */
        @JSONField(serialize = false, deserialize = false)
        public void updateOffsetNextVisibleTime(long queueOffset, long nextVisibleTime) {
            if (this.offsetNextVisibleTime == null) {
                this.offsetNextVisibleTime = new HashMap<>();
            }
            this.offsetNextVisibleTime.put(queueOffset, nextVisibleTime);
        }

        @JSONField(serialize = false, deserialize = false)
        public long getNextOffset() {
            if (offsetList == null || offsetList.isEmpty()) {
                return -2;
            }
            int num = offsetList.size();
            int i = 0;
            for (; i < num; i++) {
                if (isNotAck(i)) {
                    break;
                }
            }
            if (i == num) {
                // all ack
                // 全部消息已确认, 下一位点为最后一条消息位点加 1
                return getQueueOffset(num - 1) + 1;
            }
            return getQueueOffset(i);
        }

        /**
         * convert the offset at the index of offsetList to queue offset
         * <br>
         * 将 offsetList 指定下标转换为真实队列位点
         *
         * @param offsetIndex the index of offsetList
         * @return queue offset of message 返回消息真实队列位点
         */
        @JSONField(serialize = false, deserialize = false)
        public long getQueueOffset(int offsetIndex) {
            return getQueueOffset(this.offsetList, offsetIndex);
        }

        protected static long getQueueOffset(List<Long> offsetList, int offsetIndex) {
            if (offsetIndex == 0) {
                return offsetList.get(0);
            }
            return offsetList.get(0) + offsetList.get(offsetIndex);
        }

        @JSONField(serialize = false, deserialize = false)
        public boolean isNotAck(int offsetIndex) {
            return (commitOffsetBit & (1L << offsetIndex)) == 0;
        }

        /**
         * calculate message consumed count of each message, and put nonzero value into offsetConsumedCount
         * <br>
         * 计算每条消息消费次数, 并仅记录非零值到 offsetConsumedCount
         *
         * @param preAttemptId            上一轮拉取尝试 ID
         * @param preOffsetList           上一轮压缩位点列表
         * @param prevOffsetConsumedCount the offset list of message
         */
        @JSONField(serialize = false, deserialize = false)
        public void mergeOffsetConsumedCount(String preAttemptId, List<Long> preOffsetList, Map<Long, Integer> prevOffsetConsumedCount) {
            Map<Long, Integer> offsetConsumedCount = new HashMap<>();
            if (prevOffsetConsumedCount == null) {
                prevOffsetConsumedCount = new HashMap<>();
            }
            if (preAttemptId != null && preAttemptId.equals(this.attemptId)) {
                this.offsetConsumedCount = prevOffsetConsumedCount;
                return;
            }
            Set<Long> preQueueOffsetSet = new HashSet<>();
            for (int i = 0; i < preOffsetList.size(); i++) {
                preQueueOffsetSet.add(getQueueOffset(preOffsetList, i));
            }
            for (int i = 0; i < offsetList.size(); i++) {
                long queueOffset = this.getQueueOffset(i);
                if (preQueueOffsetSet.contains(queueOffset)) {
                    int count = 1;
                    Integer preCount = prevOffsetConsumedCount.get(queueOffset);
                    if (preCount != null) {
                        count = preCount + 1;
                    }
                    offsetConsumedCount.put(queueOffset, count);
                }
            }
            this.offsetConsumedCount = offsetConsumedCount;
        }

        /**
         * 输出顺序消费状态, 便于日志排障
         *
         * @return 状态字符串
         */
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("popTime", popTime)
                .add("invisibleTime", invisibleTime)
                .add("offsetList", offsetList)
                .add("offsetNextVisibleTime", offsetNextVisibleTime)
                .add("offsetConsumedCount", offsetConsumedCount)
                .add("lastConsumeTimestamp", lastConsumeTimestamp)
                .add("commitOffsetBit", commitOffsetBit)
                .add("attemptId", attemptId)
                .toString();
        }
    }
}
