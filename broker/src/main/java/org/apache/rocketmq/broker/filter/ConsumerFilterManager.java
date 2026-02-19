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

package org.apache.rocketmq.broker.filter;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.filter.FilterFactory;
import org.apache.rocketmq.filter.util.BloomFilter;
import org.apache.rocketmq.filter.util.BloomFilterData;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Consumer filter data manager.Just manage the consumers use expression filter.
 * <br>
 * 消费过滤数据管理器, 仅管理使用表达式过滤的消费者数据
 */
public class ConsumerFilterManager extends ConfigManager {

    /**
     * 过滤模块日志记录器
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.FILTER_LOGGER_NAME);

    /**
     * 24 小时时长常量, 单位毫秒
     */
    private static final long MS_24_HOUR = 24 * 3600 * 1000;

    /**
     * 按主题维护过滤数据映射<br>
     * key: 主题, value: 该主题下按消费组划分的过滤数据
     */
    private ConcurrentMap<String/*Topic: 主题*/, FilterDataMapByTopic>
        filterDataByTopic = new ConcurrentHashMap<>(256);

    /**
     * Broker 控制器引用, 测试场景可能为空
     */
    private transient BrokerController brokerController;
    /**
     * BloomFilter 实例, 用于构造位图过滤元数据
     */
    private transient BloomFilter bloomFilter;

    /**
     * 构造过滤管理器<br>
     * 仅用于测试场景, 使用固定 BloomFilter 参数
     */
    public ConsumerFilterManager() {
        // just for test
        // 仅用于测试
        this.bloomFilter = BloomFilter.createByFn(20, 64);
    }

    /**
     * 构造过滤管理器<br>
     * 使用 Broker 配置初始化 BloomFilter 并回填 ConsumeQueueExt 位图长度
     *
     * @param brokerController Broker 控制器
     */
    public ConsumerFilterManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.bloomFilter = BloomFilter.createByFn(
            brokerController.getBrokerConfig().getMaxErrorRateOfBloomFilter(),
            brokerController.getBrokerConfig().getExpectConsumerNumUseFilter()
        );
        // then set bit map length of store config.
        // 然后回填存储配置中的位图长度
        brokerController.getMessageStoreConfig().setBitMapLengthConsumeQueueExt(
            this.bloomFilter.getM()
        );
    }

    /**
     * Build consumer filter data.Be care, bloom filter data is not included.
     * <br>
     * 构建消费者过滤数据, 仅生成表达式与元数据, 不包含 BloomFilterData
     *
     * @return maybe null
     * 可能返回 null
     */
    public static ConsumerFilterData build(final String topic, final String consumerGroup,
        final String expression, final String type,
        final long clientVersion) {
        if (ExpressionType.isTagType(type)) {
            return null;
        }

        ConsumerFilterData consumerFilterData = new ConsumerFilterData();
        consumerFilterData.setTopic(topic);
        consumerFilterData.setConsumerGroup(consumerGroup);
        consumerFilterData.setBornTime(System.currentTimeMillis());
        consumerFilterData.setDeadTime(0);
        consumerFilterData.setExpression(expression);
        consumerFilterData.setExpressionType(type);
        consumerFilterData.setClientVersion(clientVersion);
        try {
            consumerFilterData.setCompiledExpression(
                FilterFactory.INSTANCE.get(type).compile(expression)
            );
        } catch (Throwable e) {
            log.error("parse error: expr={}, topic={}, group={}, error={}", expression, topic, consumerGroup, e.getMessage());
            return null;
        }

        return consumerFilterData;
    }

    /**
     * 批量注册消费组过滤信息<br>
     * 同时会将已不存在的主题订阅标记为死亡
     *
     * @param consumerGroup 消费组
     * @param subList 订阅列表
     */
    public void register(final String consumerGroup, final Collection<SubscriptionData> subList) {
        for (SubscriptionData subscriptionData : subList) {
            register(
                subscriptionData.getTopic(),
                consumerGroup,
                subscriptionData.getSubString(),
                subscriptionData.getExpressionType(),
                subscriptionData.getSubVersion()
            );
        }

        // make illegal topic dead.
        // 将不存在于当前订阅中的主题过滤数据标记为死亡
        Collection<ConsumerFilterData> groupFilterData = getByGroup(consumerGroup);

        Iterator<ConsumerFilterData> iterator = groupFilterData.iterator();
        while (iterator.hasNext()) {
            ConsumerFilterData filterData = iterator.next();

            boolean exist = false;
            for (SubscriptionData subscriptionData : subList) {
                if (subscriptionData.getTopic().equals(filterData.getTopic())) {
                    exist = true;
                    break;
                }
            }

            if (!exist && !filterData.isDead()) {
                filterData.setDeadTime(System.currentTimeMillis());
                log.info("Consumer filter changed: {}, make illegal topic dead:{}", consumerGroup, filterData);
            }
        }
    }

    /**
     * 注册指定主题的过滤表达式
     *
     * @param topic 主题
     * @param consumerGroup 消费组
     * @param expression 过滤表达式
     * @param type 表达式类型
     * @param clientVersion 客户端订阅版本
     * @return true 表示过滤信息已创建或更新
     */
    public boolean register(final String topic, final String consumerGroup, final String expression,
        final String type, final long clientVersion) {
        if (ExpressionType.isTagType(type)) {
            return false;
        }

        if (expression == null || expression.length() == 0) {
            return false;
        }

        FilterDataMapByTopic filterDataMapByTopic = this.filterDataByTopic.get(topic);

        if (filterDataMapByTopic == null) {
            FilterDataMapByTopic temp = new FilterDataMapByTopic(topic);
            FilterDataMapByTopic prev = this.filterDataByTopic.putIfAbsent(topic, temp);
            filterDataMapByTopic = prev != null ? prev : temp;
        }

        BloomFilterData bloomFilterData = bloomFilter.generate(consumerGroup + "#" + topic);

        return filterDataMapByTopic.register(consumerGroup, expression, type, bloomFilterData, clientVersion);
    }

    /**
     * 注销消费组过滤信息<br>
     * 对每个主题下的对应过滤条目执行死亡标记
     *
     * @param consumerGroup 消费组
     */
    public void unRegister(final String consumerGroup) {
        for (Entry<String, FilterDataMapByTopic> entry : filterDataByTopic.entrySet()) {
            entry.getValue().unRegister(consumerGroup);
        }
    }

    /**
     * 查询指定主题与消费组的过滤数据
     *
     * @param topic 主题
     * @param consumerGroup 消费组
     * @return 过滤数据, 不存在时返回 null
     */
    public ConsumerFilterData get(final String topic, final String consumerGroup) {
        if (!this.filterDataByTopic.containsKey(topic)) {
            return null;
        }
        if (this.filterDataByTopic.get(topic).getGroupFilterData().isEmpty()) {
            return null;
        }

        return this.filterDataByTopic.get(topic).getGroupFilterData().get(consumerGroup);
    }

    /**
     * 按消费组聚合查询过滤数据
     *
     * @param consumerGroup 消费组
     * @return 过滤数据集合
     */
    public Collection<ConsumerFilterData> getByGroup(final String consumerGroup) {
        Collection<ConsumerFilterData> ret = new HashSet<>();

        Iterator<FilterDataMapByTopic> topicIterator = this.filterDataByTopic.values().iterator();
        while (topicIterator.hasNext()) {
            FilterDataMapByTopic filterDataMapByTopic = topicIterator.next();

            Iterator<ConsumerFilterData> filterDataIterator = filterDataMapByTopic.getGroupFilterData().values().iterator();

            while (filterDataIterator.hasNext()) {
                ConsumerFilterData filterData = filterDataIterator.next();

                if (filterData.getConsumerGroup().equals(consumerGroup)) {
                    ret.add(filterData);
                }
            }
        }

        return ret;
    }

    /**
     * 查询指定主题的全部过滤数据
     *
     * @param topic 主题
     * @return 过滤数据集合, 不存在时返回 null
     */
    public final Collection<ConsumerFilterData> get(final String topic) {
        if (!this.filterDataByTopic.containsKey(topic)) {
            return null;
        }
        if (this.filterDataByTopic.get(topic).getGroupFilterData().isEmpty()) {
            return null;
        }

        return this.filterDataByTopic.get(topic).getGroupFilterData().values();
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    /**
     * 编码过滤管理器数据<br>
     * 默认输出非格式化 JSON
     *
     * @return JSON 字符串
     */
    @Override
    public String encode() {
        return encode(false);
    }

    /**
     * 返回过滤配置文件路径
     *
     * @return 配置文件路径
     */
    @Override
    public String configFilePath() {
        if (this.brokerController != null) {
            return BrokerPathConfigHelper.getConsumerFilterPath(
                this.brokerController.getMessageStoreConfig().getStorePathRootDir()
            );
        }
        return BrokerPathConfigHelper.getConsumerFilterPath("./unit_test");
    }

    /**
     * 解码并加载过滤数据<br>
     * 会重新编译表达式并校验 BloomFilter 兼容性
     *
     * @param jsonString 持久化 JSON 字符串
     */
    @Override
    public void decode(final String jsonString) {
        ConsumerFilterManager load = RemotingSerializable.fromJson(jsonString, ConsumerFilterManager.class);
        if (load != null && load.filterDataByTopic != null) {
            boolean bloomChanged = false;
            for (Entry<String, FilterDataMapByTopic> entry : load.filterDataByTopic.entrySet()) {
                FilterDataMapByTopic dataMapByTopic = entry.getValue();
                if (dataMapByTopic == null) {
                    continue;
                }

                for (Entry<String, ConsumerFilterData> groupEntry : dataMapByTopic.getGroupFilterData().entrySet()) {

                    ConsumerFilterData filterData = groupEntry.getValue();

                    if (filterData == null) {
                        continue;
                    }

                    try {
                        filterData.setCompiledExpression(
                                FilterFactory.INSTANCE.get(filterData.getExpressionType()).compile(filterData.getExpression())
                        );
                    } catch (Exception e) {
                        log.error("load filter data error, " + filterData, e);
                    }

                    // check whether bloom filter is changed, 检查 BloomFilter 是否变化
                    // if changed, ignore the bit map calculated before, 若变化则忽略之前计算位图
                    // 检查 BloomFilter 是否变化
                    // 若已变化, 之前持久化的位图数据将失效
                    if (!this.bloomFilter.isValid(filterData.getBloomFilterData())) {
                        bloomChanged = true;
                        log.info("Bloom filter is changed!So ignore all filter data persisted! {}, {}", this.bloomFilter, filterData.getBloomFilterData());
                        break;
                    }

                    log.info("load exist consumer filter data: {}", filterData);

                    if (filterData.getDeadTime() == 0) {
                        // we think all consumers are dead when load
                        // 加载时先视为消费者已离线, 避免脏状态长期保留
                        long deadTime = System.currentTimeMillis() - 30 * 1000;
                        filterData.setDeadTime(
                                deadTime <= filterData.getBornTime() ? filterData.getBornTime() : deadTime
                        );
                    }
                }
            }

            if (!bloomChanged) {
                this.filterDataByTopic = load.filterDataByTopic;
            }
        }
    }

    /**
     * 编码过滤管理器数据<br>
     * 编码前会先执行一次过期清理
     *
     * @param prettyFormat true 表示输出格式化 JSON
     * @return JSON 字符串
     */
    @Override
    public String encode(final boolean prettyFormat) {
        // clean
        // 编码前先清理无效过滤数据
        {
            clean();
        }
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    /**
     * 清理过期过滤数据<br>
     * 删除长时间死亡的过滤记录和空主题节点
     */
    public void clean() {
        Iterator<Map.Entry<String, FilterDataMapByTopic>> topicIterator = this.filterDataByTopic.entrySet().iterator();
        while (topicIterator.hasNext()) {
            Map.Entry<String, FilterDataMapByTopic> filterDataMapByTopic = topicIterator.next();

            Iterator<Map.Entry<String, ConsumerFilterData>> filterDataIterator
                = filterDataMapByTopic.getValue().getGroupFilterData().entrySet().iterator();

            while (filterDataIterator.hasNext()) {
                Map.Entry<String, ConsumerFilterData> filterDataByGroup = filterDataIterator.next();

                ConsumerFilterData filterData = filterDataByGroup.getValue();
                if (filterData.howLongAfterDeath() >= (this.brokerController == null ? MS_24_HOUR : this.brokerController.getBrokerConfig().getFilterDataCleanTimeSpan())) {
                    log.info("Remove filter consumer {}, died too long!", filterDataByGroup.getValue());
                    filterDataIterator.remove();
                }
            }

            if (filterDataMapByTopic.getValue().getGroupFilterData().isEmpty()) {
                log.info("Topic has no consumer, remove it! {}", filterDataMapByTopic.getKey());
                topicIterator.remove();
            }
        }
    }

    public ConcurrentMap<String, FilterDataMapByTopic> getFilterDataByTopic() {
        return filterDataByTopic;
    }

    public void setFilterDataByTopic(final ConcurrentHashMap<String, FilterDataMapByTopic> filterDataByTopic) {
        this.filterDataByTopic = filterDataByTopic;
    }

    /**
     * 单主题下的消费组过滤数据映射
     */
    public static class FilterDataMapByTopic {

        /**
         * 消费组到过滤数据映射
         */
        private ConcurrentMap<String/*consumer group: 消费组*/, ConsumerFilterData>
            groupFilterData = new ConcurrentHashMap<>();

        /**
         * 当前映射所属主题
         */
        private String topic;

        /**
         * 默认构造方法, 主要用于反序列化
         */
        public FilterDataMapByTopic() {
        }

        /**
         * 构造主题过滤映射
         *
         * @param topic 主题
         */
        public FilterDataMapByTopic(String topic) {
            this.topic = topic;
        }

        /**
         * 注销消费组过滤信息<br>
         * 当前实现通过设置死亡时间实现软删除
         *
         * @param consumerGroup 消费组
         */
        public void unRegister(String consumerGroup) {
            if (!this.groupFilterData.containsKey(consumerGroup)) {
                return;
            }

            ConsumerFilterData data = this.groupFilterData.get(consumerGroup);

            if (data == null || data.isDead()) {
                return;
            }

            long now = System.currentTimeMillis();

            log.info("Unregister consumer filter: {}, deadTime: {}", data, now);

            data.setDeadTime(now);
        }

        /**
         * 注册或更新消费组过滤数据<br>
         * 按客户端版本进行并发写保护, 仅接收更新版本的数据
         *
         * @param consumerGroup 消费组
         * @param expression 过滤表达式
         * @param type 表达式类型
         * @param bloomFilterData BloomFilter 数据
         * @param clientVersion 客户端版本
         * @return true 表示过滤数据发生变化或成功复活
         */
        public boolean register(String consumerGroup, String expression, String type, BloomFilterData bloomFilterData,
            long clientVersion) {
            ConsumerFilterData old = this.groupFilterData.get(consumerGroup);

            if (old == null) {
                ConsumerFilterData consumerFilterData = build(topic, consumerGroup, expression, type, clientVersion);
                if (consumerFilterData == null) {
                    return false;
                }
                consumerFilterData.setBloomFilterData(bloomFilterData);

                old = this.groupFilterData.putIfAbsent(consumerGroup, consumerFilterData);
                if (old == null) {
                    log.info("New consumer filter registered: {}", consumerFilterData);
                    return true;
                } else {
                    if (clientVersion <= old.getClientVersion()) {
                        if (!type.equals(old.getExpressionType()) || !expression.equals(old.getExpression())) {
                            log.warn("Ignore consumer({} : {}) filter(concurrent), because of version {} <= {}, but maybe info changed!old={}:{}, ignored={}:{}",
                                consumerGroup, topic,
                                clientVersion, old.getClientVersion(),
                                old.getExpressionType(), old.getExpression(),
                                type, expression);
                        }
                        if (clientVersion == old.getClientVersion() && old.isDead()) {
                            reAlive(old);
                            return true;
                        }

                        return false;
                    } else {
                        this.groupFilterData.put(consumerGroup, consumerFilterData);
                        log.info("New consumer filter registered(concurrent): {}, old: {}", consumerFilterData, old);
                        return true;
                    }
                }
            } else {
                if (clientVersion <= old.getClientVersion()) {
                    if (!type.equals(old.getExpressionType()) || !expression.equals(old.getExpression())) {
                        log.info("Ignore consumer({}:{}) filter, because of version {} <= {}, but maybe info changed!old={}:{}, ignored={}:{}",
                            consumerGroup, topic,
                            clientVersion, old.getClientVersion(),
                            old.getExpressionType(), old.getExpression(),
                            type, expression);
                    }
                    if (clientVersion == old.getClientVersion() && old.isDead()) {
                        reAlive(old);
                        return true;
                    }

                    return false;
                }

                boolean change = !old.getExpression().equals(expression) || !old.getExpressionType().equals(type);
                if (old.getBloomFilterData() == null && bloomFilterData != null) {
                    change = true;
                }
                if (old.getBloomFilterData() != null && !old.getBloomFilterData().equals(bloomFilterData)) {
                    change = true;
                }

                // if subscribe data is changed, or consumer is died too long.
                // 若订阅变更或历史数据异常, 则重建过滤条目
                if (change) {
                    ConsumerFilterData consumerFilterData = build(topic, consumerGroup, expression, type, clientVersion);
                    if (consumerFilterData == null) {
                        // new expression compile error, remove old, let client report error.
                        // 新表达式编译失败时删除旧数据, 由客户端感知并修复
                        this.groupFilterData.remove(consumerGroup);
                        return false;
                    }
                    consumerFilterData.setBloomFilterData(bloomFilterData);

                    this.groupFilterData.put(consumerGroup, consumerFilterData);

                    log.info("Consumer filter info change, old: {}, new: {}, change: {}",
                        old, consumerFilterData, change);

                    return true;
                } else {
                    old.setClientVersion(clientVersion);
                    if (old.isDead()) {
                        reAlive(old);
                    }
                    return true;
                }
            }
        }

        /**
         * 复活过滤数据<br>
         * 将死亡时间重置为 0
         *
         * @param filterData 过滤数据
         */
        protected void reAlive(ConsumerFilterData filterData) {
            long oldDeadTime = filterData.getDeadTime();
            filterData.setDeadTime(0);
            log.info("Re alive consumer filter: {}, oldDeadTime: {}", filterData, oldDeadTime);
        }

        public final ConsumerFilterData get(String consumerGroup) {
            return this.groupFilterData.get(consumerGroup);
        }

        public final ConcurrentMap<String, ConsumerFilterData> getGroupFilterData() {
            return this.groupFilterData;
        }

        public void setGroupFilterData(final ConcurrentHashMap<String, ConsumerFilterData> groupFilterData) {
            this.groupFilterData = groupFilterData;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(final String topic) {
            this.topic = topic;
        }
    }
}
