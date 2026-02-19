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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * LMQ 专用消费位点管理器, 在父类基础上增加轻量消息队列位点表
 */
public class LmqConsumerOffsetManager extends ConsumerOffsetManager {
    /**
     * LMQ 消费位点表, 键为 topic@group, 值为队列 0 的消费位点
     */
    private ConcurrentHashMap<String, Long> lmqOffsetTable = new ConcurrentHashMap<>(512);

    /**
     * 默认构造, 主要供反序列化场景使用
     */
    public LmqConsumerOffsetManager() {

    }

    /**
     * 使用 broker 控制器构建 LMQ 位点管理器
     *
     * @param brokerController broker 控制器实例
     */
    public LmqConsumerOffsetManager(BrokerController brokerController) {
        super(brokerController);
    }

    /**
     * 查询指定消费组在主题队列上的消费位点, LMQ 仅使用单队列位点
     *
     * @param group 消费组名
     * @param topic 主题名
     * @param queueId 队列 ID, LMQ 场景中通常为 0
     * @return 消费位点, 不存在返回 -1
     */
    @Override
    public long queryOffset(final String group, final String topic, final int queueId) {
        if (!MixAll.isLmq(group)) {
            return super.queryOffset(group, topic, queueId);
        }
        // topic@group 组合键
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        Long offset = lmqOffsetTable.get(key);
        if (offset != null) {
            return offset;
        }
        return -1;
    }

    /**
     * 查询 LMQ 消费组位点映射, 返回结果固定写入队列 0
     *
     * @param group 消费组名
     * @param topic 主题名
     * @return 队列到位点映射
     */
    @Override
    public Map<Integer, Long> queryOffset(final String group, final String topic) {
        if (!MixAll.isLmq(group)) {
            return super.queryOffset(group, topic);
        }
        Map<Integer, Long> map = new HashMap<>();
        // topic@group 组合键
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        Long offset = lmqOffsetTable.get(key);
        if (offset != null) {
            map.put(0, offset);
        }
        return map;
    }

    /**
     * 提交消费位点, 非 LMQ 组委托父类处理
     *
     * @param clientHost 客户端来源地址
     * @param group 消费组名
     * @param topic 主题名
     * @param queueId 队列 ID, LMQ 场景中通常为 0
     * @param offset 待提交位点
     */
    @Override
    public void commitOffset(final String clientHost, final String group, final String topic, final int queueId,
        final long offset) {
        if (!MixAll.isLmq(group)) {
            super.commitOffset(clientHost, group, topic, queueId, offset);
            return;
        }
        // topic@group 组合键
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        lmqOffsetTable.put(key, offset);
    }

    /**
     * 序列化当前位点管理器数据
     *
     * @return JSON 字符串
     */
    @Override
    public String encode() {
        return this.encode(false);
    }

    /**
     * 返回 LMQ 位点持久化文件路径
     *
     * @return 位点文件绝对路径
     */
    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getLmqConsumerOffsetPath(brokerController.getMessageStoreConfig().getStorePathRootDir());
    }

    /**
     * 反序列化并恢复 LMQ 位点数据
     *
     * @param jsonString 位点 JSON 字符串
     */
    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            LmqConsumerOffsetManager obj = RemotingSerializable.fromJson(jsonString, LmqConsumerOffsetManager.class);
            if (obj != null) {
                super.setOffsetTable(obj.getOffsetTable());
                this.lmqOffsetTable = obj.lmqOffsetTable;
            }
        }
    }

    /**
     * 按指定格式序列化 LMQ 位点数据
     *
     * @param prettyFormat 是否格式化输出
     * @return JSON 字符串
     */
    @Override
    public String encode(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    public ConcurrentHashMap<String, Long> getLmqOffsetTable() {
        return lmqOffsetTable;
    }

    public void setLmqOffsetTable(ConcurrentHashMap<String, Long> lmqOffsetTable) {
        this.lmqOffsetTable = lmqOffsetTable;
    }

    /**
     * 按消费组删除位点数据, LMQ 组从 lmqOffsetTable 移除
     *
     * @param group 消费组名
     */
    @Override
    public void removeOffset(String group) {
        if (!MixAll.isLmq(group)) {
            super.removeOffset(group);
            return;
        }
        Iterator<Map.Entry<String, Long>> it = this.lmqOffsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Long> next = it.next();
            String topicAtGroup = next.getKey();
            if (topicAtGroup.contains(group)) {
                String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
                if (arrays.length == 2 && group.equals(arrays[1])) {
                    it.remove();
                    removeConsumerOffset(topicAtGroup);
                    LOG.warn("clean lmq group offset {}", topicAtGroup);
                }
            }
        }
    }
}
