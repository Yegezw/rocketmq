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
package org.apache.rocketmq.broker.loadbalance;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody;

public class MessageRequestModeManager extends ConfigManager {

    /**
     * Broker 控制器引用, 用于解析消息请求模式配置文件路径
     */
    private transient BrokerController brokerController;

    /**
     * 消息请求模式映射表, 按 topic 和 consumerGroup 两级维度保存消费模式配置
     */
    private ConcurrentHashMap<String/*topic: 主题*/, ConcurrentHashMap<String/*consumerGroup: 消费组*/, SetMessageRequestModeRequestBody>>
        messageRequestModeMap = new ConcurrentHashMap<>();

    /**
     * 默认构造方法, 仅用于反序列化创建对象
     */
    public MessageRequestModeManager() {
        // empty construct for decode, 用于反序列化场景
    }

    /**
     * 使用 Broker 控制器构建管理器实例, 供运行时读写与持久化消息请求模式配置
     *
     * @param brokerController Broker 控制器
     */
    public MessageRequestModeManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void setMessageRequestMode(String topic, String consumerGroup, SetMessageRequestModeRequestBody requestBody) {
        ConcurrentHashMap<String, SetMessageRequestModeRequestBody> consumerGroup2ModeMap = messageRequestModeMap.get(topic);
        if (consumerGroup2ModeMap == null) {
            consumerGroup2ModeMap = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, SetMessageRequestModeRequestBody> pre =
                messageRequestModeMap.putIfAbsent(topic, consumerGroup2ModeMap);
            if (pre != null) {
                consumerGroup2ModeMap = pre;
            }
        }
        consumerGroup2ModeMap.put(consumerGroup, requestBody);
    }

    public SetMessageRequestModeRequestBody getMessageRequestMode(String topic, String consumerGroup) {
        ConcurrentHashMap<String, SetMessageRequestModeRequestBody> consumerGroup2ModeMap = messageRequestModeMap.get(topic);
        if (consumerGroup2ModeMap != null) {
            return consumerGroup2ModeMap.get(consumerGroup);
        }

        return null;
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<String, SetMessageRequestModeRequestBody>> getMessageRequestModeMap() {
        return this.messageRequestModeMap;
    }

    public void setMessageRequestModeMap(ConcurrentHashMap<String, ConcurrentHashMap<String, SetMessageRequestModeRequestBody>> messageRequestModeMap) {
        this.messageRequestModeMap = messageRequestModeMap;
    }

    /**
     * 编码当前配置对象, 生成紧凑 JSON 字符串
     *
     * @return 配置内容对应的 JSON 文本
     */
    @Override
    public String encode() {
        return this.encode(false);
    }

    /**
     * 返回消息请求模式配置文件路径, 用于 ConfigManager 统一落盘
     *
     * @return 配置文件绝对路径
     */
    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getMessageRequestModePath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
    }

    /**
     * 从 JSON 字符串恢复消息请求模式配置, 仅在输入有效时覆盖当前内存映射
     *
     * @param jsonString 配置 JSON 文本
     */
    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            MessageRequestModeManager obj = RemotingSerializable.fromJson(jsonString, MessageRequestModeManager.class);
            if (obj != null) {
                this.messageRequestModeMap = obj.messageRequestModeMap;
            }
        }
    }

    /**
     * 按指定格式编码当前配置对象
     *
     * @param prettyFormat 是否输出格式化 JSON
     * @return 配置内容对应的 JSON 文本
     */
    @Override
    public String encode(boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }
}
