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

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.proxy.common.ProxyContext;

import java.util.List;

/**
 * no need to implements, because the channel of producer will put into the broker's producerManager
 * <br>
 * 本地模式无需额外实现, 因为生产者通道会直接注册到 Broker 的 producerManager
 */
public class LocalTransactionService extends AbstractTransactionService {

    /**
     * Broker 运行配置
     */
    protected final BrokerConfig brokerConfig;

    /**
     * 初始化本地事务服务
     *
     * @param brokerConfig Broker 配置
     */
    public LocalTransactionService(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
    }

    /**
     * 本地模式下追加事务订阅无需额外处理
     *
     * @param ctx 请求上下文
     * @param group 生产者组
     * @param topicList 主题列表
     */
    @Override
    public void addTransactionSubscription(ProxyContext ctx, String group, List<String> topicList) {

    }

    /**
     * 本地模式下追加单个事务订阅无需额外处理
     *
     * @param ctx 请求上下文
     * @param group 生产者组
     * @param topic 主题
     */
    @Override
    public void addTransactionSubscription(ProxyContext ctx, String group, String topic) {

    }

    /**
     * 本地模式下替换事务订阅无需额外处理
     *
     * @param ctx 请求上下文
     * @param group 生产者组
     * @param topicList 主题列表
     */
    @Override
    public void replaceTransactionSubscription(ProxyContext ctx, String group, List<String> topicList) {

    }

    /**
     * 本地模式下取消事务订阅无需额外处理
     *
     * @param ctx 请求上下文
     * @param group 生产者组
     */
    @Override
    public void unSubscribeAllTransactionTopic(ProxyContext ctx, String group) {

    }

    @Override
    protected String getBrokerNameByAddr(String brokerAddr) {
        return this.brokerConfig.getBrokerName();
    }
}
