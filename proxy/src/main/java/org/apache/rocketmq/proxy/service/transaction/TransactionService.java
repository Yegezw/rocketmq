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

import java.util.List;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.proxy.common.ProxyContext;

/**
 * 事务消息相关服务接口
 */
public interface TransactionService {

    /**
     * 为生产者组追加多个事务主题订阅
     *
     * @param ctx 请求上下文
     * @param group 生产者组
     * @param topicList 待追加主题列表
     */
    void addTransactionSubscription(ProxyContext ctx, String group, List<String> topicList);

    /**
     * 为生产者组追加单个事务主题订阅
     *
     * @param ctx 请求上下文
     * @param group 生产者组
     * @param topic 待追加主题
     */
    void addTransactionSubscription(ProxyContext ctx, String group, String topic);

    /**
     * 用新主题集合替换生产者组事务订阅
     *
     * @param ctx 请求上下文
     * @param group 生产者组
     * @param topicList 新主题列表
     */
    void replaceTransactionSubscription(ProxyContext ctx, String group, List<String> topicList);

    /**
     * 取消生产者组全部事务主题订阅
     *
     * @param ctx 请求上下文
     * @param group 生产者组
     */
    void unSubscribeAllTransactionTopic(ProxyContext ctx, String group);

    /**
     * 按 Broker 地址记录事务数据
     *
     * @param ctx 请求上下文
     * @param brokerAddr Broker 地址
     * @param topic 主题
     * @param producerGroup 生产者组
     * @param tranStateTableOffset 事务状态表偏移量
     * @param commitLogOffset CommitLog 偏移量
     * @param transactionId 事务标识
     * @param message 原始消息
     * @return 新增的事务数据, 若 Broker 地址无法映射返回 null
     */
    TransactionData addTransactionDataByBrokerAddr(ProxyContext ctx, String brokerAddr, String topic, String producerGroup, long tranStateTableOffset, long commitLogOffset, String transactionId,
        Message message);

    /**
     * 按 Broker 名称记录事务数据
     *
     * @param ctx 请求上下文
     * @param brokerName Broker 名称
     * @param topic 主题
     * @param producerGroup 生产者组
     * @param tranStateTableOffset 事务状态表偏移量
     * @param commitLogOffset CommitLog 偏移量
     * @param transactionId 事务标识
     * @param message 原始消息
     * @return 新增的事务数据, 若 Broker 名称为空返回 null
     */
    TransactionData addTransactionDataByBrokerName(ProxyContext ctx, String brokerName, String topic, String producerGroup, long tranStateTableOffset, long commitLogOffset, String transactionId,
        Message message);

    /**
     * 生成结束事务请求数据
     *
     * @param ctx 请求上下文
     * @param topic 主题
     * @param producerGroup 生产者组
     * @param commitOrRollback 提交或回滚标记
     * @param fromTransactionCheck 是否来自事务回查
     * @param msgId 消息标识
     * @param transactionId 事务标识
     * @return 结束事务请求数据, 若无可用事务数据返回 null
     */
    EndTransactionRequestData genEndTransactionRequestHeader(ProxyContext ctx, String topic, String producerGroup, Integer commitOrRollback,
        boolean fromTransactionCheck, String msgId, String transactionId);

    /**
     * 发送事务状态回查失败时执行清理逻辑
     *
     * @param context 请求上下文
     * @param producerGroup 生产者组
     * @param transactionData 事务数据
     */
    void onSendCheckTransactionStateFailed(ProxyContext context, String producerGroup, TransactionData transactionData);
}
