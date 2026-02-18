/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.service.transaction;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;

/**
 * 事务服务抽象基类, 提供公共事务数据管理能力
 */
public abstract class AbstractTransactionService implements TransactionService, StartAndShutdown {

    /**
     * 事务数据管理器
     */
    protected TransactionDataManager transactionDataManager = new TransactionDataManager();

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
     * @return 新增的事务数据, 若 Broker 名称为空返回 null
     */
    @Override
    public TransactionData addTransactionDataByBrokerAddr(ProxyContext ctx, String brokerAddr, String topic, String producerGroup, long tranStateTableOffset, long commitLogOffset, String transactionId,
        Message message) {
        return this.addTransactionDataByBrokerName(ctx, this.getBrokerNameByAddr(brokerAddr), topic, producerGroup, tranStateTableOffset, commitLogOffset, transactionId, message);
    }

    /**
     * 按 Broker 名称记录事务数据并写入缓存
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
    @Override
    public TransactionData addTransactionDataByBrokerName(ProxyContext ctx, String brokerName, String topic, String producerGroup, long tranStateTableOffset, long commitLogOffset, String transactionId,
        Message message) {
        if (StringUtils.isBlank(brokerName)) {
            return null;
        }
        TransactionData transactionData = new TransactionData(
            brokerName,
            topic,
            tranStateTableOffset, commitLogOffset, transactionId,
            System.currentTimeMillis(),
            ConfigurationManager.getProxyConfig().getTransactionDataExpireMillis());

        this.transactionDataManager.addTransactionData(
            producerGroup,
            transactionId,
            transactionData
        );
        return transactionData;
    }

    /**
     * 生成结束事务请求头并绑定对应 Broker
     *
     * @param ctx 请求上下文
     * @param topic 主题
     * @param producerGroup 生产者组
     * @param commitOrRollback 提交或回滚标记
     * @param fromTransactionCheck 是否来自事务回查
     * @param msgId 消息标识
     * @param transactionId 事务标识
     * @return 结束事务请求数据, 若事务数据不存在返回 null
     */
    @Override
    public EndTransactionRequestData genEndTransactionRequestHeader(ProxyContext ctx, String topic, String producerGroup, Integer commitOrRollback,
        boolean fromTransactionCheck, String msgId, String transactionId) {
        TransactionData transactionData = this.transactionDataManager.pollNoExpireTransactionData(producerGroup, transactionId);
        if (transactionData == null) {
            return null;
        }
        EndTransactionRequestHeader header = new EndTransactionRequestHeader();
        header.setTopic(topic);
        header.setProducerGroup(producerGroup);
        header.setCommitOrRollback(commitOrRollback);
        header.setFromTransactionCheck(fromTransactionCheck);
        header.setMsgId(msgId);
        header.setTransactionId(transactionId);
        header.setTranStateTableOffset(transactionData.getTranStateTableOffset());
        header.setCommitLogOffset(transactionData.getCommitLogOffset());
        return new EndTransactionRequestData(transactionData.getBrokerName(), header);
    }

    /**
     * 回查失败时删除事务缓存数据
     *
     * @param context 请求上下文
     * @param producerGroup 生产者组
     * @param transactionData 事务数据
     */
    @Override
    public void onSendCheckTransactionStateFailed(ProxyContext context, String producerGroup, TransactionData transactionData) {
        this.transactionDataManager.removeTransactionData(producerGroup, transactionData.getTransactionId(), transactionData);
    }

    protected abstract String getBrokerNameByAddr(String brokerAddr);

    /**
     * 停止事务服务并清理事务数据线程
     */
    @Override
    public void shutdown() throws Exception {
        this.transactionDataManager.shutdown();
    }

    /**
     * 启动事务服务并启动事务数据线程
     */
    @Override
    public void start() throws Exception {
        this.transactionDataManager.start();
    }
}
