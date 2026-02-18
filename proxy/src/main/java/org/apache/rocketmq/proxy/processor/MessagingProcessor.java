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
package org.apache.rocketmq.proxy.processor;

import io.netty.channel.Channel;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ProducerChangeListener;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.proxy.common.Address;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.message.ReceiptHandleMessage;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.proxy.service.route.ProxyTopicRouteData;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

/**
 * Proxy 消息处理器抽象
 */
public interface MessagingProcessor extends StartAndShutdown {

    /**
     * 默认请求超时时间, 单位毫秒
     */
    long DEFAULT_TIMEOUT_MILLS = Duration.ofSeconds(2).toMillis();

    SubscriptionGroupConfig getSubscriptionGroupConfig(
        ProxyContext ctx,
        String consumerGroupName
    );

    ProxyTopicRouteData getTopicRouteDataForProxy(
        ProxyContext ctx,
        List<Address> requestHostAndPortList,
        String topicName
    ) throws Exception;

    /**
     * 使用默认超时发送消息
     *
     * @param ctx 请求上下文
     * @param queueSelector 队列选择器
     * @param producerGroup 生产组
     * @param sysFlag 系统标记
     * @param msg 消息列表
     * @return 异步发送结果
     */
    default CompletableFuture<List<SendResult>> sendMessage(
        ProxyContext ctx,
        QueueSelector queueSelector,
        String producerGroup,
        int sysFlag,
        List<Message> msg
    ) {
        return sendMessage(ctx, queueSelector, producerGroup, sysFlag, msg, DEFAULT_TIMEOUT_MILLS);
    }

    /**
     * 按指定超时发送消息
     *
     * @param ctx 请求上下文
     * @param queueSelector 队列选择器
     * @param producerGroup 生产组
     * @param sysFlag 系统标记
     * @param msg 消息列表
     * @param timeoutMillis 超时时间, 单位毫秒
     * @return 异步发送结果
     */
    CompletableFuture<List<SendResult>> sendMessage(
        ProxyContext ctx,
        QueueSelector queueSelector,
        String producerGroup,
        int sysFlag,
        List<Message> msg,
        long timeoutMillis
    );

    /**
     * 使用默认超时将消息转入死信队列
     *
     * @param ctx 请求上下文
     * @param handle 消费句柄
     * @param messageId 消息 ID
     * @param groupName 消费组
     * @param topicName 主题名
     * @return 异步命令结果
     */
    default CompletableFuture<RemotingCommand> forwardMessageToDeadLetterQueue(
        ProxyContext ctx,
        ReceiptHandle handle,
        String messageId,
        String groupName,
        String topicName
    ) {
        return forwardMessageToDeadLetterQueue(ctx, handle, messageId, groupName, topicName, DEFAULT_TIMEOUT_MILLS);
    }

    /**
     * 按指定超时将消息转入死信队列
     *
     * @param ctx 请求上下文
     * @param handle 消费句柄
     * @param messageId 消息 ID
     * @param groupName 消费组
     * @param topicName 主题名
     * @param timeoutMillis 超时时间, 单位毫秒
     * @return 异步命令结果
     */
    CompletableFuture<RemotingCommand> forwardMessageToDeadLetterQueue(
        ProxyContext ctx,
        ReceiptHandle handle,
        String messageId,
        String groupName,
        String topicName,
        long timeoutMillis
    );

    /**
     * 使用默认超时结束事务消息
     *
     * @param ctx 请求上下文
     * @param topic 主题名
     * @param transactionId 事务 ID
     * @param messageId 消息 ID
     * @param producerGroup 生产组
     * @param transactionStatus 事务状态
     * @param fromTransactionCheck 是否来自事务回查
     * @return 异步完成结果
     */
    default CompletableFuture<Void> endTransaction(
        ProxyContext ctx,
        String topic,
        String transactionId,
        String messageId,
        String producerGroup,
        TransactionStatus transactionStatus,
        boolean fromTransactionCheck
    ) {
        return endTransaction(ctx, topic, transactionId, messageId, producerGroup, transactionStatus, fromTransactionCheck, DEFAULT_TIMEOUT_MILLS);
    }

    /**
     * 按指定超时结束事务消息
     *
     * @param ctx 请求上下文
     * @param topic 主题名
     * @param transactionId 事务 ID
     * @param messageId 消息 ID
     * @param producerGroup 生产组
     * @param transactionStatus 事务状态
     * @param fromTransactionCheck 是否来自事务回查
     * @param timeoutMillis 超时时间, 单位毫秒
     * @return 异步完成结果
     */
    CompletableFuture<Void> endTransaction(
        ProxyContext ctx,
        String topic,
        String transactionId,
        String messageId,
        String producerGroup,
        TransactionStatus transactionStatus,
        boolean fromTransactionCheck,
        long timeoutMillis
    );

    /**
     * POP 拉取消息
     *
     * @param ctx 请求上下文
     * @param queueSelector 队列选择器
     * @param consumerGroup 消费组
     * @param topic 主题名
     * @param maxMsgNums 最大消息数
     * @param invisibleTime 不可见时长
     * @param pollTime 轮询时长
     * @param initMode 初始化模式
     * @param subscriptionData 订阅信息
     * @param fifo 是否 FIFO 消费
     * @param popMessageResultFilter POP 结果过滤器
     * @param attemptId 尝试 ID
     * @param timeoutMillis 超时时间, 单位毫秒
     * @return 异步 POP 结果
     */
    CompletableFuture<PopResult> popMessage(
        ProxyContext ctx,
        QueueSelector queueSelector,
        String consumerGroup,
        String topic,
        int maxMsgNums,
        long invisibleTime,
        long pollTime,
        int initMode,
        SubscriptionData subscriptionData,
        boolean fifo,
        PopMessageResultFilter popMessageResultFilter,
        String attemptId,
        long timeoutMillis
    );

    /**
     * 使用默认超时 ACK 消息
     *
     * @param ctx 请求上下文
     * @param handle 消费句柄
     * @param messageId 消息 ID
     * @param consumerGroup 消费组
     * @param topic 主题名
     * @return 异步 ACK 结果
     */
    default CompletableFuture<AckResult> ackMessage(
        ProxyContext ctx,
        ReceiptHandle handle,
        String messageId,
        String consumerGroup,
        String topic
    ) {
        return ackMessage(ctx, handle, messageId, consumerGroup, topic, DEFAULT_TIMEOUT_MILLS);
    }

    /**
     * 按指定超时 ACK 消息
     *
     * @param ctx 请求上下文
     * @param handle 消费句柄
     * @param messageId 消息 ID
     * @param consumerGroup 消费组
     * @param topic 主题名
     * @param timeoutMillis 超时时间, 单位毫秒
     * @return 异步 ACK 结果
     */
    CompletableFuture<AckResult> ackMessage(
        ProxyContext ctx,
        ReceiptHandle handle,
        String messageId,
        String consumerGroup,
        String topic,
        long timeoutMillis
    );

    /**
     * 使用默认超时批量 ACK 消息
     *
     * @param ctx 请求上下文
     * @param handleMessageList 回执句柄消息列表
     * @param consumerGroup 消费组
     * @param topic 主题名
     * @return 异步批量 ACK 结果
     */
    default CompletableFuture<List<BatchAckResult>> batchAckMessage(
        ProxyContext ctx,
        List<ReceiptHandleMessage> handleMessageList,
        String consumerGroup,
        String topic
    ) {
        return batchAckMessage(ctx, handleMessageList, consumerGroup, topic, DEFAULT_TIMEOUT_MILLS);
    }

    /**
     * 按指定超时批量 ACK 消息
     *
     * @param ctx 请求上下文
     * @param handleMessageList 回执句柄消息列表
     * @param consumerGroup 消费组
     * @param topic 主题名
     * @param timeoutMillis 超时时间, 单位毫秒
     * @return 异步批量 ACK 结果
     */
    CompletableFuture<List<BatchAckResult>> batchAckMessage(
        ProxyContext ctx,
        List<ReceiptHandleMessage> handleMessageList,
        String consumerGroup,
        String topic,
        long timeoutMillis
    );

    /**
     * 使用默认超时修改消息不可见时长
     *
     * @param ctx 请求上下文
     * @param handle 消费句柄
     * @param messageId 消息 ID
     * @param groupName 消费组
     * @param topicName 主题名
     * @param invisibleTime 不可见时长
     * @return 异步 ACK 结果
     */
    default CompletableFuture<AckResult> changeInvisibleTime(
        ProxyContext ctx,
        ReceiptHandle handle,
        String messageId,
        String groupName,
        String topicName,
        long invisibleTime
    ) {
        return changeInvisibleTime(ctx, handle, messageId, groupName, topicName, invisibleTime, DEFAULT_TIMEOUT_MILLS);
    }

    /**
     * 按指定超时修改消息不可见时长
     *
     * @param ctx 请求上下文
     * @param handle 消费句柄
     * @param messageId 消息 ID
     * @param groupName 消费组
     * @param topicName 主题名
     * @param invisibleTime 不可见时长
     * @param timeoutMillis 超时时间, 单位毫秒
     * @return 异步 ACK 结果
     */
    CompletableFuture<AckResult> changeInvisibleTime(
        ProxyContext ctx,
        ReceiptHandle handle,
        String messageId,
        String groupName,
        String topicName,
        long invisibleTime,
        long timeoutMillis
    );

    /**
     * 拉取消息
     *
     * @param ctx 请求上下文
     * @param messageQueue 消息队列
     * @param consumerGroup 消费组
     * @param queueOffset 队列偏移量
     * @param maxMsgNums 最大消息数
     * @param sysFlag 系统标记
     * @param commitOffset 提交偏移量
     * @param suspendTimeoutMillis 挂起超时
     * @param subscriptionData 订阅信息
     * @param timeoutMillis 超时时间, 单位毫秒
     * @return 异步拉取结果
     */
    CompletableFuture<PullResult> pullMessage(
        ProxyContext ctx,
        MessageQueue messageQueue,
        String consumerGroup,
        long queueOffset,
        int maxMsgNums,
        int sysFlag,
        long commitOffset,
        long suspendTimeoutMillis,
        SubscriptionData subscriptionData,
        long timeoutMillis
    );

    /**
     * 同步更新消费位点
     *
     * @param ctx 请求上下文
     * @param messageQueue 消息队列
     * @param consumerGroup 消费组
     * @param commitOffset 提交偏移量
     * @param timeoutMillis 超时时间, 单位毫秒
     * @return 异步完成结果
     */
    CompletableFuture<Void> updateConsumerOffset(
        ProxyContext ctx,
        MessageQueue messageQueue,
        String consumerGroup,
        long commitOffset,
        long timeoutMillis
    );

    /**
     * 异步更新消费位点
     *
     * @param ctx 请求上下文
     * @param messageQueue 消息队列
     * @param consumerGroup 消费组
     * @param commitOffset 提交偏移量
     * @param timeoutMillis 超时时间, 单位毫秒
     * @return 异步完成结果
     */
    CompletableFuture<Void> updateConsumerOffsetAsync(
        ProxyContext ctx,
        MessageQueue messageQueue,
        String consumerGroup,
        long commitOffset,
        long timeoutMillis
    );

    /**
     * 查询消费位点
     *
     * @param ctx 请求上下文
     * @param messageQueue 消息队列
     * @param consumerGroup 消费组
     * @param timeoutMillis 超时时间, 单位毫秒
     * @return 异步消费位点
     */
    CompletableFuture<Long> queryConsumerOffset(
        ProxyContext ctx,
        MessageQueue messageQueue,
        String consumerGroup,
        long timeoutMillis
    );

    /**
     * 批量锁定队列
     *
     * @param ctx 请求上下文
     * @param mqSet 队列集合
     * @param consumerGroup 消费组
     * @param clientId 客户端 ID
     * @param timeoutMillis 超时时间, 单位毫秒
     * @return 异步锁定成功队列集合
     */
    CompletableFuture<Set<MessageQueue>> lockBatchMQ(
        ProxyContext ctx,
        Set<MessageQueue> mqSet,
        String consumerGroup,
        String clientId,
        long timeoutMillis
    );

    /**
     * 批量解锁队列
     *
     * @param ctx 请求上下文
     * @param mqSet 队列集合
     * @param consumerGroup 消费组
     * @param clientId 客户端 ID
     * @param timeoutMillis 超时时间, 单位毫秒
     * @return 异步完成结果
     */
    CompletableFuture<Void> unlockBatchMQ(
        ProxyContext ctx,
        Set<MessageQueue> mqSet,
        String consumerGroup,
        String clientId,
        long timeoutMillis
    );

    CompletableFuture<Long> getMaxOffset(
        ProxyContext ctx,
        MessageQueue messageQueue,
        long timeoutMillis
    );

    CompletableFuture<Long> getMinOffset(
        ProxyContext ctx,
        MessageQueue messageQueue,
        long timeoutMillis
    );

    /**
     * 撤回延迟消息
     *
     * @param ctx 请求上下文
     * @param topic 主题名
     * @param recallHandle 撤回句柄
     * @param timeoutMillis 超时时间, 单位毫秒
     * @return 异步撤回结果
     */
    CompletableFuture<String> recallMessage(
        ProxyContext ctx,
        String topic,
        String recallHandle,
        long timeoutMillis
    );

    /**
     * 向指定 Broker 转发同步请求
     *
     * @param ctx 请求上下文
     * @param brokerName Broker 名称
     * @param request 请求命令
     * @param timeoutMillis 超时时间, 单位毫秒
     * @return 异步响应命令
     */
    CompletableFuture<RemotingCommand> request(ProxyContext ctx, String brokerName, RemotingCommand request,
        long timeoutMillis);

    /**
     * 向指定 Broker 转发单向请求
     *
     * @param ctx 请求上下文
     * @param brokerName Broker 名称
     * @param request 请求命令
     * @param timeoutMillis 超时时间, 单位毫秒
     * @return 异步完成结果
     */
    CompletableFuture<Void> requestOneway(ProxyContext ctx, String brokerName, RemotingCommand request,
        long timeoutMillis);

    /**
     * 注册生产者连接
     *
     * @param ctx 请求上下文
     * @param producerGroup 生产组
     * @param clientChannelInfo 客户端通道信息
     */
    void registerProducer(
        ProxyContext ctx,
        String producerGroup,
        ClientChannelInfo clientChannelInfo
    );

    /**
     * 反注册生产者连接
     *
     * @param ctx 请求上下文
     * @param producerGroup 生产组
     * @param clientChannelInfo 客户端通道信息
     */
    void unRegisterProducer(
        ProxyContext ctx,
        String producerGroup,
        ClientChannelInfo clientChannelInfo
    );

    /**
     * 查找生产者通道
     *
     * @param ctx 请求上下文
     * @param producerGroup 生产组
     * @param clientId 客户端 ID
     * @return 匹配通道, 未命中时返回空
     */
    Channel findProducerChannel(
        ProxyContext ctx,
        String producerGroup,
        String clientId
    );

    /**
     * 注册生产者变更监听器
     *
     * @param producerChangeListener 生产者变更监听器
     */
    void registerProducerListener(
        ProducerChangeListener producerChangeListener
    );

    /**
     * 注册消费者连接与订阅信息
     *
     * @param ctx 请求上下文
     * @param consumerGroup 消费组
     * @param clientChannelInfo 客户端通道信息
     * @param consumeType 消费类型
     * @param messageModel 消费模型
     * @param consumeFromWhere 消费起点
     * @param subList 订阅集合
     * @param updateSubscription 是否更新订阅
     */
    void registerConsumer(
        ProxyContext ctx,
        String consumerGroup,
        ClientChannelInfo clientChannelInfo,
        ConsumeType consumeType,
        MessageModel messageModel,
        ConsumeFromWhere consumeFromWhere,
        Set<SubscriptionData> subList,
        boolean updateSubscription
    );

    /**
     * 查找消费者通道信息
     *
     * @param ctx 请求上下文
     * @param consumerGroup 消费组
     * @param channel 通道
     * @return 客户端通道信息, 未命中时返回空
     */
    ClientChannelInfo findConsumerChannel(
        ProxyContext ctx,
        String consumerGroup,
        Channel channel
    );

    /**
     * 反注册消费者连接
     *
     * @param ctx 请求上下文
     * @param consumerGroup 消费组
     * @param clientChannelInfo 客户端通道信息
     */
    void unRegisterConsumer(
        ProxyContext ctx,
        String consumerGroup,
        ClientChannelInfo clientChannelInfo
    );

    /**
     * 注册消费者变更监听器
     *
     * @param consumerIdsChangeListener 消费者变更监听器
     */
    void registerConsumerListener(
        ConsumerIdsChangeListener consumerIdsChangeListener
    );

    /**
     * 处理连接关闭事件
     *
     * @param remoteAddr 远端地址
     * @param channel 关闭的通道
     */
    void doChannelCloseEvent(String remoteAddr, Channel channel);

    ConsumerGroupInfo getConsumerGroupInfo(ProxyContext ctx, String consumerGroup);

    /**
     * 增加事务主题订阅关系
     *
     * @param ctx 请求上下文
     * @param producerGroup 生产组
     * @param topic 主题名
     */
    void addTransactionSubscription(
        ProxyContext ctx,
        String producerGroup,
        String topic
    );

    ProxyRelayService getProxyRelayService();

    MetadataService getMetadataService();

    /**
     * 记录消息回执句柄
     *
     * @param ctx 请求上下文
     * @param channel 客户端通道
     * @param group 消费组
     * @param msgID 消息 ID
     * @param messageReceiptHandle 回执句柄信息
     */
    void addReceiptHandle(ProxyContext ctx, Channel channel, String group, String msgID,
        MessageReceiptHandle messageReceiptHandle);

    /**
     * 移除并返回消息回执句柄
     *
     * @param ctx 请求上下文
     * @param channel 客户端通道
     * @param group 消费组
     * @param msgID 消息 ID
     * @param receiptHandle 回执句柄字符串
     * @return 回执句柄信息, 未命中时返回空
     */
    MessageReceiptHandle removeReceiptHandle(ProxyContext ctx, Channel channel, String group, String msgID,
        String receiptHandle);
}
