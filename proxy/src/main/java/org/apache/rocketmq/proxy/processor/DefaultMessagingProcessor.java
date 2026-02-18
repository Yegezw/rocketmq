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

import com.alibaba.fastjson2.JSON;
import io.netty.channel.Channel;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ProducerChangeListener;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.common.utils.AbstractStartAndShutdown;
import org.apache.rocketmq.proxy.common.Address;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.proxy.service.ServiceManagerFactory;
import org.apache.rocketmq.proxy.service.message.ReceiptHandleMessage;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.proxy.service.route.ProxyTopicRouteData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Proxy 消息处理器
 */
public class DefaultMessagingProcessor extends AbstractStartAndShutdown implements MessagingProcessor {

    /**
     * Proxy 服务管理器
     */
    protected ServiceManager serviceManager;
    /**
     * 生产侧处理器
     */
    protected ProducerProcessor producerProcessor;
    /**
     * 消费侧处理器
     */
    protected ConsumerProcessor consumerProcessor;
    /**
     * 事务处理器
     */
    protected TransactionProcessor transactionProcessor;
    /**
     * 客户端管理处理器
     */
    protected ClientProcessor clientProcessor;
    /**
     * Broker 请求处理器
     */
    protected RequestBrokerProcessor requestBrokerProcessor;
    /**
     * 收据句柄处理器
     */
    protected ReceiptHandleProcessor receiptHandleProcessor;

    /**
     * CPU * 1 + 10000; 生产处理线程池
     */
    protected ThreadPoolExecutor producerProcessorExecutor;
    /**
     * CPU * 1 + 10000; 消费处理线程池
     */
    protected ThreadPoolExecutor consumerProcessorExecutor;
    /**
     * RocketMQ 安装目录
     */
    protected static final String ROCKETMQ_HOME = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY,
        System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    /**
     * 构造默认消息处理器, 并初始化子处理器与线程池
     *
     * @param serviceManager 服务管理器
     */
    protected DefaultMessagingProcessor(ServiceManager serviceManager) {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        this.producerProcessorExecutor = ThreadPoolMonitor.createAndMonitor(
            proxyConfig.getProducerProcessorThreadPoolNums(),
            proxyConfig.getProducerProcessorThreadPoolNums(),
            1,
            TimeUnit.MINUTES,
            "ProducerProcessorExecutor",
            proxyConfig.getProducerProcessorThreadPoolQueueCapacity()
        );
        this.consumerProcessorExecutor = ThreadPoolMonitor.createAndMonitor(
            proxyConfig.getConsumerProcessorThreadPoolNums(),
            proxyConfig.getConsumerProcessorThreadPoolNums(),
            1,
            TimeUnit.MINUTES,
            "ConsumerProcessorExecutor",
            proxyConfig.getConsumerProcessorThreadPoolQueueCapacity()
        );

        this.serviceManager = serviceManager;
        this.producerProcessor = new ProducerProcessor(this, serviceManager, this.producerProcessorExecutor);
        this.consumerProcessor = new ConsumerProcessor(this, serviceManager, this.consumerProcessorExecutor);
        this.transactionProcessor = new TransactionProcessor(this, serviceManager);
        this.clientProcessor = new ClientProcessor(this, serviceManager);
        this.requestBrokerProcessor = new RequestBrokerProcessor(this, serviceManager);
        this.receiptHandleProcessor = new ReceiptHandleProcessor(this, serviceManager);

        this.init();
    }

    /**
     * 创建本地模式消息处理器
     *
     * @param brokerController 本地 Broker 控制器
     * @return 默认消息处理器
     */
    public static DefaultMessagingProcessor createForLocalMode(BrokerController brokerController) {
        return createForLocalMode(brokerController, null);
    }

    /**
     * 创建本地模式消息处理器, 支持自定义 RPC Hook
     *
     * @param brokerController 本地 Broker 控制器
     * @param rpcHook RPC Hook
     * @return 默认消息处理器
     */
    public static DefaultMessagingProcessor createForLocalMode(BrokerController brokerController, RPCHook rpcHook) {
        return new DefaultMessagingProcessor(ServiceManagerFactory.createForLocalMode(brokerController, rpcHook));
    }

    /**
     * 创建集群模式消息处理器, 并按配置装配 ACL Hook
     *
     * @return 默认消息处理器
     */
    public static DefaultMessagingProcessor createForClusterMode() {
        RPCHook rpcHook = null;
        if (!ConfigurationManager.getProxyConfig().isEnableAclRpcHookForClusterMode()) {
            return createForClusterMode(rpcHook);
        }
        AuthConfig authConfig = ConfigurationManager.getAuthConfig();
        if (StringUtils.isNotBlank(authConfig.getInnerClientAuthenticationCredentials())) {
            SessionCredentials sessionCredentials =
                JSON.parseObject(authConfig.getInnerClientAuthenticationCredentials(), SessionCredentials.class);
            if (StringUtils.isNotBlank(sessionCredentials.getAccessKey()) && StringUtils.isNotBlank(sessionCredentials.getSecretKey())) {
                rpcHook = new AclClientRPCHook(sessionCredentials);
            }
        } else {
            rpcHook = AclUtils.getAclRPCHook(ROCKETMQ_HOME + MixAll.ACL_CONF_TOOLS_FILE);
        }
        return createForClusterMode(rpcHook);
    }

    /**
     * 创建集群模式消息处理器
     *
     * @param rpcHook RPC Hook
     * @return 默认消息处理器
     */
    public static DefaultMessagingProcessor createForClusterMode(RPCHook rpcHook) {
        return new DefaultMessagingProcessor(ServiceManagerFactory.createForClusterMode(rpcHook));
    }

    /**
     * 注册生命周期回调, 确保资源可统一关闭
     */
    protected void init() {
        this.appendStartAndShutdown(this.serviceManager);
        this.appendShutdown(this.producerProcessorExecutor::shutdown);
        this.appendShutdown(this.consumerProcessorExecutor::shutdown);
    }

    @Override
    public SubscriptionGroupConfig getSubscriptionGroupConfig(ProxyContext ctx, String consumerGroupName) {
        return this.serviceManager.getMetadataService().getSubscriptionGroupConfig(ctx, consumerGroupName);
    }

    @Override
    public ProxyTopicRouteData getTopicRouteDataForProxy(ProxyContext ctx, List<Address> requestHostAndPortList,
        String topicName) throws Exception {
        return this.serviceManager.getTopicRouteService().getTopicRouteForProxy(ctx, requestHostAndPortList, topicName);
    }

    /**
     * 发送消息到 Broker
     *
     * @param ctx           Proxy 上下文
     * @param queueSelector 队列选择器
     * @param producerGroup 生产组
     * @param sysFlag       系统标记
     * @param msg           消息集合
     * @param timeoutMillis 超时时间
     * @return 发送结果集合
     */
    @Override
    public CompletableFuture<List<SendResult>> sendMessage(ProxyContext ctx, QueueSelector queueSelector,
        String producerGroup, int sysFlag, List<Message> msg, long timeoutMillis) {
        return this.producerProcessor.sendMessage(ctx, queueSelector, producerGroup, sysFlag, msg, timeoutMillis);
    }

    /**
     * 转发消息到死信队列
     *
     * @param ctx           Proxy 上下文
     * @param handle        收据句柄
     * @param messageId     消息 ID
     * @param groupName     消费组
     * @param topicName     主题
     * @param timeoutMillis 超时时间
     * @return Broker 响应
     */
    @Override
    public CompletableFuture<RemotingCommand> forwardMessageToDeadLetterQueue(ProxyContext ctx, ReceiptHandle handle,
        String messageId, String groupName, String topicName, long timeoutMillis) {
        return this.producerProcessor.forwardMessageToDeadLetterQueue(ctx, handle, messageId, groupName, topicName, timeoutMillis);
    }

    /**
     * 结束事务状态并回传处理结果
     *
     * @param ctx                  Proxy 上下文
     * @param topic                主题
     * @param transactionId        事务 ID
     * @param messageId            消息 ID
     * @param producerGroup        生产组
     * @param transactionStatus    事务状态
     * @param fromTransactionCheck 是否来源于回查
     * @param timeoutMillis        超时时间
     * @return 异步处理结果
     */
    @Override
    public CompletableFuture<Void> endTransaction(ProxyContext ctx, String topic, String transactionId,
        String messageId, String producerGroup,
        TransactionStatus transactionStatus, boolean fromTransactionCheck,
        long timeoutMillis) {
        return this.transactionProcessor.endTransaction(ctx, topic, transactionId, messageId, producerGroup, transactionStatus, fromTransactionCheck, timeoutMillis);
    }

    /**
     * 处理 POP 消息请求
     *
     * @param ctx                    Proxy 上下文
     * @param queueSelector          队列选择器
     * @param consumerGroup          消费组
     * @param topic                  主题
     * @param maxMsgNums             最大消息数
     * @param invisibleTime          不可见时长
     * @param pollTime               轮询时长
     * @param initMode               初始化模式
     * @param subscriptionData       订阅数据
     * @param fifo                   是否 FIFO
     * @param popMessageResultFilter 结果过滤器
     * @param attemptId              尝试 ID
     * @param timeoutMillis          超时时间
     * @return POP 结果
     */
    @Override
    public CompletableFuture<PopResult> popMessage(
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
    ) {
        return this.consumerProcessor.popMessage(ctx, queueSelector, consumerGroup, topic, maxMsgNums,
            invisibleTime, pollTime, initMode, subscriptionData, fifo, popMessageResultFilter, attemptId, timeoutMillis);
    }

    /**
     * 确认单条消息
     *
     * @param ctx           Proxy 上下文
     * @param handle        收据句柄
     * @param messageId     消息 ID
     * @param consumerGroup 消费组
     * @param topic         主题
     * @param timeoutMillis 超时时间
     * @return ACK 结果
     */
    @Override
    public CompletableFuture<AckResult> ackMessage(ProxyContext ctx, ReceiptHandle handle, String messageId,
        String consumerGroup, String topic, long timeoutMillis) {
        return this.consumerProcessor.ackMessage(ctx, handle, messageId, consumerGroup, topic, timeoutMillis);
    }

    /**
     * 批量确认消息
     *
     * @param ctx               Proxy 上下文
     * @param handleMessageList 收据句柄消息集合
     * @param consumerGroup     消费组
     * @param topic             主题
     * @param timeoutMillis     超时时间
     * @return 批量 ACK 结果
     */
    @Override
    public CompletableFuture<List<BatchAckResult>> batchAckMessage(ProxyContext ctx,
        List<ReceiptHandleMessage> handleMessageList, String consumerGroup, String topic, long timeoutMillis) {
        return this.consumerProcessor.batchAckMessage(ctx, handleMessageList, consumerGroup, topic, timeoutMillis);
    }

    /**
     * 修改消息不可见时间
     *
     * @param ctx Proxy 上下文
     * @param handle 收据句柄
     * @param messageId 消息 ID
     * @param groupName 消费组
     * @param topicName 主题
     * @param invisibleTime 不可见时长
     * @param timeoutMillis 超时时间
     * @return ACK 结果
     */
    @Override
    public CompletableFuture<AckResult> changeInvisibleTime(ProxyContext ctx, ReceiptHandle handle, String messageId,
        String groupName, String topicName, long invisibleTime, long timeoutMillis) {
        return this.consumerProcessor.changeInvisibleTime(ctx, handle, messageId, groupName, topicName, invisibleTime, timeoutMillis);
    }

    /**
     * 拉取消息
     *
     * @param ctx Proxy 上下文
     * @param messageQueue 消息队列
     * @param consumerGroup 消费组
     * @param queueOffset 队列位点
     * @param maxMsgNums 最大消息数
     * @param sysFlag 系统标记
     * @param commitOffset 提交位点
     * @param suspendTimeoutMillis 挂起超时
     * @param subscriptionData 订阅数据
     * @param timeoutMillis 超时时间
     * @return 拉取结果
     */
    @Override
    public CompletableFuture<PullResult> pullMessage(ProxyContext ctx, MessageQueue messageQueue, String consumerGroup,
        long queueOffset, int maxMsgNums, int sysFlag, long commitOffset, long suspendTimeoutMillis,
        SubscriptionData subscriptionData, long timeoutMillis) {
        return this.consumerProcessor.pullMessage(ctx, messageQueue, consumerGroup, queueOffset, maxMsgNums,
            sysFlag, commitOffset, suspendTimeoutMillis, subscriptionData, timeoutMillis);
    }

    /**
     * 更新消费位点
     *
     * @param ctx Proxy 上下文
     * @param messageQueue 消息队列
     * @param consumerGroup 消费组
     * @param commitOffset 提交位点
     * @param timeoutMillis 超时时间
     * @return 异步结果
     */
    @Override
    public CompletableFuture<Void> updateConsumerOffset(ProxyContext ctx, MessageQueue messageQueue,
        String consumerGroup, long commitOffset, long timeoutMillis) {
        return this.consumerProcessor.updateConsumerOffset(ctx, messageQueue, consumerGroup, commitOffset, timeoutMillis);
    }

    /**
     * 异步更新消费位点
     *
     * @param ctx Proxy 上下文
     * @param messageQueue 消息队列
     * @param consumerGroup 消费组
     * @param commitOffset 提交位点
     * @param timeoutMillis 超时时间
     * @return 异步结果
     */
    @Override
    public CompletableFuture<Void> updateConsumerOffsetAsync(ProxyContext ctx, MessageQueue messageQueue,
        String consumerGroup, long commitOffset, long timeoutMillis) {
        return this.consumerProcessor.updateConsumerOffsetAsync(ctx, messageQueue, consumerGroup, commitOffset, timeoutMillis);
    }

    /**
     * 查询消费位点
     *
     * @param ctx Proxy 上下文
     * @param messageQueue 消息队列
     * @param consumerGroup 消费组
     * @param timeoutMillis 超时时间
     * @return 消费位点
     */
    @Override
    public CompletableFuture<Long> queryConsumerOffset(ProxyContext ctx, MessageQueue messageQueue,
        String consumerGroup, long timeoutMillis) {
        return this.consumerProcessor.queryConsumerOffset(ctx, messageQueue, consumerGroup, timeoutMillis);
    }

    /**
     * 批量锁定消息队列
     *
     * @param ctx Proxy 上下文
     * @param mqSet 队列集合
     * @param consumerGroup 消费组
     * @param clientId 客户端 ID
     * @param timeoutMillis 超时时间
     * @return 已锁定队列集合
     */
    @Override
    public CompletableFuture<Set<MessageQueue>> lockBatchMQ(ProxyContext ctx, Set<MessageQueue> mqSet,
        String consumerGroup, String clientId, long timeoutMillis) {
        return this.consumerProcessor.lockBatchMQ(ctx, mqSet, consumerGroup, clientId, timeoutMillis);
    }

    /**
     * 批量解锁消息队列
     *
     * @param ctx Proxy 上下文
     * @param mqSet 队列集合
     * @param consumerGroup 消费组
     * @param clientId 客户端 ID
     * @param timeoutMillis 超时时间
     * @return 异步结果
     */
    @Override
    public CompletableFuture<Void> unlockBatchMQ(ProxyContext ctx, Set<MessageQueue> mqSet,
        String consumerGroup,
        String clientId, long timeoutMillis) {
        return this.consumerProcessor.unlockBatchMQ(ctx, mqSet, consumerGroup, clientId, timeoutMillis);
    }

    @Override
    public CompletableFuture<Long> getMaxOffset(ProxyContext ctx, MessageQueue messageQueue, long timeoutMillis) {
        return this.consumerProcessor.getMaxOffset(ctx, messageQueue, timeoutMillis);
    }

    @Override
    public CompletableFuture<Long> getMinOffset(ProxyContext ctx, MessageQueue messageQueue, long timeoutMillis) {
        return this.consumerProcessor.getMinOffset(ctx, messageQueue, timeoutMillis);
    }

    /**
     * 撤回消息
     *
     * @param ctx Proxy 上下文
     * @param topic 主题
     * @param recallHandle 撤回句柄
     * @param timeoutMillis 超时时间
     * @return 撤回结果
     */
    @Override
    public CompletableFuture<String> recallMessage(ProxyContext ctx, String topic,
                                                   String recallHandle, long timeoutMillis) {
        return this.producerProcessor.recallMessage(ctx, topic, recallHandle, timeoutMillis);
    }

    /**
     * 请求指定 Broker 并返回响应
     *
     * @param ctx Proxy 上下文
     * @param brokerName Broker 名称
     * @param request 请求命令
     * @param timeoutMillis 超时时间
     * @return Broker 响应
     */
    @Override
    public CompletableFuture<RemotingCommand> request(ProxyContext ctx, String brokerName, RemotingCommand request,
        long timeoutMillis) {
        int originalRequestOpaque = request.getOpaque();
        request.setOpaque(RemotingCommand.createNewRequestId());
        return this.requestBrokerProcessor.request(ctx, brokerName, request, timeoutMillis).thenApply(r -> {
            request.setOpaque(originalRequestOpaque);
            return r;
        });
    }

    /**
     * 以单向方式请求指定 Broker
     *
     * @param ctx Proxy 上下文
     * @param brokerName Broker 名称
     * @param request 请求命令
     * @param timeoutMillis 超时时间
     * @return 异步结果
     */
    @Override
    public CompletableFuture<Void> requestOneway(ProxyContext ctx, String brokerName, RemotingCommand request,
        long timeoutMillis) {
        int originalRequestOpaque = request.getOpaque();
        request.setOpaque(RemotingCommand.createNewRequestId());
        return this.requestBrokerProcessor.requestOneway(ctx, brokerName, request, timeoutMillis).thenApply(r -> {
            request.setOpaque(originalRequestOpaque);
            return r;
        });
    }

    /**
     * 注册生产者
     *
     * @param ctx Proxy 上下文
     * @param producerGroup 生产组
     * @param clientChannelInfo 客户端通道信息
     */
    @Override
    public void registerProducer(ProxyContext ctx, String producerGroup, ClientChannelInfo clientChannelInfo) {
        this.clientProcessor.registerProducer(ctx, producerGroup, clientChannelInfo);
    }

    /**
     * 反注册生产者
     *
     * @param ctx Proxy 上下文
     * @param producerGroup 生产组
     * @param clientChannelInfo 客户端通道信息
     */
    @Override
    public void unRegisterProducer(ProxyContext ctx, String producerGroup, ClientChannelInfo clientChannelInfo) {
        this.clientProcessor.unRegisterProducer(ctx, producerGroup, clientChannelInfo);
    }

    /**
     * 查找生产者通道
     *
     * @param ctx Proxy 上下文
     * @param producerGroup 生产组
     * @param clientId 客户端 ID
     * @return 生产者通道
     */
    @Override
    public Channel findProducerChannel(ProxyContext ctx, String producerGroup, String clientId) {
        return this.clientProcessor.findProducerChannel(ctx, producerGroup, clientId);
    }

    /**
     * 注册生产者变化监听器
     *
     * @param producerChangeListener 监听器
     */
    @Override
    public void registerProducerListener(ProducerChangeListener producerChangeListener) {
        this.clientProcessor.registerProducerChangeListener(producerChangeListener);
    }

    /**
     * 注册消费者
     *
     * @param ctx Proxy 上下文
     * @param consumerGroup 消费组
     * @param clientChannelInfo 客户端通道信息
     * @param consumeType 消费类型
     * @param messageModel 消息模型
     * @param consumeFromWhere 消费起点
     * @param subList 订阅列表
     * @param updateSubscription 是否更新订阅
     */
    @Override
    public void registerConsumer(ProxyContext ctx, String consumerGroup, ClientChannelInfo clientChannelInfo,
        ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere,
        Set<SubscriptionData> subList, boolean updateSubscription) {
        this.clientProcessor.registerConsumer(ctx, consumerGroup, clientChannelInfo, consumeType, messageModel, consumeFromWhere, subList, updateSubscription);
    }

    /**
     * 查找消费者通道
     *
     * @param ctx Proxy 上下文
     * @param consumerGroup 消费组
     * @param channel 网络通道
     * @return 客户端通道信息
     */
    @Override
    public ClientChannelInfo findConsumerChannel(ProxyContext ctx, String consumerGroup, Channel channel) {
        return this.clientProcessor.findConsumerChannel(ctx, consumerGroup, channel);
    }

    /**
     * 反注册消费者
     *
     * @param ctx Proxy 上下文
     * @param consumerGroup 消费组
     * @param clientChannelInfo 客户端通道信息
     */
    @Override
    public void unRegisterConsumer(ProxyContext ctx, String consumerGroup, ClientChannelInfo clientChannelInfo) {
        this.clientProcessor.unRegisterConsumer(ctx, consumerGroup, clientChannelInfo);
    }

    /**
     * 注册消费者变化监听器
     *
     * @param consumerIdsChangeListener 监听器
     */
    @Override
    public void registerConsumerListener(ConsumerIdsChangeListener consumerIdsChangeListener) {
        this.clientProcessor.registerConsumerIdsChangeListener(consumerIdsChangeListener);
    }

    /**
     * 处理连接关闭事件
     *
     * @param remoteAddr 远端地址
     * @param channel 网络通道
     */
    @Override
    public void doChannelCloseEvent(String remoteAddr, Channel channel) {
        this.clientProcessor.doChannelCloseEvent(remoteAddr, channel);
    }

    @Override
    public ConsumerGroupInfo getConsumerGroupInfo(ProxyContext ctx, String consumerGroup) {
        return this.clientProcessor.getConsumerGroupInfo(ctx, consumerGroup);
    }

    /**
     * 注册事务订阅
     *
     * @param ctx Proxy 上下文
     * @param producerGroup 生产组
     * @param topic 主题
     */
    @Override
    public void addTransactionSubscription(ProxyContext ctx, String producerGroup, String topic) {
        this.transactionProcessor.addTransactionSubscription(ctx, producerGroup, topic);
    }

    @Override
    public ProxyRelayService getProxyRelayService() {
        return this.serviceManager.getProxyRelayService();
    }

    @Override
    public MetadataService getMetadataService() {
        return this.serviceManager.getMetadataService();
    }

    /**
     * 添加收据句柄
     *
     * @param ctx Proxy 上下文
     * @param channel 网络通道
     * @param group 消费组
     * @param msgID 消息 ID
     * @param messageReceiptHandle 收据句柄
     */
    @Override
    public void addReceiptHandle(ProxyContext ctx, Channel channel, String group, String msgID,
        MessageReceiptHandle messageReceiptHandle) {
        receiptHandleProcessor.addReceiptHandle(ctx, channel, group, msgID, messageReceiptHandle);
    }

    /**
     * 删除收据句柄
     *
     * @param ctx Proxy 上下文
     * @param channel 网络通道
     * @param group 消费组
     * @param msgID 消息 ID
     * @param receiptHandle 收据句柄字符串
     * @return 删除后的收据句柄
     */
    @Override
    public MessageReceiptHandle removeReceiptHandle(ProxyContext ctx, Channel channel, String group, String msgID,
        String receiptHandle) {
        return receiptHandleProcessor.removeReceiptHandle(ctx, channel, group, msgID, receiptHandle);
    }
}
