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

package org.apache.rocketmq.broker.failover;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageUtil;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.tieredstore.TieredMessageStore;

public class EscapeBridge {
    /**
     * Broker 逃逸流程日志记录器, 用于输出远程转发和消息拉取诊断信息
     */
    protected static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    /**
     * 远程发送超时时间, 单位毫秒
     */
    private static final long SEND_TIMEOUT = 3000L;
    /**
     * 远程拉消息默认超时时间, 单位毫秒
     */
    private static final long DEFAULT_PULL_TIMEOUT_MILLIS = 1000 * 10L;
    /**
     * 内部生产者组名, 用于远程逃逸写入时标识来源
     */
    private final String innerProducerGroupName;
    /**
     * 内部消费者组名, 用于逃逸读取消息时隔离内部拉取流量
     */
    private final String innerConsumerGroupName;

    /**
     * Broker 控制器引用, 提供主从判定, 路由查询和外部 RPC 能力
     */
    private final BrokerController brokerController;

    /**
     * 异步逃逸发送线程池, 仅在开启远程逃逸场景下初始化
     */
    private ExecutorService defaultAsyncSenderExecutor;

    /**
     * 构建逃逸桥接器, 初始化内部生产和消费组标识
     *
     * @param brokerController broker 运行时上下文
     */
    public EscapeBridge(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.innerProducerGroupName = "InnerProducerGroup_" + brokerController.getBrokerConfig().getBrokerName() + "_" + brokerController.getBrokerConfig().getBrokerId();
        this.innerConsumerGroupName = "InnerConsumerGroup_" + brokerController.getBrokerConfig().getBrokerName() + "_" + brokerController.getBrokerConfig().getBrokerId();
    }

    /**
     * 启动逃逸桥接组件, 在可用配置下创建异步远程发送线程池
     *
     * @throws Exception 启动过程中的异常
     */
    public void start() throws Exception {
        if (brokerController.getBrokerConfig().isEnableSlaveActingMaster() && brokerController.getBrokerConfig().isEnableRemoteEscape()) {
            final BlockingQueue<Runnable> asyncSenderThreadPoolQueue = new LinkedBlockingQueue<>(50000);
            this.defaultAsyncSenderExecutor = ThreadUtils.newThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().availableProcessors(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                asyncSenderThreadPoolQueue,
                new ThreadFactoryImpl("AsyncEscapeBridgeExecutor_", this.brokerController.getBrokerIdentity())
            );
            LOG.info("init executor for escaping messages asynchronously success.");
        }
    }

    /**
     * 关闭逃逸桥接组件, 释放异步发送线程池资源
     */
    public void shutdown() {
        if (null != this.defaultAsyncSenderExecutor) {
            this.defaultAsyncSenderExecutor.shutdown();
        }
    }

    /**
     * 同步写入消息, 优先写本地 master, 失败时按配置执行远程逃逸
     *
     * @param messageExt 待写入的 broker 内部消息
     * @return 写入结果, 包含本地或远程写入状态
     */
    public PutMessageResult putMessage(MessageExtBrokerInner messageExt) {
        BrokerController masterBroker = this.brokerController.peekMasterBroker();
        if (masterBroker != null) {
            return masterBroker.getMessageStore().putMessage(messageExt);
        } else if (this.brokerController.getBrokerConfig().isEnableSlaveActingMaster()
            && this.brokerController.getBrokerConfig().isEnableRemoteEscape()) {

            try {
                messageExt.setWaitStoreMsgOK(false);
                final SendResult sendResult = putMessageToRemoteBroker(messageExt, null);
                return transformSendResult2PutResult(sendResult);
            } catch (Exception e) {
                LOG.error("sendMessageInFailover to remote failed", e);
                return new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true);
            }
        } else {
            LOG.warn("Put message failed, enableSlaveActingMaster={}, enableRemoteEscape={}.",
                this.brokerController.getBrokerConfig().isEnableSlaveActingMaster(), this.brokerController.getBrokerConfig().isEnableRemoteEscape());
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }
    }

    /**
     * 将消息转发到指定或自动选择的远端 broker
     *
     * @param messageExt 待转发消息
     * @param brokerNameToSend 目标 broker 名称, 为空时自动选择
     * @return 远端发送结果, 发送失败时返回 null
     */
    public SendResult putMessageToRemoteBroker(MessageExtBrokerInner messageExt, String brokerNameToSend) {
        if (this.brokerController.getBrokerConfig().getBrokerName().equals(brokerNameToSend)) { // not remote broker
            // 目标 broker 与当前节点一致, 无需执行远程逃逸
            return null;
        }
        final boolean isTransHalfMessage = TransactionalMessageUtil.buildHalfTopic().equals(messageExt.getTopic());
        MessageExtBrokerInner messageToPut = messageExt;
        if (isTransHalfMessage) {
            messageToPut = TransactionalMessageUtil.buildTransactionalMessageFromHalfMessage(messageExt);
        }
        final TopicPublishInfo topicPublishInfo = this.brokerController.getTopicRouteInfoManager().tryToFindTopicPublishInfo(messageToPut.getTopic());
        if (null == topicPublishInfo || !topicPublishInfo.ok()) {
            LOG.warn("putMessageToRemoteBroker: no route info of topic {} when escaping message, msgId={}",
                messageToPut.getTopic(), messageToPut.getMsgId());
            return null;
        }

        final MessageQueue mqSelected;
        if (StringUtils.isEmpty(brokerNameToSend)) {
            mqSelected = topicPublishInfo.selectOneMessageQueue(this.brokerController.getBrokerConfig().getBrokerName());
            messageToPut.setQueueId(mqSelected.getQueueId());
            brokerNameToSend = mqSelected.getBrokerName();
            if (this.brokerController.getBrokerConfig().getBrokerName().equals(brokerNameToSend)) {
                LOG.warn("putMessageToRemoteBroker failed, remote broker not found. Topic: {}, MsgId: {}, Broker: {}",
                    messageExt.getTopic(), messageExt.getMsgId(), brokerNameToSend);
                return null;
            }
        } else {
            mqSelected = new MessageQueue(messageExt.getTopic(), brokerNameToSend, messageExt.getQueueId());
        }

        final String brokerAddrToSend = this.brokerController.getTopicRouteInfoManager().findBrokerAddressInPublish(brokerNameToSend);
        if (null == brokerAddrToSend) {
            LOG.warn("putMessageToRemoteBroker failed, remote broker address not found. Topic: {}, MsgId: {}, Broker: {}",
                messageExt.getTopic(), messageExt.getMsgId(), brokerNameToSend);
            return null;
        }

        final long beginTimestamp = System.currentTimeMillis();
        try {
            final SendResult sendResult = this.brokerController.getBrokerOuterAPI().sendMessageToSpecificBroker(
                brokerAddrToSend, brokerNameToSend,
                messageToPut, this.getProducerGroup(messageToPut), SEND_TIMEOUT);
            if (null != sendResult && SendStatus.SEND_OK.equals(sendResult.getSendStatus())) {
                return sendResult;
            } else {
                LOG.error("Escaping failed! cost {}ms, Topic: {}, MsgId: {}, Broker: {}",
                    System.currentTimeMillis() - beginTimestamp, messageExt.getTopic(),
                    messageExt.getMsgId(), brokerNameToSend);
            }
        } catch (RemotingException | MQBrokerException e) {
            LOG.error(String.format("putMessageToRemoteBroker exception, MsgId: %s, RT: %sms, Broker: %s",
                messageToPut.getMsgId(), System.currentTimeMillis() - beginTimestamp, mqSelected), e);
        } catch (InterruptedException e) {
            LOG.error(String.format("putMessageToRemoteBroker interrupted, MsgId: %s, RT: %sms, Broker: %s",
                messageToPut.getMsgId(), System.currentTimeMillis() - beginTimestamp, mqSelected), e);
            Thread.currentThread().interrupt();
        }

        return null;
    }

    /**
     * 异步写入消息, 本地不可写时走远程逃逸异步链路
     *
     * @param messageExt 待写入消息
     * @return 写入结果 Future, 调用方可异步等待结果
     */
    public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner messageExt) {
        BrokerController masterBroker = this.brokerController.peekMasterBroker();
        if (masterBroker != null) {
            return masterBroker.getMessageStore().asyncPutMessage(messageExt);
        } else if (this.brokerController.getBrokerConfig().isEnableSlaveActingMaster()
            && this.brokerController.getBrokerConfig().isEnableRemoteEscape()) {
            try {
                messageExt.setWaitStoreMsgOK(false);

                final TopicPublishInfo topicPublishInfo = this.brokerController.getTopicRouteInfoManager().tryToFindTopicPublishInfo(messageExt.getTopic());
                final String producerGroup = getProducerGroup(messageExt);

                final MessageQueue mqSelected = topicPublishInfo.selectOneMessageQueue();
                messageExt.setQueueId(mqSelected.getQueueId());

                final String brokerNameToSend = mqSelected.getBrokerName();
                final String brokerAddrToSend = this.brokerController.getTopicRouteInfoManager().findBrokerAddressInPublish(brokerNameToSend);
                final CompletableFuture<SendResult> future = this.brokerController.getBrokerOuterAPI().sendMessageToSpecificBrokerAsync(brokerAddrToSend,
                    brokerNameToSend, messageExt,
                    producerGroup, SEND_TIMEOUT);

                return future.exceptionally(throwable -> null)
                    .thenApplyAsync(this::transformSendResult2PutResult, this.defaultAsyncSenderExecutor)
                    .exceptionally(throwable -> transformSendResult2PutResult(null));

            } catch (Exception e) {
                LOG.error("sendMessageInFailover to remote failed", e);
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true));
            }
        } else {
            LOG.warn("Put message failed, enableSlaveActingMaster={}, enableRemoteEscape={}.",
                this.brokerController.getBrokerConfig().isEnableSlaveActingMaster(), this.brokerController.getBrokerConfig().isEnableRemoteEscape());
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null));
        }
    }

    /**
     * 提取消息中的生产者组, 缺失时回退到内部生产者组
     *
     * @param messageExt 目标消息
     * @return 生产者组名
     */
    private String getProducerGroup(MessageExtBrokerInner messageExt) {
        if (null == messageExt) {
            return this.innerProducerGroupName;
        }
        String producerGroup = messageExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
        if (StringUtils.isEmpty(producerGroup)) {
            producerGroup = this.innerProducerGroupName;
        }
        return producerGroup;
    }

    /**
     * 将消息写入特定队列, 同步等待远端发送结果
     *
     * @param messageExt 待写入消息
     * @return 写入结果
     */
    public PutMessageResult putMessageToSpecificQueue(MessageExtBrokerInner messageExt) {
        BrokerController masterBroker = this.brokerController.peekMasterBroker();
        if (masterBroker != null) {
            return masterBroker.getMessageStore().putMessage(messageExt);
        }
        try {
            return asyncRemotePutMessageToSpecificQueue(messageExt).get(SEND_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOG.error("Put message to specific queue error", e);
            return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null, true);
        }
    }

    /**
     * 异步写入特定队列, 本地 master 存在时直接走本地存储链路
     *
     * @param messageExt 待写入消息
     * @return 异步写入结果
     */
    public CompletableFuture<PutMessageResult> asyncPutMessageToSpecificQueue(MessageExtBrokerInner messageExt) {
        BrokerController masterBroker = this.brokerController.peekMasterBroker();
        if (masterBroker != null) {
            return masterBroker.getMessageStore().asyncPutMessage(messageExt);
        }
        return asyncRemotePutMessageToSpecificQueue(messageExt);
    }

    /**
     * 异步逃逸写入固定队列, 根据 topic 和存储主机哈希选择目标队列
     *
     * @param messageExt 待写入消息
     * @return 异步写入结果
     */
    public CompletableFuture<PutMessageResult> asyncRemotePutMessageToSpecificQueue(MessageExtBrokerInner messageExt) {
        if (this.brokerController.getBrokerConfig().isEnableSlaveActingMaster()
            && this.brokerController.getBrokerConfig().isEnableRemoteEscape()) {
            try {
                messageExt.setWaitStoreMsgOK(false);

                final TopicPublishInfo topicPublishInfo = this.brokerController.getTopicRouteInfoManager().tryToFindTopicPublishInfo(messageExt.getTopic());
                List<MessageQueue> mqs = topicPublishInfo.getMessageQueueList();

                if (null == mqs || mqs.isEmpty()) {
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true));
                }

                String id = messageExt.getTopic() + messageExt.getStoreHost();
                final int index = Math.floorMod(id.hashCode(), mqs.size());

                MessageQueue mq = mqs.get(index);
                messageExt.setQueueId(mq.getQueueId());

                String brokerNameToSend = mq.getBrokerName();
                String brokerAddrToSend = this.brokerController.getTopicRouteInfoManager().findBrokerAddressInPublish(brokerNameToSend);
                return this.brokerController.getBrokerOuterAPI().sendMessageToSpecificBrokerAsync(
                    brokerAddrToSend, brokerNameToSend,
                    messageExt, this.getProducerGroup(messageExt), SEND_TIMEOUT).thenCompose(sendResult -> CompletableFuture.completedFuture(transformSendResult2PutResult(sendResult)));
            } catch (Exception e) {
                LOG.error("sendMessageInFailover to remote failed", e);
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true));
            }
        } else {
            LOG.warn("Put message to specific queue failed, enableSlaveActingMaster={}, enableRemoteEscape={}.",
                this.brokerController.getBrokerConfig().isEnableSlaveActingMaster(), this.brokerController.getBrokerConfig().isEnableRemoteEscape());
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null));
        }
    }

    /**
     * 将远程发送结果映射为存储层写入结果, 统一逃逸链路状态语义
     *
     * @param sendResult 远程发送结果
     * @return 存储层写入结果
     */
    private PutMessageResult transformSendResult2PutResult(SendResult sendResult) {
        if (sendResult == null) {
            return new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true);
        }
        switch (sendResult.getSendStatus()) {
            case SEND_OK:
                return new PutMessageResult(PutMessageStatus.PUT_OK, null, true);
            case SLAVE_NOT_AVAILABLE:
                return new PutMessageResult(PutMessageStatus.SLAVE_NOT_AVAILABLE, null, true);
            case FLUSH_DISK_TIMEOUT:
                return new PutMessageResult(PutMessageStatus.FLUSH_DISK_TIMEOUT, null, true);
            case FLUSH_SLAVE_TIMEOUT:
                return new PutMessageResult(PutMessageStatus.FLUSH_SLAVE_TIMEOUT, null, true);
            default:
                return new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true);
        }
    }

    /**
     * 同步获取指定消息, 对异步查询接口做 join 封装
     *
     * @param topic 主题名
     * @param offset 队列偏移量
     * @param queueId 队列编号
     * @param brokerName broker 名称
     * @param deCompressBody 是否解压消息体
     * @return 三元组: 消息体, 附加信息和是否建议重试
     */
    public Triple<MessageExt, String, Boolean> getMessage(String topic, long offset, int queueId, String brokerName,
        boolean deCompressBody) {
        return getMessageAsync(topic, offset, queueId, brokerName, deCompressBody).join();
    }

    // Triple<MessageExt, info, needRetry>, check info and retry if and only if MessageExt is null
    // 三元组语义: 消息体, 附加信息, 是否需要重试, 仅当消息为空时依据后两项判定
    /**
     * 异步读取指定消息, 优先读取本地存储, 未命中时回退到远端 broker
     *
     * @param topic 主题名
     * @param offset 队列偏移量
     * @param queueId 队列编号
     * @param brokerName broker 名称
     * @param deCompressBody 是否解压消息体
     * @return 三元组: 消息体, 附加信息和是否建议重试
     */
    public CompletableFuture<Triple<MessageExt, String, Boolean>> getMessageAsync(String topic, long offset,
        int queueId, String brokerName, boolean deCompressBody) {
        MessageStore messageStore = brokerController.getMessageStoreByBrokerName(brokerName);
        if (messageStore != null) {
            return messageStore.getMessageAsync(innerConsumerGroupName, topic, queueId, offset, 1, null)
                .thenApply(result -> {
                    if (result == null) {
                        LOG.warn("getMessageResult is null , innerConsumerGroupName {}, topic {}, offset {}, queueId {}", innerConsumerGroupName, topic, offset, queueId);
                        // 本地存储读取失败通常不是瞬时网络问题, 不建议立即重试
                        return Triple.of(null, "getMessageResult is null", false); // local store, so no retry
                    }
                    List<MessageExt> list = decodeMsgList(result, deCompressBody);
                    if (list == null || list.isEmpty()) {
                        // OFFSET_FOUND_NULL returned by TieredMessageStore indicates exception occurred
                        // TieredMessageStore 返回 OFFSET_FOUND_NULL 表示读取链路可能发生异常
                        boolean needRetry = GetMessageStatus.OFFSET_FOUND_NULL.equals(result.getStatus())
                            && messageStore instanceof TieredMessageStore;
                        LOG.warn("Can not get msg , topic {}, offset {}, queueId {}, needRetry {}, result is {}",
                            topic, offset, queueId, needRetry, result);
                        return Triple.of(null, "Can not get msg", needRetry);
                    }
                    return Triple.of(list.get(0), "", false);
                });
        } else {
            return getMessageFromRemoteAsync(topic, offset, queueId, brokerName);
        }
    }

    /**
     * 将存储层返回的消息缓冲区解码为消息对象列表, 并释放底层资源
     *
     * @param getMessageResult 存储层查询结果
     * @param deCompressBody 是否解压消息体
     * @return 解码后的消息列表
     */
    protected List<MessageExt> decodeMsgList(GetMessageResult getMessageResult, boolean deCompressBody) {
        List<MessageExt> foundList = new ArrayList<>();
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            if (messageBufferList != null) {
                for (int i = 0; i < messageBufferList.size(); i++) {
                    ByteBuffer bb = messageBufferList.get(i);
                    if (bb == null) {
                        LOG.error("bb is null {}", getMessageResult);
                        continue;
                    }
                    MessageExt msgExt = MessageDecoder.decode(bb, true, deCompressBody);
                    if (msgExt == null) {
                        LOG.error("decode msgExt is null {}", getMessageResult);
                        continue;
                    }
                    // use CQ offset, not offset in Message
                    // 使用 ConsumeQueue 偏移量, 不使用消息体内偏移量
                    msgExt.setQueueOffset(getMessageResult.getMessageQueueOffset().get(i));
                    foundList.add(msgExt);
                }
            }
        } finally {
            getMessageResult.release();
        }

        return foundList;
    }

    /**
     * 同步从远端 broker 获取消息, 对异步查询接口做 join 封装
     *
     * @param topic 主题名
     * @param offset 队列偏移量
     * @param queueId 队列编号
     * @param brokerName broker 名称
     * @return 三元组: 消息体, 附加信息和是否建议重试
     */
    protected Triple<MessageExt, String, Boolean> getMessageFromRemote(String topic, long offset, int queueId,
        String brokerName) {
        return getMessageFromRemoteAsync(topic, offset, queueId, brokerName).join();
    }

    // Triple<MessageExt, info, needRetry>, check info and retry if and only if MessageExt is null
    // 三元组语义: 消息体, 附加信息, 是否需要重试, 仅当消息为空时依据后两项判定
    /**
     * 异步从远端 broker 拉取单条消息, 地址缺失时尝试刷新路由后重试
     *
     * @param topic 主题名
     * @param offset 队列偏移量
     * @param queueId 队列编号
     * @param brokerName broker 名称
     * @return 三元组: 消息体, 附加信息和是否建议重试
     */
    protected CompletableFuture<Triple<MessageExt, String, Boolean>> getMessageFromRemoteAsync(String topic,
        long offset, int queueId, String brokerName) {
        try {
            String brokerAddr = this.brokerController.getTopicRouteInfoManager().findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, false);
            if (null == brokerAddr) {
                this.brokerController.getTopicRouteInfoManager().updateTopicRouteInfoFromNameServer(topic, true, false);
                brokerAddr = this.brokerController.getTopicRouteInfoManager().findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, false);

                if (null == brokerAddr) {
                    LOG.warn("can't find broker address for topic {}, {}", topic, brokerName);
                    // 目标节点可能暂时离线, 建议上层稍后重试
                    return CompletableFuture.completedFuture(Triple.of(null, "brokerAddress not found", true)); // maybe offline temporarily, so need retry
                }
            }

            return this.brokerController.getBrokerOuterAPI().pullMessageFromSpecificBrokerAsync(brokerName,
                    brokerAddr, this.innerConsumerGroupName, topic, queueId, offset, 1, DEFAULT_PULL_TIMEOUT_MILLIS)
                .thenApply(pullResult -> {
                    if (pullResult.getLeft() != null
                        && PullStatus.FOUND.equals(pullResult.getLeft().getPullStatus())
                        && CollectionUtils.isNotEmpty(pullResult.getLeft().getMsgFoundList())) {
                        return Triple.of(pullResult.getLeft().getMsgFoundList().get(0), "", false);
                    }
                    return Triple.of(null, pullResult.getMiddle(), pullResult.getRight());
                });
        } catch (Exception e) {
            LOG.error("Get message from remote failed. {}, {}, {}, {}", topic, offset, queueId, brokerName, e);
        }

        // 远端拉取出现异常, 返回可重试标记供上层兜底
        return CompletableFuture.completedFuture(Triple.of(null, "Get message from remote failed", true)); // need retry
    }
}
