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

package org.apache.rocketmq.proxy.service.sysmessage;

import com.alibaba.fastjson.JSON;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.service.admin.AdminService;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

/**
 * 系统消息同步抽象基类, 封装广播主题发送与消费初始化逻辑
 */
public abstract class AbstractSystemMessageSyncer implements StartAndShutdown, MessageListenerConcurrently {
    /**
     * Proxy 日志记录器
     */
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    /**
     * 主题路由服务
     */
    protected final TopicRouteService topicRouteService;
    /**
     * 管理服务
     */
    protected final AdminService adminService;
    /**
     * MQ 客户端工厂
     */
    protected final MQClientAPIFactory mqClientAPIFactory;
    /**
     * 远程调用钩子
     */
    protected final RPCHook rpcHook;
    /**
     * 系统主题广播消费者
     */
    protected DefaultMQPushConsumer defaultMQPushConsumer;

    /**
     * 初始化系统消息同步器
     *
     * @param topicRouteService 主题路由服务
     * @param adminService 管理服务
     * @param mqClientAPIFactory MQ 客户端工厂
     * @param rpcHook 远程调用钩子
     */
    public AbstractSystemMessageSyncer(TopicRouteService topicRouteService, AdminService adminService, MQClientAPIFactory mqClientAPIFactory, RPCHook rpcHook) {
        this.topicRouteService = topicRouteService;
        this.adminService = adminService;
        this.mqClientAPIFactory = mqClientAPIFactory;
        this.rpcHook = rpcHook;
    }

    protected String getSystemMessageProducerId() {
        return "PID_" + getBroadcastTopicName();
    }

    protected String getSystemMessageConsumerId() {
        return "CID_" + getBroadcastTopicName();
    }

    protected String getBroadcastTopicName() {
        return ConfigurationManager.getProxyConfig().getHeartbeatSyncerTopicName();
    }

    protected String getSubTag() {
        return "*";
    }

    protected String getBroadcastTopicClusterName() {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        return proxyConfig.getHeartbeatSyncerTopicClusterName();
    }

    protected int getBroadcastTopicQueueNum() {
        return 1;
    }

    public RPCHook getRpcHook() {
        return rpcHook;
    }

    /**
     * 发送系统广播消息
     *
     * @param data 广播数据对象
     */
    protected void sendSystemMessage(Object data) {
        String targetTopic = this.getBroadcastTopicName();
        try {
            Message message = new Message(
                targetTopic,
                JSON.toJSONString(data).getBytes(StandardCharsets.UTF_8)
            );

            AddressableMessageQueue messageQueue = this.topicRouteService.getAllMessageQueueView(ProxyContext.createForInner(this.getClass()), targetTopic)
                .getWriteSelector().selectOne(true);
            this.mqClientAPIFactory.getClient().sendMessageAsync(
                messageQueue.getBrokerAddr(),
                messageQueue.getBrokerName(),
                message,
                buildSendMessageRequestHeader(message, this.getSystemMessageProducerId(), messageQueue.getQueueId()),
                Duration.ofSeconds(3).toMillis()
            ).whenCompleteAsync((result, throwable) -> {
                if (throwable != null) {
                    log.error("send system message failed. data: {}, topic: {}", data, getBroadcastTopicName(), throwable);
                    return;
                }
                if (SendStatus.SEND_OK != result.getSendStatus()) {
                    log.error("send system message failed. data: {}, topic: {}, sendResult:{}", data, getBroadcastTopicName(), result);
                }
            });
        } catch (Throwable t) {
            log.error("send system message failed. data: {}, topic: {}", data, targetTopic, t);
        }
    }

    /**
     * 构造发送系统消息请求头
     *
     * @param message 消息对象
     * @param producerGroup 生产者组
     * @param queueId 队列标识
     * @return 发送请求头
     */
    protected SendMessageRequestHeader buildSendMessageRequestHeader(Message message,
        String producerGroup, int queueId) {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();

        requestHeader.setProducerGroup(producerGroup);
        requestHeader.setTopic(message.getTopic());
        requestHeader.setDefaultTopic(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC);
        requestHeader.setDefaultTopicQueueNums(0);
        requestHeader.setQueueId(queueId);
        requestHeader.setSysFlag(0);
        requestHeader.setBornTimestamp(System.currentTimeMillis());
        requestHeader.setFlag(message.getFlag());
        requestHeader.setProperties(MessageDecoder.messageProperties2String(message.getProperties()));
        requestHeader.setReconsumeTimes(0);
        requestHeader.setBatch(false);
        return requestHeader;
    }

    /**
     * 启动系统消息同步器并初始化广播消费者
     *
     * @throws Exception 启动异常
     */
    @Override
    public void start() throws Exception {
        this.createSysTopic();
        RPCHook rpcHook = this.getRpcHook();
        this.defaultMQPushConsumer = new DefaultMQPushConsumer(this.getSystemMessageConsumerId(), rpcHook);

        this.defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        this.defaultMQPushConsumer.setMessageModel(MessageModel.BROADCASTING);
        try {
            this.defaultMQPushConsumer.subscribe(this.getBroadcastTopicName(), this.getSubTag());
        } catch (MQClientException e) {
            throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, "subscribe to broadcast topic " + this.getBroadcastTopicName() + " failed. " + e.getMessage());
        }
        this.defaultMQPushConsumer.registerMessageListener(this);
        this.defaultMQPushConsumer.start();
    }

    /**
     * 创建系统广播主题
     */
    protected void createSysTopic() {
        String clusterName = this.getBroadcastTopicClusterName();
        if (StringUtils.isEmpty(clusterName)) {
            throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, "system topic cluster cannot be empty");
        }

        boolean createSuccess = this.adminService.createTopicOnTopicBrokerIfNotExist(
            this.getBroadcastTopicName(),
            clusterName,
            this.getBroadcastTopicQueueNum(),
            this.getBroadcastTopicQueueNum(),
            true,
            3
        );
        if (!createSuccess) {
            throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, "create system broadcast topic " + this.getBroadcastTopicName() + " failed on cluster " + clusterName);
        }
    }

    /**
     * 停止系统消息同步器
     *
     * @throws Exception 停止异常
     */
    @Override
    public void shutdown() throws Exception {
        this.defaultMQPushConsumer.shutdown();
    }
}
