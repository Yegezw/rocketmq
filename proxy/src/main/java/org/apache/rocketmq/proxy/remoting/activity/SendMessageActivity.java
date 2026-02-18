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

package org.apache.rocketmq.proxy.remoting.activity;

import io.netty.channel.ChannelHandlerContext;
import java.time.Duration;
import java.util.Map;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.validator.DefaultTopicMessageTypeValidator;
import org.apache.rocketmq.proxy.processor.validator.TopicMessageTypeValidator;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;

/**
 * 发送消息请求处理活动
 */
public class SendMessageActivity extends AbstractRemotingActivity {
    /**
     * 主题消息类型校验器
     */
    TopicMessageTypeValidator topicMessageTypeValidator;

    /**
     * 构造发送消息活动处理器
     *
     * @param requestPipeline 请求处理管道
     * @param messagingProcessor 消息处理核心组件
     */
    public SendMessageActivity(RequestPipeline requestPipeline,
        MessagingProcessor messagingProcessor) {
        super(requestPipeline, messagingProcessor);
        this.topicMessageTypeValidator = new DefaultTopicMessageTypeValidator();
    }

    /**
     * 根据请求码分发发送相关请求
     *
     * @param ctx Netty 上下文
     * @param request 请求命令
     * @param context Proxy 上下文
     * @return 请求处理结果
     * @throws Exception 处理异常
     */
    @Override
    protected RemotingCommand processRequest0(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        switch (request.getCode()) {
            case RequestCode.SEND_MESSAGE:
            case RequestCode.SEND_MESSAGE_V2:
            case RequestCode.SEND_BATCH_MESSAGE: {
                return sendMessage(ctx, request, context);
            }
            case RequestCode.CONSUMER_SEND_MSG_BACK: {
                return consumerSendMessage(ctx, request, context);
            }
            default:
                break;
        }
        return null;
    }

    /**
     * 处理生产者发送请求, 并执行消息类型校验
     *
     * @param ctx Netty 上下文
     * @param request 请求命令
     * @param context Proxy 上下文
     * @return Broker 响应
     * @throws Exception 处理异常
     */
    protected RemotingCommand sendMessage(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        SendMessageRequestHeader requestHeader = SendMessageRequestHeader.parseRequestHeader(request);
        String topic = requestHeader.getTopic();
        Map<String, String> property = MessageDecoder.string2messageProperties(requestHeader.getProperties());
        TopicMessageType messageType = TopicMessageType.parseFromMessageProperty(property);
        if (isNeedCheckTopicMessageType(property)) {
            if (topicMessageTypeValidator != null) {
                // Do not check retry or dlq topic
                // 重试主题与死信主题不做消息类型校验
                if (!NamespaceUtil.isRetryTopic(topic) && !NamespaceUtil.isDLQTopic(topic)) {
                    TopicMessageType topicMessageType = messagingProcessor.getMetadataService().getTopicMessageType(context, topic);
                    topicMessageTypeValidator.validate(topicMessageType, messageType);
                }
            }
        }
        if (!NamespaceUtil.isRetryTopic(topic) && !NamespaceUtil.isDLQTopic(topic)) {
            if (TopicMessageType.TRANSACTION.equals(messageType)) {
                messagingProcessor.addTransactionSubscription(context, requestHeader.getProducerGroup(), requestHeader.getTopic());
            }
        }
        return request(ctx, request, context, Duration.ofSeconds(3).toMillis());
    }

    /**
     * 处理消费者回退消息请求
     *
     * @param ctx Netty 上下文
     * @param request 请求命令
     * @param context Proxy 上下文
     * @return Broker 响应
     * @throws Exception 处理异常
     */
    protected RemotingCommand consumerSendMessage(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        return request(ctx, request, context, Duration.ofSeconds(3).toMillis());
    }

    /**
     * 判断是否需要进行主题消息类型校验
     *
     * @param property 消息属性
     * @return 是否需要校验
     */
    private boolean isNeedCheckTopicMessageType(Map<String, String> property) {
        return ConfigurationManager.getProxyConfig().isEnableTopicMessageTypeCheck()
            && !property.containsKey(MessageConst.PROPERTY_TRANSFER_FLAG);
    }
}
