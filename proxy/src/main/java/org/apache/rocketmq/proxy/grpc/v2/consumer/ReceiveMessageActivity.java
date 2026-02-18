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
package org.apache.rocketmq.proxy.grpc.v2.consumer;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.FilterExpression;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import com.google.protobuf.util.Durations;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.v2.AbstractMessingActivity;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcConverter;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.QueueSelector;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueSelector;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

/**
 * 接收消息活动实现, 负责 POP 拉取与结果回写
 */
public class ReceiveMessageActivity extends AbstractMessingActivity {
    /**
     * 引入非法轮询时间错误码的最小客户端版本
     */
    private static final String ILLEGAL_POLLING_TIME_INTRODUCED_CLIENT_VERSION = "5.0.3";

    /**
     * 构造接收消息活动对象
     *
     * @param messagingProcessor 消息处理器
     * @param grpcClientSettingsManager gRPC 客户端设置管理器
     * @param grpcChannelManager gRPC 通道管理器
     */
    public ReceiveMessageActivity(MessagingProcessor messagingProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager, GrpcChannelManager grpcChannelManager) {
        super(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    /**
     * 处理接收消息请求并异步回写流响应
     *
     * @param ctx Proxy 上下文
     * @param request 接收消息请求
     * @param responseObserver 响应观察者
     */
    public void receiveMessage(ProxyContext ctx, ReceiveMessageRequest request,
        StreamObserver<ReceiveMessageResponse> responseObserver) {
        ReceiveMessageResponseStreamWriter writer = createWriter(ctx, responseObserver);

        try {
            Settings settings = this.grpcClientSettingsManager.getClientSettings(ctx);
            Subscription subscription = settings.getSubscription();
            boolean fifo = subscription.getFifo();
            int maxAttempts = settings.getBackoffPolicy().getMaxAttempts();
            ProxyConfig config = ConfigurationManager.getProxyConfig();

            Long timeRemaining = ctx.getRemainingMs();
            long pollingTime;
            if (request.hasLongPollingTimeout()) {
                pollingTime = Durations.toMillis(request.getLongPollingTimeout());
            } else {
                pollingTime = timeRemaining - Durations.toMillis(settings.getRequestTimeout()) / 2;
            }
            if (pollingTime < config.getGrpcClientConsumerMinLongPollingTimeoutMillis()) {
                pollingTime = config.getGrpcClientConsumerMinLongPollingTimeoutMillis();
            }
            if (pollingTime > config.getGrpcClientConsumerMaxLongPollingTimeoutMillis()) {
                pollingTime = config.getGrpcClientConsumerMaxLongPollingTimeoutMillis();
            }

            if (pollingTime > timeRemaining) {
                if (timeRemaining >= config.getGrpcClientConsumerMinLongPollingTimeoutMillis()) {
                    pollingTime = timeRemaining;
                } else {
                    final String clientVersion = ctx.getClientVersion();
                    Code code =
                        null == clientVersion || ILLEGAL_POLLING_TIME_INTRODUCED_CLIENT_VERSION.compareTo(clientVersion) > 0 ?
                        Code.BAD_REQUEST : Code.ILLEGAL_POLLING_TIME;
                    writer.writeAndComplete(ctx, code, "The deadline time remaining is not enough" +
                        " for polling, please check network condition");
                    return;
                }
            }

            validateTopicAndConsumerGroup(request.getMessageQueue().getTopic(), request.getGroup());
            String topic = request.getMessageQueue().getTopic().getName();
            String group = request.getGroup().getName();

            long actualInvisibleTime = Durations.toMillis(request.getInvisibleDuration());
            ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
            if (proxyConfig.isEnableProxyAutoRenew() && request.getAutoRenew()) {
                actualInvisibleTime = proxyConfig.getDefaultInvisibleTimeMills();
            } else {
                validateInvisibleTime(actualInvisibleTime,
                    ConfigurationManager.getProxyConfig().getMinInvisibleTimeMillsForRecv());
            }

            FilterExpression filterExpression = request.getFilterExpression();
            SubscriptionData subscriptionData;
            try {
                subscriptionData = FilterAPI.build(topic, filterExpression.getExpression(),
                    GrpcConverter.getInstance().buildExpressionType(filterExpression.getType()));
            } catch (Exception e) {
                writer.writeAndComplete(ctx, Code.ILLEGAL_FILTER_EXPRESSION, e.getMessage());
                return;
            }

            this.messagingProcessor.popMessage(
                    ctx,
                    new ReceiveMessageQueueSelector(
                        request.getMessageQueue().getBroker().getName()
                    ),
                    group,
                    topic,
                    request.getBatchSize(),
                    actualInvisibleTime,
                    pollingTime,
                    ConsumeInitMode.MAX,
                    subscriptionData,
                    fifo,
                    new PopMessageResultFilterImpl(maxAttempts),
                    request.hasAttemptId() ? request.getAttemptId() : null,
                    timeRemaining
                ).thenAccept(popResult -> {
                    if (proxyConfig.isEnableProxyAutoRenew() && request.getAutoRenew()) {
                        if (PopStatus.FOUND.equals(popResult.getPopStatus())) {
                            List<MessageExt> messageExtList = popResult.getMsgFoundList();
                            for (MessageExt messageExt : messageExtList) {
                                String receiptHandle = messageExt.getProperty(MessageConst.PROPERTY_POP_CK);
                                if (receiptHandle != null) {
                                    MessageReceiptHandle messageReceiptHandle =
                                        new MessageReceiptHandle(group, topic, messageExt.getQueueId(), receiptHandle, messageExt.getMsgId(),
                                            messageExt.getQueueOffset(), messageExt.getReconsumeTimes());
                                    messagingProcessor.addReceiptHandle(ctx, grpcChannelManager.getChannel(ctx.getClientID()), group, messageExt.getMsgId(), messageReceiptHandle);
                                }
                            }
                        }
                    }
                    writer.writeAndComplete(ctx, request, popResult);
                })
                .exceptionally(t -> {
                    writer.writeAndComplete(ctx, request, t);
                    return null;
                });
        } catch (Throwable t) {
            writer.writeAndComplete(ctx, request, t);
        }
    }

    /**
     * 创建接收消息响应流写入器
     *
     * @param ctx Proxy 上下文
     * @param responseObserver 响应观察者
     * @return 响应流写入器
     */
    protected ReceiveMessageResponseStreamWriter createWriter(ProxyContext ctx,
        StreamObserver<ReceiveMessageResponse> responseObserver) {
        return new ReceiveMessageResponseStreamWriter(
            this.messagingProcessor,
            responseObserver
        );
    }

    /**
     * 接收消息队列选择器
     */
    protected static class ReceiveMessageQueueSelector implements QueueSelector {

        /**
         * 目标 broker 名称
         */
        private final String brokerName;

        /**
         * 初始化队列选择器
         *
         * @param brokerName 目标 broker 名称
         */
        public ReceiveMessageQueueSelector(String brokerName) {
            this.brokerName = brokerName;
        }

        /**
         * 选择用于接收消息的目标队列
         *
         * @param ctx Proxy 上下文
         * @param messageQueueView 队列视图
         * @return 目标队列
         */
        @Override
        public AddressableMessageQueue select(ProxyContext ctx, MessageQueueView messageQueueView) {
            try {
                AddressableMessageQueue addressableMessageQueue = null;
                MessageQueueSelector messageQueueSelector = messageQueueView.getReadSelector();

                if (StringUtils.isNotBlank(brokerName)) {
                    addressableMessageQueue = messageQueueSelector.getQueueByBrokerName(brokerName);
                }

                if (addressableMessageQueue == null) {
                    addressableMessageQueue = messageQueueSelector.selectOne(true);
                }
                return addressableMessageQueue;
            } catch (Throwable t) {
                return null;
            }
        }
    }
}
