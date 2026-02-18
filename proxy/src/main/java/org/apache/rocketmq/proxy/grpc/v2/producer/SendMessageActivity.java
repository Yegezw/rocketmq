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
package org.apache.rocketmq.proxy.grpc.v2.producer;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Encoding;
import apache.rocketmq.v2.MessageType;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.SendResultEntry;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.v2.AbstractMessingActivity;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcProxyException;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcValidator;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.QueueSelector;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;

/**
 * 发送消息活动实现, 负责 gRPC 发送请求校验与发送结果转换
 */
public class SendMessageActivity extends AbstractMessingActivity {

    /**
     * 构造发送消息活动对象
     *
     * @param messagingProcessor 消息处理器
     * @param grpcClientSettingsManager gRPC 客户端设置管理器
     * @param grpcChannelManager gRPC 通道管理器
     */
    public SendMessageActivity(MessagingProcessor messagingProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager, GrpcChannelManager grpcChannelManager) {
        super(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    /**
     * 处理发送消息请求
     *
     * @param ctx Proxy 上下文
     * @param request 发送消息请求
     * @return 发送消息响应 Future
     */
    public CompletableFuture<SendMessageResponse> sendMessage(ProxyContext ctx, SendMessageRequest request) {
        CompletableFuture<SendMessageResponse> future = new CompletableFuture<>();

        try {
            if (request.getMessagesCount() <= 0) {
                throw new GrpcProxyException(Code.MESSAGE_CORRUPTED, "no message to send");
            }

            List<apache.rocketmq.v2.Message> messageList = request.getMessagesList();
            apache.rocketmq.v2.Message message = messageList.get(0);
            Resource topic = message.getTopic();
            validateTopic(topic);

            future = this.messagingProcessor.sendMessage(
                ctx,
                new SendMessageQueueSelector(request),
                topic.getName(),
                buildSysFlag(message),
                buildMessage(ctx, request.getMessagesList(), topic)
            ).thenApply(result -> convertToSendMessageResponse(ctx, request, result));
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    /**
     * 将 gRPC 消息列表构建为内部消息列表
     *
     * @param context Proxy 上下文
     * @param protoMessageList gRPC 消息列表
     * @param topic 主题资源
     * @return 内部消息列表
     */
    protected List<Message> buildMessage(ProxyContext context, List<apache.rocketmq.v2.Message> protoMessageList,
        Resource topic) {
        String topicName = topic.getName();
        List<Message> messageExtList = new ArrayList<>();
        for (apache.rocketmq.v2.Message protoMessage : protoMessageList) {
            if (!protoMessage.getTopic().equals(topic)) {
                throw new GrpcProxyException(Code.MESSAGE_CORRUPTED, "topic in message is not same");
            }
            // here use topicName as producerGroup for transactional checker.
            // 这里使用 topicName 作为事务回查使用的 producerGroup
            messageExtList.add(buildMessage(context, protoMessage, topicName));
        }
        return messageExtList;
    }

    /**
     * 将单条 gRPC 消息构建为内部消息对象
     *
     * @param context Proxy 上下文
     * @param protoMessage gRPC 消息
     * @param producerGroup 生产者组
     * @return 内部消息对象
     */
    protected Message buildMessage(ProxyContext context, apache.rocketmq.v2.Message protoMessage, String producerGroup) {
        String topicName = protoMessage.getTopic().getName();

        validateMessageBodySize(protoMessage.getBody());
        Message messageExt = new Message();
        messageExt.setTopic(topicName);
        messageExt.setBody(protoMessage.getBody().toByteArray());
        Map<String, String> messageProperty = this.buildMessageProperty(context, protoMessage, producerGroup);

        MessageAccessor.setProperties(messageExt, messageProperty);
        return messageExt;
    }

    /**
     * 构建消息系统标记位
     *
     * @param protoMessage gRPC 消息
     * @return 系统标记位
     */
    protected int buildSysFlag(apache.rocketmq.v2.Message protoMessage) {
        // sysFlag (body encoding & message type)
        // 系统标记位包含消息体编码与消息类型
        int sysFlag = 0;
        Encoding bodyEncoding = protoMessage.getSystemProperties().getBodyEncoding();
        if (bodyEncoding.equals(Encoding.GZIP)) {
            sysFlag |= MessageSysFlag.COMPRESSED_FLAG;
        }
        // transaction
        // 事务消息标记
        MessageType messageType = protoMessage.getSystemProperties().getMessageType();
        if (messageType.equals(MessageType.TRANSACTION)) {
            sysFlag |= MessageSysFlag.TRANSACTION_PREPARED_TYPE;
        }
        return sysFlag;
    }

    /**
     * 校验消息体大小
     *
     * @param body 消息体
     */
    protected void validateMessageBodySize(ByteString body) {
        if (ConfigurationManager.getProxyConfig().isEnableMessageBodyEmptyCheck()) {
            if (body.isEmpty()) {
                throw new GrpcProxyException(Code.MESSAGE_BODY_EMPTY, "message body cannot be empty");
            }
        }
        int max = ConfigurationManager.getProxyConfig().getMaxMessageSize();
        if (max <= 0) {
            return;
        }
        if (body.size() > max) {
            throw new GrpcProxyException(Code.MESSAGE_BODY_TOO_LARGE, "message body cannot exceed the max " + max);
        }
    }

    /**
     * 校验消息键值
     *
     * @param key 消息键值
     */
    protected void validateMessageKey(String key) {
        if (StringUtils.isNotEmpty(key)) {
            if (StringUtils.isBlank(key)) {
                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_KEY, "key cannot be the char sequence of whitespace");
            }
            if (GrpcValidator.getInstance().containControlCharacter(key)) {
                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_KEY, "key cannot contain control character");
            }
        }
    }

    /**
     * 校验消息分组
     *
     * @param messageGroup 消息分组
     */
    protected void validateMessageGroup(String messageGroup) {
        if (StringUtils.isNotEmpty(messageGroup)) {
            if (StringUtils.isBlank(messageGroup)) {
                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_GROUP, "message group cannot be the char sequence of whitespace");
            }
            int maxSize = ConfigurationManager.getProxyConfig().getMaxMessageGroupSize();
            if (maxSize <= 0) {
                return;
            }
            if (messageGroup.getBytes(StandardCharsets.UTF_8).length >= maxSize) {
                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_GROUP, "message group exceed the max size " + maxSize);
            }
            if (GrpcValidator.getInstance().containControlCharacter(messageGroup)) {
                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_GROUP, "message group cannot contain control character");
            }
        }
    }

    /**
     * 校验延迟投递时间
     *
     * @param deliveryTimestampMs 投递时间戳毫秒值
     */
    protected void validateDelayTime(long deliveryTimestampMs) {
        long maxDelay = ConfigurationManager.getProxyConfig().getMaxDelayTimeMills();
        if (maxDelay <= 0) {
            return;
        }
        if (deliveryTimestampMs - System.currentTimeMillis() > maxDelay) {
            throw new GrpcProxyException(Code.ILLEGAL_DELIVERY_TIME, "the max delay time of message is too large, max is " + maxDelay);
        }
    }

    /**
     * 校验事务回查恢复时间
     *
     * @param transactionRecoverySecond 恢复时间秒值
     */
    protected void validateTransactionRecoverySecond(long transactionRecoverySecond) {
        long maxTransactionRecoverySecond = ConfigurationManager.getProxyConfig().getMaxTransactionRecoverySecond();
        if (maxTransactionRecoverySecond <= 0) {
            return;
        }
        if (transactionRecoverySecond > maxTransactionRecoverySecond) {
            throw new GrpcProxyException(Code.BAD_REQUEST, "the max transaction recovery time of message is too large, max is " + maxTransactionRecoverySecond);
        }
    }

    /**
     * 构建内部消息属性
     *
     * @param context Proxy 上下文
     * @param message gRPC 消息
     * @param producerGroup 生产者组
     * @return 消息属性表
     */
    protected Map<String, String> buildMessageProperty(ProxyContext context, apache.rocketmq.v2.Message message, String producerGroup) {
        long userPropertySize = 0;
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        org.apache.rocketmq.common.message.Message messageWithHeader = new org.apache.rocketmq.common.message.Message();
        // set user properties
        // 设置用户属性
        Map<String, String> userProperties = message.getUserPropertiesMap();
        if (userProperties.size() > config.getUserPropertyMaxNum()) {
            throw new GrpcProxyException(Code.MESSAGE_PROPERTIES_TOO_LARGE, "too many user properties, max is " + config.getUserPropertyMaxNum());
        }
        for (Map.Entry<String, String> userPropertiesEntry : userProperties.entrySet()) {
            if (MessageConst.STRING_HASH_SET.contains(userPropertiesEntry.getKey())) {
                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_PROPERTY_KEY, "property is used by system: " + userPropertiesEntry.getKey());
            }
            if (GrpcValidator.getInstance().containControlCharacter(userPropertiesEntry.getKey())) {
                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_PROPERTY_KEY, "the key of property cannot contain control character");
            }
            if (GrpcValidator.getInstance().containControlCharacter(userPropertiesEntry.getValue())) {
                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_PROPERTY_KEY, "the value of property cannot contain control character");
            }
            userPropertySize += userPropertiesEntry.getKey().getBytes(StandardCharsets.UTF_8).length;
            userPropertySize += userPropertiesEntry.getValue().getBytes(StandardCharsets.UTF_8).length;
        }
        MessageAccessor.setProperties(messageWithHeader, Maps.newHashMap(userProperties));

        // set tag
        // 设置标签
        String tag = message.getSystemProperties().getTag();
        GrpcValidator.getInstance().validateTag(tag);
        messageWithHeader.setTags(tag);
        userPropertySize += tag.getBytes(StandardCharsets.UTF_8).length;

        // set keys
        // 设置 keys
        List<String> keysList = message.getSystemProperties().getKeysList();
        for (String key : keysList) {
            validateMessageKey(key);
            userPropertySize += key.getBytes(StandardCharsets.UTF_8).length;
        }
        if (keysList.size() > 0) {
            messageWithHeader.setKeys(keysList);
        }

        if (userPropertySize > config.getMaxUserPropertySize()) {
            throw new GrpcProxyException(Code.MESSAGE_PROPERTIES_TOO_LARGE, "the total size of user property is too large, max is " + config.getMaxUserPropertySize());
        }

        // set message id
        // 设置消息标识
        String messageId = message.getSystemProperties().getMessageId();
        if (StringUtils.isBlank(messageId)) {
            throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_ID, "message id cannot be empty");
        }
        MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, messageId);

        // set transaction property
        // 设置事务属性
        MessageType messageType = message.getSystemProperties().getMessageType();
        if (messageType.equals(MessageType.TRANSACTION)) {
            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");

            if (message.getSystemProperties().hasOrphanedTransactionRecoveryDuration()) {
                long transactionRecoverySecond = Durations.toSeconds(message.getSystemProperties().getOrphanedTransactionRecoveryDuration());
                validateTransactionRecoverySecond(transactionRecoverySecond);
                MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS,
                    String.valueOf(transactionRecoverySecond));
            }
        }

        // set delay level or deliver timestamp
        // 设置延迟级别或投递时间戳
        fillDelayMessageProperty(message, messageWithHeader);

        // set reconsume times
        // 设置重试次数
        int reconsumeTimes = message.getSystemProperties().getDeliveryAttempt();
        MessageAccessor.setReconsumeTime(messageWithHeader, String.valueOf(reconsumeTimes));
        // set producer group
        // 设置生产者组
        MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_PRODUCER_GROUP, producerGroup);
        // set message group
        // 设置消息分组
        String messageGroup = message.getSystemProperties().getMessageGroup();
        if (StringUtils.isNotEmpty(messageGroup)) {
            validateMessageGroup(messageGroup);
            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_SHARDING_KEY, messageGroup);
        }
        // set trace context
        // 设置追踪上下文
        String traceContext = message.getSystemProperties().getTraceContext();
        if (!traceContext.isEmpty()) {
            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_TRACE_CONTEXT, traceContext);
        }

        String bornHost = message.getSystemProperties().getBornHost();
        if (StringUtils.isBlank(bornHost)) {
            bornHost = context.getRemoteAddress();
        }
        if (StringUtils.isNotBlank(bornHost)) {
            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_BORN_HOST, bornHost);
        }

        Timestamp bornTimestamp = message.getSystemProperties().getBornTimestamp();
        if (Timestamps.isValid(bornTimestamp)) {
            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_BORN_TIMESTAMP, String.valueOf(Timestamps.toMillis(bornTimestamp)));
        }

        return messageWithHeader.getProperties();
    }

    /**
     * 填充延迟消息属性
     *
     * @param message gRPC 消息
     * @param messageWithHeader 内部消息对象
     */
    protected void fillDelayMessageProperty(apache.rocketmq.v2.Message message, org.apache.rocketmq.common.message.Message messageWithHeader) {
        if (message.getSystemProperties().hasDeliveryTimestamp()) {
            Timestamp deliveryTimestamp = message.getSystemProperties().getDeliveryTimestamp();
            long deliveryTimestampMs = Timestamps.toMillis(deliveryTimestamp);
            validateDelayTime(deliveryTimestampMs);

            ProxyConfig config = ConfigurationManager.getProxyConfig();
            if (config.isUseDelayLevel()) {
                int delayLevel = config.computeDelayLevel(deliveryTimestampMs);
                MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_DELAY_TIME_LEVEL, String.valueOf(delayLevel));
            }

            String timestampString = String.valueOf(deliveryTimestampMs);
            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_TIMER_DELIVER_MS, timestampString);
        }
    }

    /**
     * 将内部发送结果转换为 gRPC 响应
     *
     * @param ctx Proxy 上下文
     * @param request 发送请求
     * @param resultList 发送结果列表
     * @return 发送响应
     */
    protected SendMessageResponse convertToSendMessageResponse(ProxyContext ctx, SendMessageRequest request,
        List<SendResult> resultList) {
        SendMessageResponse.Builder builder = SendMessageResponse.newBuilder();

        Set<Code> responseCodes = new HashSet<>();
        for (SendResult result : resultList) {
            SendResultEntry resultEntry;
            switch (result.getSendStatus()) {
                case FLUSH_DISK_TIMEOUT:
                    resultEntry = SendResultEntry.newBuilder()
                        .setStatus(ResponseBuilder.getInstance().buildStatus(Code.MASTER_PERSISTENCE_TIMEOUT, "send message failed, sendStatus=" + result.getSendStatus()))
                        .build();
                    break;
                case FLUSH_SLAVE_TIMEOUT:
                    resultEntry = SendResultEntry.newBuilder()
                        .setStatus(ResponseBuilder.getInstance().buildStatus(Code.SLAVE_PERSISTENCE_TIMEOUT, "send message failed, sendStatus=" + result.getSendStatus()))
                        .build();
                    break;
                case SLAVE_NOT_AVAILABLE:
                    resultEntry = SendResultEntry.newBuilder()
                        .setStatus(ResponseBuilder.getInstance().buildStatus(Code.HA_NOT_AVAILABLE, "send message failed, sendStatus=" + result.getSendStatus()))
                        .build();
                    break;
                case SEND_OK:
                    resultEntry = SendResultEntry.newBuilder()
                        .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
                        .setOffset(result.getQueueOffset())
                        .setMessageId(StringUtils.defaultString(result.getMsgId()))
                        .setTransactionId(StringUtils.defaultString(result.getTransactionId()))
                        .setRecallHandle(StringUtils.defaultString(result.getRecallHandle()))
                        .build();
                    break;
                default:
                    resultEntry = SendResultEntry.newBuilder()
                        .setStatus(ResponseBuilder.getInstance().buildStatus(Code.INTERNAL_SERVER_ERROR, "send message failed, sendStatus=" + result.getSendStatus()))
                        .build();
                    break;
            }
            builder.addEntries(resultEntry);
            responseCodes.add(resultEntry.getStatus().getCode());
        }
        if (responseCodes.size() > 1) {
            builder.setStatus(ResponseBuilder.getInstance().buildStatus(Code.MULTIPLE_RESULTS, Code.MULTIPLE_RESULTS.name()));
        } else if (responseCodes.size() == 1) {
            Code code = responseCodes.stream().findAny().get();
            builder.setStatus(ResponseBuilder.getInstance().buildStatus(code, code.name()));
        } else {
            builder.setStatus(ResponseBuilder.getInstance().buildStatus(Code.INTERNAL_SERVER_ERROR, "send status is empty"));
        }
        return builder.build();
    }

    /**
     * 发送消息队列选择器
     */
    protected static class SendMessageQueueSelector implements QueueSelector {

        /**
         * 发送请求
         */
        private final SendMessageRequest request;

        /**
         * 初始化队列选择器
         *
         * @param request 发送请求
         */
        public SendMessageQueueSelector(SendMessageRequest request) {
            this.request = request;
        }

        /**
         * 按消息属性选择目标队列
         *
         * @param ctx Proxy 上下文
         * @param messageQueueView 队列视图
         * @return 目标队列
         */
        @Override
        public AddressableMessageQueue select(ProxyContext ctx, MessageQueueView messageQueueView) {
            try {
                apache.rocketmq.v2.Message message = request.getMessages(0);
                String shardingKey = null;
                if (request.getMessagesCount() == 1) {
                    shardingKey = message.getSystemProperties().getMessageGroup();
                }
                AddressableMessageQueue targetMessageQueue;
                if (StringUtils.isNotEmpty(shardingKey)) {
                    // With shardingKey
                    // 使用 shardingKey 做一致性哈希选队列
                    List<AddressableMessageQueue> writeQueues = messageQueueView.getWriteSelector().getQueues();
                    int bucket = Hashing.consistentHash(shardingKey.hashCode(), writeQueues.size());
                    targetMessageQueue = writeQueues.get(bucket);
                } else {
                    targetMessageQueue = messageQueueView.getWriteSelector().selectOneByPipeline(false);
                }
                return targetMessageQueue;
            } catch (Exception e) {
                return null;
            }
        }
    }

}
