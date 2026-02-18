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

import apache.rocketmq.v2.AckMessageEntry;
import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.AckMessageResultEntry;
import apache.rocketmq.v2.Code;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.AbstractMessingActivity;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.processor.BatchAckResult;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.message.ReceiptHandleMessage;

/**
 * Ack 消息活动实现, 支持批量与逐条 Ack
 */
public class AckMessageActivity extends AbstractMessingActivity {

    /**
     * 构造 Ack 消息活动对象
     *
     * @param messagingProcessor 消息处理器
     * @param grpcClientSettingsManager gRPC 客户端设置管理器
     * @param grpcChannelManager gRPC 通道管理器
     */
    public AckMessageActivity(MessagingProcessor messagingProcessor, GrpcClientSettingsManager grpcClientSettingsManager,
        GrpcChannelManager grpcChannelManager) {
        super(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    /**
     * 处理 Ack 消息请求
     *
     * @param ctx Proxy 上下文
     * @param request Ack 请求
     * @return Ack 响应 Future
     */
    public CompletableFuture<AckMessageResponse> ackMessage(ProxyContext ctx, AckMessageRequest request) {
        CompletableFuture<AckMessageResponse> future = new CompletableFuture<>();

        try {
            validateTopicAndConsumerGroup(request.getTopic(), request.getGroup());
            String group = request.getGroup().getName();
            String topic = request.getTopic().getName();
            if (ConfigurationManager.getProxyConfig().isEnableBatchAck()) {
                future = ackMessageInBatch(ctx, group, topic, request);
            } else {
                future = ackMessageOneByOne(ctx, group, topic, request);
            }
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    /**
     * 按批处理 Ack 请求
     *
     * @param ctx Proxy 上下文
     * @param group 消费组
     * @param topic 主题
     * @param request Ack 请求
     * @return Ack 响应 Future
     */
    protected CompletableFuture<AckMessageResponse> ackMessageInBatch(ProxyContext ctx, String group, String topic, AckMessageRequest request) {
        List<ReceiptHandleMessage> handleMessageList = new ArrayList<>(request.getEntriesCount());

        for (AckMessageEntry ackMessageEntry : request.getEntriesList()) {
            String handleString = getHandleString(ctx, group, request, ackMessageEntry);
            handleMessageList.add(new ReceiptHandleMessage(ReceiptHandle.decode(handleString), ackMessageEntry.getMessageId()));
        }
        return this.messagingProcessor.batchAckMessage(ctx, handleMessageList, group, topic)
            .thenApply(batchAckResultList -> {
                AckMessageResponse.Builder responseBuilder = AckMessageResponse.newBuilder();
                Set<Code> responseCodes = new HashSet<>();
                for (BatchAckResult batchAckResult : batchAckResultList) {
                    AckMessageResultEntry entry = convertToAckMessageResultEntry(batchAckResult);
                    responseBuilder.addEntries(entry);
                    responseCodes.add(entry.getStatus().getCode());
                }
                setAckResponseStatus(responseBuilder, responseCodes);
                return responseBuilder.build();
            });
    }

    /**
     * 将批量 Ack 结果转换为结果条目
     *
     * @param batchAckResult 批量 Ack 结果
     * @return Ack 结果条目
     */
    protected AckMessageResultEntry convertToAckMessageResultEntry(BatchAckResult batchAckResult) {
        ReceiptHandleMessage handleMessage = batchAckResult.getReceiptHandleMessage();
        AckMessageResultEntry.Builder resultBuilder = AckMessageResultEntry.newBuilder()
            .setMessageId(handleMessage.getMessageId())
            .setReceiptHandle(handleMessage.getReceiptHandle().getReceiptHandle());
        if (batchAckResult.getProxyException() != null) {
            resultBuilder.setStatus(ResponseBuilder.getInstance().buildStatus(batchAckResult.getProxyException()));
        } else {
            AckResult ackResult = batchAckResult.getAckResult();
            if (AckStatus.OK.equals(ackResult.getStatus())) {
                resultBuilder.setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()));
            } else {
                resultBuilder.setStatus(ResponseBuilder.getInstance().buildStatus(Code.INTERNAL_SERVER_ERROR, "ack failed: status is abnormal"));
            }
        }
        return resultBuilder.build();
    }

    /**
     * 逐条处理 Ack 请求
     *
     * @param ctx Proxy 上下文
     * @param group 消费组
     * @param topic 主题
     * @param request Ack 请求
     * @return Ack 响应 Future
     */
    protected CompletableFuture<AckMessageResponse> ackMessageOneByOne(ProxyContext ctx, String group, String topic, AckMessageRequest request) {
        CompletableFuture<AckMessageResponse> resultFuture = new CompletableFuture<>();
        CompletableFuture<AckMessageResultEntry>[] futures = new CompletableFuture[request.getEntriesCount()];
        for (int i = 0; i < request.getEntriesCount(); i++) {
            futures[i] = processAckMessage(ctx, group, topic, request, request.getEntries(i));
        }
        CompletableFuture.allOf(futures).whenComplete((val, throwable) -> {
            if (throwable != null) {
                resultFuture.completeExceptionally(throwable);
                return;
            }

            Set<Code> responseCodes = new HashSet<>();
            List<AckMessageResultEntry> entryList = new ArrayList<>();
            for (CompletableFuture<AckMessageResultEntry> entryFuture : futures) {
                AckMessageResultEntry entryResult = entryFuture.join();
                responseCodes.add(entryResult.getStatus().getCode());
                entryList.add(entryResult);
            }
            AckMessageResponse.Builder responseBuilder = AckMessageResponse.newBuilder()
                .addAllEntries(entryList);
            setAckResponseStatus(responseBuilder, responseCodes);
            resultFuture.complete(responseBuilder.build());
        });
        return resultFuture;
    }

    /**
     * 处理单条 Ack 条目
     *
     * @param ctx Proxy 上下文
     * @param group 消费组
     * @param topic 主题
     * @param request Ack 请求
     * @param ackMessageEntry Ack 条目
     * @return Ack 结果条目 Future
     */
    protected CompletableFuture<AckMessageResultEntry> processAckMessage(ProxyContext ctx, String group, String topic, AckMessageRequest request,
        AckMessageEntry ackMessageEntry) {
        CompletableFuture<AckMessageResultEntry> future = new CompletableFuture<>();

        try {
            String handleString = this.getHandleString(ctx, group, request, ackMessageEntry);
            CompletableFuture<AckResult> ackResultFuture = this.messagingProcessor.ackMessage(
                ctx,
                ReceiptHandle.decode(handleString),
                ackMessageEntry.getMessageId(),
                group,
                topic
            );
            ackResultFuture.thenAccept(result -> {
                future.complete(convertToAckMessageResultEntry(ctx, ackMessageEntry, result));
            }).exceptionally(t -> {
                future.complete(convertToAckMessageResultEntry(ctx, ackMessageEntry, t));
                return null;
            });
        } catch (Throwable t) {
            future.complete(convertToAckMessageResultEntry(ctx, ackMessageEntry, t));
        }
        return future;
    }

    /**
     * 将异常转换为 Ack 结果条目
     *
     * @param ctx Proxy 上下文
     * @param ackMessageEntry Ack 条目
     * @param throwable 异常对象
     * @return Ack 结果条目
     */
    protected AckMessageResultEntry convertToAckMessageResultEntry(ProxyContext ctx, AckMessageEntry ackMessageEntry, Throwable throwable) {
        return AckMessageResultEntry.newBuilder()
            .setStatus(ResponseBuilder.getInstance().buildStatus(throwable))
            .setMessageId(ackMessageEntry.getMessageId())
            .setReceiptHandle(ackMessageEntry.getReceiptHandle())
            .build();
    }

    /**
     * 将 Ack 结果转换为 Ack 结果条目
     *
     * @param ctx Proxy 上下文
     * @param ackMessageEntry Ack 条目
     * @param ackResult Ack 结果
     * @return Ack 结果条目
     */
    protected AckMessageResultEntry convertToAckMessageResultEntry(ProxyContext ctx, AckMessageEntry ackMessageEntry,
        AckResult ackResult) {
        if (AckStatus.OK.equals(ackResult.getStatus())) {
            return AckMessageResultEntry.newBuilder()
                .setMessageId(ackMessageEntry.getMessageId())
                .setReceiptHandle(ackMessageEntry.getReceiptHandle())
                .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
                .build();
        }
        return AckMessageResultEntry.newBuilder()
            .setMessageId(ackMessageEntry.getMessageId())
            .setReceiptHandle(ackMessageEntry.getReceiptHandle())
            .setStatus(ResponseBuilder.getInstance().buildStatus(Code.INTERNAL_SERVER_ERROR, "ack failed: status is abnormal"))
            .build();
    }

    /**
     * 根据条目状态集合设置整体 Ack 响应状态
     *
     * @param responseBuilder 响应构建器
     * @param responseCodes 状态码集合
     */
    protected void setAckResponseStatus(AckMessageResponse.Builder responseBuilder, Set<Code> responseCodes) {
        if (responseCodes.size() > 1) {
            responseBuilder.setStatus(ResponseBuilder.getInstance().buildStatus(Code.MULTIPLE_RESULTS, Code.MULTIPLE_RESULTS.name()));
        } else if (responseCodes.size() == 1) {
            Code code = responseCodes.stream().findAny().get();
            responseBuilder.setStatus(ResponseBuilder.getInstance().buildStatus(code, code.name()));
        } else {
            responseBuilder.setStatus(ResponseBuilder.getInstance().buildStatus(Code.INTERNAL_SERVER_ERROR, "ack message result is empty"));
        }
    }

    /**
     * 获取有效的 receiptHandle 字符串
     *
     * @param ctx Proxy 上下文
     * @param group 消费组
     * @param request Ack 请求
     * @param ackMessageEntry Ack 条目
     * @return 有效 receiptHandle 字符串
     */
    protected String getHandleString(ProxyContext ctx, String group, AckMessageRequest request, AckMessageEntry ackMessageEntry) {
        String handleString = ackMessageEntry.getReceiptHandle();
        GrpcClientChannel channel = grpcChannelManager.getChannel(ctx.getClientID());
        if (channel != null) {
            MessageReceiptHandle messageReceiptHandle = messagingProcessor.removeReceiptHandle(ctx, channel, group, ackMessageEntry.getMessageId(), ackMessageEntry.getReceiptHandle());
            if (messageReceiptHandle != null) {
                handleString = messageReceiptHandle.getReceiptHandleStr();
            }
        }
        return handleString;
    }
}
