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

import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.AbstractMessingActivity;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 转发消息到死信队列活动实现
 */
public class ForwardMessageToDLQActivity extends AbstractMessingActivity {

    /**
     * 构造转发死信活动对象
     *
     * @param messagingProcessor 消息处理器
     * @param grpcClientSettingsManager gRPC 客户端设置管理器
     * @param grpcChannelManager gRPC 通道管理器
     */
    public ForwardMessageToDLQActivity(MessagingProcessor messagingProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager, GrpcChannelManager grpcChannelManager) {
        super(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    /**
     * 处理转发消息到死信队列请求
     *
     * @param ctx Proxy 上下文
     * @param request 转发死信请求
     * @return 转发死信响应 Future
     */
    public CompletableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(ProxyContext ctx,
        ForwardMessageToDeadLetterQueueRequest request) {
        CompletableFuture<ForwardMessageToDeadLetterQueueResponse> future = new CompletableFuture<>();
        try {
            validateTopicAndConsumerGroup(request.getTopic(), request.getGroup());

            String group = request.getGroup().getName();
            String handleString = request.getReceiptHandle();
            MessageReceiptHandle messageReceiptHandle = messagingProcessor.removeReceiptHandle(ctx, grpcChannelManager.getChannel(ctx.getClientID()), group, request.getMessageId(), request.getReceiptHandle());
            if (messageReceiptHandle != null) {
                handleString = messageReceiptHandle.getReceiptHandleStr();
            }
            ReceiptHandle receiptHandle = ReceiptHandle.decode(handleString);

            return this.messagingProcessor.forwardMessageToDeadLetterQueue(
                ctx,
                receiptHandle,
                request.getMessageId(),
                request.getGroup().getName(),
                request.getTopic().getName()
            ).thenApply(result -> convertToForwardMessageToDeadLetterQueueResponse(ctx, result));
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    /**
     * 将内部返回码转换为 gRPC 转发死信响应
     *
     * @param ctx Proxy 上下文
     * @param result 内部返回命令
     * @return gRPC 转发死信响应
     */
    protected ForwardMessageToDeadLetterQueueResponse convertToForwardMessageToDeadLetterQueueResponse(ProxyContext ctx,
        RemotingCommand result) {
        return ForwardMessageToDeadLetterQueueResponse.newBuilder()
            .setStatus(ResponseBuilder.getInstance().buildStatus(result.getCode(), result.getRemark()))
            .build();
    }
}
