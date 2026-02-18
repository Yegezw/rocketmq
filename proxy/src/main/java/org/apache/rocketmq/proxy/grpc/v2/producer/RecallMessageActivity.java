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
import apache.rocketmq.v2.RecallMessageRequest;
import apache.rocketmq.v2.RecallMessageResponse;
import apache.rocketmq.v2.Resource;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.AbstractMessingActivity;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * 撤回消息活动实现
 */
public class RecallMessageActivity extends AbstractMessingActivity {

    /**
     * 构造撤回消息活动对象
     *
     * @param messagingProcessor 消息处理器
     * @param grpcClientSettingsManager gRPC 客户端设置管理器
     * @param grpcChannelManager gRPC 通道管理器
     */
    public RecallMessageActivity(MessagingProcessor messagingProcessor,
                                 GrpcClientSettingsManager grpcClientSettingsManager, GrpcChannelManager grpcChannelManager) {
        super(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    /**
     * 处理撤回消息请求
     *
     * @param ctx Proxy 上下文
     * @param request 撤回消息请求
     * @return 撤回消息响应 Future
     */
    public CompletableFuture<RecallMessageResponse> recallMessage(ProxyContext ctx,
        RecallMessageRequest request) {
        CompletableFuture<RecallMessageResponse> future = new CompletableFuture<>();

        try {
            Resource topic = request.getTopic();
            validateTopic(topic);

            future = this.messagingProcessor.recallMessage(
                ctx,
                topic.getName(),
                request.getRecallHandle(),
                Duration.ofSeconds(2).toMillis()
            ).thenApply(result -> RecallMessageResponse.newBuilder()
                .setMessageId(result)
                .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
                .build());
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }
}
