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

package org.apache.rocketmq.proxy.grpc.v2;

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.EndTransactionResponse;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.NotifyClientTerminationResponse;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.RecallMessageRequest;
import apache.rocketmq.v2.RecallMessageResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.TelemetryCommand;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.common.utils.StartAndShutdown;

/**
 * gRPC V2 消息活动接口, 定义 Proxy 对外暴露的核心消息操作
 */
public interface GrpcMessingActivity extends StartAndShutdown {

    /**
     * 查询主题路由信息
     *
     * @param ctx Proxy 上下文
     * @param request 路由查询请求
     * @return 路由查询响应
     */
    CompletableFuture<QueryRouteResponse> queryRoute(ProxyContext ctx, QueryRouteRequest request);

    /**
     * 处理客户端心跳
     *
     * @param ctx Proxy 上下文
     * @param request 心跳请求
     * @return 心跳响应
     */
    CompletableFuture<HeartbeatResponse> heartbeat(ProxyContext ctx, HeartbeatRequest request);

    /**
     * 处理发送消息请求
     *
     * @param ctx Proxy 上下文
     * @param request 发送请求
     * @return 发送响应
     */
    CompletableFuture<SendMessageResponse> sendMessage(ProxyContext ctx, SendMessageRequest request);

    /**
     * 查询消费分配结果
     *
     * @param ctx Proxy 上下文
     * @param request 分配查询请求
     * @return 分配查询响应
     */
    CompletableFuture<QueryAssignmentResponse> queryAssignment(ProxyContext ctx, QueryAssignmentRequest request);

    /**
     * 发起流式拉取消息
     *
     * @param ctx Proxy 上下文
     * @param request 拉取请求
     * @param responseObserver 响应观察者
     */
    void receiveMessage(ProxyContext ctx, ReceiveMessageRequest request,
        StreamObserver<ReceiveMessageResponse> responseObserver);

    /**
     * 处理消息确认请求
     *
     * @param ctx Proxy 上下文
     * @param request 确认请求
     * @return 确认响应
     */
    CompletableFuture<AckMessageResponse> ackMessage(ProxyContext ctx, AckMessageRequest request);

    /**
     * 转发消息到死信队列
     *
     * @param ctx Proxy 上下文
     * @param request 转发请求
     * @return 转发响应
     */
    CompletableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(ProxyContext ctx,
        ForwardMessageToDeadLetterQueueRequest request);

    /**
     * 结束事务消息状态
     *
     * @param ctx Proxy 上下文
     * @param request 事务结束请求
     * @return 事务结束响应
     */
    CompletableFuture<EndTransactionResponse> endTransaction(ProxyContext ctx, EndTransactionRequest request);

    /**
     * 通知客户端终止
     *
     * @param ctx Proxy 上下文
     * @param request 终止通知请求
     * @return 终止通知响应
     */
    CompletableFuture<NotifyClientTerminationResponse> notifyClientTermination(ProxyContext ctx,
        NotifyClientTerminationRequest request);

    /**
     * 修改消息不可见时长
     *
     * @param ctx Proxy 上下文
     * @param request 时长变更请求
     * @return 时长变更响应
     */
    CompletableFuture<ChangeInvisibleDurationResponse> changeInvisibleDuration(ProxyContext ctx,
        ChangeInvisibleDurationRequest request);

    /**
     * 撤回延时消息
     *
     * @param ctx Proxy 上下文
     * @param request 撤回请求
     * @return 撤回响应
     */
    CompletableFuture<RecallMessageResponse> recallMessage(ProxyContext ctx, RecallMessageRequest request);

    /**
     * 建立遥测双向流
     *
     * @param responseObserver 服务端响应观察者
     * @return 带上下文的请求观察者
     */
    ContextStreamObserver<TelemetryCommand> telemetry(StreamObserver<TelemetryCommand> responseObserver);
}
