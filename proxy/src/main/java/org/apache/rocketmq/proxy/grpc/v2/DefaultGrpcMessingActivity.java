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

import apache.rocketmq.v2.*;
import io.grpc.stub.StreamObserver;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.AbstractStartAndShutdown;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.client.ClientActivity;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.consumer.AckMessageActivity;
import org.apache.rocketmq.proxy.grpc.v2.consumer.ChangeInvisibleDurationActivity;
import org.apache.rocketmq.proxy.grpc.v2.consumer.ReceiveMessageActivity;
import org.apache.rocketmq.proxy.grpc.v2.producer.ForwardMessageToDLQActivity;
import org.apache.rocketmq.proxy.grpc.v2.producer.RecallMessageActivity;
import org.apache.rocketmq.proxy.grpc.v2.producer.SendMessageActivity;
import org.apache.rocketmq.proxy.grpc.v2.route.RouteActivity;
import org.apache.rocketmq.proxy.grpc.v2.transaction.EndTransactionActivity;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;

import java.util.concurrent.CompletableFuture;

/**
 * gRPC V2 默认消息活动实现, 负责聚合各子活动并对外提供统一入口
 */
public class DefaultGrpcMessingActivity extends AbstractStartAndShutdown implements GrpcMessingActivity {
    /**
     * Proxy 模块日志记录器
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    /**
     * gRPC 客户端设置管理器
     */
    protected GrpcClientSettingsManager grpcClientSettingsManager;
    /**
     * gRPC 通道管理器
     */
    protected GrpcChannelManager grpcChannelManager;

    /**
     * 拉取消息活动
     */
    protected ReceiveMessageActivity receiveMessageActivity;
    /**
     * 消息确认活动
     */
    protected AckMessageActivity ackMessageActivity;
    /**
     * 修改不可见时长活动
     */
    protected ChangeInvisibleDurationActivity changeInvisibleDurationActivity;
    /**
     * 发送消息活动
     */
    protected SendMessageActivity sendMessageActivity;
    /**
     * 消息撤回活动
     */
    protected RecallMessageActivity recallMessageActivity;
    /**
     * 转发死信活动
     */
    protected ForwardMessageToDLQActivity forwardMessageToDLQActivity;
    /**
     * 结束事务活动
     */
    protected EndTransactionActivity endTransactionActivity;
    /**
     * 路由活动
     */
    protected RouteActivity routeActivity;
    /**
     * 客户端管理活动
     */
    protected ClientActivity clientActivity;

    /**
     * 构造默认消息活动实现
     *
     * @param messagingProcessor 消息处理器
     */
    protected DefaultGrpcMessingActivity(MessagingProcessor messagingProcessor) {
        this.init(messagingProcessor);
    }

    /**
     * 初始化活动依赖与子活动实例
     *
     * @param messagingProcessor 消息处理器
     */
    protected void init(MessagingProcessor messagingProcessor) {
        this.grpcClientSettingsManager = new GrpcClientSettingsManager(messagingProcessor);
        this.grpcChannelManager = new GrpcChannelManager(messagingProcessor.getProxyRelayService(), this.grpcClientSettingsManager);

        this.receiveMessageActivity = new ReceiveMessageActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.ackMessageActivity = new AckMessageActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.changeInvisibleDurationActivity = new ChangeInvisibleDurationActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.sendMessageActivity = new SendMessageActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.recallMessageActivity = new RecallMessageActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.forwardMessageToDLQActivity = new ForwardMessageToDLQActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.endTransactionActivity = new EndTransactionActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.routeActivity = new RouteActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.clientActivity = new ClientActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);

        this.appendStartAndShutdown(this.grpcClientSettingsManager);
    }

    /**
     * 查询主题路由信息
     *
     * @param ctx Proxy 上下文
     * @param request 路由查询请求
     * @return 路由查询响应
     */
    @Override
    public CompletableFuture<QueryRouteResponse> queryRoute(ProxyContext ctx, QueryRouteRequest request) {
        return this.routeActivity.queryRoute(ctx, request);
    }

    /**
     * 处理客户端心跳
     *
     * @param ctx Proxy 上下文
     * @param request 心跳请求
     * @return 心跳响应
     */
    @Override
    public CompletableFuture<HeartbeatResponse> heartbeat(ProxyContext ctx, HeartbeatRequest request) {
        return this.clientActivity.heartbeat(ctx, request);
    }

    /**
     * 处理发送消息请求
     *
     * @param ctx Proxy 上下文
     * @param request 发送请求
     * @return 发送响应
     */
    @Override
    public CompletableFuture<SendMessageResponse> sendMessage(ProxyContext ctx, SendMessageRequest request) {
        return this.sendMessageActivity.sendMessage(ctx, request);
    }

    /**
     * 查询消费分配结果
     *
     * @param ctx Proxy 上下文
     * @param request 分配查询请求
     * @return 分配查询响应
     */
    @Override
    public CompletableFuture<QueryAssignmentResponse> queryAssignment(ProxyContext ctx,
        QueryAssignmentRequest request) {
        return this.routeActivity.queryAssignment(ctx, request);
    }

    /**
     * 处理流式拉取消息
     *
     * @param ctx Proxy 上下文
     * @param request 拉取请求
     * @param responseObserver 响应观察者
     */
    @Override
    public void receiveMessage(ProxyContext ctx, ReceiveMessageRequest request,
        StreamObserver<ReceiveMessageResponse> responseObserver) {
        this.receiveMessageActivity.receiveMessage(ctx, request, responseObserver);
    }

    /**
     * 处理消息确认请求
     *
     * @param ctx Proxy 上下文
     * @param request 确认请求
     * @return 确认响应
     */
    @Override
    public CompletableFuture<AckMessageResponse> ackMessage(ProxyContext ctx, AckMessageRequest request) {
        return this.ackMessageActivity.ackMessage(ctx, request);
    }

    /**
     * 转发消息到死信队列
     *
     * @param ctx Proxy 上下文
     * @param request 转发请求
     * @return 转发响应
     */
    @Override
    public CompletableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(ProxyContext ctx,
        ForwardMessageToDeadLetterQueueRequest request) {
        return this.forwardMessageToDLQActivity.forwardMessageToDeadLetterQueue(ctx, request);
    }

    /**
     * 结束事务状态
     *
     * @param ctx Proxy 上下文
     * @param request 事务结束请求
     * @return 事务结束响应
     */
    @Override
    public CompletableFuture<EndTransactionResponse> endTransaction(ProxyContext ctx, EndTransactionRequest request) {
        return this.endTransactionActivity.endTransaction(ctx, request);
    }

    /**
     * 通知客户端终止
     *
     * @param ctx Proxy 上下文
     * @param request 终止通知请求
     * @return 终止通知响应
     */
    @Override
    public CompletableFuture<NotifyClientTerminationResponse> notifyClientTermination(ProxyContext ctx,
        NotifyClientTerminationRequest request) {
        return this.clientActivity.notifyClientTermination(ctx, request);
    }

    /**
     * 修改消息不可见时长
     *
     * @param ctx Proxy 上下文
     * @param request 时长变更请求
     * @return 时长变更响应
     */
    @Override
    public CompletableFuture<ChangeInvisibleDurationResponse> changeInvisibleDuration(ProxyContext ctx,
        ChangeInvisibleDurationRequest request) {
        return this.changeInvisibleDurationActivity.changeInvisibleDuration(ctx, request);
    }

    /**
     * 撤回延时消息
     *
     * @param ctx Proxy 上下文
     * @param request 撤回请求
     * @return 撤回响应
     */
    @Override
    public CompletableFuture<RecallMessageResponse> recallMessage(ProxyContext ctx,
        RecallMessageRequest request) {
        return this.recallMessageActivity.recallMessage(ctx, request);
    }

    /**
     * 建立遥测双向流
     *
     * @param responseObserver 服务端响应观察者
     * @return 带上下文的请求观察者
     */
    @Override
    public ContextStreamObserver<TelemetryCommand> telemetry(StreamObserver<TelemetryCommand> responseObserver) {
        return this.clientActivity.telemetry(responseObserver);
    }
}
