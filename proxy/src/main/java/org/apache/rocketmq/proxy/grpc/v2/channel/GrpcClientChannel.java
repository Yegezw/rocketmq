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
package org.apache.rocketmq.proxy.grpc.v2.channel;

import apache.rocketmq.v2.PrintThreadStackTraceCommand;
import apache.rocketmq.v2.RecoverOrphanedTransactionCommand;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.VerifyMessageCommand;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import com.google.protobuf.util.JsonFormat;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.channel.ChannelHelper;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcConverter;
import org.apache.rocketmq.proxy.processor.channel.ChannelExtendAttributeGetter;
import org.apache.rocketmq.proxy.processor.channel.ChannelProtocolType;
import org.apache.rocketmq.proxy.processor.channel.RemoteChannel;
import org.apache.rocketmq.proxy.processor.channel.RemoteChannelConverter;
import org.apache.rocketmq.proxy.service.relay.ProxyChannel;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayResult;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.proxy.service.transaction.TransactionData;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;

/**
 * gRPC 客户端通道实现, 负责遥测命令下发与远端通道能力转换
 */
public class GrpcClientChannel extends ProxyChannel implements ChannelExtendAttributeGetter, RemoteChannelConverter {
    /**
     * Proxy 模块日志记录器
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    /**
     * gRPC 通道管理器
     */
    private final GrpcChannelManager grpcChannelManager;
    /**
     * gRPC 客户端设置管理器
     */
    private final GrpcClientSettingsManager grpcClientSettingsManager;

    /**
     * 遥测写入观察者引用
     */
    private final AtomicReference<StreamObserver<TelemetryCommand>> telemetryCommandRef = new AtomicReference<>();
    /**
     * 遥测写入锁
     */
    private final Object telemetryWriteLock = new Object();
    /**
     * 客户端标识
     */
    private final String clientId;

    /**
     * 构造 gRPC 客户端通道
     *
     * @param proxyRelayService 代理转发服务
     * @param grpcClientSettingsManager gRPC 客户端设置管理器
     * @param grpcChannelManager gRPC 通道管理器
     * @param ctx Proxy 上下文
     * @param clientId 客户端标识
     */
    public GrpcClientChannel(ProxyRelayService proxyRelayService, GrpcClientSettingsManager grpcClientSettingsManager,
        GrpcChannelManager grpcChannelManager, ProxyContext ctx, String clientId) {
        super(proxyRelayService, null, new GrpcChannelId(clientId),
            ctx.getRemoteAddress(),
            ctx.getLocalAddress());
        this.grpcChannelManager = grpcChannelManager;
        this.grpcClientSettingsManager = grpcClientSettingsManager;
        this.clientId = clientId;
    }

    @Override
    public String getChannelExtendAttribute() {
        Settings settings = this.grpcClientSettingsManager.getRawClientSettings(this.clientId);
        if (settings == null) {
            return null;
        }
        try {
            return JsonFormat.printer().print(settings);
        } catch (InvalidProtocolBufferException e) {
            log.error("convert settings to json data failed. settings:{}", settings, e);
        }
        return null;
    }

    /**
     * 从 Channel 扩展属性中解析客户端设置
     *
     * @param channel 网络通道
     * @return 客户端设置, 不可解析时返回 null
     */
    public static Settings parseChannelExtendAttribute(Channel channel) {
        if (ChannelHelper.getChannelProtocolType(channel).equals(ChannelProtocolType.GRPC_V2) &&
            channel instanceof ChannelExtendAttributeGetter) {
            String attr = ((ChannelExtendAttributeGetter) channel).getChannelExtendAttribute();
            if (attr == null) {
                return null;
            }

            Settings.Builder builder = Settings.newBuilder();
            try {
                JsonFormat.parser().merge(attr, builder);
                return builder.build();
            } catch (InvalidProtocolBufferException e) {
                log.error("convert settings json data to settings failed. data:{}", attr, e);
                return null;
            }
        }
        return null;
    }

    /**
     * 转换为远端通道描述对象
     *
     * @return 远端通道对象
     */
    @Override
    public RemoteChannel toRemoteChannel() {
        return new RemoteChannel(
            ConfigurationManager.getProxyConfig().getLocalServeAddr(),
            this.getRemoteAddress(),
            this.getLocalAddress(),
            ChannelProtocolType.GRPC_V2,
            this.getChannelExtendAttribute());
    }

    /**
     * gRPC 通道标识实现
     */
    protected static class GrpcChannelId implements ChannelId {

        /**
         * 客户端标识
         */
        private final String clientId;

        /**
         * 构造通道标识
         *
         * @param clientId 客户端标识
         */
        public GrpcChannelId(String clientId) {
            this.clientId = clientId;
        }

        /**
         * 返回短文本标识
         *
         * @return 短文本标识
         */
        @Override
        public String asShortText() {
            return this.clientId;
        }

        /**
         * 返回长文本标识
         *
         * @return 长文本标识
         */
        @Override
        public String asLongText() {
            return this.clientId;
        }

        /**
         * 比较通道标识顺序
         *
         * @param o 目标通道标识
         * @return 比较结果
         */
        @Override
        public int compareTo(ChannelId o) {
            if (this == o) {
                return 0;
            }
            if (o instanceof GrpcChannelId) {
                GrpcChannelId other = (GrpcChannelId) o;
                return ComparisonChain.start()
                    .compare(this.clientId, other.clientId)
                    .result();
            }

            return asLongText().compareTo(o.asLongText());
        }
    }

    public void setClientObserver(StreamObserver<TelemetryCommand> future) {
        this.telemetryCommandRef.set(future);
    }

    /**
     * 在观察者匹配时清理遥测观察者引用
     *
     * @param future 目标观察者
     */
    protected void clearClientObserver(StreamObserver<TelemetryCommand> future) {
        this.telemetryCommandRef.compareAndSet(future, null);
    }

    @Override
    public boolean isOpen() {
        return this.telemetryCommandRef.get() != null;
    }

    @Override
    public boolean isActive() {
        return this.telemetryCommandRef.get() != null;
    }

    @Override
    public boolean isWritable() {
        return this.telemetryCommandRef.get() != null;
    }

    /**
     * 处理非标准消息并在需要时下发遥测命令
     *
     * @param msg 待处理消息
     * @return 异步执行结果
     */
    @Override
    protected CompletableFuture<Void> processOtherMessage(Object msg) {
        if (msg instanceof TelemetryCommand) {
            TelemetryCommand response = (TelemetryCommand) msg;
            this.writeTelemetryCommand(response);
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * 处理事务回查请求并下发 RecoverOrphanedTransactionCommand
     *
     * @param header 事务回查请求头
     * @param messageExt 消息内容
     * @param transactionData 事务数据
     * @param responseFuture 响应 Future
     * @return 异步执行结果
     */
    @Override
    protected CompletableFuture<Void> processCheckTransaction(CheckTransactionStateRequestHeader header,
        MessageExt messageExt, TransactionData transactionData, CompletableFuture<ProxyRelayResult<Void>> responseFuture) {
        CompletableFuture<Void> writeFuture = new CompletableFuture<>();
        try {
            this.writeTelemetryCommand(TelemetryCommand.newBuilder()
                .setRecoverOrphanedTransactionCommand(RecoverOrphanedTransactionCommand.newBuilder()
                    .setTransactionId(transactionData.getTransactionId())
                    .setMessage(GrpcConverter.getInstance().buildMessage(messageExt))
                    .build())
                .build());
            responseFuture.complete(null);
            writeFuture.complete(null);
        } catch (Throwable t) {
            responseFuture.completeExceptionally(t);
            writeFuture.completeExceptionally(t);
        }
        return writeFuture;
    }

    /**
     * 处理消费端运行信息查询请求
     *
     * @param command remoting 命令
     * @param header 查询请求头
     * @param responseFuture 响应 Future
     * @return 异步执行结果
     */
    @Override
    protected CompletableFuture<Void> processGetConsumerRunningInfo(RemotingCommand command,
        GetConsumerRunningInfoRequestHeader header,
        CompletableFuture<ProxyRelayResult<ConsumerRunningInfo>> responseFuture) {
        if (Objects.isNull(header) || !header.isJstackEnable()) {
            return CompletableFuture.completedFuture(null);
        }
        this.writeTelemetryCommand(TelemetryCommand.newBuilder()
            .setPrintThreadStackTraceCommand(PrintThreadStackTraceCommand.newBuilder()
                .setNonce(this.grpcChannelManager.addResponseFuture(responseFuture))
                .build())
            .build());
        return CompletableFuture.completedFuture(null);
    }

    /**
     * 处理直接消费校验请求并下发 VerifyMessageCommand
     *
     * @param command remoting 命令
     * @param header 请求头
     * @param messageExt 消息内容
     * @param responseFuture 响应 Future
     * @return 异步执行结果
     */
    @Override
    protected CompletableFuture<Void> processConsumeMessageDirectly(RemotingCommand command,
        ConsumeMessageDirectlyResultRequestHeader header,
        MessageExt messageExt, CompletableFuture<ProxyRelayResult<ConsumeMessageDirectlyResult>> responseFuture) {
        this.writeTelemetryCommand(TelemetryCommand.newBuilder()
            .setVerifyMessageCommand(VerifyMessageCommand.newBuilder()
                .setNonce(this.grpcChannelManager.addResponseFuture(responseFuture))
                .setMessage(GrpcConverter.getInstance().buildMessage(messageExt))
                .build())
            .build());
        return CompletableFuture.completedFuture(null);
    }

    public String getClientId() {
        return clientId;
    }

    /**
     * 向客户端写入遥测命令
     *
     * @param command 遥测命令
     */
    public void writeTelemetryCommand(TelemetryCommand command) {
        StreamObserver<TelemetryCommand> observer = this.telemetryCommandRef.get();
        if (observer == null) {
            log.warn("telemetry command observer is null when try to write data. command:{}, channel:{}", TextFormat.shortDebugString(command), this);
            return;
        }
        synchronized (this.telemetryWriteLock) {
            observer = this.telemetryCommandRef.get();
            if (observer == null) {
                log.warn("telemetry command observer is null when try to write data. command:{}, channel:{}", TextFormat.shortDebugString(command), this);
                return;
            }
            try {
                observer.onNext(command);
            } catch (StatusRuntimeException | IllegalStateException exception) {
                log.warn("write telemetry failed. command:{}", command, exception);
                this.clearClientObserver(observer);
            }
        }
    }

    /**
     * 返回通道可读字符串
     *
     * @return 通道字符串表示
     */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("clientId", clientId)
            .add("remoteAddress", getRemoteAddress())
            .add("localAddress", getLocalAddress())
            .toString();
    }
}
