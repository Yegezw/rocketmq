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

package org.apache.rocketmq.proxy.remoting.channel;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.google.common.base.MoreObjects;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelMetadata;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.channel.ChannelHelper;
import org.apache.rocketmq.common.utils.ExceptionUtils;
import org.apache.rocketmq.common.utils.FutureUtils;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.processor.channel.ChannelExtendAttributeGetter;
import org.apache.rocketmq.proxy.processor.channel.ChannelProtocolType;
import org.apache.rocketmq.proxy.processor.channel.RemoteChannel;
import org.apache.rocketmq.proxy.processor.channel.RemoteChannelConverter;
import org.apache.rocketmq.proxy.remoting.RemotingProxyOutClient;
import org.apache.rocketmq.proxy.remoting.common.RemotingConverter;
import org.apache.rocketmq.proxy.service.relay.ProxyChannel;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayResult;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.proxy.service.transaction.TransactionData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

/**
 * Remoting 代理通道实现, 负责将代理请求透传到原始连接并处理回包
 */
public class RemotingChannel extends ProxyChannel implements RemoteChannelConverter, ChannelExtendAttributeGetter {
    /**
     * Proxy 模块日志记录器
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    /**
     * 默认客户端超时时间, 单位毫秒
     */
    private static final long DEFAULT_MQ_CLIENT_TIMEOUT = Duration.ofSeconds(3).toMillis();
    /**
     * 客户端标识
     */
    private final String clientId;
    /**
     * 远端地址
     */
    private final String remoteAddress;
    /**
     * 本地地址
     */
    private final String localAddress;
    /**
     * Remoting 外呼客户端
     */
    private final RemotingProxyOutClient remotingProxyOutClient;
    /**
     * 订阅数据集合
     */
    private final Set<SubscriptionData> subscriptionData;

    /**
     * 构造 Remoting 代理通道
     *
     * @param remotingProxyOutClient Remoting 外呼客户端
     * @param proxyRelayService 代理转发服务
     * @param parent 原始连接
     * @param clientId 客户端标识
     * @param subscriptionData 订阅数据集合
     */
    public RemotingChannel(RemotingProxyOutClient remotingProxyOutClient, ProxyRelayService proxyRelayService,
        Channel parent,
        String clientId, Set<SubscriptionData> subscriptionData) {
        super(proxyRelayService, parent, parent.id(),
            NetworkUtil.socketAddress2String(parent.remoteAddress()),
            NetworkUtil.socketAddress2String(parent.localAddress()));
        this.remotingProxyOutClient = remotingProxyOutClient;
        this.clientId = clientId;
        this.remoteAddress = NetworkUtil.socketAddress2String(parent.remoteAddress());
        this.localAddress = NetworkUtil.socketAddress2String(parent.localAddress());
        this.subscriptionData = subscriptionData;
    }

    @Override
    public boolean isOpen() {
        return this.parent().isOpen();
    }

    @Override
    public boolean isActive() {
        return this.parent().isActive();
    }

    @Override
    public boolean isWritable() {
        return this.parent().isWritable();
    }

    /**
     * 关闭原始连接
     *
     * @return 关闭 Future
     */
    @Override
    public ChannelFuture close() {
        return this.parent().close();
    }

    /**
     * 获取原始连接配置
     *
     * @return 连接配置
     */
    @Override
    public ChannelConfig config() {
        return this.parent().config();
    }

    /**
     * 获取原始连接元数据
     *
     * @return 连接元数据
     */
    @Override
    public ChannelMetadata metadata() {
        return this.parent().metadata();
    }

    /**
     * 透传其他类型消息到原始连接
     *
     * @param msg 待发送消息
     * @return 异步执行结果
     */
    @Override
    protected CompletableFuture<Void> processOtherMessage(Object msg) {
        this.parent().writeAndFlush(msg);
        return CompletableFuture.completedFuture(null);
    }

    /**
     * 处理事务回查请求并透传到原始连接
     *
     * @param header 请求头
     * @param messageExt 消息内容
     * @param transactionData 事务数据
     * @param responseFuture 响应 Future
     * @return 异步执行结果
     */
    @Override
    protected CompletableFuture<Void> processCheckTransaction(CheckTransactionStateRequestHeader header,
        MessageExt messageExt, TransactionData transactionData,
        CompletableFuture<ProxyRelayResult<Void>> responseFuture) {
        CompletableFuture<Void> writeFuture = new CompletableFuture<>();
        try {
            CheckTransactionStateRequestHeader requestHeader = new CheckTransactionStateRequestHeader();
            requestHeader.setTopic(messageExt.getTopic());
            requestHeader.setCommitLogOffset(transactionData.getCommitLogOffset());
            requestHeader.setTranStateTableOffset(transactionData.getTranStateTableOffset());
            requestHeader.setTransactionId(transactionData.getTransactionId());
            requestHeader.setMsgId(header.getMsgId());

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CHECK_TRANSACTION_STATE, requestHeader);
            request.setBody(RemotingConverter.getInstance().convertMsgToBytes(messageExt));

            this.parent().writeAndFlush(request).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    responseFuture.complete(null);
                    writeFuture.complete(null);
                } else {
                    Exception e = new RemotingException("write and flush data failed");
                    responseFuture.completeExceptionally(e);
                    writeFuture.completeExceptionally(e);
                }
            });
        } catch (Throwable t) {
            responseFuture.completeExceptionally(t);
            writeFuture.completeExceptionally(t);
        }
        return writeFuture;
    }

    /**
     * 处理获取消费者运行信息请求
     *
     * @param command 原始命令
     * @param header 请求头
     * @param responseFuture 响应 Future
     * @return 异步执行结果
     */
    @Override
    protected CompletableFuture<Void> processGetConsumerRunningInfo(RemotingCommand command,
        GetConsumerRunningInfoRequestHeader header,
        CompletableFuture<ProxyRelayResult<ConsumerRunningInfo>> responseFuture) {
        try {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO, header);
            this.remotingProxyOutClient.invokeToClient(this.parent(), request, DEFAULT_MQ_CLIENT_TIMEOUT)
                .thenAccept(response -> {
                    if (response.getCode() == ResponseCode.SUCCESS) {
                        ConsumerRunningInfo consumerRunningInfo = ConsumerRunningInfo.decode(response.getBody(), ConsumerRunningInfo.class);
                        responseFuture.complete(new ProxyRelayResult<>(ResponseCode.SUCCESS, "", consumerRunningInfo));
                    } else {
                        String errMsg = String.format("get consumer running info failed, code:%s remark:%s", response.getCode(), response.getRemark());
                        RuntimeException e = new RuntimeException(errMsg);
                        responseFuture.completeExceptionally(e);
                    }
                })
                .exceptionally(t -> {
                    responseFuture.completeExceptionally(ExceptionUtils.getRealException(t));
                    return null;
                });
            return CompletableFuture.completedFuture(null);
        } catch (Throwable t) {
            responseFuture.completeExceptionally(t);
            return FutureUtils.completeExceptionally(t);
        }
    }

    /**
     * 处理直接消费消息请求
     *
     * @param command 原始命令
     * @param header 请求头
     * @param messageExt 消息内容
     * @param responseFuture 响应 Future
     * @return 异步执行结果
     */
    @Override
    protected CompletableFuture<Void> processConsumeMessageDirectly(RemotingCommand command,
        ConsumeMessageDirectlyResultRequestHeader header, MessageExt messageExt,
        CompletableFuture<ProxyRelayResult<ConsumeMessageDirectlyResult>> responseFuture) {
        try {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUME_MESSAGE_DIRECTLY, header);
            request.setBody(RemotingConverter.getInstance().convertMsgToBytes(messageExt));

            this.remotingProxyOutClient.invokeToClient(this.parent(), request, DEFAULT_MQ_CLIENT_TIMEOUT)
                .thenAccept(response -> {
                    if (response.getCode() == ResponseCode.SUCCESS) {
                        ConsumeMessageDirectlyResult result = ConsumeMessageDirectlyResult.decode(response.getBody(), ConsumeMessageDirectlyResult.class);
                        responseFuture.complete(new ProxyRelayResult<>(ResponseCode.SUCCESS, "", result));
                    } else {
                        String errMsg = String.format("consume message directly failed, code:%s remark:%s", response.getCode(), response.getRemark());
                        RuntimeException e = new RuntimeException(errMsg);
                        responseFuture.completeExceptionally(e);
                    }
                })
                .exceptionally(t -> {
                    responseFuture.completeExceptionally(ExceptionUtils.getRealException(t));
                    return null;
                });
            return CompletableFuture.completedFuture(null);
        } catch (Throwable t) {
            responseFuture.completeExceptionally(t);
            return FutureUtils.completeExceptionally(t);
        }
    }

    public String getClientId() {
        return clientId;
    }

    @Override
    public String getChannelExtendAttribute() {
        if (this.subscriptionData == null) {
            return null;
        }
        return JSON.toJSONString(this.subscriptionData);
    }

    /**
     * 从通道扩展属性中解析订阅数据集合
     *
     * @param channel 网络通道
     * @return 订阅数据集合, 解析失败时返回 null
     */
    public static Set<SubscriptionData> parseChannelExtendAttribute(Channel channel) {
        if (ChannelHelper.getChannelProtocolType(channel).equals(ChannelProtocolType.REMOTING) &&
            channel instanceof ChannelExtendAttributeGetter) {
            String attr = ((ChannelExtendAttributeGetter) channel).getChannelExtendAttribute();
            if (attr == null) {
                return null;
            }

            try {
                return JSON.parseObject(attr, new TypeReference<Set<SubscriptionData>>() {
                });
            } catch (Exception e) {
                log.error("convert remoting extend attribute to subscriptionDataSet failed. data:{}", attr, e);
                return null;
            }
        }
        return null;
    }

    /**
     * 转换为远端通道对象
     *
     * @return 远端通道对象
     */
    @Override
    public RemoteChannel toRemoteChannel() {
        return new RemoteChannel(
            ConfigurationManager.getProxyConfig().getLocalServeAddr(),
            this.getRemoteAddress(),
            this.getLocalAddress(),
            ChannelProtocolType.REMOTING,
            this.getChannelExtendAttribute());
    }

    /**
     * 返回通道可读字符串
     *
     * @return 通道字符串表示
     */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("parent", parent())
            .add("clientId", clientId)
            .add("remoteAddress", remoteAddress)
            .add("localAddress", localAddress)
            .add("subscriptionData", subscriptionData)
            .toString();
    }
}
