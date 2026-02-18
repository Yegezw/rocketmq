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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupEvent;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ProducerChangeListener;
import org.apache.rocketmq.broker.client.ProducerGroupEvent;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.channel.RemotingChannel;
import org.apache.rocketmq.proxy.remoting.channel.RemotingChannelManager;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.AttributeKeys;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UnregisterClientResponseHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.ProducerData;

import java.util.Set;

/**
 * 客户端管理请求处理活动
 */
public class ClientManagerActivity extends AbstractRemotingActivity {

    /**
     * Remoting 客户端通道管理器
     */
    private final RemotingChannelManager remotingChannelManager;

    /**
     * 构造客户端管理活动处理器
     *
     * @param requestPipeline 请求处理管道
     * @param messagingProcessor 消息处理核心组件
     * @param manager Remoting 通道管理器
     */
    public ClientManagerActivity(RequestPipeline requestPipeline, MessagingProcessor messagingProcessor,
        RemotingChannelManager manager) {
        super(requestPipeline, messagingProcessor);
        this.remotingChannelManager = manager;
        this.init();
    }

    /**
     * 初始化生产者与消费者变更监听器
     */
    protected void init() {
        this.messagingProcessor.registerConsumerListener(new ConsumerIdsChangeListenerImpl());
        this.messagingProcessor.registerProducerListener(new ProducerChangeListenerImpl());
    }

    /**
     * 根据请求码分发客户端管理相关请求
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
            case RequestCode.HEART_BEAT:
                return this.heartBeat(ctx, request, context);
            case RequestCode.UNREGISTER_CLIENT:
                return this.unregisterClient(ctx, request, context);
            case RequestCode.CHECK_CLIENT_CONFIG:
                return this.checkClientConfig(ctx, request, context);
            default:
                break;
        }
        return null;
    }

    /**
     * 处理客户端心跳, 并注册生产者与消费者通道
     *
     * @param ctx Netty 上下文
     * @param request 请求命令
     * @param context Proxy 上下文
     * @return 心跳响应
     */
    protected RemotingCommand heartBeat(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) {
        HeartbeatData heartbeatData = HeartbeatData.decode(request.getBody(), HeartbeatData.class);
        String clientId = heartbeatData.getClientID();

        for (ProducerData data : heartbeatData.getProducerDataSet()) {
            ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
                this.remotingChannelManager.createProducerChannel(context, ctx.channel(), data.getGroupName(), clientId),
                clientId, request.getLanguage(),
                request.getVersion());
            setClientPropertiesToChannelAttr(clientChannelInfo);
            messagingProcessor.registerProducer(context, data.getGroupName(), clientChannelInfo);
        }

        for (ConsumerData data : heartbeatData.getConsumerDataSet()) {
            ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
                this.remotingChannelManager.createConsumerChannel(context, ctx.channel(), data.getGroupName(), clientId, data.getSubscriptionDataSet()),
                clientId, request.getLanguage(),
                request.getVersion());
            setClientPropertiesToChannelAttr(clientChannelInfo);
            messagingProcessor.registerConsumer(context, data.getGroupName(), clientChannelInfo, data.getConsumeType(),
                data.getMessageModel(), data.getConsumeFromWhere(), data.getSubscriptionDataSet(), true);
        }

        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark("");
        return response;
    }

    /**
     * 将客户端属性写入父通道属性
     *
     * @param clientChannelInfo 客户端通道信息
     */
    private void setClientPropertiesToChannelAttr(final ClientChannelInfo clientChannelInfo) {
        Channel channel = clientChannelInfo.getChannel();
        if (channel instanceof RemotingChannel) {
            RemotingChannel remotingChannel = (RemotingChannel) channel;
            Channel parent = remotingChannel.parent();
            RemotingHelper.setPropertyToAttr(parent, AttributeKeys.CLIENT_ID_KEY, clientChannelInfo.getClientId());
            RemotingHelper.setPropertyToAttr(parent, AttributeKeys.LANGUAGE_CODE_KEY, clientChannelInfo.getLanguage());
            RemotingHelper.setPropertyToAttr(parent, AttributeKeys.VERSION_KEY, clientChannelInfo.getVersion());
        }

    }

    /**
     * 处理客户端反注册请求
     *
     * @param ctx Netty 上下文
     * @param request 请求命令
     * @param context Proxy 上下文
     * @return 反注册响应
     * @throws RemotingCommandException 请求头解析异常
     */
    protected RemotingCommand unregisterClient(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(UnregisterClientResponseHeader.class);
        final UnregisterClientRequestHeader requestHeader =
            (UnregisterClientRequestHeader) request.decodeCommandCustomHeader(UnregisterClientRequestHeader.class);
        final String producerGroup = requestHeader.getProducerGroup();
        if (producerGroup != null) {
            RemotingChannel channel = this.remotingChannelManager.removeProducerChannel(context, producerGroup, ctx.channel());
            if (channel != null) {
                ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
                    channel,
                    requestHeader.getClientID(),
                    request.getLanguage(),
                    request.getVersion());
                this.messagingProcessor.unRegisterProducer(context, producerGroup, clientChannelInfo);
            } else {
                log.warn("unregister producer failed, channel not exist, may has been removed, producerGroup={}, channel={}", producerGroup, ctx.channel());
            }
        }
        final String consumerGroup = requestHeader.getConsumerGroup();
        if (consumerGroup != null) {
            RemotingChannel channel = this.remotingChannelManager.removeConsumerChannel(context, consumerGroup, ctx.channel());
            if (channel != null) {
                ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
                    channel,
                    requestHeader.getClientID(),
                    request.getLanguage(),
                    request.getVersion());
                this.messagingProcessor.unRegisterConsumer(context, consumerGroup, clientChannelInfo);
            } else {
                log.warn("unregister consumer failed, channel not exist, may has been removed, consumerGroup={}, channel={}", consumerGroup, ctx.channel());
            }
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark("");
        return response;
    }

    /**
     * 处理客户端配置校验请求
     *
     * @param ctx Netty 上下文
     * @param request 请求命令
     * @param context Proxy 上下文
     * @return 校验响应
     */
    protected RemotingCommand checkClientConfig(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark("");
        return response;
    }

    /**
     * 处理底层连接关闭事件
     *
     * @param remoteAddr 远端地址
     * @param channel 底层通道
     */
    public void doChannelCloseEvent(String remoteAddr, Channel channel) {
        Set<RemotingChannel> remotingChannelSet = this.remotingChannelManager.removeChannel(channel);
        for (RemotingChannel remotingChannel : remotingChannelSet) {
            this.messagingProcessor.doChannelCloseEvent(remoteAddr, remotingChannel);
        }
    }

    /**
     * 消费者变更监听实现
     */
    protected class ConsumerIdsChangeListenerImpl implements ConsumerIdsChangeListener {

        /**
         * 处理消费者组事件
         *
         * @param event 事件类型
         * @param group 消费组
         * @param args 扩展参数
         */
        @Override
        public void handle(ConsumerGroupEvent event, String group, Object... args) {
            if (event == ConsumerGroupEvent.CLIENT_UNREGISTER) {
                if (args == null || args.length < 1) {
                    return;
                }
                if (args[0] instanceof ClientChannelInfo) {
                    ClientChannelInfo clientChannelInfo = (ClientChannelInfo) args[0];
                    remotingChannelManager.removeConsumerChannel(ProxyContext.createForInner(this.getClass()), group, clientChannelInfo.getChannel());
                    log.info("remove remoting channel when client unregister. clientChannelInfo:{}", clientChannelInfo);
                }
            }
        }

        /**
         * 关闭监听器
         */
        @Override
        public void shutdown() {

        }
    }

    /**
     * 生产者变更监听实现
     */
    protected class ProducerChangeListenerImpl implements ProducerChangeListener {

        /**
         * 处理生产者组事件
         *
         * @param event 事件类型
         * @param group 生产组
         * @param clientChannelInfo 客户端通道信息
         */
        @Override
        public void handle(ProducerGroupEvent event, String group, ClientChannelInfo clientChannelInfo) {
            if (event == ProducerGroupEvent.CLIENT_UNREGISTER) {
                remotingChannelManager.removeProducerChannel(ProxyContext.createForInner(this.getClass()), group, clientChannelInfo.getChannel());
            }
        }
    }
}
