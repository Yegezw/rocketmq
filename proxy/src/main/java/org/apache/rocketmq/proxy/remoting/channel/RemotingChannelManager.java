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

import io.netty.channel.Channel;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.remoting.RemotingProxyOutClient;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

/**
 * Remoting 通道管理器, 负责按分组维护原始连接与代理通道映射
 */
public class RemotingChannelManager implements StartAndShutdown {
    /**
     * Proxy 模块日志记录器
     */
    protected final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    /**
     * 代理转发服务
     */
    private final ProxyRelayService proxyRelayService;
    /**
     * 分组到通道映射表
     */
    protected final ConcurrentMap<String /* group */, Map<Channel /* raw channel */, RemotingChannel>> groupChannelMap = new ConcurrentHashMap<>();

    /**
     * Remoting 外呼客户端
     */
    private final RemotingProxyOutClient remotingProxyOutClient;

    /**
     * 构造通道管理器
     *
     * @param remotingProxyOutClient Remoting 外呼客户端
     * @param proxyRelayService 代理转发服务
     */
    public RemotingChannelManager(RemotingProxyOutClient remotingProxyOutClient, ProxyRelayService proxyRelayService) {
        this.remotingProxyOutClient = remotingProxyOutClient;
        this.proxyRelayService = proxyRelayService;
    }

    /**
     * 构建生产者分组键
     *
     * @param group 分组名
     * @return 生产者分组键
     */
    protected String buildProducerKey(String group) {
        return buildKey("p", group);
    }

    /**
     * 构建消费者分组键
     *
     * @param group 分组名
     * @return 消费者分组键
     */
    protected String buildConsumerKey(String group) {
        return buildKey("c", group);
    }

    /**
     * 构建通用分组键
     *
     * @param prefix 前缀
     * @param group 分组名
     * @return 拼接后的分组键
     */
    protected String buildKey(String prefix, String group) {
        return prefix + group;
    }

    /**
     * 创建或获取生产者通道
     *
     * @param ctx Proxy 上下文
     * @param channel 原始连接
     * @param group 分组名
     * @param clientId 客户端标识
     * @return 生产者代理通道
     */
    public RemotingChannel createProducerChannel(ProxyContext ctx, Channel channel, String group, String clientId) {
        return createChannel(channel, buildProducerKey(group), clientId, Collections.emptySet());
    }

    /**
     * 创建或获取消费者通道
     *
     * @param ctx Proxy 上下文
     * @param channel 原始连接
     * @param group 分组名
     * @param clientId 客户端标识
     * @param subscriptionData 订阅数据集合
     * @return 消费者代理通道
     */
    public RemotingChannel createConsumerChannel(ProxyContext ctx, Channel channel, String group, String clientId, Set<SubscriptionData> subscriptionData) {
        return createChannel(channel, buildConsumerKey(group), clientId, subscriptionData);
    }

    /**
     * 创建或获取指定分组下的代理通道
     *
     * @param channel 原始连接
     * @param group 分组键
     * @param clientId 客户端标识
     * @param subscriptionData 订阅数据集合
     * @return 代理通道
     */
    protected RemotingChannel createChannel(Channel channel, String group, String clientId, Set<SubscriptionData> subscriptionData) {
        this.groupChannelMap.compute(group, (groupKey, clientIdMap) -> {
            if (clientIdMap == null) {
                clientIdMap = new ConcurrentHashMap<>();
            }
            clientIdMap.computeIfAbsent(channel, clientIdKey -> new RemotingChannel(remotingProxyOutClient, proxyRelayService, channel, clientId, subscriptionData));
            return clientIdMap;
        });
        return getChannel(group, channel);
    }

    protected RemotingChannel getChannel(String group, Channel channel) {
        Map<Channel, RemotingChannel> clientIdChannelMap = this.groupChannelMap.get(group);
        if (clientIdChannelMap == null) {
            return null;
        }
        return clientIdChannelMap.get(channel);
    }

    /**
     * 按原始连接移除所有分组下的代理通道
     *
     * @param channel 原始连接
     * @return 被移除通道集合
     */
    public Set<RemotingChannel> removeChannel(Channel channel) {
        Set<RemotingChannel> removedChannelSet = new HashSet<>();
        Set<String> groupKeySet = groupChannelMap.keySet();
        for (String group : groupKeySet) {
            RemotingChannel remotingChannel = removeChannel(group, channel);
            if (remotingChannel != null) {
                removedChannelSet.add(remotingChannel);
            }
        }
        return removedChannelSet;
    }

    /**
     * 移除生产者通道
     *
     * @param ctx Proxy 上下文
     * @param group 分组名
     * @param channel 原始连接
     * @return 被移除通道
     */
    public RemotingChannel removeProducerChannel(ProxyContext ctx, String group, Channel channel) {
        return removeChannel(buildProducerKey(group), channel);
    }

    /**
     * 移除消费者通道
     *
     * @param ctx Proxy 上下文
     * @param group 分组名
     * @param channel 原始连接
     * @return 被移除通道
     */
    public RemotingChannel removeConsumerChannel(ProxyContext ctx, String group, Channel channel) {
        return removeChannel(buildConsumerKey(group), channel);
    }

    /**
     * 按分组键移除代理通道
     *
     * @param group 分组键
     * @param channel 原始连接
     * @return 被移除通道, 不存在时返回 null
     */
    protected RemotingChannel removeChannel(String group, Channel channel) {
        AtomicReference<RemotingChannel> channelRef = new AtomicReference<>();

        this.groupChannelMap.computeIfPresent(group, (groupKey, channelMap) -> {
            channelRef.set(channelMap.remove(getOrgRawChannel(channel)));
            if (channelMap.isEmpty()) {
                return null;
            }
            return channelMap;
        });
        return channelRef.get();
    }

    /**
     * to get the org channel pass by nettyRemotingServer
     * 获取 nettyRemotingServer 传入的原始连接对象
     *
     * @param channel 当前连接对象
     * @return 原始连接对象
     */
    protected Channel getOrgRawChannel(Channel channel) {
        if (channel instanceof RemotingChannel) {
            return channel.parent();
        }
        return channel;
    }

    /**
     * 关闭通道管理器
     *
     * @throws Exception 关闭异常
     */
    @Override
    public void shutdown() throws Exception {

    }

    /**
     * 启动通道管理器
     *
     * @throws Exception 启动异常
     */
    @Override
    public void start() throws Exception {

    }
}
