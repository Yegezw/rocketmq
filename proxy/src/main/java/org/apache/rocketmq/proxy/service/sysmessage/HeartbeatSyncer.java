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

package org.apache.rocketmq.proxy.service.sysmessage;

import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupEvent;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.proxy.common.channel.ChannelHelper;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.processor.channel.RemoteChannel;
import org.apache.rocketmq.proxy.service.admin.AdminService;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

/**
 * 消费者心跳同步器, 负责在 Proxy 集群间同步客户端注册状态
 */
public class HeartbeatSyncer extends AbstractSystemMessageSyncer {

    /**
     * 心跳同步任务线程池
     */
    protected ThreadPoolExecutor threadPoolExecutor;
    /**
     * 消费者管理器
     */
    protected ConsumerManager consumerManager;
    /**
     * 远程通道缓存, key: group@channelId
     */
    protected final Map<String /* group @ channelId as longText */, RemoteChannel> remoteChannelMap = new ConcurrentHashMap<>();
    /**
     * 本地 Proxy 唯一标识
     */
    protected String localProxyId;

    /**
     * 初始化心跳同步器
     *
     * @param topicRouteService 主题路由服务
     * @param adminService 管理服务
     * @param consumerManager 消费者管理器
     * @param mqClientAPIFactory MQ 客户端工厂
     * @param rpcHook 远程调用钩子
     */
    public HeartbeatSyncer(TopicRouteService topicRouteService, AdminService adminService,
                           ConsumerManager consumerManager, MQClientAPIFactory mqClientAPIFactory, RPCHook rpcHook) {
        super(topicRouteService, adminService, mqClientAPIFactory, rpcHook);
        this.consumerManager = consumerManager;
        this.localProxyId = buildLocalProxyId();
        this.init();
    }

    /**
     * 初始化线程池与消费者事件监听器
     */
    protected void init() {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        this.threadPoolExecutor = ThreadPoolMonitor.createAndMonitor(
            proxyConfig.getHeartbeatSyncerThreadPoolNums(),
            proxyConfig.getHeartbeatSyncerThreadPoolNums(),
            1,
            TimeUnit.MINUTES,
            "HeartbeatSyncer",
            proxyConfig.getHeartbeatSyncerThreadPoolQueueCapacity()
        );
        this.consumerManager.appendConsumerIdsChangeListener(new ConsumerIdsChangeListener() {
            /**
             * 处理消费者组变更事件
             *
             * @param event 事件类型
             * @param group 消费者组
             * @param args 附加参数
             */
            @Override
            public void handle(ConsumerGroupEvent event, String group, Object... args) {
                processConsumerGroupEvent(event, group, args);
            }

            /**
             * 监听器关闭回调, 当前无需额外处理
             */
            @Override
            public void shutdown() {

            }
        });
    }

    /**
     * 停止心跳同步器并释放线程池
     *
     * @throws Exception 停止异常
     */
    @Override
    public void shutdown() throws Exception {
        this.threadPoolExecutor.shutdown();
        super.shutdown();
    }

    /**
     * 处理消费者组事件, 在客户端注销时清理远程通道缓存
     *
     * @param event 消费者组事件
     * @param group 消费者组
     * @param args 附加参数
     */
    protected void processConsumerGroupEvent(ConsumerGroupEvent event, String group, Object... args) {
        if (event == ConsumerGroupEvent.CLIENT_UNREGISTER) {
            if (args == null || args.length < 1) {
                return;
            }
            if (args[0] instanceof ClientChannelInfo) {
                ClientChannelInfo clientChannelInfo = (ClientChannelInfo) args[0];
                remoteChannelMap.remove(buildKey(group, clientChannelInfo.getChannel()));
            }
        }
    }

    /**
     * 在本地消费者注册时广播注册心跳
     *
     * @param consumerGroup 消费者组
     * @param clientChannelInfo 客户端通道信息
     * @param consumeType 消费类型
     * @param messageModel 消息模型
     * @param consumeFromWhere 消费起始位置
     * @param subList 订阅列表
     */
    public void onConsumerRegister(String consumerGroup, ClientChannelInfo clientChannelInfo,
        ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere,
        Set<SubscriptionData> subList) {
        if (clientChannelInfo == null || ChannelHelper.isRemote(clientChannelInfo.getChannel())) {
            return;
        }
        try {
            this.threadPoolExecutor.submit(() -> {
                try {
                    RemoteChannel remoteChannel = RemoteChannel.create(clientChannelInfo.getChannel());
                    if (remoteChannel == null) {
                        return;
                    }
                    HeartbeatSyncerData data = new HeartbeatSyncerData(
                        HeartbeatType.REGISTER,
                        clientChannelInfo.getClientId(),
                        clientChannelInfo.getLanguage(),
                        clientChannelInfo.getVersion(),
                        consumerGroup,
                        consumeType,
                        messageModel,
                        consumeFromWhere,
                        localProxyId,
                        remoteChannel.encode()
                    );
                    data.setSubscriptionDataSet(subList);

                    log.debug("sync register heart beat. topic:{}, data:{}", this.getBroadcastTopicName(), data);
                    this.sendSystemMessage(data);
                } catch (Throwable t) {
                    log.error("heartbeat register broadcast failed. group:{}, clientChannelInfo:{}, consumeType:{}, messageModel:{}, consumeFromWhere:{}, subList:{}",
                        consumerGroup, clientChannelInfo, consumeType, messageModel, consumeFromWhere, subList, t);
                }
            });
        } catch (Throwable t) {
            log.error("heartbeat submit register broadcast failed. group:{}, clientChannelInfo:{}, consumeType:{}, messageModel:{}, consumeFromWhere:{}, subList:{}",
                consumerGroup, clientChannelInfo, consumeType, messageModel, consumeFromWhere, subList, t);
        }
    }

    /**
     * 在本地消费者注销时广播注销心跳
     *
     * @param consumerGroup 消费者组
     * @param clientChannelInfo 客户端通道信息
     */
    public void onConsumerUnRegister(String consumerGroup, ClientChannelInfo clientChannelInfo) {
        if (clientChannelInfo == null || ChannelHelper.isRemote(clientChannelInfo.getChannel())) {
            return;
        }
        try {
            this.threadPoolExecutor.submit(() -> {
                try {
                    RemoteChannel remoteChannel = RemoteChannel.create(clientChannelInfo.getChannel());
                    if (remoteChannel == null) {
                        return;
                    }
                    HeartbeatSyncerData data = new HeartbeatSyncerData(
                        HeartbeatType.UNREGISTER,
                        clientChannelInfo.getClientId(),
                        clientChannelInfo.getLanguage(),
                        clientChannelInfo.getVersion(),
                        consumerGroup,
                        null,
                        null,
                        null,
                        localProxyId,
                        remoteChannel.encode()
                    );

                    log.debug("sync unregister heart beat. topic:{}, data:{}", this.getBroadcastTopicName(), data);
                    this.sendSystemMessage(data);
                } catch (Throwable t) {
                    log.error("heartbeat unregister broadcast failed. group:{}, clientChannelInfo:{}, consumeType:{}",
                        consumerGroup, clientChannelInfo, t);
                }
            });
        } catch (Throwable t) {
            log.error("heartbeat submit unregister broadcast failed. group:{}, clientChannelInfo:{}, consumeType:{}",
                consumerGroup, clientChannelInfo, t);
        }
    }

    /**
     * 消费心跳广播消息并在本地更新消费者连接状态
     *
     * @param msgs 消息列表
     * @param context 消费上下文
     * @return 消费结果
     */
    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        if (msgs == null || msgs.isEmpty()) {
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }

        for (MessageExt msg : msgs) {
            try {
                HeartbeatSyncerData data = JSON.parseObject(new String(msg.getBody(), StandardCharsets.UTF_8), HeartbeatSyncerData.class);
                if (data.getLocalProxyId().equals(localProxyId)) {
                    continue;
                }

                RemoteChannel decodedChannel = RemoteChannel.decode(data.getChannelData());
                RemoteChannel channel = remoteChannelMap.computeIfAbsent(buildKey(data.getGroup(), decodedChannel), key -> decodedChannel);
                channel.setExtendAttribute(decodedChannel.getChannelExtendAttribute());
                ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
                    channel,
                    data.getClientId(),
                    data.getLanguage(),
                    data.getVersion()
                );
                log.debug("start process remote channel. data:{}, clientChannelInfo:{}", data, clientChannelInfo);
                if (data.getHeartbeatType().equals(HeartbeatType.REGISTER)) {
                    this.consumerManager.registerConsumer(
                        data.getGroup(),
                        clientChannelInfo,
                        data.getConsumeType(),
                        data.getMessageModel(),
                        data.getConsumeFromWhere(),
                        data.getSubscriptionDataSet(),
                        false
                    );
                } else {
                    this.consumerManager.unregisterConsumer(
                        data.getGroup(),
                        clientChannelInfo,
                        false
                    );
                }
            } catch (Throwable t) {
                log.error("heartbeat consume message failed. msg:{}, data:{}", msg, new String(msg.getBody(), StandardCharsets.UTF_8), t);
            }
        }

        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    /**
     * 构建本地 Proxy 唯一标识
     *
     * @return 本地 Proxy 标识
     */
    private String buildLocalProxyId() {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        // use local address, remoting port and grpc port to build unique local proxy Id
        // 使用本地地址, remoting 端口与 grpc 端口构建本地 Proxy 唯一标识
        return proxyConfig.getLocalServeAddr() + "%" + proxyConfig.getRemotingListenPort() + "%" + proxyConfig.getGrpcServerPort();
    }

    /**
     * 构造远程通道缓存 key
     *
     * @param group 消费者组
     * @param channel 客户端通道
     * @return 缓存 key
     */
    private static String buildKey(String group, Channel channel) {
        return group + "@" + channel.id().asLongText();
    }
}
