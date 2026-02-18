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
package org.apache.rocketmq.proxy.grpc.v2.route;

import apache.rocketmq.v2.Address;
import apache.rocketmq.v2.AddressScheme;
import apache.rocketmq.v2.Assignment;
import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Endpoints;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.MessageType;
import apache.rocketmq.v2.Permission;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.Resource;
import com.google.common.net.HostAndPort;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.AbstractMessingActivity;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.route.ProxyTopicRouteData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

/**
 * 路由活动实现, 负责查询主题路由与队列分配
 */
public class RouteActivity extends AbstractMessingActivity {

    /**
     * 构造路由活动对象
     *
     * @param messagingProcessor 消息处理器
     * @param grpcClientSettingsManager gRPC 客户端设置管理器
     * @param grpcChannelManager gRPC 通道管理器
     */
    public RouteActivity(MessagingProcessor messagingProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager, GrpcChannelManager grpcChannelManager) {
        super(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    /**
     * 查询主题路由并返回队列列表
     *
     * @param ctx Proxy 上下文
     * @param request 路由查询请求
     * @return 路由查询响应 Future
     */
    public CompletableFuture<QueryRouteResponse> queryRoute(ProxyContext ctx, QueryRouteRequest request) {
        CompletableFuture<QueryRouteResponse> future = new CompletableFuture<>();
        try {
            validateTopic(request.getTopic());
            List<org.apache.rocketmq.proxy.common.Address> addressList = this.convertToAddressList(request.getEndpoints());

            String topicName = request.getTopic().getName();
            ProxyTopicRouteData proxyTopicRouteData = this.messagingProcessor.getTopicRouteDataForProxy(
                ctx, addressList, topicName);

            List<MessageQueue> messageQueueList = new ArrayList<>();
            Map<String, Map<Long, Broker>> brokerMap = buildBrokerMap(proxyTopicRouteData.getBrokerDatas());

            TopicMessageType topicMessageType = messagingProcessor.getMetadataService().getTopicMessageType(ctx, topicName);
            for (QueueData queueData : proxyTopicRouteData.getQueueDatas()) {
                String brokerName = queueData.getBrokerName();
                Map<Long, Broker> brokerIdMap = brokerMap.get(brokerName);
                if (brokerIdMap == null) {
                    break;
                }
                for (Broker broker : brokerIdMap.values()) {
                    messageQueueList.addAll(this.genMessageQueueFromQueueData(queueData, request.getTopic(), topicMessageType, broker));
                }
            }

            QueryRouteResponse response = QueryRouteResponse.newBuilder()
                .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
                .addAllMessageQueues(messageQueueList)
                .build();
            future.complete(response);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    /**
     * 查询消费分配结果
     *
     * @param ctx Proxy 上下文
     * @param request 分配查询请求
     * @return 分配查询响应 Future
     */
    public CompletableFuture<QueryAssignmentResponse> queryAssignment(ProxyContext ctx,
        QueryAssignmentRequest request) {
        CompletableFuture<QueryAssignmentResponse> future = new CompletableFuture<>();

        try {
            validateTopicAndConsumerGroup(request.getTopic(), request.getGroup());
            List<org.apache.rocketmq.proxy.common.Address> addressList = this.convertToAddressList(request.getEndpoints());

            ProxyTopicRouteData proxyTopicRouteData = this.messagingProcessor.getTopicRouteDataForProxy(
                ctx,
                addressList,
                request.getTopic().getName());

            boolean fifo = false;
            SubscriptionGroupConfig config = this.messagingProcessor.getSubscriptionGroupConfig(ctx,
                request.getGroup().getName());
            if (config != null && config.isConsumeMessageOrderly()) {
                fifo = true;
            }

            List<Assignment> assignments = new ArrayList<>();
            Map<String, Map<Long, Broker>> brokerMap = buildBrokerMap(proxyTopicRouteData.getBrokerDatas());
            for (QueueData queueData : proxyTopicRouteData.getQueueDatas()) {
                if (PermName.isReadable(queueData.getPerm()) && queueData.getReadQueueNums() > 0) {
                    Map<Long, Broker> brokerIdMap = brokerMap.get(queueData.getBrokerName());
                    if (brokerIdMap != null) {
                        Broker broker = brokerIdMap.get(MixAll.MASTER_ID);
                        Permission permission = this.convertToPermission(queueData.getPerm());
                        if (fifo) {
                            for (int i = 0; i < queueData.getReadQueueNums(); i++) {
                                MessageQueue defaultMessageQueue = MessageQueue.newBuilder()
                                    .setTopic(request.getTopic())
                                    .setId(i)
                                    .setPermission(permission)
                                    .setBroker(broker)
                                    .build();
                                assignments.add(Assignment.newBuilder()
                                    .setMessageQueue(defaultMessageQueue)
                                    .build());
                            }
                        } else {
                            MessageQueue defaultMessageQueue = MessageQueue.newBuilder()
                                .setTopic(request.getTopic())
                                .setId(-1)
                                .setPermission(permission)
                                .setBroker(broker)
                                .build();
                            assignments.add(Assignment.newBuilder()
                                .setMessageQueue(defaultMessageQueue)
                                .build());
                        }

                    }
                }
            }

            QueryAssignmentResponse response;
            if (assignments.isEmpty()) {
                response = QueryAssignmentResponse.newBuilder()
                    .setStatus(ResponseBuilder.getInstance().buildStatus(Code.FORBIDDEN, "no readable queue"))
                    .build();
            } else {
                response = QueryAssignmentResponse.newBuilder()
                    .addAllAssignments(assignments)
                    .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
                    .build();
            }
            future.complete(response);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    /**
     * 将权限位转换为 gRPC 权限枚举
     *
     * @param perm 权限位
     * @return gRPC 权限枚举
     */
    protected Permission convertToPermission(int perm) {
        boolean isReadable = PermName.isReadable(perm);
        boolean isWriteable = PermName.isWriteable(perm);
        if (isReadable && isWriteable) {
            return Permission.READ_WRITE;
        }
        if (isReadable) {
            return Permission.READ;
        }
        if (isWriteable) {
            return Permission.WRITE;
        }
        return Permission.NONE;
    }

    /**
     * 将 gRPC Endpoints 转换为内部地址列表
     *
     * @param endpoints gRPC Endpoints
     * @return 内部地址列表
     */
    protected List<org.apache.rocketmq.proxy.common.Address> convertToAddressList(Endpoints endpoints) {

        boolean useEndpointPort = ConfigurationManager.getProxyConfig().isUseEndpointPortFromRequest();

        List<org.apache.rocketmq.proxy.common.Address> addressList = new ArrayList<>();
        for (Address address : endpoints.getAddressesList()) {
            int port = ConfigurationManager.getProxyConfig().getGrpcServerPort();
            if (useEndpointPort) {
                port = address.getPort();
            }
            addressList.add(new org.apache.rocketmq.proxy.common.Address(
                org.apache.rocketmq.proxy.common.Address.AddressScheme.valueOf(endpoints.getScheme().name()),
                HostAndPort.fromParts(address.getHost(), port)));
        }

        return addressList;

    }

    /**
     * 构建 brokerName 到 Broker 对象映射
     *
     * @param brokerDataList Proxy 路由中的 Broker 数据
     * @return brokerName 到 brokerId 映射表
     */
    protected Map<String /*brokerName*/, Map<Long /*brokerID*/, Broker>> buildBrokerMap(
        List<ProxyTopicRouteData.ProxyBrokerData> brokerDataList) {
        Map<String, Map<Long, Broker>> brokerMap = new HashMap<>();
        for (ProxyTopicRouteData.ProxyBrokerData brokerData : brokerDataList) {
            Map<Long, Broker> brokerIdMap = new HashMap<>();
            String brokerName = brokerData.getBrokerName();
            for (Map.Entry<Long, List<org.apache.rocketmq.proxy.common.Address>> entry : brokerData.getBrokerAddrs().entrySet()) {
                Long brokerId = entry.getKey();
                List<Address> addressList = new ArrayList<>();
                AddressScheme addressScheme = AddressScheme.IPv4;
                for (org.apache.rocketmq.proxy.common.Address address : entry.getValue()) {
                    addressScheme = AddressScheme.valueOf(address.getAddressScheme().name());
                    addressList.add(Address.newBuilder()
                        .setHost(address.getHostAndPort().getHost())
                        .setPort(address.getHostAndPort().getPort())
                        .build());
                }

                Broker broker = Broker.newBuilder()
                    .setName(brokerName)
                    .setId(Math.toIntExact(brokerId))
                    .setEndpoints(Endpoints.newBuilder()
                        .setScheme(addressScheme)
                        .addAllAddresses(addressList)
                        .build())
                    .build();

                brokerIdMap.put(brokerId, broker);
            }
            brokerMap.put(brokerName, brokerIdMap);
        }
        return brokerMap;
    }

    /**
     * 根据队列路由数据生成 gRPC MessageQueue 列表
     *
     * @param queueData 队列路由数据
     * @param topic 主题资源
     * @param topicMessageType 主题消息类型
     * @param broker Broker 节点
     * @return MessageQueue 列表
     */
    protected List<MessageQueue> genMessageQueueFromQueueData(QueueData queueData, Resource topic,
        TopicMessageType topicMessageType, Broker broker) {
        List<MessageQueue> messageQueueList = new ArrayList<>();

        int r = 0;
        int w = 0;
        int rw = 0;
        int n = 0;
        if (PermName.isWriteable(queueData.getPerm()) && PermName.isReadable(queueData.getPerm())) {
            rw = Math.min(queueData.getWriteQueueNums(), queueData.getReadQueueNums());
            r = queueData.getReadQueueNums() - rw;
            w = queueData.getWriteQueueNums() - rw;
        } else if (PermName.isWriteable(queueData.getPerm())) {
            w = queueData.getWriteQueueNums();
        } else if (PermName.isReadable(queueData.getPerm())) {
            r = queueData.getReadQueueNums();
        } else if (!PermName.isAccessible(queueData.getPerm())) {
            n = Math.max(1, Math.max(queueData.getWriteQueueNums(), queueData.getReadQueueNums()));
        }

        // r here means readOnly queue nums, w means writeOnly queue nums, while rw means both readable and writable queue nums.
        // r 表示只读队列数量, w 表示只写队列数量, rw 表示可读可写队列数量
        int queueIdIndex = 0;
        for (int i = 0; i < r; i++) {
            MessageQueue messageQueue = MessageQueue.newBuilder().setBroker(broker).setTopic(topic)
                .setId(queueIdIndex++)
                .setPermission(Permission.READ)
                .addAllAcceptMessageTypes(parseTopicMessageType(topicMessageType))
                .build();
            messageQueueList.add(messageQueue);
        }

        for (int i = 0; i < w; i++) {
            MessageQueue messageQueue = MessageQueue.newBuilder().setBroker(broker).setTopic(topic)
                .setId(queueIdIndex++)
                .setPermission(Permission.WRITE)
                .addAllAcceptMessageTypes(parseTopicMessageType(topicMessageType))
                .build();
            messageQueueList.add(messageQueue);
        }

        for (int i = 0; i < rw; i++) {
            MessageQueue messageQueue = MessageQueue.newBuilder().setBroker(broker).setTopic(topic)
                .setId(queueIdIndex++)
                .setPermission(Permission.READ_WRITE)
                .addAllAcceptMessageTypes(parseTopicMessageType(topicMessageType))
                .build();
            messageQueueList.add(messageQueue);
        }

        for (int i = 0; i < n; i++) {
            MessageQueue messageQueue = MessageQueue.newBuilder().setBroker(broker).setTopic(topic)
                .setId(queueIdIndex++)
                .setPermission(Permission.NONE)
                .addAllAcceptMessageTypes(parseTopicMessageType(topicMessageType))
                .build();
            messageQueueList.add(messageQueue);
        }

        return messageQueueList;
    }

    /**
     * 将主题消息类型转换为 gRPC 消息类型列表
     *
     * @param topicMessageType 主题消息类型
     * @return gRPC 消息类型列表
     */
    private List<MessageType> parseTopicMessageType(TopicMessageType topicMessageType) {
        switch (topicMessageType) {
            case NORMAL:
                return Collections.singletonList(MessageType.NORMAL);
            case FIFO:
                return Collections.singletonList(MessageType.FIFO);
            case TRANSACTION:
                return Collections.singletonList(MessageType.TRANSACTION);
            case DELAY:
                return Collections.singletonList(MessageType.DELAY);
            case MIXED:
                return Arrays.asList(MessageType.NORMAL, MessageType.FIFO, MessageType.DELAY, MessageType.TRANSACTION);
            default:
                return Collections.singletonList(MessageType.MESSAGE_TYPE_UNSPECIFIED);
        }
    }
}
