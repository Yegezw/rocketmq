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
package org.apache.rocketmq.namesrv.route;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Zone 路由 RPC Hook, 按请求中的 Zone 信息过滤返回路由
 */
public class ZoneRouteRPCHook implements RPCHook {

    /**
     * 请求前置钩子, 当前无需处理
     *
     * @param remoteAddr 远端地址
     * @param request    远程请求命令
     */
    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {

    }

    /**
     * 响应后置钩子, 对 Topic 路由结果按 Zone 进行过滤
     *
     * @param remoteAddr 远端地址
     * @param request    远程请求命令
     * @param response   远程响应命令
     */
    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
        // 仅处理按 Topic 查询路由请求
        if (RequestCode.GET_ROUTEINFO_BY_TOPIC != request.getCode()) {
            return;
        }

        // 响应为空、无请求体或非成功状态时直接返回
        if (response == null || response.getBody() == null || ResponseCode.SUCCESS != response.getCode()) {
            return;
        }

        // 未开启 Zone 模式时不做路由裁剪
        boolean zoneMode = Boolean.parseBoolean(request.getExtFields().get(MixAll.ZONE_MODE));
        if (!zoneMode) {
            return;
        }

        // 未指定 Zone 名称时不做路由裁剪
        String zoneName = request.getExtFields().get(MixAll.ZONE_NAME);
        if (StringUtils.isBlank(zoneName)) {
            return;
        }

        // 反序列化路由后按 Zone 过滤并写回响应体
        TopicRouteData topicRouteData = RemotingSerializable.decode(response.getBody(), TopicRouteData.class);
        response.setBody(filterByZoneName(topicRouteData, zoneName).encode());
    }

    /**
     * 按 Zone 名称过滤 Broker、Queue 和 FilterServer 路由信息
     *
     * @param topicRouteData 原始 Topic 路由数据
     * @param zoneName       目标 Zone 名称
     * @return 过滤后的 Topic 路由数据
     */
    private TopicRouteData filterByZoneName(TopicRouteData topicRouteData, String zoneName) {
        // 记录保留和剔除的 Broker
        List<BrokerData> brokerDataReserved = new ArrayList<>();
        Map<String, BrokerData> brokerDataRemoved = new HashMap<>();
        for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
            if (brokerData.getBrokerAddrs() == null) {
                continue;
            }

            // Master 宕机时消费者从 Slave 消费, 这会打破就近路由规则
            if (brokerData.getBrokerAddrs().get(MixAll.MASTER_ID) == null
                || StringUtils.equalsIgnoreCase(brokerData.getZoneName(), zoneName)) {
                brokerDataReserved.add(brokerData);
            } else {
                brokerDataRemoved.put(brokerData.getBrokerName(), brokerData);
            }
        }
        topicRouteData.setBrokerDatas(brokerDataReserved);

        // 仅保留未被剔除 Broker 对应的 Queue
        List<QueueData> queueDataReserved = new ArrayList<>();
        for (QueueData queueData : topicRouteData.getQueueDatas()) {
            if (!brokerDataRemoved.containsKey(queueData.getBrokerName())) {
                queueDataReserved.add(queueData);
            }
        }
        topicRouteData.setQueueDatas(queueDataReserved);

        // 按 Broker 地址移除 FilterServer 映射
        if (topicRouteData.getFilterServerTable() != null && !topicRouteData.getFilterServerTable().isEmpty()) {
            for (Entry<String, BrokerData> entry : brokerDataRemoved.entrySet()) {
                BrokerData brokerData = entry.getValue();
                brokerData.getBrokerAddrs().values()
                    .forEach(brokerAddr -> topicRouteData.getFilterServerTable().remove(brokerAddr));
            }
        }
        return topicRouteData;
    }
}
