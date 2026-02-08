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
package org.apache.rocketmq.namesrv.routeinfo;

import io.netty.channel.Channel;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.ChannelEventListener;

/**
 * Broker 连接事件清理服务, 负责在通道异常场景触发路由清理
 */
public class BrokerHousekeepingService implements ChannelEventListener {

    /**
     * NameServer 控制器引用, 用于访问路由管理器
     */
    private final NamesrvController namesrvController;

    /**
     * 创建 BrokerHousekeepingService 实例
     *
     * @param namesrvController NameServer 控制器
     */
    public BrokerHousekeepingService(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }

    /**
     * 处理通道建立事件, 当前无需额外处理
     *
     * @param remoteAddr 远端地址
     * @param channel    Netty 通道
     */
    @Override
    public void onChannelConnect(String remoteAddr, Channel channel) {
    }

    /**
     * 处理通道关闭事件, 触发路由清理
     *
     * @param remoteAddr 远端地址
     * @param channel    Netty 通道
     */
    @Override
    public void onChannelClose(String remoteAddr, Channel channel) {
        // 通道关闭后移除对应 Broker 路由信息
        this.namesrvController.getRouteInfoManager().onChannelDestroy(channel);
    }

    /**
     * 处理通道异常事件, 触发路由清理
     *
     * @param remoteAddr 远端地址
     * @param channel    Netty 通道
     */
    @Override
    public void onChannelException(String remoteAddr, Channel channel) {
        // 通道异常后移除对应 Broker 路由信息
        this.namesrvController.getRouteInfoManager().onChannelDestroy(channel);
    }

    /**
     * 处理通道空闲事件, 触发路由清理
     *
     * @param remoteAddr 远端地址
     * @param channel    Netty 通道
     */
    @Override
    public void onChannelIdle(String remoteAddr, Channel channel) {
        // 通道空闲后移除对应 Broker 路由信息
        this.namesrvController.getRouteInfoManager().onChannelDestroy(channel);
    }

    /**
     * 处理通道活跃事件, 当前无需额外处理
     *
     * @param remoteAddr 远端地址
     * @param channel    Netty 通道
     */
    @Override
    public void onChannelActive(String remoteAddr, Channel channel) {

    }
}
