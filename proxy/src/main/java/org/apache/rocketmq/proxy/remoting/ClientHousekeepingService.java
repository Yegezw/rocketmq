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

package org.apache.rocketmq.proxy.remoting;

import io.netty.channel.Channel;
import org.apache.rocketmq.proxy.remoting.activity.ClientManagerActivity;
import org.apache.rocketmq.remoting.ChannelEventListener;

/**
 * 客户端连接事件清理服务<br>
 * 负责将连接关闭、异常、空闲等事件转发给客户端管理活动
 */
public class ClientHousekeepingService implements ChannelEventListener {

    /**
     * 客户端管理活动
     */
    private final ClientManagerActivity clientManagerActivity;

    /**
     * 构造客户端连接事件清理服务
     *
     * @param clientManagerActivity 客户端管理活动
     */
    public ClientHousekeepingService(ClientManagerActivity clientManagerActivity) {
        this.clientManagerActivity = clientManagerActivity;
    }

    /**
     * 处理连接建立事件
     *
     * @param remoteAddr 远端地址
     * @param channel 连接通道
     */
    @Override
    public void onChannelConnect(String remoteAddr, Channel channel) {

    }

    /**
     * 处理连接关闭事件
     *
     * @param remoteAddr 远端地址
     * @param channel 连接通道
     */
    @Override
    public void onChannelClose(String remoteAddr, Channel channel) {
        this.clientManagerActivity.doChannelCloseEvent(remoteAddr, channel);
    }

    /**
     * 处理连接异常事件
     *
     * @param remoteAddr 远端地址
     * @param channel 连接通道
     */
    @Override
    public void onChannelException(String remoteAddr, Channel channel) {
        this.clientManagerActivity.doChannelCloseEvent(remoteAddr, channel);
    }

    /**
     * 处理连接空闲事件
     *
     * @param remoteAddr 远端地址
     * @param channel 连接通道
     */
    @Override
    public void onChannelIdle(String remoteAddr, Channel channel) {
        this.clientManagerActivity.doChannelCloseEvent(remoteAddr, channel);
    }

    /**
     * 处理连接激活事件
     *
     * @param remoteAddr 远端地址
     * @param channel 连接通道
     */
    @Override
    public void onChannelActive(String remoteAddr, Channel channel) {

    }
}
