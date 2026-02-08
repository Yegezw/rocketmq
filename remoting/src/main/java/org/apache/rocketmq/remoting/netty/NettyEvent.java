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
package org.apache.rocketmq.remoting.netty;

import io.netty.channel.Channel;

public class NettyEvent {
    /**
     * 事件类型
     */
    private final NettyEventType type;
    /**
     * 远端地址
     */
    private final String remoteAddr;
    /**
     * 事件关联 Channel
     */
    private final Channel channel;

    /**
     * 构造 Netty 事件
     *
     * @param type       事件类型
     * @param remoteAddr 远端地址
     * @param channel    事件关联 Channel
     */
    public NettyEvent(NettyEventType type, String remoteAddr, Channel channel) {
        this.type = type;
        this.remoteAddr = remoteAddr;
        this.channel = channel;
    }

    public NettyEventType getType() {
        return type;
    }

    public String getRemoteAddr() {
        return remoteAddr;
    }

    public Channel getChannel() {
        return channel;
    }

    /**
     * 返回事件描述字符串
     *
     * @return 事件描述
     */
    @Override
    public String toString() {
        return "NettyEvent [type=" + type + ", remoteAddr=" + remoteAddr + ", channel=" + channel + "]";
    }
}
