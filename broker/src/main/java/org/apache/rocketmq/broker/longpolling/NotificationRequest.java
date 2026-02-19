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
package org.apache.rocketmq.broker.longpolling;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import io.netty.channel.Channel;

/**
 * 封装通知型长轮询请求, 用于在消息到达时回写客户端
 */
public class NotificationRequest {
    /**
     * 通知请求的协议命令对象, 被唤醒时直接用于应答
     */
    private RemotingCommand remotingCommand;
    /**
     * 发起请求的客户端连接, 用于结果回写
     */
    private Channel channel;
    /**
     * 请求过期时间戳, 由 bornTime 与 pollTime 计算得到
     */
    private long expired;
    /**
     * 请求完成标记, 确保同一个请求只被处理一次
     */
    private AtomicBoolean complete = new AtomicBoolean(false);

    /**
     * 创建通知型长轮询请求实例
     *
     * @param remotingCommand 原始网络请求命令
     * @param channel 客户端连接
     * @param expired 请求过期时间戳
     */
    public NotificationRequest(RemotingCommand remotingCommand, Channel channel, long expired) {
        this.channel = channel;
        this.remotingCommand = remotingCommand;
        this.expired = expired;
    }

    public Channel getChannel() {
        return channel;
    }

    public RemotingCommand getRemotingCommand() {
        return remotingCommand;
    }

    public boolean isTimeout() {
        return System.currentTimeMillis() > (expired - 500);
    }

    /**
     * 原子化标记请求完成状态, 防止重复唤醒
     *
     * @return 首次完成返回 true, 重复调用返回 false
     */
    public boolean complete() {
        return complete.compareAndSet(false, true);
    }

    /**
     * 输出请求调试信息, 直接复用 RemotingCommand 字符串内容
     *
     * @return 当前请求的可读文本
     */
    @Override
    public String toString() {
        return remotingCommand.toString();
    }
}
