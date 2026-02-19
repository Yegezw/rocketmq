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

import io.netty.channel.Channel;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.store.MessageFilter;

/**
 * Pull 长轮询请求上下文, 保存挂起与过滤判断所需信息
 */
public class PullRequest {
    /**
     * 原始 pull 请求命令, 唤醒时直接交给处理器继续执行
     */
    private final RemotingCommand requestCommand;
    /**
     * 发起请求的客户端连接
     */
    private final Channel clientChannel;
    /**
     * 客户端声明的最长挂起时间, 单位毫秒
     */
    private final long timeoutMillis;
    /**
     * 请求被挂起的服务器时间戳
     */
    private final long suspendTimestamp;
    /**
     * 客户端期望拉取的起始偏移量
     */
    private final long pullFromThisOffset;
    /**
     * 订阅数据快照, 用于标签与属性匹配
     */
    private final SubscriptionData subscriptionData;
    /**
     * 服务端过滤器, 负责 consumeQueue 与 commitLog 两阶段匹配
     */
    private final MessageFilter messageFilter;

    /**
     * 创建 Pull 挂起请求
     *
     * @param requestCommand 原始请求命令
     * @param clientChannel 客户端连接
     * @param timeoutMillis 超时时间
     * @param suspendTimestamp 挂起时间戳
     * @param pullFromThisOffset 期望起始偏移量
     * @param subscriptionData 订阅数据
     * @param messageFilter 消息过滤器
     */
    public PullRequest(RemotingCommand requestCommand, Channel clientChannel, long timeoutMillis, long suspendTimestamp,
        long pullFromThisOffset, SubscriptionData subscriptionData,
        MessageFilter messageFilter) {
        this.requestCommand = requestCommand;
        this.clientChannel = clientChannel;
        this.timeoutMillis = timeoutMillis;
        this.suspendTimestamp = suspendTimestamp;
        this.pullFromThisOffset = pullFromThisOffset;
        this.subscriptionData = subscriptionData;
        this.messageFilter = messageFilter;
    }

    public RemotingCommand getRequestCommand() {
        return requestCommand;
    }

    public Channel getClientChannel() {
        return clientChannel;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public long getSuspendTimestamp() {
        return suspendTimestamp;
    }

    public long getPullFromThisOffset() {
        return pullFromThisOffset;
    }

    public SubscriptionData getSubscriptionData() {
        return subscriptionData;
    }

    public MessageFilter getMessageFilter() {
        return messageFilter;
    }
}
