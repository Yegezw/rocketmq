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
import io.netty.channel.ChannelHandlerContext;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.store.MessageFilter;

/**
 * POP 长轮询请求封装, 持有唤醒执行与过滤匹配所需上下文
 */
public class PopRequest {
    /**
     * 全局递增序号, 用于在过期时间相同场景下保证排序稳定
     */
    private static final AtomicLong COUNTER = new AtomicLong(Long.MIN_VALUE);

    /**
     * 原始 POP 请求命令
     */
    private final RemotingCommand remotingCommand;
    /**
     * Netty 上下文, 用于后续异步回写响应
     */
    private final ChannelHandlerContext ctx;
    /**
     * 请求完成标记, 防止重复唤醒
     */
    private final AtomicBoolean complete = new AtomicBoolean(false);
    /**
     * 当前请求的序号, 用于并发集合排序去重
     */
    private final long op = COUNTER.getAndIncrement();

    /**
     * 请求过期时间戳
     */
    private final long expired;
    /**
     * 订阅信息快照, 用于消费过滤判断
     */
    private final SubscriptionData subscriptionData;
    /**
     * 消息过滤器实例
     */
    private final MessageFilter messageFilter;

    /**
     * 创建 POP 挂起请求
     *
     * @param remotingCommand 原始请求命令
     * @param ctx 网络上下文
     * @param expired 请求过期时间戳
     * @param subscriptionData 订阅数据
     * @param messageFilter 消息过滤器
     */
    public PopRequest(RemotingCommand remotingCommand, ChannelHandlerContext ctx,
        long expired, SubscriptionData subscriptionData, MessageFilter messageFilter) {

        this.ctx = ctx;
        this.remotingCommand = remotingCommand;
        this.expired = expired;
        this.subscriptionData = subscriptionData;
        this.messageFilter = messageFilter;
    }

    public Channel getChannel() {
        return ctx.channel();
    }

    public ChannelHandlerContext getCtx() {
        return ctx;
    }

    public RemotingCommand getRemotingCommand() {
        return remotingCommand;
    }

    public boolean isTimeout() {
        return System.currentTimeMillis() > (expired - 50);
    }

    /**
     * 原子化完成请求, 仅允许首次调用成功
     *
     * @return 首次完成返回 true, 其余返回 false
     */
    public boolean complete() {
        return complete.compareAndSet(false, true);
    }

    public long getExpired() {
        return expired;
    }

    public SubscriptionData getSubscriptionData() {
        return subscriptionData;
    }

    public MessageFilter getMessageFilter() {
        return messageFilter;
    }

    /**
     * 输出请求关键字段, 便于排查长轮询问题
     *
     * @return 当前请求字符串描述
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PopRequest{");
        sb.append("cmd=").append(remotingCommand);
        sb.append(", ctx=").append(ctx);
        sb.append(", expired=").append(expired);
        sb.append(", complete=").append(complete);
        sb.append(", op=").append(op);
        sb.append('}');
        return sb.toString();
    }

    /**
     * 挂起请求排序器, 先按过期时间再按序号排序
     */
    public static final Comparator<PopRequest> COMPARATOR = (o1, o2) -> {
        int ret = (int) (o1.getExpired() - o2.getExpired());

        if (ret != 0) {
            return ret;
        }
        ret = (int) (o1.op - o2.op);
        if (ret != 0) {
            return ret;
        }
        return -1;
    };
}
