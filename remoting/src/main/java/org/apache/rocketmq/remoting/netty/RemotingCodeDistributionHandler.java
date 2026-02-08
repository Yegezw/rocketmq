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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * RemotingCode 分布统计处理器
 */
@ChannelHandler.Sharable
public class RemotingCodeDistributionHandler extends ChannelDuplexHandler {

    /**
     * 入站请求码计数映射
     */
    private final ConcurrentMap<Integer, LongAdder> inboundDistribution;
    /**
     * 出站响应码计数映射
     */
    private final ConcurrentMap<Integer, LongAdder> outboundDistribution;

    /**
     * 构造分布统计处理器
     */
    public RemotingCodeDistributionHandler() {
        inboundDistribution = new ConcurrentHashMap<>();
        outboundDistribution = new ConcurrentHashMap<>();
    }

    /**
     * 累计入站请求码计数
     *
     * @param requestCode 请求码
     */
    private void countInbound(int requestCode) {
        LongAdder item = inboundDistribution.computeIfAbsent(requestCode, k -> new LongAdder());
        item.increment();
    }

    /**
     * 累计出站响应码计数
     *
     * @param responseCode 响应码
     */
    private void countOutbound(int responseCode) {
        LongAdder item = outboundDistribution.computeIfAbsent(responseCode, k -> new LongAdder());
        item.increment();
    }

    /**
     * 统计入站 RemotingCommand 请求码分布
     *
     * @param ctx Channel 上下文
     * @param msg 入站消息
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof RemotingCommand) {
            RemotingCommand cmd = (RemotingCommand) msg;
            countInbound(cmd.getCode());
        }
        ctx.fireChannelRead(msg);
    }

    /**
     * 统计出站 RemotingCommand 响应码分布
     *
     * @param ctx     Channel 上下文
     * @param msg     出站消息
     * @param promise 写入 Promise
     * @throws Exception 写入异常
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof RemotingCommand) {
            RemotingCommand cmd = (RemotingCommand) msg;
            countOutbound(cmd.getCode());
        }
        ctx.write(msg, promise);
    }

    /**
     * 获取分布快照, 并重置当前计数
     *
     * @param countMap 计数映射
     * @return 快照映射
     */
    private Map<Integer, Long> getDistributionSnapshot(Map<Integer, LongAdder> countMap) {
        Map<Integer, Long> map = new HashMap<>(countMap.size());
        for (Map.Entry<Integer, LongAdder> entry : countMap.entrySet()) {
            map.put(entry.getKey(), entry.getValue().sumThenReset());
        }
        return map;
    }

    /**
     * 将分布快照转换为字符串
     *
     * @param distribution 分布快照
     * @return 快照字符串, 无有效数据时返回 null
     */
    private String snapshotToString(Map<Integer, Long> distribution) {
        if (null != distribution && !distribution.isEmpty()) {
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<Integer, Long> entry : distribution.entrySet()) {
                if (0L == entry.getValue()) {
                    continue;
                }
                sb.append(first ? "" : ", ").append(entry.getKey()).append(":").append(entry.getValue());
                first = false;
            }
            if (first) {
                return null;
            }
            sb.append("}");
            return sb.toString();
        }
        return null;
    }

    public String getInBoundSnapshotString() {
        return this.snapshotToString(this.getDistributionSnapshot(this.inboundDistribution));
    }

    public String getOutBoundSnapshotString() {
        return this.snapshotToString(this.getDistributionSnapshot(this.outboundDistribution));
    }
}
