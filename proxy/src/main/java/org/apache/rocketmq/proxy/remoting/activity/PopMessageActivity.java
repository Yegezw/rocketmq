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

package org.apache.rocketmq.proxy.remoting.activity;

import io.netty.channel.ChannelHandlerContext;
import java.time.Duration;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * POP 消息请求处理活动
 */
public class PopMessageActivity extends AbstractRemotingActivity {
    /**
     * 构造 POP 活动处理器
     *
     * @param requestPipeline 请求处理管道
     * @param messagingProcessor 消息处理核心组件
     */
    public PopMessageActivity(RequestPipeline requestPipeline,
        MessagingProcessor messagingProcessor) {
        super(requestPipeline, messagingProcessor);
    }

    /**
     * 处理 POP 请求并计算代理转发超时
     *
     * @param ctx Netty 上下文
     * @param request 请求命令
     * @param context Proxy 上下文
     * @return Broker 响应
     * @throws Exception 处理异常
     */
    @Override
    protected RemotingCommand processRequest0(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        PopMessageRequestHeader popMessageRequestHeader = (PopMessageRequestHeader) request.decodeCommandCustomHeader(PopMessageRequestHeader.class);
        // 超时时间使用客户端轮询时长, 并额外预留网络传输时间
        long timeoutMillis = popMessageRequestHeader.getPollTime();
        return request(ctx, request, context, timeoutMillis + Duration.ofSeconds(10).toMillis());
    }
}
