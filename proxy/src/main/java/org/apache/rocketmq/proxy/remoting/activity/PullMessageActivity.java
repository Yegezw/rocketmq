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
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

/**
 * 拉消息请求处理活动
 */
public class PullMessageActivity extends AbstractRemotingActivity {
    /**
     * 构造拉消息活动处理器
     *
     * @param requestPipeline 请求处理管道
     * @param messagingProcessor 消息处理核心组件
     */
    public PullMessageActivity(RequestPipeline requestPipeline,
        MessagingProcessor messagingProcessor) {
        super(requestPipeline, messagingProcessor);
    }

    /**
     * 处理拉消息请求, 并在缺失订阅时补齐订阅信息
     *
     * @param ctx Netty 上下文
     * @param request 请求命令
     * @param context Proxy 上下文
     * @return Broker 响应或错误响应
     * @throws Exception 处理异常
     */
    @Override
    protected RemotingCommand processRequest0(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        PullMessageRequestHeader requestHeader = (PullMessageRequestHeader) request.decodeCommandCustomHeader(PullMessageRequestHeader.class);
        int sysFlag = requestHeader.getSysFlag();
        if (!PullSysFlag.hasSubscriptionFlag(sysFlag)) {
            // 请求未携带订阅信息时, 从当前消费组元数据中补齐订阅
            ConsumerGroupInfo consumerInfo = messagingProcessor.getConsumerGroupInfo(context, requestHeader.getConsumerGroup());
            if (consumerInfo == null) {
                return RemotingCommand.buildErrorResponse(ResponseCode.SUBSCRIPTION_NOT_LATEST,
                    "the consumer's subscription not latest");
            }
            SubscriptionData subscriptionData = consumerInfo.findSubscriptionData(requestHeader.getTopic());
            if (subscriptionData == null) {
                return RemotingCommand.buildErrorResponse(ResponseCode.SUBSCRIPTION_NOT_EXIST,
                    "the consumer's subscription not exist");
            }
            requestHeader.setSysFlag(PullSysFlag.buildSysFlagWithSubscription(sysFlag));
            requestHeader.setSubscription(subscriptionData.getSubString());
            requestHeader.setExpressionType(subscriptionData.getExpressionType());
            request.writeCustomHeader(requestHeader);
            request.makeCustomHeaderToNet();
        }
        // Broker 处理超时 = 长轮询挂起时间 + 网络与转发预留时间
        long timeoutMillis = requestHeader.getSuspendTimeoutMillis() + Duration.ofSeconds(10).toMillis();
        return request(ctx, request, context, timeoutMillis);
    }
}
