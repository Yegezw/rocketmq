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
package org.apache.rocketmq.proxy.grpc.v2.consumer;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.utils.FilterUtils;
import org.apache.rocketmq.proxy.processor.PopMessageResultFilter;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

/**
 * POP 消息结果过滤器实现
 */
public class PopMessageResultFilterImpl implements PopMessageResultFilter {

    /**
     * 允许的最大重试次数
     */
    private final int maxAttempts;

    /**
     * 构造 POP 结果过滤器
     *
     * @param maxAttempts 最大重试次数
     */
    public PopMessageResultFilterImpl(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    /**
     * 按 tag 与重试次数过滤消息
     *
     * @param ctx Proxy 上下文
     * @param consumerGroup 消费组
     * @param subscriptionData 订阅数据
     * @param messageExt 消息对象
     * @return 过滤结果
     */
    @Override
    public FilterResult filterMessage(ProxyContext ctx, String consumerGroup, SubscriptionData subscriptionData,
        MessageExt messageExt) {
        if (!FilterUtils.isTagMatched(subscriptionData.getTagsSet(), messageExt.getTags())) {
            return FilterResult.NO_MATCH;
        }
        if (messageExt.getReconsumeTimes() >= maxAttempts) {
            return FilterResult.TO_DLQ;
        }
        return FilterResult.MATCH;
    }
}
