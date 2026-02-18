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
package org.apache.rocketmq.proxy.grpc.v2;

import apache.rocketmq.v2.Resource;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcValidator;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;

/**
 * gRPC 消息活动抽象基类, 提供公共依赖与参数校验能力
 */
public abstract class AbstractMessingActivity {
    /**
     * Proxy 模块日志记录器
     */
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    /**
     * 消息处理器
     */
    protected final MessagingProcessor messagingProcessor;
    /**
     * gRPC 客户端设置管理器
     */
    protected final GrpcClientSettingsManager grpcClientSettingsManager;
    /**
     * gRPC 通道管理器
     */
    protected final GrpcChannelManager grpcChannelManager;

    /**
     * 构造抽象活动基类
     *
     * @param messagingProcessor 消息处理器
     * @param grpcClientSettingsManager gRPC 客户端设置管理器
     * @param grpcChannelManager gRPC 通道管理器
     */
    public AbstractMessingActivity(MessagingProcessor messagingProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager, GrpcChannelManager grpcChannelManager) {
        this.messagingProcessor = messagingProcessor;
        this.grpcClientSettingsManager = grpcClientSettingsManager;
        this.grpcChannelManager = grpcChannelManager;
    }

    /**
     * 校验主题资源合法性
     *
     * @param topic 主题资源
     */
    protected void validateTopic(Resource topic) {
        GrpcValidator.getInstance().validateTopic(topic);
    }

    /**
     * 校验消费组资源合法性
     *
     * @param consumerGroup 消费组资源
     */
    protected void validateConsumerGroup(Resource consumerGroup) {
        GrpcValidator.getInstance().validateConsumerGroup(consumerGroup);
    }

    /**
     * 同时校验主题与消费组资源合法性
     *
     * @param topic 主题资源
     * @param consumerGroup 消费组资源
     */
    protected void validateTopicAndConsumerGroup(Resource topic, Resource consumerGroup) {
        GrpcValidator.getInstance().validateTopicAndConsumerGroup(topic, consumerGroup);
    }

    /**
     * 校验不可见时长是否合法
     *
     * @param invisibleTime 不可见时长
     */
    protected void validateInvisibleTime(long invisibleTime) {
        GrpcValidator.getInstance().validateInvisibleTime(invisibleTime);
    }

    /**
     * 按最小阈值校验不可见时长是否合法
     *
     * @param invisibleTime 不可见时长
     * @param minInvisibleTime 最小不可见时长
     */
    protected void validateInvisibleTime(long invisibleTime, long minInvisibleTime) {
        GrpcValidator.getInstance().validateInvisibleTime(invisibleTime, minInvisibleTime);
    }
}
