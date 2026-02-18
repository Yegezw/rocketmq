/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.grpc.v2.common;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Resource;
import com.google.common.base.CharMatcher;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;

/**
 * gRPC 请求参数校验器<br>
 * 负责主题、消费组、不可见时长与标签等基础校验
 */
public class GrpcValidator {
    /**
     * Proxy 日志记录器
     */
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    /**
     * 单例创建锁
     */
    protected static final Object INSTANCE_CREATE_LOCK = new Object();
    /**
     * 校验器单例
     */
    protected static volatile GrpcValidator instance;

    public static GrpcValidator getInstance() {
        if (instance == null) {
            synchronized (INSTANCE_CREATE_LOCK) {
                if (instance == null) {
                    instance = new GrpcValidator();
                }
            }
        }
        return instance;
    }

    /**
     * 校验主题资源对象
     *
     * @param topic 主题资源
     */
    public void validateTopic(Resource topic) {
        validateTopic(topic.getName());
    }

    /**
     * 校验主题名称合法性
     *
     * @param topicName 主题名称
     */
    public void validateTopic(String topicName) {
        try {
            Validators.checkTopic(topicName);
        } catch (MQClientException mqClientException) {
            throw new GrpcProxyException(Code.ILLEGAL_TOPIC, mqClientException.getErrorMessage());
        }
        if (TopicValidator.isSystemTopic(topicName)) {
            throw new GrpcProxyException(Code.ILLEGAL_TOPIC, "cannot access system topic");
        }
    }

    /**
     * 校验消费组资源对象
     *
     * @param consumerGroup 消费组资源
     */
    public void validateConsumerGroup(Resource consumerGroup) {
        validateConsumerGroup(consumerGroup.getName());
    }

    /**
     * 校验消费组名称合法性
     *
     * @param consumerGroupName 消费组名称
     */
    public void validateConsumerGroup(String consumerGroupName) {
        try {
            Validators.checkGroup(consumerGroupName);
        } catch (MQClientException mqClientException) {
            throw new GrpcProxyException(Code.ILLEGAL_CONSUMER_GROUP, mqClientException.getErrorMessage());
        }
        if (MixAll.isSysConsumerGroup(consumerGroupName)) {
            throw new GrpcProxyException(Code.ILLEGAL_CONSUMER_GROUP, "cannot use system consumer group");
        }
    }

    /**
     * 同时校验主题与消费组
     *
     * @param topic 主题资源
     * @param consumerGroup 消费组资源
     */
    public void validateTopicAndConsumerGroup(Resource topic, Resource consumerGroup) {
        validateTopic(topic);
        validateConsumerGroup(consumerGroup);
    }

    /**
     * 校验不可见时长
     *
     * @param invisibleTime 不可见时长 毫秒
     */
    public void validateInvisibleTime(long invisibleTime) {
        validateInvisibleTime(invisibleTime, 0);
    }

    /**
     * 按最小值和最大值校验不可见时长
     *
     * @param invisibleTime 不可见时长 毫秒
     * @param minInvisibleTime 最小不可见时长 毫秒
     */
    public void validateInvisibleTime(long invisibleTime, long minInvisibleTime) {
        if (invisibleTime < minInvisibleTime) {
            throw new GrpcProxyException(Code.ILLEGAL_INVISIBLE_TIME, "the invisibleTime is too small. min is " + minInvisibleTime);
        }
        long maxInvisibleTime = ConfigurationManager.getProxyConfig().getMaxInvisibleTimeMills();
        if (maxInvisibleTime <= 0) {
            return;
        }
        if (invisibleTime > maxInvisibleTime) {
            throw new GrpcProxyException(Code.ILLEGAL_INVISIBLE_TIME, "the invisibleTime is too large. max is " + maxInvisibleTime);
        }
    }

    /**
     * 校验消息标签合法性
     *
     * @param tag 标签内容
     */
    public void validateTag(String tag) {
        if (StringUtils.isNotEmpty(tag)) {
            if (StringUtils.isBlank(tag)) {
                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_TAG, "tag cannot be the char sequence of whitespace");
            }
            if (tag.contains("|")) {
                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_TAG, "tag cannot contain '|'");
            }
            if (containControlCharacter(tag)) {
                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_TAG, "tag cannot contain control character");
            }
        }
    }

    /**
     * 判断字符串是否包含控制字符
     *
     * @param data 待校验字符串
     * @return true 表示包含控制字符
     */
    public boolean containControlCharacter(String data) {
        for (int i = 0; i < data.length(); i++) {
            if (CharMatcher.javaIsoControl().matches(data.charAt(i))) {
                return true;
            }
        }
        return false;
    }
}
