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
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.validator.DefaultTopicMessageTypeValidator;
import org.apache.rocketmq.proxy.processor.validator.TopicMessageTypeValidator;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.header.RecallMessageRequestHeader;

import java.time.Duration;

/**
 * 延迟消息撤回请求处理活动
 */
public class RecallMessageActivity extends AbstractRemotingActivity {
    /**
     * 主题消息类型校验器
     */
    TopicMessageTypeValidator topicMessageTypeValidator;

    /**
     * 构造撤回消息活动处理器
     *
     * @param requestPipeline 请求处理管道
     * @param messagingProcessor 消息处理核心组件
     */
    public RecallMessageActivity(RequestPipeline requestPipeline,
                                 MessagingProcessor messagingProcessor) {
        super(requestPipeline, messagingProcessor);
        this.topicMessageTypeValidator = new DefaultTopicMessageTypeValidator();
    }

    /**
     * 处理撤回消息请求并校验主题类型
     *
     * @param ctx Netty 上下文
     * @param request 请求命令
     * @param context Proxy 上下文
     * @return Broker 响应
     * @throws Exception 处理异常
     */
    @Override
    public RemotingCommand processRequest0(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        RecallMessageRequestHeader requestHeader = request.decodeCommandCustomHeader(RecallMessageRequestHeader.class);
        String topic = requestHeader.getTopic();
        if (ConfigurationManager.getProxyConfig().isEnableTopicMessageTypeCheck()) {
            TopicMessageType messageType = messagingProcessor.getMetadataService().getTopicMessageType(context, topic);
            topicMessageTypeValidator.validate(messageType, TopicMessageType.DELAY);
        }
        return request(ctx, request, context, Duration.ofSeconds(2).toMillis());
    }
}
