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

package org.apache.rocketmq.proxy.remoting.pipeline;

import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authorization.AuthorizationEvaluator;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * Remoting 鉴权流水线, 负责请求权限校验
 */
public class AuthorizationPipeline implements RequestPipeline {
    /**
     * Proxy 模块日志记录器
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    /**
     * 鉴权配置
     */
    private final AuthConfig authConfig;
    /**
     * 鉴权执行器
     */
    private final AuthorizationEvaluator authorizationEvaluator;

    /**
     * 构造鉴权流水线
     *
     * @param authConfig 鉴权配置
     * @param messagingProcessor 消息处理器
     */
    public AuthorizationPipeline(AuthConfig authConfig, MessagingProcessor messagingProcessor) {
        this.authConfig = authConfig;
        this.authorizationEvaluator = AuthorizationFactory.getEvaluator(authConfig, messagingProcessor::getMetadataService);
    }

    /**
     * 执行鉴权流程
     *
     * @param ctx Netty 处理上下文
     * @param request remoting 请求命令
     * @param context Proxy 上下文
     * @throws Exception 执行异常
     */
    @Override
    public void execute(ChannelHandlerContext ctx, RemotingCommand request, ProxyContext context) throws Exception {
        if (!authConfig.isAuthorizationEnabled()) {
            return;
        }
        try {
            List<AuthorizationContext> contexts = newContexts(request, ctx, context);
            authorizationEvaluator.evaluate(contexts);
        } catch (AuthorizationException | AuthenticationException ex) {
            throw ex;
        }  catch (Throwable ex) {
            LOGGER.error("authorize failed, request:{}", request, ex);
            throw ex;
        }
    }

    /**
     * 构建鉴权上下文列表
     *
     * @param request remoting 请求命令
     * @param ctx Netty 处理上下文
     * @param context Proxy 上下文
     * @return 鉴权上下文列表
     */
    protected List<AuthorizationContext> newContexts(RemotingCommand request, ChannelHandlerContext ctx, ProxyContext context) {
        return AuthorizationFactory.newContexts(authConfig, ctx, request);
    }
}
