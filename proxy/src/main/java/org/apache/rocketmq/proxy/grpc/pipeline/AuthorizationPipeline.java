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
package org.apache.rocketmq.proxy.grpc.pipeline;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
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

/**
 * 鉴权流水线, 负责校验当前主体是否具备目标资源访问权限
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
     * @param messagingProcessor 消息处理器, 用于提供元数据服务
     */
    public AuthorizationPipeline(AuthConfig authConfig, MessagingProcessor messagingProcessor) {
        this.authConfig = authConfig;
        this.authorizationEvaluator = AuthorizationFactory.getEvaluator(authConfig, messagingProcessor::getMetadataService);
    }

    /**
     * 执行鉴权流程, 按配置决定是否启用权限校验
     *
     * @param context Proxy 上下文
     * @param headers gRPC 请求头
     * @param request 请求体
     */
    @Override
    public void execute(ProxyContext context, Metadata headers, GeneratedMessageV3 request) {
        if (!authConfig.isAuthorizationEnabled()) {
            return;
        }
        try {
            List<AuthorizationContext> contexts = newContexts(context, headers, request);
            authorizationEvaluator.evaluate(contexts);
        } catch (AuthorizationException | AuthenticationException ex) {
            throw ex;
        }  catch (Throwable ex) {
            LOGGER.error("authorize failed, request:{}", request, ex);
            throw ex;
        }
    }

    /**
     * 构建鉴权上下文集合, 供鉴权执行器逐条校验
     *
     * @param context Proxy 上下文
     * @param headers gRPC 请求头
     * @param request 请求体
     * @return 鉴权上下文列表
     */
    protected List<AuthorizationContext> newContexts(ProxyContext context, Metadata headers, GeneratedMessageV3 request) {
        return AuthorizationFactory.newContexts(authConfig, headers, request);
    }
}
