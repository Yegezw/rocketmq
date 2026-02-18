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
import io.grpc.Context;
import io.grpc.Metadata;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.channel.ChannelProtocolType;

/**
 * 请求上下文初始化流水线, 将 gRPC 元数据写入 ProxyContext
 */
public class ContextInitPipeline implements RequestPipeline {
    /**
     * 从请求头和调用上下文中提取信息并初始化 ProxyContext
     *
     * @param context Proxy 上下文
     * @param headers gRPC 请求头
     * @param request 请求体
     */
    @Override
    public void execute(ProxyContext context, Metadata headers, GeneratedMessageV3 request) {
        Context ctx = Context.current();
        context.setLocalAddress(getDefaultStringMetadataInfo(headers, GrpcConstants.LOCAL_ADDRESS))
            .setRemoteAddress(getDefaultStringMetadataInfo(headers, GrpcConstants.REMOTE_ADDRESS))
            .setClientID(getDefaultStringMetadataInfo(headers, GrpcConstants.CLIENT_ID))
            .setProtocolType(ChannelProtocolType.GRPC_V2.getName())
            .setLanguage(getDefaultStringMetadataInfo(headers, GrpcConstants.LANGUAGE))
            .setClientVersion(getDefaultStringMetadataInfo(headers, GrpcConstants.CLIENT_VERSION))
            .setAction(getDefaultStringMetadataInfo(headers, GrpcConstants.SIMPLE_RPC_NAME))
            .setNamespace(getDefaultStringMetadataInfo(headers, GrpcConstants.NAMESPACE_ID));
        if (ctx.getDeadline() != null) {
            context.setRemainingMs(ctx.getDeadline().timeRemaining(TimeUnit.MILLISECONDS));
        }
    }

    /**
     * 从 Metadata 中读取字符串值并在空值场景下返回默认空串
     *
     * @param headers gRPC 请求头
     * @param key 目标键
     * @return 非 null 的字符串值
     */
    protected String getDefaultStringMetadataInfo(Metadata headers, Metadata.Key<String> key) {
        return StringUtils.defaultString(headers.get(key));
    }
}
