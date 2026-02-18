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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.proxy.grpc.pipeline;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import org.apache.rocketmq.proxy.common.ProxyContext;

/**
 * gRPC 请求处理流水线接口, 用于串联多个请求处理阶段
 */
public interface RequestPipeline {

    /**
     * 执行当前流水线节点逻辑
     *
     * @param context Proxy 上下文
     * @param headers gRPC 请求头
     * @param request 请求体
     */
    void execute(ProxyContext context, Metadata headers, GeneratedMessageV3 request);

    /**
     * 将当前节点拼接到给定上游节点之后
     *
     * @param source 上游流水线节点
     * @return 组合后的流水线
     */
    default RequestPipeline pipe(RequestPipeline source) {
        return (ctx, headers, request) -> {
            source.execute(ctx, headers, request);
            execute(ctx, headers, request);
        };
    }
}
