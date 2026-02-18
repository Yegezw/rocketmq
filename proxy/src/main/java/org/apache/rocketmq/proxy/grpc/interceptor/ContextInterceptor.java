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

package org.apache.rocketmq.proxy.grpc.interceptor;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.apache.rocketmq.common.constant.GrpcConstants;

/**
 * gRPC 服务端拦截器, 将本次调用元数据绑定到 Context
 */
public class ContextInterceptor implements ServerInterceptor {

    /**
     * 在调用进入业务处理器前写入 Metadata, 供下游组件读取链路头信息
     *
     * @param call 当前 RPC 调用
     * @param headers 当前调用头信息
     * @param next 下一个调用处理器
     * @return 封装 Metadata 后的监听器
     */
    @Override
    public <R, W> ServerCall.Listener<R> interceptCall(
        ServerCall<R, W> call,
        Metadata headers,
        ServerCallHandler<R, W> next
    ) {
        Context context = Context.current().withValue(GrpcConstants.METADATA, headers);
        return Contexts.interceptCall(context, call, headers, next);
    }
}
