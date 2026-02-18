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

package org.apache.rocketmq.common.constant;

import io.grpc.Context;
import io.grpc.Metadata;

/**
 * gRPC 请求头与上下文键常量定义
 */
public class GrpcConstants {
    /**
     * gRPC Context 中存储 Metadata 的键
     */
    public static final Context.Key<Metadata> METADATA = Context.key("rpc-metadata");

    /**
     * Remote address key in attributes of call
     * <br>
     * 调用属性中的远端地址键
     */
    public static final Metadata.Key<String> REMOTE_ADDRESS
        = Metadata.Key.of("rpc-remote-address", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * Local address key in attributes of call
     * <br>
     * 调用属性中的本地地址键
     */
    public static final Metadata.Key<String> LOCAL_ADDRESS
        = Metadata.Key.of("rpc-local-address", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * 授权信息键
     */
    public static final Metadata.Key<String> AUTHORIZATION
        = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * 命名空间标识键
     */
    public static final Metadata.Key<String> NAMESPACE_ID
        = Metadata.Key.of("x-mq-namespace", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * 请求时间键
     */
    public static final Metadata.Key<String> DATE_TIME
        = Metadata.Key.of("x-mq-date-time", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * 请求 ID 键
     */
    public static final Metadata.Key<String> REQUEST_ID
        = Metadata.Key.of("x-mq-request-id", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * 客户端语言键
     */
    public static final Metadata.Key<String> LANGUAGE
        = Metadata.Key.of("x-mq-language", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * 客户端版本键
     */
    public static final Metadata.Key<String> CLIENT_VERSION
        = Metadata.Key.of("x-mq-client-version", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * 协议版本键
     */
    public static final Metadata.Key<String> PROTOCOL_VERSION
        = Metadata.Key.of("x-mq-protocol", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * RPC 全名称键
     */
    public static final Metadata.Key<String> RPC_NAME
        = Metadata.Key.of("x-mq-rpc-name", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * RPC 简名键
     */
    public static final Metadata.Key<String> SIMPLE_RPC_NAME
            = Metadata.Key.of("x-mq-simple-rpc-name", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * 会话令牌键
     */
    public static final Metadata.Key<String> SESSION_TOKEN
        = Metadata.Key.of("x-mq-session-token", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * 客户端标识键
     */
    public static final Metadata.Key<String> CLIENT_ID
        = Metadata.Key.of("x-mq-client-id", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * 授权 AccessKey 键
     */
    public static final Metadata.Key<String> AUTHORIZATION_AK
        = Metadata.Key.of("x-mq-authorization-ak", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * 连接通道标识键
     */
    public static final Metadata.Key<String> CHANNEL_ID
        = Metadata.Key.of("x-mq-channel-id", Metadata.ASCII_STRING_MARSHALLER);
}
