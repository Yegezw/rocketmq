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
package org.apache.rocketmq.proxy.grpc.constant;

import io.grpc.Attributes;
import org.apache.rocketmq.common.constant.HAProxyConstants;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * gRPC 连接属性键集合, 用于在 Netty 与 gRPC 流程中传递链路信息
 */
public class AttributeKeys {

    /**
     * 当前连接唯一标识
     */
    public static final Attributes.Key<String> CHANNEL_ID =
        Attributes.Key.create(HAProxyConstants.CHANNEL_ID);

    /**
     * Proxy Protocol 源地址
     */
    public static final Attributes.Key<String> PROXY_PROTOCOL_ADDR =
            Attributes.Key.create(HAProxyConstants.PROXY_PROTOCOL_ADDR);

    /**
     * Proxy Protocol 源端口
     */
    public static final Attributes.Key<String> PROXY_PROTOCOL_PORT =
            Attributes.Key.create(HAProxyConstants.PROXY_PROTOCOL_PORT);

    /**
     * Proxy Protocol 目标地址
     */
    public static final Attributes.Key<String> PROXY_PROTOCOL_SERVER_ADDR =
            Attributes.Key.create(HAProxyConstants.PROXY_PROTOCOL_SERVER_ADDR);

    /**
     * Proxy Protocol 目标端口
     */
    public static final Attributes.Key<String> PROXY_PROTOCOL_SERVER_PORT =
            Attributes.Key.create(HAProxyConstants.PROXY_PROTOCOL_SERVER_PORT);

    /**
     * 动态属性键缓存, 避免重复创建同名 Attributes.Key
     */
    private static final Map<String, Attributes.Key<String>> ATTRIBUTES_KEY_MAP = new ConcurrentHashMap<>();

    /**
     * 根据名称返回 Attributes.Key, 首次访问时自动创建并缓存
     *
     * @param name 属性键名称
     * @return 属性键对象
     */
    public static Attributes.Key<String> valueOf(String name) {
        return ATTRIBUTES_KEY_MAP.computeIfAbsent(name, key -> Attributes.Key.create(name));
    }
}
