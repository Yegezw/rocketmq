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
package org.apache.rocketmq.remoting.netty;

import io.netty.util.AttributeKey;
import org.apache.rocketmq.common.constant.HAProxyConstants;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AttributeKeys {

    /**
     * 远端地址属性键, 用于记录客户端连接地址
     */
    public static final AttributeKey<String> REMOTE_ADDR_KEY = AttributeKey.valueOf("RemoteAddr");

    /**
     * 客户端标识属性键, 用于关联客户端唯一标识
     */
    public static final AttributeKey<String> CLIENT_ID_KEY = AttributeKey.valueOf("ClientId");

    /**
     * 协议版本属性键, 用于记录请求版本号
     */
    public static final AttributeKey<Integer> VERSION_KEY = AttributeKey.valueOf("Version");

    /**
     * 语言编码属性键, 用于标识客户端语言类型
     */
    public static final AttributeKey<LanguageCode> LANGUAGE_CODE_KEY = AttributeKey.valueOf("LanguageCode");

    /**
     * PROXY 协议客户端地址属性键, 用于保存透传来源地址
     */
    public static final AttributeKey<String> PROXY_PROTOCOL_ADDR =
            AttributeKey.valueOf(HAProxyConstants.PROXY_PROTOCOL_ADDR);

    /**
     * PROXY 协议客户端端口属性键, 用于保存透传来源端口
     */
    public static final AttributeKey<String> PROXY_PROTOCOL_PORT =
            AttributeKey.valueOf(HAProxyConstants.PROXY_PROTOCOL_PORT);

    /**
     * PROXY 协议服务端地址属性键, 用于保存透传目标地址
     */
    public static final AttributeKey<String> PROXY_PROTOCOL_SERVER_ADDR =
            AttributeKey.valueOf(HAProxyConstants.PROXY_PROTOCOL_SERVER_ADDR);

    /**
     * PROXY 协议服务端端口属性键, 用于保存透传目标端口
     */
    public static final AttributeKey<String> PROXY_PROTOCOL_SERVER_PORT =
            AttributeKey.valueOf(HAProxyConstants.PROXY_PROTOCOL_SERVER_PORT);

    /**
     * 字符串属性键缓存, 用于复用动态创建的 AttributeKey
     */
    private static final Map<String, AttributeKey<String>> ATTRIBUTE_KEY_MAP = new ConcurrentHashMap<>();

    /**
     * 根据名称获取属性键, 若缓存不存在则创建并写入缓存
     *
     * @param name 属性键名称
     * @return 与名称对应的字符串属性键
     */
    public static AttributeKey<String> valueOf(String name) {
        return ATTRIBUTE_KEY_MAP.computeIfAbsent(name, AttributeKey::valueOf);
    }
}
