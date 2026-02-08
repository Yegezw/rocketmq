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

package org.apache.rocketmq.remoting.common;

/**
 * For server, three SSL modes are supported: disabled, permissive and enforcing.
 * <ol>
 *     <li><strong>disabled:</strong> SSL is not supported; any incoming SSL handshake will be rejected, causing connection closed.</li>
 *     <li><strong>permissive:</strong> SSL is optional, aka, server in this mode can serve client connections with or without SSL;</li>
 *     <li><strong>enforcing:</strong> SSL is required, aka, non SSL connection will be rejected.</li>
 * </ol>
 * 服务端支持三种 SSL 模式: disabled, permissive, enforcing<br>
 * disabled: 不支持 SSL, 任意 SSL 握手请求都会被拒绝并关闭连接<br>
 * permissive: SSL 可选, 服务端可同时处理 SSL 与非 SSL 连接<br>
 * enforcing: 必须使用 SSL, 非 SSL 连接会被拒绝
 */
public enum TlsMode {

    /**
     * 禁用 TLS 模式<br>
     * 对应配置值 disabled
     */
    DISABLED("disabled"),
    /**
     * 宽松 TLS 模式<br>
     * 对应配置值 permissive
     */
    PERMISSIVE("permissive"),
    /**
     * 强制 TLS 模式<br>
     * 对应配置值 enforcing
     */
    ENFORCING("enforcing");

    /**
     * 枚举模式名称<br>
     * 用于与配置字符串进行匹配
     */
    private String name;

    /**
     * 构造 TLS 模式枚举<br>
     * 将外部配置字符串与枚举常量建立映射
     */
    TlsMode(String name) {
        this.name = name;
    }

    /**
     * 解析 TLS 模式字符串<br>
     * 当未匹配到任何模式时回退为 PERMISSIVE
     */
    public static TlsMode parse(String mode) {
        for (TlsMode tlsMode : TlsMode.values()) {
            if (tlsMode.name.equals(mode)) {
                return tlsMode;
            }
        }

        return PERMISSIVE;
    }

    public String getName() {
        return name;
    }
}
