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

/**
 * HAProxy Protocol 相关常量定义
 */
public class HAProxyConstants {

    /**
     * 通道 ID 键名
     */
    public static final String CHANNEL_ID = "channel_id";
    /**
     * Proxy Protocol 前缀
     */
    public static final String PROXY_PROTOCOL_PREFIX = "proxy_protocol_";
    /**
     * 源地址键名
     */
    public static final String PROXY_PROTOCOL_ADDR = PROXY_PROTOCOL_PREFIX + "addr";
    /**
     * 源端口键名
     */
    public static final String PROXY_PROTOCOL_PORT = PROXY_PROTOCOL_PREFIX + "port";
    /**
     * 目标地址键名
     */
    public static final String PROXY_PROTOCOL_SERVER_ADDR = PROXY_PROTOCOL_PREFIX + "server_addr";
    /**
     * 目标端口键名
     */
    public static final String PROXY_PROTOCOL_SERVER_PORT = PROXY_PROTOCOL_PREFIX + "server_port";
    /**
     * TLV 扩展字段键名前缀
     */
    public static final String PROXY_PROTOCOL_TLV_PREFIX = PROXY_PROTOCOL_PREFIX + "tlv_0x";
}
