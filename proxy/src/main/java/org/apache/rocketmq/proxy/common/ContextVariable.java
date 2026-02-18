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

package org.apache.rocketmq.proxy.common;

/**
 * Proxy 上下文变量键定义
 */
public class ContextVariable {
    /**
     * 远端地址键
     */
    public static final String REMOTE_ADDRESS = "remote-address";
    /**
     * 本地地址键
     */
    public static final String LOCAL_ADDRESS = "local-address";
    /**
     * 客户端 ID 键
     */
    public static final String CLIENT_ID = "client-id";
    /**
     * 通道对象键
     */
    public static final String CHANNEL = "channel";
    /**
     * 客户端语言键
     */
    public static final String LANGUAGE = "language";
    /**
     * 客户端版本键
     */
    public static final String CLIENT_VERSION = "client-version";
    /**
     * 剩余处理时长键
     */
    public static final String REMAINING_MS = "remaining-ms";
    /**
     * 动作名键
     */
    public static final String ACTION = "action";
    /**
     * 协议类型键
     */
    public static final String PROTOCOL_TYPE = "protocol-type";
    /**
     * 命名空间键
     */
    public static final String NAMESPACE = "namespace";
}
