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
package org.apache.rocketmq.proxy.metrics;

/**
 * Proxy 指标常量定义
 */
public class ProxyMetricsConstant {
    /**
     * Proxy 存活状态指标名
     */
    public static final String GAUGE_PROXY_UP = "rocketmq_proxy_up";

    /**
     * Proxy 模式标签名
     */
    public static final String LABEL_PROXY_MODE = "proxy_mode";
    /**
     * Proxy 节点类型标签值
     */
    public static final String NODE_TYPE_PROXY = "proxy";
}
