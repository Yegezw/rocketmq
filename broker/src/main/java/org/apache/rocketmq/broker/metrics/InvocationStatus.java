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

package org.apache.rocketmq.broker.metrics;

/**
 * 指标调用状态枚举
 */
public enum InvocationStatus {
    /**
     * 调用成功状态
     */
    SUCCESS("success"),
    /**
     * 调用失败状态
     */
    FAILURE("failure");

    /**
     * 状态名称
     */
    private final String name;

    /**
     * 初始化调用状态
     *
     * @param name 状态名称
     */
    InvocationStatus(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
