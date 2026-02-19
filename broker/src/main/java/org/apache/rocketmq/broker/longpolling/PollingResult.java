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

package org.apache.rocketmq.broker.longpolling;

/**
 * 长轮询入队结果枚举, 用于指示请求当前处理状态
 */
public enum PollingResult {
    /**
     * 请求成功进入长轮询等待队列
     */
    POLLING_SUC,
    /**
     * 轮询队列达到容量上限
     */
    POLLING_FULL,
    /**
     * 请求在入队前已经接近超时
     */
    POLLING_TIMEOUT,
    /**
     * 当前请求不满足长轮询条件
     */
    NOT_POLLING;
}
