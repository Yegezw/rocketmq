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
package org.apache.rocketmq.store.config;

/**
 * Broker 运行角色枚举<br>
 * 用于定义主从复制语义与请求处理策略
 */
public enum BrokerRole {
    /**
     * 异步主节点<br>
     * 主节点写入成功后不等待从节点确认
     */
    ASYNC_MASTER,
    /**
     * 同步主节点<br>
     * 主节点写入成功前需要等待从节点复制确认
     */
    SYNC_MASTER,
    /**
     * 从节点<br>
     * 通过复制链路追随主节点并提供从读能力
     */
    SLAVE;
}
