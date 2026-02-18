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
 * Proxy 异常码枚举
 */
public enum ProxyExceptionCode {
    /**
     * Broker 名称非法或不存在
     */
    INVALID_BROKER_NAME,
    /**
     * 未找到事务关联数据
     */
    TRANSACTION_DATA_NOT_FOUND,
    /**
     * 请求被访问控制策略拒绝
     */
    FORBIDDEN,
    /**
     * 消息属性与消息类型冲突
     */
    MESSAGE_PROPERTY_CONFLICT_WITH_TYPE,
    /**
     * ReceiptHandle 非法或已过期
     */
    INVALID_RECEIPT_HANDLE,
    /**
     * Proxy 内部处理异常
     */
    INTERNAL_SERVER_ERROR,
}
