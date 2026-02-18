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
 * Proxy 统一异常类型<br>
 * 使用内部错误码表达异常语义
 */
public class ProxyException extends RuntimeException {

    /**
     * Proxy 异常码
     */
    private final ProxyExceptionCode code;

    /**
     * 构造 Proxy 异常
     *
     * @param code 异常码
     * @param message 异常信息
     */
    public ProxyException(ProxyExceptionCode code, String message) {
        super(message);
        this.code = code;
    }

    /**
     * 构造包含根因的 Proxy 异常
     *
     * @param code 异常码
     * @param message 异常信息
     * @param cause 根因异常
     */
    public ProxyException(ProxyExceptionCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public ProxyExceptionCode getCode() {
        return code;
    }
}
