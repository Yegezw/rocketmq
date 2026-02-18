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
package org.apache.rocketmq.proxy.grpc.v2.common;

import apache.rocketmq.v2.Code;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * gRPC 层异常类型<br>
 * 用于将 Proxy 内部异常映射为 gRPC 状态码
 */
public class GrpcProxyException extends RuntimeException {

    /**
     * Proxy 内部异常对象
     */
    private ProxyException proxyException;
    /**
     * gRPC 状态码
     */
    private Code code;

    /**
     * Proxy 异常码到 gRPC 状态码的映射表
     */
    protected static final Map<ProxyExceptionCode, Code> CODE_MAPPING = new ConcurrentHashMap<>();

    static {
        CODE_MAPPING.put(ProxyExceptionCode.INVALID_BROKER_NAME, Code.BAD_REQUEST);
        CODE_MAPPING.put(ProxyExceptionCode.INVALID_RECEIPT_HANDLE, Code.INVALID_RECEIPT_HANDLE);
        CODE_MAPPING.put(ProxyExceptionCode.FORBIDDEN, Code.FORBIDDEN);
        CODE_MAPPING.put(ProxyExceptionCode.INTERNAL_SERVER_ERROR, Code.INTERNAL_SERVER_ERROR);
        CODE_MAPPING.put(ProxyExceptionCode.MESSAGE_PROPERTY_CONFLICT_WITH_TYPE, Code.MESSAGE_PROPERTY_CONFLICT_WITH_TYPE);
    }

    /**
     * 按 gRPC 状态码构造异常
     *
     * @param code gRPC 状态码
     * @param message 异常消息
     */
    public GrpcProxyException(Code code, String message) {
        super(message);
        this.code = code;
    }

    /**
     * 按 gRPC 状态码和根因构造异常
     *
     * @param code gRPC 状态码
     * @param message 异常消息
     * @param t 根因异常
     */
    public GrpcProxyException(Code code, String message, Throwable t) {
        super(message, t);
        this.code = code;
    }

    /**
     * 按 Proxy 内部异常构造 gRPC 异常
     *
     * @param proxyException Proxy 内部异常
     */
    public GrpcProxyException(ProxyException proxyException) {
        super(proxyException);
        this.proxyException = proxyException;
    }

    public Code getCode() {
        if (this.code != null) {
            return this.code;
        }
        if (this.proxyException != null) {
            return CODE_MAPPING.getOrDefault(this.proxyException.getCode(), Code.INTERNAL_SERVER_ERROR);
        }
        return Code.INTERNAL_SERVER_ERROR;
    }

    public ProxyException getProxyException() {
        return proxyException;
    }
}
