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
package org.apache.rocketmq.remoting.rpc;

import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * RPC 异常类型
 */
public class RpcException extends RemotingException {
    /**
     * RPC 错误码
     */
    private int errorCode;

    /**
     * 按错误码和消息构造 RPC 异常
     *
     * @param errorCode 错误码
     * @param message 异常消息
     */
    public RpcException(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    /**
     * 按错误码 消息和根因构造 RPC 异常
     *
     * @param errorCode 错误码
     * @param message 异常消息
     * @param cause 根因异常
     */
    public RpcException(int errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }
}
