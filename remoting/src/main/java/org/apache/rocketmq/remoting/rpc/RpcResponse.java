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

import org.apache.rocketmq.remoting.CommandCustomHeader;

/**
 * RPC 响应对象
 */
public class RpcResponse   {
    /**
     * 响应码
     */
    private int code;
    /**
     * 响应头
     */
    private CommandCustomHeader header;
    /**
     * 响应体
     */
    private Object body;
    /**
     * 异常信息
     */
    public RpcException exception;

    /**
     * 构造空响应对象
     */
    public RpcResponse() {

    }

    /**
     * 按响应码 头和体构造响应
     *
     * @param code 响应码
     * @param header 响应头
     * @param body 响应体
     */
    public RpcResponse(int code, CommandCustomHeader header, Object body) {
        this.code = code;
        this.header = header;
        this.body = body;
    }

    /**
     * 按异常构造响应
     *
     * @param rpcException RPC 异常
     */
    public RpcResponse(RpcException rpcException) {
        this.code = rpcException.getErrorCode();
        this.exception = rpcException;
    }

    public int getCode() {
        return code;
    }

    public CommandCustomHeader getHeader() {
        return header;
    }

    public void setHeader(CommandCustomHeader header) {
        this.header = header;
    }

    public Object getBody() {
        return body;
    }

    public void setBody(Object body) {
        this.body = body;
    }

    public RpcException getException() {
        return exception;
    }

    public void setException(RpcException exception) {
        this.exception = exception;
    }

}
