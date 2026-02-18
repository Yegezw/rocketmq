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
package org.apache.rocketmq.remoting.exception;

/**
 * remoting 请求超时异常
 */
public class RemotingTimeoutException extends RemotingException {

    /**
     * 序列化版本号
     */
    private static final long serialVersionUID = 4106899185095245979L;

    /**
     * 按异常消息构造超时异常
     *
     * @param message 异常消息
     */
    public RemotingTimeoutException(String message) {
        super(message);
    }

    /**
     * 按地址和超时时间构造超时异常
     *
     * @param addr 目标地址
     * @param timeoutMillis 超时时间 毫秒
     */
    public RemotingTimeoutException(String addr, long timeoutMillis) {
        this(addr, timeoutMillis, null);
    }

    /**
     * 按地址 超时时间和根因构造超时异常
     *
     * @param addr 目标地址
     * @param timeoutMillis 超时时间 毫秒
     * @param cause 根因异常
     */
    public RemotingTimeoutException(String addr, long timeoutMillis, Throwable cause) {
        super("wait response on the channel <" + addr + "> timeout, " + timeoutMillis + "(ms)", cause);
    }
}
