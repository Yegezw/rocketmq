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
 * remoting 命令异常
 */
public class RemotingCommandException extends RemotingException {
    /**
     * 序列化版本号
     */
    private static final long serialVersionUID = -6061365915274953096L;

    /**
     * 按异常消息构造异常
     *
     * @param message 异常消息
     */
    public RemotingCommandException(String message) {
        super(message, null);
    }

    /**
     * 按异常消息和根因构造异常
     *
     * @param message 异常消息
     * @param cause 根因异常
     */
    public RemotingCommandException(String message, Throwable cause) {
        super(message, cause);
    }
}
