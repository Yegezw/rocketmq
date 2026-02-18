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
package org.apache.rocketmq.remoting;

import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 异步调用回调接口<br>
 * 用于处理 remoting 异步请求的完成 成功 与 失败事件
 */
public interface InvokeCallback {
    /**
     * This method is expected to be invoked after {@link #operationSucceed(RemotingCommand)}
     * or {@link #operationFail(Throwable)}
     * <br>
     * 该方法应在成功回调或失败回调之后执行
     *
     * @param responseFuture the returned object contains response or exception<br>返回对象, 包含响应或异常
     */
    void operationComplete(final ResponseFuture responseFuture);

    /**
     * 异步调用成功回调
     *
     * @param response 响应命令
     */
    default void operationSucceed(final RemotingCommand response) {

    }

    /**
     * 异步调用失败回调
     *
     * @param throwable 异常对象
     */
    default void operationFail(final Throwable throwable) {

    }
}
