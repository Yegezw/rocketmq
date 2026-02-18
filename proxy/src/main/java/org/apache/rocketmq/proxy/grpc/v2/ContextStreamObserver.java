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

package org.apache.rocketmq.proxy.grpc.v2;

import org.apache.rocketmq.proxy.common.ProxyContext;

/**
 * 带 ProxyContext 的流式观察者接口
 *
 * @param <V> 消息类型
 */
public interface ContextStreamObserver<V> {

    /**
     * 处理下一条消息
     *
     * @param ctx Proxy 上下文
     * @param value 消息内容
     */
    void onNext(ProxyContext ctx, V value);

    /**
     * 处理流式异常
     *
     * @param t 异常对象
     */
    void onError(Throwable t);

    /**
     * 处理流式完成事件
     */
    void onCompleted();
}
