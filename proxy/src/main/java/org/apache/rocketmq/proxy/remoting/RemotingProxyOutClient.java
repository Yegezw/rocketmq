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

package org.apache.rocketmq.proxy.remoting;

import io.netty.channel.Channel;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * Remoting 代理外呼客户端接口, 负责向客户端通道发起异步请求
 */
public interface RemotingProxyOutClient {

    /**
     * 向指定客户端通道发起请求并异步返回响应
     *
     * @param channel 客户端通道
     * @param request 请求命令
     * @param timeoutMillis 超时时间, 单位毫秒
     * @return 异步响应命令
     */
    CompletableFuture<RemotingCommand> invokeToClient(Channel channel, RemotingCommand request, long timeoutMillis);
}
