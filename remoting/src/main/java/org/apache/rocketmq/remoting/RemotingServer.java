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

import io.netty.channel.Channel;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.concurrent.ExecutorService;

public interface RemotingServer extends RemotingService {

    /**
     * 注册请求处理器
     *
     * @param requestCode 请求码
     * @param processor   处理器
     * @param executor    执行器, 为 null 时使用公共线程池
     */
    void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
        final ExecutorService executor);

    /**
     * 注册默认处理器
     *
     * @param processor 处理器
     * @param executor  执行器
     */
    void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executor);

    /**
     * 获取本地监听端口
     *
     * @return 监听端口
     */
    int localListenPort();

    /**
     * 获取请求处理器
     *
     * @param requestCode 请求码
     */
    Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(final int requestCode);

    /**
     * 获取默认处理器
     */
    Pair<NettyRequestProcessor, ExecutorService> getDefaultProcessorPair();

    /**
     * 创建子 Remoting 服务
     *
     * @param port 监听端口
     * @return 新建的 Remoting 服务
     */
    RemotingServer newRemotingServer(int port);

    /**
     * 移除子 Remoting 服务
     *
     * @param port 监听端口
     */
    void removeRemotingServer(int port);

    /**
     * 同步调用
     *
     * @param channel       Channel
     * @param request       请求命令
     * @param timeoutMillis 超时时间, 单位为毫秒
     * @return 响应命令
     * @throws InterruptedException         等待期间线程中断
     * @throws RemotingSendRequestException 发送异常
     * @throws RemotingTimeoutException     超时异常
     */
    RemotingCommand invokeSync(final Channel channel, final RemotingCommand request,
        final long timeoutMillis) throws InterruptedException, RemotingSendRequestException,
        RemotingTimeoutException;

    /**
     * 异步调用
     *
     * @param channel        Channel
     * @param request        请求命令
     * @param timeoutMillis  超时时间, 单位为毫秒
     * @param invokeCallback 回调
     * @throws InterruptedException            等待期间线程中断
     * @throws RemotingTooMuchRequestException 请求过多
     * @throws RemotingTimeoutException        超时异常
     * @throws RemotingSendRequestException    发送异常
     */
    void invokeAsync(final Channel channel, final RemotingCommand request, final long timeoutMillis,
        final InvokeCallback invokeCallback) throws InterruptedException,
        RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 单向调用
     *
     * @param channel       Channel
     * @param request       请求命令
     * @param timeoutMillis 超时时间, 单位为毫秒
     * @throws InterruptedException            等待期间线程中断
     * @throws RemotingTooMuchRequestException 请求过多
     * @throws RemotingTimeoutException        超时异常
     * @throws RemotingSendRequestException    发送异常
     */
    void invokeOneway(final Channel channel, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException,
        RemotingSendRequestException;

}
