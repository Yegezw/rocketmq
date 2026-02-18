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

import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * remoting 客户端接口<br>
 * 定义名称服务地址管理与请求调用能力
 */
public interface RemotingClient extends RemotingService {

    /**
     * 更新 NameServer 地址列表
     *
     * @param addrs NameServer 地址列表
     */
    void updateNameServerAddressList(final List<String> addrs);

    List<String> getNameServerAddressList();

    List<String> getAvailableNameSrvList();

    /**
     * 同步调用远端请求
     *
     * @param addr 目标地址
     * @param request 请求命令
     * @param timeoutMillis 超时时间 毫秒
     * @return 响应命令
     * @throws InterruptedException 线程中断异常
     * @throws RemotingConnectException 连接异常
     * @throws RemotingSendRequestException 请求发送异常
     * @throws RemotingTimeoutException 请求超时异常
     */
    RemotingCommand invokeSync(final String addr, final RemotingCommand request,
        final long timeoutMillis) throws InterruptedException, RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException;

    /**
     * 异步调用远端请求
     *
     * @param addr 目标地址
     * @param request 请求命令
     * @param timeoutMillis 超时时间 毫秒
     * @param invokeCallback 回调对象
     * @throws InterruptedException 线程中断异常
     * @throws RemotingConnectException 连接异常
     * @throws RemotingTooMuchRequestException 请求过载异常
     * @throws RemotingTimeoutException 请求超时异常
     * @throws RemotingSendRequestException 请求发送异常
     */
    void invokeAsync(final String addr, final RemotingCommand request, final long timeoutMillis,
        final InvokeCallback invokeCallback) throws InterruptedException, RemotingConnectException,
        RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 单向调用远端请求
     *
     * @param addr 目标地址
     * @param request 请求命令
     * @param timeoutMillis 超时时间 毫秒
     * @throws InterruptedException 线程中断异常
     * @throws RemotingConnectException 连接异常
     * @throws RemotingTooMuchRequestException 请求过载异常
     * @throws RemotingTimeoutException 请求超时异常
     * @throws RemotingSendRequestException 请求发送异常
     */
    void invokeOneway(final String addr, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException,
        RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 以 CompletableFuture 形式发起异步调用
     *
     * @param addr 目标地址
     * @param request 请求命令
     * @param timeoutMillis 超时时间 毫秒
     * @return 异步响应 Future
     */
    default CompletableFuture<RemotingCommand> invoke(final String addr, final RemotingCommand request,
        final long timeoutMillis) {
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        try {
            invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {

                @Override
                public void operationComplete(ResponseFuture responseFuture) {

                }

                @Override
                public void operationSucceed(RemotingCommand response) {
                    future.complete(response);
                }

                @Override
                public void operationFail(Throwable throwable) {
                    future.completeExceptionally(throwable);
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    /**
     * 注册请求处理器
     *
     * @param requestCode 请求码
     * @param processor 请求处理器
     * @param executor 处理线程池
     */
    void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
        final ExecutorService executor);

    void setCallbackExecutor(final ExecutorService callbackExecutor);

    boolean isChannelWritable(final String addr);

    boolean isAddressReachable(final String addr);

    /**
     * 批量关闭连接通道
     *
     * @param addrList 目标地址列表
     */
    void closeChannels(final List<String> addrList);
}
