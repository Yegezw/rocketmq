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
package org.apache.rocketmq.remoting.netty;

import io.netty.channel.Channel;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 响应 Future, 用于关联请求发送状态与响应结果
 */
public class ResponseFuture {
    /**
     * 目标 Channel
     */
    private final Channel channel;
    /**
     * 请求唯一标识
     */
    private final int opaque;
    /**
     * 原始请求命令
     */
    private final RemotingCommand request;
    /**
     * 超时时间, 单位为毫秒
     */
    private final long timeoutMillis;
    /**
     * 调用回调
     */
    private final InvokeCallback invokeCallback;
    /**
     * 创建时间戳, 单位为毫秒
     */
    private final long beginTimestamp = System.currentTimeMillis();
    /**
     * 同步等待响应的倒计时器
     */
    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    /**
     * 信号量一次性释放器
     */
    private final SemaphoreReleaseOnlyOnce once;

    /**
     * 回调仅执行一次的保护标记
     */
    private final AtomicBoolean executeCallbackOnlyOnce = new AtomicBoolean(false);
    /**
     * 响应命令
     */
    private volatile RemotingCommand responseCommand;
    /**
     * 请求发送是否成功
     */
    private volatile boolean sendRequestOK = true;
    /**
     * 失败原因
     */
    private volatile Throwable cause;
    /**
     * 是否已中断
     */
    private volatile boolean interrupted = false;

    /**
     * 构造响应 Future
     *
     * @param channel        目标 Channel
     * @param opaque         请求唯一标识
     * @param timeoutMillis  超时时间, 单位为毫秒
     * @param invokeCallback 调用回调
     * @param once           信号量一次性释放器
     */
    public ResponseFuture(Channel channel, int opaque, long timeoutMillis, InvokeCallback invokeCallback,
                          SemaphoreReleaseOnlyOnce once) {
        this(channel, opaque, null, timeoutMillis, invokeCallback, once);
    }

    /**
     * 构造响应 Future
     *
     * @param channel        目标 Channel
     * @param opaque         请求唯一标识
     * @param request        原始请求命令
     * @param timeoutMillis  超时时间, 单位为毫秒
     * @param invokeCallback 调用回调
     * @param once           信号量一次性释放器
     */
    public ResponseFuture(Channel channel, int opaque, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback,
                          SemaphoreReleaseOnlyOnce once) {
        this.channel = channel;
        this.opaque = opaque;
        this.request = request;
        this.timeoutMillis = timeoutMillis;
        this.invokeCallback = invokeCallback;
        this.once = once;
    }

    /**
     * 执行回调逻辑
     */
    public void executeInvokeCallback() {
        // 仅在回调存在且未执行过时继续
        if (invokeCallback != null) {
            if (this.executeCallbackOnlyOnce.compareAndSet(false, true)) {
                RemotingCommand response = getResponseCommand();
                // 响应存在时走成功回调
                if (response != null) {
                    invokeCallback.operationSucceed(response);
                } else {
                    // 无响应时按失败类型构造异常
                    if (!isSendRequestOK()) {
                        invokeCallback.operationFail(new RemotingSendRequestException(channel.remoteAddress().toString(), getCause()));
                    } else if (isTimeout()) {
                        invokeCallback.operationFail(new RemotingTimeoutException(channel.remoteAddress().toString(), getTimeoutMillis(), getCause()));
                    } else {
                        invokeCallback.operationFail(new RemotingException(getRequestCommand().toString(), getCause()));
                    }
                }
                // 统一触发完成回调
                invokeCallback.operationComplete(this);
            }
        }
    }

    /**
     * 中断当前 Future 并触发回调
     */
    public void interrupt() {
        interrupted = true;
        executeInvokeCallback();
    }

    /**
     * 释放关联信号量
     */
    public void release() {
        if (this.once != null) {
            this.once.release();
        }
    }

    /**
     * 判断当前请求是否超时
     *
     * @return 超时时返回 true, 否则返回 false
     */
    public boolean isTimeout() {
        long diff = System.currentTimeMillis() - this.beginTimestamp;
        return diff > this.timeoutMillis;
    }

    /**
     * 等待响应命令
     *
     * @param timeoutMillis 等待超时时间, 单位为毫秒
     * @return 响应命令
     * @throws InterruptedException 等待期间线程中断
     */
    public RemotingCommand waitResponse(final long timeoutMillis) throws InterruptedException {
        this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        return this.responseCommand;
    }

    /**
     * 设置响应命令并唤醒等待线程
     *
     * @param responseCommand 响应命令
     */
    public void putResponse(final RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
        this.countDownLatch.countDown();
    }

    public long getBeginTimestamp() {
        return beginTimestamp;
    }

    public boolean isSendRequestOK() {
        return sendRequestOK;
    }

    public void setSendRequestOK(boolean sendRequestOK) {
        this.sendRequestOK = sendRequestOK;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public InvokeCallback getInvokeCallback() {
        return invokeCallback;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }

    public RemotingCommand getResponseCommand() {
        return responseCommand;
    }

    public void setResponseCommand(RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
    }

    public int getOpaque() {
        return opaque;
    }

    public RemotingCommand getRequestCommand() {
        return request;
    }

    public Channel getChannel() {
        return channel;
    }

    public boolean isInterrupted() {
        return interrupted;
    }

    /**
     * 返回调试字符串
     *
     * @return 当前 Future 状态字符串
     */
    @Override
    public String toString() {
        return "ResponseFuture [responseCommand=" + responseCommand + ", sendRequestOK=" + sendRequestOK
            + ", cause=" + cause + ", opaque=" + opaque + ", timeoutMillis=" + timeoutMillis
            + ", invokeCallback=" + invokeCallback + ", beginTimestamp=" + beginTimestamp
            + ", countDownLatch=" + countDownLatch + "]";
    }
}
