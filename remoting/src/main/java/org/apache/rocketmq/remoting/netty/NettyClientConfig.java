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

import org.apache.rocketmq.remoting.common.TlsMode;

import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_ENABLE;

/**
 * Netty 客户端配置
 */
public class NettyClientConfig {
    /**
     * Worker thread number
     * <br>
     * Worker 线程数量
     */
    private int clientWorkerThreads = NettySystemConfig.clientWorkerSize;
    /**
     * 回调执行线程数量, 默认值为当前机器可用 CPU 核数
     */
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();
    /**
     * 单向调用并发信号量阈值, 用于限制 oneway 请求并发数量
     */
    private int clientOnewaySemaphoreValue = NettySystemConfig.CLIENT_ONEWAY_SEMAPHORE_VALUE;
    /**
     * 异步调用并发信号量阈值, 用于限制 async 请求并发数量
     */
    private int clientAsyncSemaphoreValue = NettySystemConfig.CLIENT_ASYNC_SEMAPHORE_VALUE;
    /**
     * 建立连接超时时间, 单位为毫秒
     */
    private int connectTimeoutMillis = NettySystemConfig.connectTimeoutMillis;
    /**
     * 通道非活跃检查间隔, 单位为毫秒
     */
    private long channelNotActiveInterval = 1000 * 60;

    /**
     * 是否扫描可用 NameServer, true 表示启用扫描
     */
    private boolean isScanAvailableNameSrv = true;

    /**
     * IdleStateEvent will be triggered when neither read nor write was performed for
     * the specified period of this time. Specify {@code 0} to disable
     * <br>
     * 当在指定时间段内既没有执行读操作也没有执行写操作时, 会触发 IdleStateEvent<br>
     * 指定 {@code 0} 可禁用该行为
     */
    private int clientChannelMaxIdleTimeSeconds = NettySystemConfig.clientChannelMaxIdleTimeSeconds;

    /**
     * 客户端 Socket 发送缓冲区大小
     */
    private int clientSocketSndBufSize = NettySystemConfig.socketSndbufSize;
    /**
     * 客户端 Socket 接收缓冲区大小
     */
    private int clientSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;
    /**
     * 是否启用客户端 Pooled ByteBuf 分配器
     */
    private boolean clientPooledByteBufAllocatorEnable = false;
    /**
     * 请求超时后是否主动关闭 Socket
     */
    private boolean clientCloseSocketIfTimeout = NettySystemConfig.clientCloseSocketIfTimeout;

    /**
     * 是否启用 TLS, 默认值由系统属性与 TLS 模式共同决定
     */
    private boolean useTLS = Boolean.parseBoolean(System.getProperty(TLS_ENABLE,
        String.valueOf(TlsSystemConfig.tlsMode == TlsMode.ENFORCING)));

    /**
     * Socks 代理配置 JSON 字符串
     */
    private String socksProxyConfig = "{}";

    /**
     * 写缓冲区高水位线
     */
    private int writeBufferHighWaterMark = NettySystemConfig.writeBufferHighWaterMark;
    /**
     * 写缓冲区低水位线
     */
    private int writeBufferLowWaterMark = NettySystemConfig.writeBufferLowWaterMark;

    /**
     * 是否禁用回调执行器
     */
    private boolean disableCallbackExecutor = false;
    /**
     * 是否禁用 Netty Worker Group
     */
    private boolean disableNettyWorkerGroup = false;

    /**
     * 最大重连间隔时间, 单位为秒
     */
    private long maxReconnectIntervalTimeSeconds = 60;

    /**
     * 收到 GoAway 后是否启用重连
     */
    private boolean enableReconnectForGoAway = true;

    public boolean isClientCloseSocketIfTimeout() {
        return clientCloseSocketIfTimeout;
    }

    public void setClientCloseSocketIfTimeout(final boolean clientCloseSocketIfTimeout) {
        this.clientCloseSocketIfTimeout = clientCloseSocketIfTimeout;
    }

    public int getClientWorkerThreads() {
        return clientWorkerThreads;
    }

    public void setClientWorkerThreads(int clientWorkerThreads) {
        this.clientWorkerThreads = clientWorkerThreads;
    }

    public int getClientOnewaySemaphoreValue() {
        return clientOnewaySemaphoreValue;
    }

    public void setClientOnewaySemaphoreValue(int clientOnewaySemaphoreValue) {
        this.clientOnewaySemaphoreValue = clientOnewaySemaphoreValue;
    }

    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public void setConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    public int getClientCallbackExecutorThreads() {
        return clientCallbackExecutorThreads;
    }

    public void setClientCallbackExecutorThreads(int clientCallbackExecutorThreads) {
        this.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
    }

    public long getChannelNotActiveInterval() {
        return channelNotActiveInterval;
    }

    public void setChannelNotActiveInterval(long channelNotActiveInterval) {
        this.channelNotActiveInterval = channelNotActiveInterval;
    }

    public int getClientAsyncSemaphoreValue() {
        return clientAsyncSemaphoreValue;
    }

    public void setClientAsyncSemaphoreValue(int clientAsyncSemaphoreValue) {
        this.clientAsyncSemaphoreValue = clientAsyncSemaphoreValue;
    }

    public int getClientChannelMaxIdleTimeSeconds() {
        return clientChannelMaxIdleTimeSeconds;
    }

    public void setClientChannelMaxIdleTimeSeconds(int clientChannelMaxIdleTimeSeconds) {
        this.clientChannelMaxIdleTimeSeconds = clientChannelMaxIdleTimeSeconds;
    }

    public int getClientSocketSndBufSize() {
        return clientSocketSndBufSize;
    }

    public void setClientSocketSndBufSize(int clientSocketSndBufSize) {
        this.clientSocketSndBufSize = clientSocketSndBufSize;
    }

    public int getClientSocketRcvBufSize() {
        return clientSocketRcvBufSize;
    }

    public void setClientSocketRcvBufSize(int clientSocketRcvBufSize) {
        this.clientSocketRcvBufSize = clientSocketRcvBufSize;
    }

    public boolean isClientPooledByteBufAllocatorEnable() {
        return clientPooledByteBufAllocatorEnable;
    }

    public void setClientPooledByteBufAllocatorEnable(boolean clientPooledByteBufAllocatorEnable) {
        this.clientPooledByteBufAllocatorEnable = clientPooledByteBufAllocatorEnable;
    }

    public boolean isUseTLS() {
        return useTLS;
    }

    public void setUseTLS(boolean useTLS) {
        this.useTLS = useTLS;
    }

    public int getWriteBufferLowWaterMark() {
        return writeBufferLowWaterMark;
    }

    public void setWriteBufferLowWaterMark(int writeBufferLowWaterMark) {
        this.writeBufferLowWaterMark = writeBufferLowWaterMark;
    }

    public int getWriteBufferHighWaterMark() {
        return writeBufferHighWaterMark;
    }

    public void setWriteBufferHighWaterMark(int writeBufferHighWaterMark) {
        this.writeBufferHighWaterMark = writeBufferHighWaterMark;
    }

    public boolean isDisableCallbackExecutor() {
        return disableCallbackExecutor;
    }

    public void setDisableCallbackExecutor(boolean disableCallbackExecutor) {
        this.disableCallbackExecutor = disableCallbackExecutor;
    }

    public boolean isDisableNettyWorkerGroup() {
        return disableNettyWorkerGroup;
    }

    public void setDisableNettyWorkerGroup(boolean disableNettyWorkerGroup) {
        this.disableNettyWorkerGroup = disableNettyWorkerGroup;
    }

    public long getMaxReconnectIntervalTimeSeconds() {
        return maxReconnectIntervalTimeSeconds;
    }

    public void setMaxReconnectIntervalTimeSeconds(long maxReconnectIntervalTimeSeconds) {
        this.maxReconnectIntervalTimeSeconds = maxReconnectIntervalTimeSeconds;
    }

    public boolean isEnableReconnectForGoAway() {
        return enableReconnectForGoAway;
    }

    public void setEnableReconnectForGoAway(boolean enableReconnectForGoAway) {
        this.enableReconnectForGoAway = enableReconnectForGoAway;
    }

    public String getSocksProxyConfig() {
        return socksProxyConfig;
    }

    public void setSocksProxyConfig(String socksProxyConfig) {
        this.socksProxyConfig = socksProxyConfig;
    }

    public boolean isScanAvailableNameSrv() {
        return isScanAvailableNameSrv;
    }

    public void setScanAvailableNameSrv(boolean scanAvailableNameSrv) {
        this.isScanAvailableNameSrv = scanAvailableNameSrv;
    }
}
