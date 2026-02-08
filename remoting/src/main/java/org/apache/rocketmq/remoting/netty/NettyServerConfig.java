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

/**
 * Netty 服务端配置
 */
public class NettyServerConfig implements Cloneable {

    /**
     * Bind address may be hostname, IPv4 or IPv6.
     * By default, it's wildcard address, listening all network interfaces.
     * <br>
     * 绑定地址可为 hostname, IPv4 或 IPv6, 默认使用通配地址, 监听所有网络接口
     */
    private String bindAddress = "0.0.0.0";
    /**
     * 服务端监听端口
     */
    private int listenPort = 0;
    /**
     * 服务端业务工作线程数量
     */
    private int serverWorkerThreads = 8;
    /**
     * 服务端回调执行线程数量
     */
    private int serverCallbackExecutorThreads = 0;
    /**
     * 服务端 Selector 线程数量
     */
    private int serverSelectorThreads = 3;
    /**
     * 服务端单向调用并发信号量阈值
     */
    private int serverOnewaySemaphoreValue = 256;
    /**
     * 服务端异步调用并发信号量阈值
     */
    private int serverAsyncSemaphoreValue = 64;
    /**
     * 服务端通道最大空闲时间, 单位为秒
     */
    private int serverChannelMaxIdleTimeSeconds = 120;

    /**
     * 服务端 Socket 发送缓冲区大小
     */
    private int serverSocketSndBufSize = NettySystemConfig.socketSndbufSize;
    /**
     * 服务端 Socket 接收缓冲区大小
     */
    private int serverSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;
    /**
     * 写缓冲区高水位线
     */
    private int writeBufferHighWaterMark = NettySystemConfig.writeBufferHighWaterMark;
    /**
     * 写缓冲区低水位线
     */
    private int writeBufferLowWaterMark = NettySystemConfig.writeBufferLowWaterMark;
    /**
     * 服务端 Socket backlog 大小
     */
    private int serverSocketBacklog = NettySystemConfig.socketBacklog;
    /**
     * 是否启用服务端 Netty Worker Group
     */
    private boolean serverNettyWorkerGroupEnable = true;
    /**
     * 是否启用服务端 Pooled ByteBuf 分配器
     */
    private boolean serverPooledByteBufAllocatorEnable = true;

    /**
     * 是否启用优雅关闭
     */
    private boolean enableShutdownGracefully = false;
    /**
     * 优雅关闭等待时间, 单位为秒
     */
    private int shutdownWaitTimeSeconds = 30;

    /**
     * make install
     * ../glibc-2.10.1/configure \ --prefix=/usr \ --with-headers=/usr/include \
     * --host=x86_64-linux-gnu \ --build=x86_64-pc-linux-gnu \ --without-gd
     * <br>
     * 该段内容为 glibc 编译安装示例命令
     */
    private boolean useEpollNativeSelector = false;

    public String getBindAddress() {
        return bindAddress;
    }

    public void setBindAddress(String bindAddress) {
        this.bindAddress = bindAddress;
    }

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public int getServerWorkerThreads() {
        return serverWorkerThreads;
    }

    public void setServerWorkerThreads(int serverWorkerThreads) {
        this.serverWorkerThreads = serverWorkerThreads;
    }

    public int getServerSelectorThreads() {
        return serverSelectorThreads;
    }

    public void setServerSelectorThreads(int serverSelectorThreads) {
        this.serverSelectorThreads = serverSelectorThreads;
    }

    public int getServerOnewaySemaphoreValue() {
        return serverOnewaySemaphoreValue;
    }

    public void setServerOnewaySemaphoreValue(int serverOnewaySemaphoreValue) {
        this.serverOnewaySemaphoreValue = serverOnewaySemaphoreValue;
    }

    public int getServerCallbackExecutorThreads() {
        return serverCallbackExecutorThreads;
    }

    public void setServerCallbackExecutorThreads(int serverCallbackExecutorThreads) {
        this.serverCallbackExecutorThreads = serverCallbackExecutorThreads;
    }

    public int getServerAsyncSemaphoreValue() {
        return serverAsyncSemaphoreValue;
    }

    public void setServerAsyncSemaphoreValue(int serverAsyncSemaphoreValue) {
        this.serverAsyncSemaphoreValue = serverAsyncSemaphoreValue;
    }

    public int getServerChannelMaxIdleTimeSeconds() {
        return serverChannelMaxIdleTimeSeconds;
    }

    public void setServerChannelMaxIdleTimeSeconds(int serverChannelMaxIdleTimeSeconds) {
        this.serverChannelMaxIdleTimeSeconds = serverChannelMaxIdleTimeSeconds;
    }

    public int getServerSocketSndBufSize() {
        return serverSocketSndBufSize;
    }

    public void setServerSocketSndBufSize(int serverSocketSndBufSize) {
        this.serverSocketSndBufSize = serverSocketSndBufSize;
    }

    public int getServerSocketRcvBufSize() {
        return serverSocketRcvBufSize;
    }

    public void setServerSocketRcvBufSize(int serverSocketRcvBufSize) {
        this.serverSocketRcvBufSize = serverSocketRcvBufSize;
    }

    public int getServerSocketBacklog() {
        return serverSocketBacklog;
    }

    public void setServerSocketBacklog(int serverSocketBacklog) {
        this.serverSocketBacklog = serverSocketBacklog;
    }

    public boolean isServerPooledByteBufAllocatorEnable() {
        return serverPooledByteBufAllocatorEnable;
    }

    public void setServerPooledByteBufAllocatorEnable(boolean serverPooledByteBufAllocatorEnable) {
        this.serverPooledByteBufAllocatorEnable = serverPooledByteBufAllocatorEnable;
    }

    public boolean isUseEpollNativeSelector() {
        return useEpollNativeSelector;
    }

    public void setUseEpollNativeSelector(boolean useEpollNativeSelector) {
        this.useEpollNativeSelector = useEpollNativeSelector;
    }

    /**
     * 克隆当前配置对象
     *
     * @return 配置对象副本
     * @throws CloneNotSupportedException 不支持克隆时抛出
     */
    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
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

    public boolean isServerNettyWorkerGroupEnable() {
        return serverNettyWorkerGroupEnable;
    }

    public void setServerNettyWorkerGroupEnable(boolean serverNettyWorkerGroupEnable) {
        this.serverNettyWorkerGroupEnable = serverNettyWorkerGroupEnable;
    }

    public boolean isEnableShutdownGracefully() {
        return enableShutdownGracefully;
    }

    public void setEnableShutdownGracefully(boolean enableShutdownGracefully) {
        this.enableShutdownGracefully = enableShutdownGracefully;
    }

    public int getShutdownWaitTimeSeconds() {
        return shutdownWaitTimeSeconds;
    }

    public void setShutdownWaitTimeSeconds(int shutdownWaitTimeSeconds) {
        this.shutdownWaitTimeSeconds = shutdownWaitTimeSeconds;
    }
}
