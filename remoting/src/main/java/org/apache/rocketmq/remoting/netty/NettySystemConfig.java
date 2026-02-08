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
 * Netty 系统级配置项
 */
public class NettySystemConfig {
    /**
     * 是否启用 Netty Pooled ByteBuf 分配器系统属性键
     */
    public static final String COM_ROCKETMQ_REMOTING_NETTY_POOLED_BYTE_BUF_ALLOCATOR_ENABLE =
        "com.rocketmq.remoting.nettyPooledByteBufAllocatorEnable";
    /**
     * Socket 发送缓冲区大小系统属性键
     */
    public static final String COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE =
        "com.rocketmq.remoting.socket.sndbuf.size";
    /**
     * Socket 接收缓冲区大小系统属性键
     */
    public static final String COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE =
        "com.rocketmq.remoting.socket.rcvbuf.size";
    /**
     * Socket backlog 系统属性键
     */
    public static final String COM_ROCKETMQ_REMOTING_SOCKET_BACKLOG =
        "com.rocketmq.remoting.socket.backlog";
    /**
     * 客户端异步调用信号量阈值系统属性键
     */
    public static final String COM_ROCKETMQ_REMOTING_CLIENT_ASYNC_SEMAPHORE_VALUE =
        "com.rocketmq.remoting.clientAsyncSemaphoreValue";
    /**
     * 客户端单向调用信号量阈值系统属性键
     */
    public static final String COM_ROCKETMQ_REMOTING_CLIENT_ONEWAY_SEMAPHORE_VALUE =
        "com.rocketmq.remoting.clientOnewaySemaphoreValue";
    /**
     * 客户端工作线程数量系统属性键
     */
    public static final String COM_ROCKETMQ_REMOTING_CLIENT_WORKER_SIZE =
        "com.rocketmq.remoting.client.worker.size";
    /**
     * 客户端连接超时系统属性键
     */
    public static final String COM_ROCKETMQ_REMOTING_CLIENT_CONNECT_TIMEOUT =
        "com.rocketmq.remoting.client.connect.timeout";
    /**
     * 客户端通道最大空闲时间系统属性键
     */
    public static final String COM_ROCKETMQ_REMOTING_CLIENT_CHANNEL_MAX_IDLE_SECONDS =
        "com.rocketmq.remoting.client.channel.maxIdleTimeSeconds";
    /**
     * 客户端超时后是否关闭 Socket 系统属性键
     */
    public static final String COM_ROCKETMQ_REMOTING_CLIENT_CLOSE_SOCKET_IF_TIMEOUT =
        "com.rocketmq.remoting.client.closeSocketIfTimeout";
    /**
     * 写缓冲区高水位线系统属性键
     */
    public static final String COM_ROCKETMQ_REMOTING_WRITE_BUFFER_HIGH_WATER_MARK_VALUE =
        "com.rocketmq.remoting.write.buffer.high.water.mark";
    /**
     * 写缓冲区低水位线系统属性键
     */
    public static final String COM_ROCKETMQ_REMOTING_WRITE_BUFFER_LOW_WATER_MARK =
        "com.rocketmq.remoting.write.buffer.low.water.mark";

    /**
     * 是否启用 Netty Pooled ByteBuf 分配器
     */
    public static final boolean NETTY_POOLED_BYTE_BUF_ALLOCATOR_ENABLE = //
        Boolean.parseBoolean(System.getProperty(COM_ROCKETMQ_REMOTING_NETTY_POOLED_BYTE_BUF_ALLOCATOR_ENABLE, "false"));
    /**
     * 客户端异步调用信号量阈值
     */
    public static final int CLIENT_ASYNC_SEMAPHORE_VALUE = //
        Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_CLIENT_ASYNC_SEMAPHORE_VALUE, "65535"));
    /**
     * 客户端单向调用信号量阈值
     */
    public static final int CLIENT_ONEWAY_SEMAPHORE_VALUE =
        Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_CLIENT_ONEWAY_SEMAPHORE_VALUE, "65535"));
    /**
     * Socket 发送缓冲区大小
     */
    public static int socketSndbufSize =
        Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE, "0"));
    /**
     * Socket 接收缓冲区大小
     */
    public static int socketRcvbufSize =
        Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE, "0"));
    /**
     * Socket backlog 大小
     */
    public static int socketBacklog =
        Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_SOCKET_BACKLOG, "1024"));
    /**
     * 客户端工作线程数量
     */
    public static int clientWorkerSize =
        Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_CLIENT_WORKER_SIZE, "4"));
    /**
     * 连接超时时间, 单位为毫秒
     */
    public static int connectTimeoutMillis =
        Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_CLIENT_CONNECT_TIMEOUT, "3000"));
    /**
     * 客户端通道最大空闲时间, 单位为秒
     */
    public static int clientChannelMaxIdleTimeSeconds =
        Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_CLIENT_CHANNEL_MAX_IDLE_SECONDS, "120"));
    /**
     * 请求超时后是否关闭 Socket
     */
    public static boolean clientCloseSocketIfTimeout =
        Boolean.parseBoolean(System.getProperty(COM_ROCKETMQ_REMOTING_CLIENT_CLOSE_SOCKET_IF_TIMEOUT, "true"));
    /**
     * 写缓冲区高水位线
     */
    public static int writeBufferHighWaterMark =
        Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_WRITE_BUFFER_HIGH_WATER_MARK_VALUE, "0"));
    /**
     * 写缓冲区低水位线
     */
    public static int writeBufferLowWaterMark =
        Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_WRITE_BUFFER_LOW_WATER_MARK, "0"));

}
