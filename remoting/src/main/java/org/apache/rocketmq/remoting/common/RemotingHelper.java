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
package org.apache.rocketmq.remoting.common;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.AttributeKeys;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Remoting 辅助工具类, 提供地址解析、同步调用和网络连接能力
 */
public class RemotingHelper {
    /**
     * 默认字符集
     */
    public static final String DEFAULT_CHARSET = "UTF-8";
    /**
     * 默认全网段 CIDR
     */
    public static final String DEFAULT_CIDR_ALL = "0.0.0.0/0";

    /**
     * Remoting 日志对象
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);

    /**
     * 请求码描述映射表
     */
    public static final Map<Integer, String> REQUEST_CODE_MAP = new HashMap<Integer, String>() {
        {
            try {
                Field[] f = RequestCode.class.getFields();
                for (Field field : f) {
                    if (field.getType() == int.class) {
                        put((int) field.get(null), field.getName().toLowerCase());
                    }
                }
            } catch (IllegalAccessException ignore) {
            }
        }
    };

    /**
     * 响应码描述映射表
     */
    public static final Map<Integer, String> RESPONSE_CODE_MAP = new HashMap<Integer, String>() {
        {
            try {
                Field[] f = ResponseCode.class.getFields();
                for (Field field : f) {
                    if (field.getType() == int.class) {
                        put((int) field.get(null), field.getName().toLowerCase());
                    }
                }
            } catch (IllegalAccessException ignore) {
            }
        }
    };

    /**
     * 获取 Channel 属性值
     *
     * @param key     属性键
     * @param channel Netty 通道
     * @param <T>     属性值类型
     * @return 属性值, 未命中时返回 null
     */
    public static <T> T getAttributeValue(AttributeKey<T> key, final Channel channel) {
        if (channel.hasAttr(key)) {
            Attribute<T> attribute = channel.attr(key);
            return attribute.get();
        }
        return null;
    }

    /**
     * 设置 Channel 属性值
     *
     * @param channel Netty 通道
     * @param attributeKey 属性键
     * @param value 属性值
     * @param <T> 属性值类型
     */
    public static <T> void setPropertyToAttr(final Channel channel, AttributeKey<T> attributeKey, T value) {
        if (channel == null) {
            return;
        }
        channel.attr(attributeKey).set(value);
    }

    /**
     * 将地址字符串转换为 SocketAddress
     *
     * @param addr 地址字符串, 格式为 host:port
     * @return SocketAddress 对象
     */
    public static SocketAddress string2SocketAddress(final String addr) {
        int split = addr.lastIndexOf(":");
        String host = addr.substring(0, split);
        String port = addr.substring(split + 1);
        InetSocketAddress isa = new InetSocketAddress(host, Integer.parseInt(port));
        return isa;
    }

    /**
     * 同步调用远程地址
     *
     * @param addr          远程地址
     * @param request       请求命令
     * @param timeoutMillis 超时时间, 单位为毫秒
     * @return 远程响应命令
     * @throws InterruptedException         中断异常
     * @throws RemotingConnectException     连接异常
     * @throws RemotingSendRequestException 请求发送异常
     * @throws RemotingTimeoutException     调用超时异常
     * @throws RemotingCommandException     命令编解码异常
     */
    public static RemotingCommand invokeSync(final String addr, final RemotingCommand request,
        final long timeoutMillis) throws InterruptedException, RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException {
        // 记录调用开始时间并建立网络连接
        long beginTime = System.currentTimeMillis();
        SocketAddress socketAddress = NetworkUtil.string2SocketAddress(addr);
        SocketChannel socketChannel = connect(socketAddress);
        if (socketChannel != null) {
            boolean sendRequestOK = false;

            try {

                socketChannel.configureBlocking(true);

                //bugfix  http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4614802
                // 修复历史 JDK Bug, 详见上述链接
                socketChannel.socket().setSoTimeout((int) timeoutMillis);

                // 编码并发送请求体
                ByteBuffer byteBufferRequest = request.encode();
                while (byteBufferRequest.hasRemaining()) {
                    int length = socketChannel.write(byteBufferRequest);
                    if (length > 0) {
                        if (byteBufferRequest.hasRemaining()) {
                            if ((System.currentTimeMillis() - beginTime) > timeoutMillis) {

                                throw new RemotingSendRequestException(addr);
                            }
                        }
                    } else {
                        throw new RemotingSendRequestException(addr);
                    }

                    Thread.sleep(1);
                }

                sendRequestOK = true;

                // 先读取响应总长度
                ByteBuffer byteBufferSize = ByteBuffer.allocate(4);
                while (byteBufferSize.hasRemaining()) {
                    int length = socketChannel.read(byteBufferSize);
                    if (length > 0) {
                        if (byteBufferSize.hasRemaining()) {
                            if ((System.currentTimeMillis() - beginTime) > timeoutMillis) {

                                throw new RemotingTimeoutException(addr, timeoutMillis);
                            }
                        }
                    } else {
                        throw new RemotingTimeoutException(addr, timeoutMillis);
                    }

                    Thread.sleep(1);
                }

                // 再按长度读取完整响应体
                int size = byteBufferSize.getInt(0);
                ByteBuffer byteBufferBody = ByteBuffer.allocate(size);
                while (byteBufferBody.hasRemaining()) {
                    int length = socketChannel.read(byteBufferBody);
                    if (length > 0) {
                        if (byteBufferBody.hasRemaining()) {
                            if ((System.currentTimeMillis() - beginTime) > timeoutMillis) {

                                throw new RemotingTimeoutException(addr, timeoutMillis);
                            }
                        }
                    } else {
                        throw new RemotingTimeoutException(addr, timeoutMillis);
                    }

                    Thread.sleep(1);
                }

                // 解码并返回响应命令
                byteBufferBody.flip();
                return RemotingCommand.decode(byteBufferBody);
            } catch (IOException e) {
                log.error("invokeSync failure", e);

                if (sendRequestOK) {
                    throw new RemotingTimeoutException(addr, timeoutMillis);
                } else {
                    throw new RemotingSendRequestException(addr);
                }
            } finally {
                try {
                    socketChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {
            throw new RemotingConnectException(addr);
        }
    }

    /**
     * 解析 Channel 远端地址
     *
     * @param channel Netty 通道
     * @return 远端地址字符串
     */
    public static String parseChannelRemoteAddr(final Channel channel) {
        if (null == channel) {
            return "";
        }

        // 优先使用 Proxy Protocol 地址
        String addr = getProxyProtocolAddress(channel);
        if (StringUtils.isNotBlank(addr)) {
            return addr;
        }

        // 尝试复用缓存地址
        Attribute<String> att = channel.attr(AttributeKeys.REMOTE_ADDR_KEY);
        if (att == null) {
            // mocked in unit test
            // 单元测试中可能使用 mocked channel
            return parseChannelRemoteAddr0(channel);
        }
        addr = att.get();
        if (addr == null) {
            // 首次解析后写入缓存
            addr = parseChannelRemoteAddr0(channel);
            att.set(addr);
        }
        return addr;
    }

    /**
     * 获取 Proxy Protocol 地址
     *
     * @param channel Netty 通道
     * @return Proxy Protocol 地址字符串
     */
    private static String getProxyProtocolAddress(Channel channel) {
        if (!channel.hasAttr(AttributeKeys.PROXY_PROTOCOL_ADDR)) {
            return null;
        }
        String proxyProtocolAddr = getAttributeValue(AttributeKeys.PROXY_PROTOCOL_ADDR, channel);
        String proxyProtocolPort = getAttributeValue(AttributeKeys.PROXY_PROTOCOL_PORT, channel);
        if (StringUtils.isBlank(proxyProtocolAddr) || proxyProtocolPort == null) {
            return null;
        }
        return proxyProtocolAddr + ":" + proxyProtocolPort;
    }

    /**
     * 解析 Channel 远端地址, 不使用缓存
     *
     * @param channel Netty 通道
     * @return 远端地址字符串
     */
    private static String parseChannelRemoteAddr0(final Channel channel) {
        SocketAddress remote = channel.remoteAddress();
        final String addr = remote != null ? remote.toString() : "";

        if (addr.length() > 0) {
            int index = addr.lastIndexOf("/");
            if (index >= 0) {
                return addr.substring(index + 1);
            }

            return addr;
        }

        return "";
    }

    /**
     * 解析 Channel 本地地址
     *
     * @param channel Netty 通道
     * @return 本地地址字符串
     */
    public static String parseChannelLocalAddr(final Channel channel) {
        SocketAddress remote = channel.localAddress();
        final String addr = remote != null ? remote.toString() : "";

        if (addr.length() > 0) {
            int index = addr.lastIndexOf("/");
            if (index >= 0) {
                return addr.substring(index + 1);
            }

            return addr;
        }

        return "";
    }

    /**
     * 从地址字符串中提取主机
     *
     * @param address 地址字符串
     * @return 主机字符串
     */
    public static String parseHostFromAddress(String address) {
        if (address == null) {
            return "";
        }

        String[] addressSplits = address.split(":");
        if (addressSplits.length < 1) {
            return "";
        }

        return addressSplits[0];
    }

    /**
     * 解析 SocketAddress 的地址部分
     *
     * @param socketAddress SocketAddress 对象
     * @return 地址部分字符串
     */
    public static String parseSocketAddressAddr(SocketAddress socketAddress) {
        if (socketAddress != null) {
            // Default toString of InetSocketAddress is "hostName/IP:port"
            // InetSocketAddress 默认 toString 格式为 hostName/IP:port
            final String addr = socketAddress.toString();
            int index = addr.lastIndexOf("/");
            return (index != -1) ? addr.substring(index + 1) : addr;
        }
        return "";
    }

    /**
     * 解析 SocketAddress 的端口
     *
     * @param socketAddress SocketAddress 对象
     * @return 端口号, 非 InetSocketAddress 时返回 -1
     */
    public static Integer parseSocketAddressPort(SocketAddress socketAddress) {
        if (socketAddress instanceof InetSocketAddress) {
            return ((InetSocketAddress) socketAddress).getPort();
        }
        return -1;
    }

    /**
     * 将 IPv4 地址转换为整数
     *
     * @param ip IPv4 地址
     * @return 整数形式的 IP
     */
    public static int ipToInt(String ip) {
        String[] ips = ip.split("\\.");
        return (Integer.parseInt(ips[0]) << 24)
            | (Integer.parseInt(ips[1]) << 16)
            | (Integer.parseInt(ips[2]) << 8)
            | Integer.parseInt(ips[3]);
    }

    /**
     * 判断 IP 是否位于指定 CIDR 内
     *
     * @param ip   IPv4 地址
     * @param cidr CIDR 网段
     * @return true 表示命中网段
     */
    public static boolean ipInCIDR(String ip, String cidr) {
        int ipAddr = ipToInt(ip);
        String[] cidrArr = cidr.split("/");
        int netId = Integer.parseInt(cidrArr[1]);
        int mask = 0xFFFFFFFF << (32 - netId);
        int cidrIpAddr = ipToInt(cidrArr[0]);

        return (ipAddr & mask) == (cidrIpAddr & mask);
    }

    /**
     * 使用默认超时建立 Socket 连接
     *
     * @param remote 远端地址
     * @return SocketChannel, 建连失败时返回 null
     */
    public static SocketChannel connect(SocketAddress remote) {
        return connect(remote, 1000 * 5);
    }

    /**
     * 建立 Socket 连接
     *
     * @param remote        远端地址
     * @param timeoutMillis 连接超时, 单位为毫秒
     * @return SocketChannel, 建连失败时返回 null
     */
    public static SocketChannel connect(SocketAddress remote, final int timeoutMillis) {
        SocketChannel sc = null;
        try {
            // 创建并配置通道
            sc = SocketChannel.open();
            sc.configureBlocking(true);
            sc.socket().setSoLinger(false, -1);
            sc.socket().setTcpNoDelay(true);

            // 按系统配置设置收发缓冲区
            if (NettySystemConfig.socketSndbufSize > 0) {
                sc.socket().setReceiveBufferSize(NettySystemConfig.socketSndbufSize);
            }
            if (NettySystemConfig.socketRcvbufSize > 0) {
                sc.socket().setSendBufferSize(NettySystemConfig.socketRcvbufSize);
            }

            // 发起连接并切回非阻塞模式
            sc.socket().connect(remote, timeoutMillis);
            sc.configureBlocking(false);
            return sc;
        } catch (Exception e) {
            // 连接失败时关闭通道并返回 null
            if (sc != null) {
                try {
                    sc.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }

        return null;
    }

    /**
     * 关闭 Netty 通道
     *
     * @param channel Netty 通道
     */
    public static void closeChannel(Channel channel) {
        final String addrRemote = RemotingHelper.parseChannelRemoteAddr(channel);
        if ("".equals(addrRemote)) {
            channel.close();
        } else {
            // 关闭后输出结果日志
            channel.close().addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    log.info("closeChannel: close the connection to remote address[{}] result: {}", addrRemote,
                        future.isSuccess());
                }
            });
        }
    }

    /**
     * 将 ChannelFuture 转换为 CompletableFuture
     *
     * @param channelFuture Netty ChannelFuture
     * @return CompletableFuture 对象
     */
    public static CompletableFuture<Void> convertChannelFutureToCompletableFuture(ChannelFuture channelFuture) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        channelFuture.addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                completableFuture.complete(null);
            } else {
                // 失败时包装为 RemotingConnectException
                completableFuture.completeExceptionally(new RemotingConnectException(channelFuture.channel().remoteAddress().toString(), future.cause()));
            }
        });
        return completableFuture;
    }

    /**
     * 获取请求码描述
     *
     * @param code 请求码
     * @return 请求码描述字符串
     */
    public static String getRequestCodeDesc(int code) {
        return REQUEST_CODE_MAP.getOrDefault(code, String.valueOf(code));
    }

    /**
     * 获取响应码描述
     *
     * @param code 响应码
     * @return 响应码描述字符串
     */
    public static String getResponseCodeDesc(int code) {
        return RESPONSE_CODE_MAP.getOrDefault(code, String.valueOf(code));
    }
}
