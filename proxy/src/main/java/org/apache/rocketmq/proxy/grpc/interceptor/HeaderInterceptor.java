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

package org.apache.rocketmq.proxy.grpc.interceptor;

import com.google.common.net.HostAndPort;
import io.grpc.Attributes;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.HAProxyConstants;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.common.utils.GrpcUtils;
import org.apache.rocketmq.proxy.grpc.constant.AttributeKeys;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * gRPC 头信息拦截器, 补齐远端地址, 本地地址与代理透传头
 */
public class HeaderInterceptor implements ServerInterceptor {
    /**
     * 解析并回填调用链路关键头信息
     *
     * @param call 当前 RPC 调用
     * @param headers 当前调用头信息
     * @param next 下一个调用处理器
     * @return 继续处理后的监听器
     */
    @Override
    public <R, W> ServerCall.Listener<R> interceptCall(
        ServerCall<R, W> call,
        Metadata headers,
        ServerCallHandler<R, W> next
    ) {
        String remoteAddress = getProxyProtocolAddress(call.getAttributes());
        if (StringUtils.isBlank(remoteAddress)) {
            SocketAddress remoteSocketAddress = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
            remoteAddress = parseSocketAddress(remoteSocketAddress);
        }
        GrpcUtils.putHeaderIfNotExist(headers, GrpcConstants.REMOTE_ADDRESS, remoteAddress);

        SocketAddress localSocketAddress = call.getAttributes().get(Grpc.TRANSPORT_ATTR_LOCAL_ADDR);
        String localAddress = parseSocketAddress(localSocketAddress);
        GrpcUtils.putHeaderIfNotExist(headers, GrpcConstants.LOCAL_ADDRESS, localAddress);

        // 透传 HAProxy 协议相关属性, 便于后续链路获取真实客户端信息
        for (Attributes.Key<?> key : call.getAttributes().keys()) {
            if (!StringUtils.startsWith(key.toString(), HAProxyConstants.PROXY_PROTOCOL_PREFIX)) {
                continue;
            }
            Metadata.Key<String> headerKey
                    = Metadata.Key.of(key.toString(), Metadata.ASCII_STRING_MARSHALLER);
            String headerValue = String.valueOf(call.getAttributes().get(key));
            GrpcUtils.putHeaderIfNotExist(headers, headerKey, headerValue);
        }

        String channelId = call.getAttributes().get(AttributeKeys.CHANNEL_ID);
        if (StringUtils.isNotBlank(channelId)) {
            GrpcUtils.putHeaderIfNotExist(headers, GrpcConstants.CHANNEL_ID, channelId);
        }

        return next.startCall(call, headers);
    }

    /**
     * 将 SocketAddress 转换为 host:port 字符串
     *
     * @param socketAddress 传输层地址对象
     * @return 地址字符串, 解析失败时返回空字符串
     */
    private String parseSocketAddress(SocketAddress socketAddress) {
        if (socketAddress instanceof InetSocketAddress) {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
            return HostAndPort.fromParts(
                inetSocketAddress.getAddress()
                    .getHostAddress(),
                inetSocketAddress.getPort()
            ).toString();
        }

        return "";
    }

    /**
     * 从属性中读取 Proxy Protocol 地址与端口
     *
     * @param attributes gRPC 调用属性
     * @return 真实客户端地址, 信息缺失时返回 null
     */
    private String getProxyProtocolAddress(Attributes attributes) {
        String proxyProtocolAddr = attributes.get(AttributeKeys.PROXY_PROTOCOL_ADDR);
        String proxyProtocolPort = attributes.get(AttributeKeys.PROXY_PROTOCOL_PORT);
        if (StringUtils.isBlank(proxyProtocolAddr) || StringUtils.isBlank(proxyProtocolPort)) {
            return null;
        }
        return proxyProtocolAddr + ":" + proxyProtocolPort;
    }
}
