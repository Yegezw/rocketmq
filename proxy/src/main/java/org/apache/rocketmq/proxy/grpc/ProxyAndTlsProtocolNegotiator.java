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
package org.apache.rocketmq.proxy.grpc;

import io.grpc.Attributes;
import io.grpc.netty.shaded.io.grpc.netty.*;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.ByteBufUtil;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandler;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandlerContext;
import io.grpc.netty.shaded.io.netty.channel.ChannelInboundHandlerAdapter;
import io.grpc.netty.shaded.io.netty.handler.codec.ByteToMessageDecoder;
import io.grpc.netty.shaded.io.netty.handler.codec.ProtocolDetectionResult;
import io.grpc.netty.shaded.io.netty.handler.codec.ProtocolDetectionState;
import io.grpc.netty.shaded.io.netty.handler.codec.haproxy.HAProxyMessage;
import io.grpc.netty.shaded.io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import io.grpc.netty.shaded.io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.grpc.netty.shaded.io.netty.handler.codec.haproxy.HAProxyTLV;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslHandler;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.SelfSignedCertificate;
import io.grpc.netty.shaded.io.netty.util.AsciiString;
import io.grpc.netty.shaded.io.netty.util.CharsetUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.HAProxyConstants;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.BinaryUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.constant.AttributeKeys;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * 协议协商器, 负责按顺序处理 HAProxy Protocol 与 TLS 协商
 */
public class ProxyAndTlsProtocolNegotiator implements InternalProtocolNegotiator.ProtocolNegotiator {
    /**
     * Proxy 模块日志记录器
     */
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    /**
     * Pipeline 中 HAProxy 解码器名称
     */
    private static final String HA_PROXY_DECODER = "HAProxyDecoder";
    /**
     * Pipeline 中 HAProxy 处理器名称
     */
    private static final String HA_PROXY_HANDLER = "HAProxyHandler";
    /**
     * Pipeline 中 TLS 处理器名称
     */
    private static final String TLS_MODE_HANDLER = "TlsModeHandler";
    /**
     * the length of the ssl record header (in bytes)
     * <br>
     * SSL 记录头长度, 单位字节
     */
    private static final int SSL_RECORD_HEADER_LENGTH = 5;

    /**
     * gRPC TLS 上下文
     */
    private static SslContext sslContext;

    /**
     * 构造协议协商器并加载 TLS 上下文
     */
    public ProxyAndTlsProtocolNegotiator() {
        sslContext = loadSslContext();
    }

    /**
     * 返回协商后的传输方案
     *
     * @return 传输方案标识
     */
    @Override
    public AsciiString scheme() {
        return AsciiString.of("https");
    }

    /**
     * 创建协议协商处理器
     *
     * @param grpcHandler gRPC HTTP2 连接处理器
     * @return 协商处理器
     */
    @Override
    public ChannelHandler newHandler(GrpcHttp2ConnectionHandler grpcHandler) {
        return new ProxyAndTlsProtocolHandler(grpcHandler);
    }

    /**
     * 关闭协商器资源
     */
    @Override
    public void close() {
    }

    /**
     * 加载 TLS 上下文, 支持测试证书或指定证书文件
     *
     * @return TLS 上下文
     */
    private static SslContext loadSslContext() {
        try {
            ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
            if (proxyConfig.isTlsTestModeEnable()) {
                SelfSignedCertificate selfSignedCertificate = new SelfSignedCertificate();
                return GrpcSslContexts.forServer(selfSignedCertificate.certificate(),
                                selfSignedCertificate.privateKey())
                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                        .clientAuth(ClientAuth.NONE)
                        .build();
            } else {
                String tlsKeyPath = ConfigurationManager.getProxyConfig().getTlsKeyPath();
                String tlsCertPath = ConfigurationManager.getProxyConfig().getTlsCertPath();
                try (InputStream serverKeyInputStream = Files.newInputStream(
                        Paths.get(tlsKeyPath));
                     InputStream serverCertificateStream = Files.newInputStream(
                             Paths.get(tlsCertPath))) {
                    SslContext res = GrpcSslContexts.forServer(serverCertificateStream,
                                    serverKeyInputStream)
                            .trustManager(InsecureTrustManagerFactory.INSTANCE)
                            .clientAuth(ClientAuth.NONE)
                            .build();
                    log.info("grpc load TLS configured OK");
                    return res;
                }
            }
        } catch (Exception e) {
            log.error("grpc tls set failed. msg: {}, e:", e.getMessage(), e);
            throw new RuntimeException("grpc tls set failed: " + e.getMessage());
        }
    }

    /**
     * 首段协议探测处理器, 先识别 HAProxy 协议再切换 TLS 处理器
     */
    private class ProxyAndTlsProtocolHandler extends ByteToMessageDecoder {

        /**
         * gRPC HTTP2 连接处理器
         */
        private final GrpcHttp2ConnectionHandler grpcHandler;

        /**
         * 协议协商事件上下文
         */
        private ProtocolNegotiationEvent pne = InternalProtocolNegotiationEvent.getDefault();

        /**
         * 构造协议探测处理器
         *
         * @param grpcHandler gRPC HTTP2 连接处理器
         */
        public ProxyAndTlsProtocolHandler(GrpcHttp2ConnectionHandler grpcHandler) {
            this.grpcHandler = grpcHandler;
        }

        /**
         * 探测入站协议并安装对应处理器链
         *
         * @param ctx Channel 上下文
         * @param in 入站字节流
         * @param out 解码输出列表
         */
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            try {
                ProtocolDetectionResult<HAProxyProtocolVersion> ha = HAProxyMessageDecoder.detectProtocol(
                        in);
                if (ha.state() == ProtocolDetectionState.NEEDS_MORE_DATA) {
                    return;
                }
                if (ha.state() == ProtocolDetectionState.DETECTED) {
                    ctx.pipeline().addAfter(ctx.name(), HA_PROXY_DECODER, new HAProxyMessageDecoder())
                            .addAfter(HA_PROXY_DECODER, HA_PROXY_HANDLER, new HAProxyMessageHandler())
                            .addAfter(HA_PROXY_HANDLER, TLS_MODE_HANDLER, new TlsModeHandler(grpcHandler));
                } else {
                    ctx.pipeline().addAfter(ctx.name(), TLS_MODE_HANDLER, new TlsModeHandler(grpcHandler));
                }

                Attributes.Builder builder = InternalProtocolNegotiationEvent.getAttributes(pne).toBuilder();
                builder.set(AttributeKeys.CHANNEL_ID, ctx.channel().id().asLongText());

                ctx.fireUserEventTriggered(InternalProtocolNegotiationEvent.withAttributes(pne, builder.build()));
                ctx.pipeline().remove(this);
            } catch (Exception e) {
                log.error("process proxy protocol negotiator failed.", e);
                throw e;
            }
        }

        /**
         * 记录协商事件, 供后续处理器透传连接属性
         *
         * @param ctx Channel 上下文
         * @param evt 用户事件
         * @throws Exception 事件处理异常
         */
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof ProtocolNegotiationEvent) {
                pne = (ProtocolNegotiationEvent) evt;
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }
    }

    /**
     * HAProxy 报文处理器, 将解析结果写入协商属性
     */
    private class HAProxyMessageHandler extends ChannelInboundHandlerAdapter {

        /**
         * 协议协商事件上下文
         */
        private ProtocolNegotiationEvent pne = InternalProtocolNegotiationEvent.getDefault();

        /**
         * 处理 HAProxy 报文并透传协商事件
         *
         * @param ctx Channel 上下文
         * @param msg 入站消息
         * @throws Exception 处理异常
         */
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof HAProxyMessage) {
                handleWithMessage((HAProxyMessage) msg);
                ctx.fireUserEventTriggered(pne);
            } else {
                super.channelRead(ctx, msg);
            }
            ctx.pipeline().remove(this);
        }

        /**
         * The definition of key refers to the implementation of nginx
         * <a href="https://nginx.org/en/docs/http/ngx_http_core_module.html#var_proxy_protocol_addr">ngx_http_core_module</a>
         * <br>
         * 键定义参考 nginx 对 proxy protocol 变量的实现
         *
         * @param msg HAProxy 报文对象
         */
        private void handleWithMessage(HAProxyMessage msg) {
            try {
                Attributes.Builder builder = InternalProtocolNegotiationEvent.getAttributes(pne).toBuilder();
                if (StringUtils.isNotBlank(msg.sourceAddress())) {
                    builder.set(AttributeKeys.PROXY_PROTOCOL_ADDR, msg.sourceAddress());
                }
                if (msg.sourcePort() > 0) {
                    builder.set(AttributeKeys.PROXY_PROTOCOL_PORT, String.valueOf(msg.sourcePort()));
                }
                if (StringUtils.isNotBlank(msg.destinationAddress())) {
                    builder.set(AttributeKeys.PROXY_PROTOCOL_SERVER_ADDR, msg.destinationAddress());
                }
                if (msg.destinationPort() > 0) {
                    builder.set(AttributeKeys.PROXY_PROTOCOL_SERVER_PORT, String.valueOf(msg.destinationPort()));
                }
                if (CollectionUtils.isNotEmpty(msg.tlvs())) {
                    msg.tlvs().forEach(tlv -> handleHAProxyTLV(tlv, builder));
                }
                pne = InternalProtocolNegotiationEvent
                        .withAttributes(InternalProtocolNegotiationEvent.getDefault(), builder.build());
            } finally {
                msg.release();
            }
        }

        /**
         * 记录协商事件, 供 HAProxy 解析结果合并属性
         *
         * @param ctx Channel 上下文
         * @param evt 用户事件
         * @throws Exception 事件处理异常
         */
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof ProtocolNegotiationEvent) {
                pne = (ProtocolNegotiationEvent) evt;
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }
    }

    /**
     * 处理 HAProxy TLV 扩展属性并写入协商属性
     *
     * @param tlv TLV 扩展对象
     * @param builder 协商属性构建器
     */
    protected void handleHAProxyTLV(HAProxyTLV tlv, Attributes.Builder builder) {
        byte[] valueBytes = ByteBufUtil.getBytes(tlv.content());
        if (!BinaryUtil.isAscii(valueBytes)) {
            return;
        }
        Attributes.Key<String> key = AttributeKeys.valueOf(
            HAProxyConstants.PROXY_PROTOCOL_TLV_PREFIX + String.format("%02x", tlv.typeByteValue()));
        builder.set(key, new String(valueBytes, CharsetUtil.UTF_8));
    }

    /**
     * TLS 模式处理器, 根据全局 TLS 模式或报文内容选择加密与明文处理链
     */
    private class TlsModeHandler extends ByteToMessageDecoder {

        /**
         * 协议协商事件上下文
         */
        private ProtocolNegotiationEvent pne = InternalProtocolNegotiationEvent.getDefault();

        /**
         * TLS 处理器
         */
        private final ChannelHandler ssl;
        /**
         * 明文处理器
         */
        private final ChannelHandler plaintext;

        /**
         * 构造 TLS 模式处理器
         *
         * @param grpcHandler gRPC HTTP2 连接处理器
         */
        public TlsModeHandler(GrpcHttp2ConnectionHandler grpcHandler) {
            this.ssl = InternalProtocolNegotiators.serverTls(sslContext)
                    .newHandler(grpcHandler);
            this.plaintext = InternalProtocolNegotiators.serverPlaintext()
                    .newHandler(grpcHandler);
        }

        /**
         * 根据 TLS 模式与报文特征选择后续处理器
         *
         * @param ctx Channel 上下文
         * @param in 入站字节流
         * @param out 解码输出列表
         */
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            try {
                TlsMode tlsMode = TlsSystemConfig.tlsMode;
                if (TlsMode.ENFORCING.equals(tlsMode)) {
                    ctx.pipeline().addAfter(ctx.name(), null, this.ssl);
                } else if (TlsMode.DISABLED.equals(tlsMode)) {
                    ctx.pipeline().addAfter(ctx.name(), null, this.plaintext);
                } else {
                    // in SslHandler.isEncrypted, it need at least 5 bytes to judge is encrypted or not
                    // 判断是否为 TLS 报文前, 需要至少 5 字节记录头
                    if (in.readableBytes() < SSL_RECORD_HEADER_LENGTH) {
                        return;
                    }
                    if (SslHandler.isEncrypted(in)) {
                        ctx.pipeline().addAfter(ctx.name(), null, this.ssl);
                    } else {
                        ctx.pipeline().addAfter(ctx.name(), null, this.plaintext);
                    }
                }
                ctx.fireUserEventTriggered(pne);
                ctx.pipeline().remove(this);
            } catch (Exception e) {
                log.error("process ssl protocol negotiator failed.", e);
                throw e;
            }
        }

        /**
         * 记录协商事件, 供 TLS 处理链复用连接属性
         *
         * @param ctx Channel 上下文
         * @param evt 用户事件
         * @throws Exception 事件处理异常
         */
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof ProtocolNegotiationEvent) {
                pne = (ProtocolNegotiationEvent) evt;
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }
    }
}
