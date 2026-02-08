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

import io.netty.handler.ssl.*;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.cert.CertificateException;
import java.util.Properties;

import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.*;

/**
 * TLS 工具类, 用于构建客户端与服务端 SSL 上下文
 */
public class TlsHelper {

    /**
     * 私钥解密策略
     */
    public interface DecryptionStrategy {
        /**
         * Decrypt the target encrpted private key file.
         * <br>
         * 解密目标加密私钥文件
         *
         * @param privateKeyEncryptPath A pathname string<br>私钥文件路径
         * @param forClient tells whether it's a client-side key file<br>标记当前是否为客户端密钥文件
         * @return An input stream for a decrypted key file<br>解密后私钥文件输入流
         * @throws IOException if an I/O error has occurred<br>发生 I/O 错误时抛出
         */
        InputStream decryptPrivateKey(String privateKeyEncryptPath, boolean forClient) throws IOException;
    }

    /**
     * Remoting 日志记录器
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);

    /**
     * 当前私钥解密策略, 默认按文件原样读取
     */
    private static DecryptionStrategy decryptionStrategy = new DecryptionStrategy() {

        /**
         * 解密私钥文件
         *
         * @param privateKeyEncryptPath 私钥文件路径
         * @param forClient             标记当前是否为客户端密钥文件
         * @return 私钥输入流
         * @throws IOException 读取文件失败时抛出
         */
        @Override
        public InputStream decryptPrivateKey(final String privateKeyEncryptPath,
            final boolean forClient) throws IOException {
            return new FileInputStream(privateKeyEncryptPath);
        }
    };

    /**
     * 注册私钥解密策略
     *
     * @param decryptionStrategy 私钥解密策略实现
     */
    public static void registerDecryptionStrategy(final DecryptionStrategy decryptionStrategy) {
        TlsHelper.decryptionStrategy = decryptionStrategy;
    }

    /**
     * 构建 SSL 上下文
     *
     * @param forClient true 表示构建客户端上下文, false 表示构建服务端上下文
     * @return 构建完成的 SSL 上下文
     * @throws IOException          读取证书或私钥失败时抛出
     * @throws CertificateException 证书处理失败时抛出
     */
    public static SslContext buildSslContext(boolean forClient) throws IOException, CertificateException {
        // 加载 TLS 配置文件并打印最终生效配置
        File configFile = new File(TlsSystemConfig.tlsConfigFile);
        extractTlsConfigFromFile(configFile);
        logTheFinalUsedTlsConfig();

        // 优先使用 OpenSSL, 不可用时回退 JDK SSL
        SslProvider provider;
        if (OpenSsl.isAvailable()) {
            provider = SslProvider.OPENSSL;
            LOGGER.info("Using OpenSSL provider");
        } else {
            provider = SslProvider.JDK;
            LOGGER.info("Using JDK SSL provider");
        }

        // 按客户端或服务端分别构建上下文
        if (forClient) {
            if (tlsTestModeEnable) {
                // 测试模式下客户端关闭服务端证书校验
                return SslContextBuilder
                    .forClient()
                    .sslProvider(SslProvider.JDK)
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .build();
            } else {
                SslContextBuilder sslContextBuilder = SslContextBuilder.forClient().sslProvider(SslProvider.JDK);

                // 根据配置选择信任策略
                if (!tlsClientAuthServer) {
                    sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
                } else {
                    if (!isNullOrEmpty(tlsClientTrustCertPath)) {
                        sslContextBuilder.trustManager(new File(tlsClientTrustCertPath));
                    }
                }

                // 配置客户端证书与私钥
                return sslContextBuilder.keyManager(
                    !isNullOrEmpty(tlsClientCertPath) ? new FileInputStream(tlsClientCertPath) : null,
                    !isNullOrEmpty(tlsClientKeyPath) ? decryptionStrategy.decryptPrivateKey(tlsClientKeyPath, true) : null,
                    !isNullOrEmpty(tlsClientKeyPassword) ? tlsClientKeyPassword : null)
                    .build();
            }
        } else {
            if (tlsTestModeEnable) {
                // 测试模式下服务端使用自签名证书
                SelfSignedCertificate selfSignedCertificate = new SelfSignedCertificate();
                return SslContextBuilder
                    .forServer(selfSignedCertificate.certificate(), selfSignedCertificate.privateKey())
                    .sslProvider(provider)
                    .clientAuth(ClientAuth.OPTIONAL)
                    .build();
            } else {
                // 正式模式下服务端从配置文件加载证书与私钥
                SslContextBuilder sslContextBuilder = SslContextBuilder.forServer(
                    !isNullOrEmpty(tlsServerCertPath) ? new FileInputStream(tlsServerCertPath) : null,
                    !isNullOrEmpty(tlsServerKeyPath) ? decryptionStrategy.decryptPrivateKey(tlsServerKeyPath, false) : null,
                    !isNullOrEmpty(tlsServerKeyPassword) ? tlsServerKeyPassword : null)
                    .sslProvider(provider);

                // 根据配置决定是否校验客户端证书
                if (!tlsServerAuthClient) {
                    sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
                } else {
                    if (!isNullOrEmpty(tlsServerTrustCertPath)) {
                        sslContextBuilder.trustManager(new File(tlsServerTrustCertPath));
                    }
                }

                // 设置客户端认证模式并构建上下文
                sslContextBuilder.clientAuth(parseClientAuthMode(tlsServerNeedClientAuth));
                return sslContextBuilder.build();
            }
        }
    }

    /**
     * 从文件提取 TLS 配置
     *
     * @param configFile TLS 配置文件
     */
    private static void extractTlsConfigFromFile(final File configFile) {
        // 文件不可读时直接跳过
        if (!(configFile.exists() && configFile.isFile() && configFile.canRead())) {
            LOGGER.info("Tls config file doesn't exist, skip it");
            return;
        }

        Properties properties = new Properties();
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(configFile);
            properties.load(inputStream);
        } catch (IOException ignore) {
            // 配置加载失败时保持默认值
        } finally {
            if (null != inputStream) {
                try {
                    inputStream.close();
                } catch (IOException ignore) {
                    // 关闭输入流失败时忽略
                }
            }
        }

        // 使用配置值覆盖当前 TLS 参数
        tlsTestModeEnable = Boolean.parseBoolean(properties.getProperty(TLS_TEST_MODE_ENABLE, String.valueOf(tlsTestModeEnable)));
        tlsServerNeedClientAuth = properties.getProperty(TLS_SERVER_NEED_CLIENT_AUTH, tlsServerNeedClientAuth);
        tlsServerKeyPath = properties.getProperty(TLS_SERVER_KEYPATH, tlsServerKeyPath);
        tlsServerKeyPassword = properties.getProperty(TLS_SERVER_KEYPASSWORD, tlsServerKeyPassword);
        tlsServerCertPath = properties.getProperty(TLS_SERVER_CERTPATH, tlsServerCertPath);
        tlsServerAuthClient = Boolean.parseBoolean(properties.getProperty(TLS_SERVER_AUTHCLIENT, String.valueOf(tlsServerAuthClient)));
        tlsServerTrustCertPath = properties.getProperty(TLS_SERVER_TRUSTCERTPATH, tlsServerTrustCertPath);

        tlsClientKeyPath = properties.getProperty(TLS_CLIENT_KEYPATH, tlsClientKeyPath);
        tlsClientKeyPassword = properties.getProperty(TLS_CLIENT_KEYPASSWORD, tlsClientKeyPassword);
        tlsClientCertPath = properties.getProperty(TLS_CLIENT_CERTPATH, tlsClientCertPath);
        tlsClientAuthServer = Boolean.parseBoolean(properties.getProperty(TLS_CLIENT_AUTHSERVER, String.valueOf(tlsClientAuthServer)));
        tlsClientTrustCertPath = properties.getProperty(TLS_CLIENT_TRUSTCERTPATH, tlsClientTrustCertPath);
    }

    /**
     * 记录最终生效的 TLS 配置
     */
    private static void logTheFinalUsedTlsConfig() {
        LOGGER.info("Log the final used tls related configuration");
        LOGGER.info("{} = {}", TLS_TEST_MODE_ENABLE, tlsTestModeEnable);
        LOGGER.debug("{} = {}", TLS_SERVER_NEED_CLIENT_AUTH, tlsServerNeedClientAuth);
        LOGGER.debug("{} = {}", TLS_SERVER_KEYPATH, tlsServerKeyPath);
        LOGGER.debug("{} = {}", TLS_SERVER_CERTPATH, tlsServerCertPath);
        LOGGER.debug("{} = {}", TLS_SERVER_AUTHCLIENT, tlsServerAuthClient);
        LOGGER.debug("{} = {}", TLS_SERVER_TRUSTCERTPATH, tlsServerTrustCertPath);

        LOGGER.debug("{} = {}", TLS_CLIENT_KEYPATH, tlsClientKeyPath);
        LOGGER.debug("{} = {}", TLS_CLIENT_CERTPATH, tlsClientCertPath);
        LOGGER.debug("{} = {}", TLS_CLIENT_AUTHSERVER, tlsClientAuthServer);
        LOGGER.debug("{} = {}", TLS_CLIENT_TRUSTCERTPATH, tlsClientTrustCertPath);
    }

    /**
     * 解析客户端认证模式
     *
     * @param authMode 认证模式字符串
     * @return ClientAuth 枚举值, 无法识别时返回 ClientAuth.NONE
     */
    private static ClientAuth parseClientAuthMode(String authMode) {
        // 未配置认证模式时返回 NONE
        if (null == authMode || authMode.trim().isEmpty()) {
            return ClientAuth.NONE;
        }

        // 按忽略大小写方式匹配枚举值
        String authModeUpper = authMode.toUpperCase();
        for (ClientAuth clientAuth : ClientAuth.values()) {
            if (clientAuth.name().equals(authModeUpper)) {
                return clientAuth;
            }
        }

        // 未匹配时返回 NONE
        return ClientAuth.NONE;
    }

    /**
     * Determine if a string is {@code null} or {@link String#isEmpty()} returns {@code true}.
     * <br>
     * 判断字符串是否为 {@code null} 或 {@link String#isEmpty()} 返回 {@code true}
     */
    private static boolean isNullOrEmpty(String s) {
        return s == null || s.isEmpty();
    }
}
