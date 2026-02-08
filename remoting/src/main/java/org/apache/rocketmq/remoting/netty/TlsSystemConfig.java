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

import io.netty.handler.ssl.SslContext;
import org.apache.rocketmq.remoting.common.TlsMode;

/**
 * TLS 系统配置项
 */
public class TlsSystemConfig {
    /**
     * 服务端 TLS 模式系统属性键
     */
    public static final String TLS_SERVER_MODE = "tls.server.mode";
    /**
     * 是否启用 TLS 的系统属性键
     */
    public static final String TLS_ENABLE = "tls.enable";
    /**
     * TLS 配置文件路径系统属性键
     */
    public static final String TLS_CONFIG_FILE = "tls.config.file";
    /**
     * 是否启用 TLS 测试模式系统属性键
     */
    public static final String TLS_TEST_MODE_ENABLE = "tls.test.mode.enable";

    /**
     * 服务端客户端认证模式系统属性键
     */
    public static final String TLS_SERVER_NEED_CLIENT_AUTH = "tls.server.need.client.auth";
    /**
     * 服务端私钥路径系统属性键
     */
    public static final String TLS_SERVER_KEYPATH = "tls.server.keyPath";
    /**
     * 服务端私钥密码系统属性键
     */
    public static final String TLS_SERVER_KEYPASSWORD = "tls.server.keyPassword";
    /**
     * 服务端证书路径系统属性键
     */
    public static final String TLS_SERVER_CERTPATH = "tls.server.certPath";
    /**
     * 服务端是否校验客户端证书系统属性键
     */
    public static final String TLS_SERVER_AUTHCLIENT = "tls.server.authClient";
    /**
     * 服务端信任证书路径系统属性键
     */
    public static final String TLS_SERVER_TRUSTCERTPATH = "tls.server.trustCertPath";

    /**
     * 客户端私钥路径系统属性键
     */
    public static final String TLS_CLIENT_KEYPATH = "tls.client.keyPath";
    /**
     * 客户端私钥密码系统属性键
     */
    public static final String TLS_CLIENT_KEYPASSWORD = "tls.client.keyPassword";
    /**
     * 客户端证书路径系统属性键
     */
    public static final String TLS_CLIENT_CERTPATH = "tls.client.certPath";
    /**
     * 客户端是否校验服务端证书系统属性键
     */
    public static final String TLS_CLIENT_AUTHSERVER = "tls.client.authServer";
    /**
     * 客户端信任证书路径系统属性键
     */
    public static final String TLS_CLIENT_TRUSTCERTPATH = "tls.client.trustCertPath";

    /**
     * To determine whether use SSL in client-side, include SDK client and BrokerOuterAPI
     * <br>
     * 用于确定客户端侧是否使用 SSL, 包括 SDK client 和 BrokerOuterAPI
     */
    public static boolean tlsEnable = Boolean.parseBoolean(System.getProperty(TLS_ENABLE, "false"));

    /**
     * To determine whether use test mode when initialize TLS context
     * <br>
     * 用于确定初始化 TLS 上下文时是否使用测试模式
     */
    public static boolean tlsTestModeEnable = Boolean.parseBoolean(System.getProperty(TLS_TEST_MODE_ENABLE, "true"));

    /**
     * Indicates the state of the {@link javax.net.ssl.SSLEngine} with respect to client authentication.
     * This configuration item really only applies when building the server-side {@link SslContext},
     * and can be set to none, require or optional.
     * <br>表示 {@link javax.net.ssl.SSLEngine} 在客户端认证方面的状态
     * <br>该配置项仅在构建服务端 {@link SslContext} 时生效, 可设置为 none, require 或 optional
     */
    public static String tlsServerNeedClientAuth = System.getProperty(TLS_SERVER_NEED_CLIENT_AUTH, "none");
    /**
     * The store path of server-side private key
     * <br>
     * 服务端私钥存储路径
     */
    public static String tlsServerKeyPath = System.getProperty(TLS_SERVER_KEYPATH, null);

    /**
     * The password of the server-side private key
     * <br>
     * 服务端私钥密码
     */
    public static String tlsServerKeyPassword = System.getProperty(TLS_SERVER_KEYPASSWORD, null);

    /**
     * The store path of server-side X.509 certificate chain in PEM format
     * <br>
     * PEM 格式服务端 X.509 证书链存储路径
     */
    public static String tlsServerCertPath = System.getProperty(TLS_SERVER_CERTPATH, null);

    /**
     * To determine whether verify the client endpoint's certificate strictly
     * <br>
     * 用于确定是否严格校验客户端端点证书
     */
    public static boolean tlsServerAuthClient = Boolean.parseBoolean(System.getProperty(TLS_SERVER_AUTHCLIENT, "false"));

    /**
     * The store path of trusted certificates for verifying the client endpoint's certificate
     * <br>
     * 用于校验客户端端点证书的信任证书存储路径
     */
    public static String tlsServerTrustCertPath = System.getProperty(TLS_SERVER_TRUSTCERTPATH, null);

    /**
     * The store path of client-side private key
     * <br>
     * 客户端私钥存储路径
     */
    public static String tlsClientKeyPath = System.getProperty(TLS_CLIENT_KEYPATH, null);

    /**
     * The  password of the client-side private key
     * <br>
     * 客户端私钥密码
     */
    public static String tlsClientKeyPassword = System.getProperty(TLS_CLIENT_KEYPASSWORD, null);

    /**
     * The store path of client-side X.509 certificate chain in PEM format
     * <br>
     * PEM 格式客户端 X.509 证书链存储路径
     */
    public static String tlsClientCertPath = System.getProperty(TLS_CLIENT_CERTPATH, null);

    /**
     * To determine whether verify the server endpoint's certificate strictly
     * <br>
     * 用于确定是否严格校验服务端端点证书
     */
    public static boolean tlsClientAuthServer = Boolean.parseBoolean(System.getProperty(TLS_CLIENT_AUTHSERVER, "false"));

    /**
     * The store path of trusted certificates for verifying the server endpoint's certificate
     * <br>
     * 用于校验服务端端点证书的信任证书存储路径
     */
    public static String tlsClientTrustCertPath = System.getProperty(TLS_CLIENT_TRUSTCERTPATH, null);

    /**
     * For server, three SSL modes are supported: disabled, permissive and enforcing.
     * For client, use {@link TlsSystemConfig#tlsEnable} to determine whether use SSL.
     * <ol>
     *     <li><strong>disabled:</strong> SSL is not supported; any incoming SSL handshake will be rejected, causing connection closed.</li>
     *     <li><strong>permissive:</strong> SSL is optional, aka, server in this mode can serve client connections with or without SSL;</li>
     *     <li><strong>enforcing:</strong> SSL is required, aka, non SSL connection will be rejected.</li>
     * </ol>
     * <br>对服务端而言, 支持 disabled, permissive, enforcing 三种 SSL 模式
     * <br>对客户端而言, 使用 {@link TlsSystemConfig#tlsEnable} 判断是否启用 SSL
     * <br>disabled: 不支持 SSL, 任意入站 SSL 握手都会被拒绝并导致连接关闭
     * <br>permissive: SSL 可选, 即该模式下服务端可同时服务启用或未启用 SSL 的客户端连接
     * <br>enforcing: 必须启用 SSL, 非 SSL 连接会被拒绝
     */
    public static TlsMode tlsMode = TlsMode.parse(System.getProperty(TLS_SERVER_MODE, "permissive"));

    /**
     * A config file to store the above TLS related configurations,
     * except {@link TlsSystemConfig#tlsMode} and {@link TlsSystemConfig#tlsEnable}
     * <br>用于存储以上 TLS 相关配置的配置文件, 不包含 {@link TlsSystemConfig#tlsMode} 与 {@link TlsSystemConfig#tlsEnable}
     */
    public static String tlsConfigFile = System.getProperty(TLS_CONFIG_FILE, "/etc/rocketmq/tls.properties");
}
