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

import io.grpc.BindableService;
import io.grpc.ServerInterceptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollServerSocketChannel;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.interceptor.ContextInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.GlobalExceptionInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.HeaderInterceptor;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * gRPC 服务构建器, 负责初始化网络参数, 线程模型与拦截器链
 */
public class GrpcServerBuilder {
    /**
     * Proxy 模块日志记录器
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    /**
     * Netty gRPC 服务构建器
     */
    protected NettyServerBuilder serverBuilder;

    /**
     * 服务关闭等待时长
     */
    protected long time = 30;

    /**
     * 服务关闭等待时长单位
     */
    protected TimeUnit unit = TimeUnit.SECONDS;

    /**
     * 创建 gRPC 服务构建器
     *
     * @param executor 业务执行线程池
     * @param port 监听端口
     * @return 构建器实例
     */
    public static GrpcServerBuilder newBuilder(ThreadPoolExecutor executor, int port) {
        return new GrpcServerBuilder(executor, port);
    }

    /**
     * 构造构建器并完成核心网络参数初始化
     *
     * @param executor 业务执行线程池
     * @param port 监听端口
     */
    protected GrpcServerBuilder(ThreadPoolExecutor executor, int port) {
        serverBuilder = NettyServerBuilder.forPort(port);

        serverBuilder.protocolNegotiator(new ProxyAndTlsProtocolNegotiator());

        // build server
        int bossLoopNum = ConfigurationManager.getProxyConfig().getGrpcBossLoopNum();                     // 1
        int workerLoopNum = ConfigurationManager.getProxyConfig().getGrpcWorkerLoopNum();                 // CPU * 2
        int maxInboundMessageSize = ConfigurationManager.getProxyConfig().getGrpcMaxInboundMessageSize(); // 130M
        long idleTimeMills = ConfigurationManager.getProxyConfig().getGrpcClientIdleTimeMills();          // 120 seconds

        if (ConfigurationManager.getProxyConfig().isEnableGrpcEpoll()) {
            serverBuilder.bossEventLoopGroup(new EpollEventLoopGroup(bossLoopNum))
                .workerEventLoopGroup(new EpollEventLoopGroup(workerLoopNum))
                .channelType(EpollServerSocketChannel.class)
                .executor(executor); // gRPC 业务线程, 用于执行服务调用逻辑
        } else {
            serverBuilder.bossEventLoopGroup(new NioEventLoopGroup(bossLoopNum))
                .workerEventLoopGroup(new NioEventLoopGroup(workerLoopNum))
                .channelType(NioServerSocketChannel.class)
                .executor(executor); // gRPC 业务线程, 用于执行服务调用逻辑
        }

        serverBuilder.maxInboundMessageSize(maxInboundMessageSize)
                .maxConnectionIdle(idleTimeMills, TimeUnit.MILLISECONDS);

        log.info("grpc server has built. port: {}, bossLoopNum: {}, workerLoopNum: {}, maxInboundMessageSize: {}",
            port, bossLoopNum, workerLoopNum, maxInboundMessageSize);
    }

    /**
     * 设置服务关闭等待时间
     *
     * @param time 等待时长
     * @param unit 等待时长单位
     * @return 当前构建器
     */
    public GrpcServerBuilder shutdownTime(long time, TimeUnit unit) {
        this.time = time;
        this.unit = unit;
        return this;
    }

    /**
     * 注册 BindableService 服务实现
     *
     * @param service gRPC 服务实现
     * @return 当前构建器
     */
    public GrpcServerBuilder addService(BindableService service) {
        this.serverBuilder.addService(service);
        return this;
    }

    /**
     * 注册 ServerServiceDefinition 服务定义
     *
     * @param service gRPC 服务定义
     * @return 当前构建器
     */
    public GrpcServerBuilder addService(ServerServiceDefinition service) {
        this.serverBuilder.addService(service);
        return this;
    }

    /**
     * 追加服务端拦截器
     *
     * @param interceptor 服务端拦截器
     * @return 当前构建器
     */
    public GrpcServerBuilder appendInterceptor(ServerInterceptor interceptor) {
        this.serverBuilder.intercept(interceptor);
        return this;
    }

    /**
     * 构建 gRPC 服务实例
     *
     * @return gRPC 服务封装对象
     */
    public GrpcServer build() {
        return new GrpcServer(this.serverBuilder.build(), time, unit);
    }

    /**
     * 配置 Proxy 默认拦截器链
     *
     * @return 当前构建器
     */
    public GrpcServerBuilder configInterceptor() {
        this.serverBuilder
            .intercept(new GlobalExceptionInterceptor()) //       | -> response
            .intercept(new ContextInterceptor())         //       |    |
            .intercept(new HeaderInterceptor());         // request    |
        return this;
    }
}
