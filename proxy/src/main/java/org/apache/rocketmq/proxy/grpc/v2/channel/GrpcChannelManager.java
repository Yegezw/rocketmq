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

package org.apache.rocketmq.proxy.grpc.v2.channel;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayResult;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

/**
 * gRPC 客户端通道管理器, 负责通道生命周期与异步回包关联管理
 */
public class GrpcChannelManager implements StartAndShutdown {
    /**
     * 代理转发服务
     */
    private final ProxyRelayService proxyRelayService;
    /**
     * gRPC 客户端设置管理器
     */
    private final GrpcClientSettingsManager grpcClientSettingsManager;
    /**
     * clientId 到通道对象的映射
     */
    protected final ConcurrentMap<String, GrpcClientChannel> clientIdChannelMap = new ConcurrentHashMap<>();

    /**
     * nonce 生成器
     */
    protected final AtomicLong nonceIdGenerator = new AtomicLong(0);
    /**
     * nonce 到结果 Future 的映射
     */
    protected final ConcurrentMap<String /* nonce */, ResultFuture> resultNonceFutureMap = new ConcurrentHashMap<>();

    /**
     * 定时扫描任务线程池
     */
    protected final ScheduledExecutorService scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor(
        new ThreadFactoryImpl("GrpcChannelManager_")
    );

    /**
     * 构造通道管理器
     *
     * @param proxyRelayService 代理转发服务
     * @param grpcClientSettingsManager gRPC 客户端设置管理器
     */
    public GrpcChannelManager(ProxyRelayService proxyRelayService, GrpcClientSettingsManager grpcClientSettingsManager) {
        this.proxyRelayService = proxyRelayService;
        this.grpcClientSettingsManager = grpcClientSettingsManager;
        this.init();
    }

    /**
     * 初始化定时扫描任务
     */
    protected void init() {
        this.scheduledExecutorService.scheduleAtFixedRate(
            this::scanExpireResultFuture,
            10, 1, TimeUnit.SECONDS
        );
    }

    /**
     * 按 clientId 创建或获取 gRPC 客户端通道
     *
     * @param ctx Proxy 上下文
     * @param clientId 客户端标识
     * @return gRPC 客户端通道
     */
    public GrpcClientChannel createChannel(ProxyContext ctx, String clientId) {
        return this.clientIdChannelMap.computeIfAbsent(clientId,
            k -> new GrpcClientChannel(proxyRelayService, grpcClientSettingsManager, this, ctx, clientId));
    }

    public GrpcClientChannel getChannel(String clientId) {
        return clientIdChannelMap.get(clientId);
    }

    /**
     * 按 clientId 删除并返回通道
     *
     * @param clientId 客户端标识
     * @return 被删除的通道, 不存在时返回 null
     */
    public GrpcClientChannel removeChannel(String clientId) {
        return this.clientIdChannelMap.remove(clientId);
    }

    /**
     * 注册异步响应 Future 并返回关联 nonce
     *
     * @param responseFuture 异步响应 Future
     * @param <T> 响应类型
     * @return 关联 nonce
     */
    public <T> String addResponseFuture(CompletableFuture<ProxyRelayResult<T>> responseFuture) {
        String nonce = this.nextNonce();
        this.resultNonceFutureMap.put(nonce, new ResultFuture<>(responseFuture));
        return nonce;
    }

    public <T> CompletableFuture<ProxyRelayResult<T>> getAndRemoveResponseFuture(String nonce) {
        ResultFuture<T> resultFuture = this.resultNonceFutureMap.remove(nonce);
        if (resultFuture != null) {
            return resultFuture.future;
        }
        return null;
    }

    /**
     * 生成下一次请求 nonce
     *
     * @return nonce 字符串
     */
    protected String nextNonce() {
        return String.valueOf(this.nonceIdGenerator.getAndIncrement());
    }

    /**
     * 扫描并完成超时的异步响应 Future
     */
    protected void scanExpireResultFuture() {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        long timeOutMs = TimeUnit.SECONDS.toMillis(proxyConfig.getGrpcProxyRelayRequestTimeoutInSeconds());

        Set<String> nonceSet = this.resultNonceFutureMap.keySet();
        for (String nonce : nonceSet) {
            ResultFuture<?> resultFuture = this.resultNonceFutureMap.get(nonce);
            if (resultFuture == null) {
                continue;
            }
            if (System.currentTimeMillis() - resultFuture.createTime > timeOutMs) {
                resultFuture = this.resultNonceFutureMap.remove(nonce);
                if (resultFuture != null) {
                    resultFuture.future.complete(new ProxyRelayResult<>(ResponseCode.SYSTEM_BUSY, "call remote timeout", null));
                }
            }
        }
    }

    /**
     * 关闭通道管理器资源
     *
     * @throws Exception 关闭异常
     */
    @Override
    public void shutdown() throws Exception {
        this.scheduledExecutorService.shutdown();
    }

    /**
     * 启动通道管理器
     *
     * @throws Exception 启动异常
     */
    @Override
    public void start() throws Exception {

    }

    /**
     * 结果 Future 包装对象
     *
     * @param <T> 响应类型
     */
    protected static class ResultFuture<T> {
        /**
         * 异步响应 Future
         */
        public CompletableFuture<ProxyRelayResult<T>> future;
        /**
         * 创建时间戳, 单位毫秒
         */
        public long createTime = System.currentTimeMillis();

        /**
         * 构造结果 Future 包装对象
         *
         * @param future 异步响应 Future
         */
        public ResultFuture(CompletableFuture<ProxyRelayResult<T>> future) {
            this.future = future;
        }
    }
}
