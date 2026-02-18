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

import io.grpc.Server;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * gRPC 服务生命周期管理器, 统一封装启动与优雅关闭逻辑
 */
public class GrpcServer implements StartAndShutdown {
    /**
     * Proxy 模块日志记录器
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    /**
     * gRPC 原生服务实例
     */
    private final Server server;

    /**
     * 关闭阶段最大等待时长
     */
    private final long timeout;

    /**
     * 关闭等待时长单位
     */
    private final TimeUnit unit;

    /**
     * 构造 gRPC 服务对象
     *
     * @param server gRPC 原生服务实例
     * @param timeout 关闭阶段最大等待时长
     * @param unit 关闭等待时长单位
     */
    protected GrpcServer(Server server, long timeout, TimeUnit unit) {
        this.server = server;
        this.timeout = timeout;
        this.unit = unit;
    }

    /**
     * 启动 gRPC 服务
     *
     * @throws Exception 启动失败时抛出异常
     */
    @Override
    public void start() throws Exception {
        this.server.start();
        log.info("grpc server start successfully.");
    }

    /**
     * 触发 gRPC 服务关闭并在指定时间内等待终止完成
     */
    @Override
    public void shutdown() {
        try {
            this.server.shutdown().awaitTermination(timeout, unit);
            log.info("grpc server shutdown successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
