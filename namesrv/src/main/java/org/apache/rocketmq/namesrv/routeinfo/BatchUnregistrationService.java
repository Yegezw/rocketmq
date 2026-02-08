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

package org.apache.rocketmq.namesrv.routeinfo;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.header.namesrv.UnRegisterBrokerRequestHeader;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * BatchUnregistrationService provides a mechanism to unregister brokers in batch manner, which speeds up broker-offline
 * process.
 * <br>
 * BatchUnregistrationService 提供批量注销 Broker 的机制, 可加速 Broker 下线流程
 */
public class BatchUnregistrationService extends ServiceThread {
    /**
     * 路由信息管理器, 负责执行 Broker 注销
     */
    private final RouteInfoManager routeInfoManager;
    /**
     * 注销请求队列, 用于批量聚合待处理请求 3000
     */
    private BlockingQueue<UnRegisterBrokerRequestHeader> unregistrationQueue;
    /**
     * NameServer 日志对象
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    /**
     * 创建 BatchUnregistrationService 实例
     *
     * @param routeInfoManager 路由信息管理器
     * @param namesrvConfig NameServer 配置
     */
    public BatchUnregistrationService(RouteInfoManager routeInfoManager, NamesrvConfig namesrvConfig) {
        this.routeInfoManager = routeInfoManager;
        this.unregistrationQueue = new LinkedBlockingQueue<>(namesrvConfig.getUnRegisterBrokerQueueCapacity());
    }

    /**
     * Submits an unregister request to this queue.
     * 提交一个 Broker 注销请求到队列
     *
     * @param unRegisterRequest the request to submit 待提交的请求
     * @return {@code true} if the request was added to this queue, else {@code false}
     */
    public boolean submit(UnRegisterBrokerRequestHeader unRegisterRequest) {
        return unregistrationQueue.offer(unRegisterRequest);
    }

    /**
     * 获取服务名称
     *
     * @return 服务名称
     */
    @Override
    public String getServiceName() {
        return BatchUnregistrationService.class.getName();
    }

    /**
     * 服务主循环, 拉取并批量处理 Broker 注销请求
     */
    @Override
    public void run() {
        while (!this.isStopped()) {
            try {
                // 先阻塞获取一个请求作为本批次基准
                final UnRegisterBrokerRequestHeader request = unregistrationQueue.take();

                // 再将当前队列中的其余请求批量取出
                Set<UnRegisterBrokerRequestHeader> unregistrationRequests = new HashSet<>();
                unregistrationQueue.drainTo(unregistrationRequests);

                // Add polled request
                // 添加已拉取的请求
                unregistrationRequests.add(request);

                // 执行批量注销
                this.routeInfoManager.unRegisterBroker(unregistrationRequests);
            } catch (Throwable e) {
                log.error("Handle unregister broker request failed", e);
            }
        }
    }

    // For test only
    // 仅用于测试
    /**
     * 获取当前注销队列长度
     *
     * @return 队列长度
     */
    int queueLength() {
        return this.unregistrationQueue.size();
    }
}
