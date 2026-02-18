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

package org.apache.rocketmq.common.thread;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * 基于队列积压长度的线程池监控器
 */
public class ThreadPoolQueueSizeMonitor implements ThreadPoolStatusMonitor {

    /**
     * 队列容量上限
     */
    private final int maxQueueCapacity;

    /**
     * 构建队列大小监控器
     *
     * @param maxQueueCapacity 队列容量上限
     */
    public ThreadPoolQueueSizeMonitor(int maxQueueCapacity) {
        this.maxQueueCapacity = maxQueueCapacity;
    }

    /**
     * 返回监控项名称
     *
     * @return 监控项名称
     */
    @Override
    public String describe() {
        return "queueSize";
    }

    /**
     * 计算当前队列长度
     *
     * @param executor 线程池执行器
     * @return 队列长度
     */
    @Override
    public double value(ThreadPoolExecutor executor) {
        return executor.getQueue().size();
    }

    /**
     * 当队列使用率超过阈值时触发线程栈打印
     *
     * @param executor 线程池执行器
     * @param value 当前队列长度
     * @return 是否打印线程栈
     */
    @Override
    public boolean needPrintJstack(ThreadPoolExecutor executor, double value) {
        return value > maxQueueCapacity * 0.85;
    }
}
