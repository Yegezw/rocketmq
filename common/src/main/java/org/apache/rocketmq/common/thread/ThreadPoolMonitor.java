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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * 线程池状态监控中心
 */
public class ThreadPoolMonitor {
    /**
     * 线程栈日志记录器
     */
    private static Logger jstackLogger = LoggerFactory.getLogger(ThreadPoolMonitor.class);
    /**
     * 水位监控日志记录器
     */
    private static Logger waterMarkLogger = LoggerFactory.getLogger(ThreadPoolMonitor.class);

    /**
     * 被监控线程池列表
     */
    private static final List<ThreadPoolWrapper> MONITOR_EXECUTOR = new CopyOnWriteArrayList<>();
    /**
     * 监控调度线程池
     */
    private static final ScheduledExecutorService MONITOR_SCHEDULED = ThreadUtils.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("ThreadPoolMonitor-%d").build()
    );

    /**
     * 线程池状态打印周期毫秒值
     */
    private static volatile long threadPoolStatusPeriodTime = TimeUnit.SECONDS.toMillis(3);
    /**
     * 是否启用线程栈打印
     */
    private static volatile boolean enablePrintJstack = true;
    /**
     * 线程栈打印最小间隔毫秒值
     */
    private static volatile long jstackPeriodTime = 60000;
    /**
     * 上次线程栈打印时间戳
     */
    private static volatile long jstackTime = System.currentTimeMillis();

    /**
     * 配置线程池监控参数
     *
     * @param jstackLoggerConfig 线程栈日志记录器
     * @param waterMarkLoggerConfig 水位日志记录器
     * @param enablePrintJstack 是否启用线程栈打印
     * @param jstackPeriodTimeConfig 线程栈打印最小间隔毫秒值
     * @param threadPoolStatusPeriodTimeConfig 状态打印周期毫秒值
     */
    public static void config(Logger jstackLoggerConfig, Logger waterMarkLoggerConfig,
        boolean enablePrintJstack, long jstackPeriodTimeConfig, long threadPoolStatusPeriodTimeConfig) {
        jstackLogger = jstackLoggerConfig;
        waterMarkLogger = waterMarkLoggerConfig;
        threadPoolStatusPeriodTime = threadPoolStatusPeriodTimeConfig;
        ThreadPoolMonitor.enablePrintJstack = enablePrintJstack;
        jstackPeriodTime = jstackPeriodTimeConfig;
    }

    /**
     * 创建线程池并加入监控
     *
     * @param corePoolSize 核心线程数
     * @param maximumPoolSize 最大线程数
     * @param keepAliveTime 空闲线程存活时长
     * @param unit 时间单位
     * @param name 线程池名称
     * @param queueCapacity 队列容量
     * @return 线程池执行器
     */
    public static ThreadPoolExecutor createAndMonitor(int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        String name,
        int queueCapacity) {
        return createAndMonitor(corePoolSize, maximumPoolSize, keepAliveTime, unit, name, queueCapacity, Collections.emptyList());
    }

    /**
     * 创建线程池并绑定可变参数监控器
     *
     * @param corePoolSize 核心线程数
     * @param maximumPoolSize 最大线程数
     * @param keepAliveTime 空闲线程存活时长
     * @param unit 时间单位
     * @param name 线程池名称
     * @param queueCapacity 队列容量
     * @param threadPoolStatusMonitors 自定义监控器数组
     * @return 线程池执行器
     */
    public static ThreadPoolExecutor createAndMonitor(int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        String name,
        int queueCapacity,
        ThreadPoolStatusMonitor... threadPoolStatusMonitors) {
        return createAndMonitor(corePoolSize, maximumPoolSize, keepAliveTime, unit, name, queueCapacity,
            Lists.newArrayList(threadPoolStatusMonitors));
    }

    /**
     * 创建线程池并绑定自定义监控器
     *
     * @param corePoolSize 核心线程数
     * @param maximumPoolSize 最大线程数
     * @param keepAliveTime 空闲线程存活时长
     * @param unit 时间单位
     * @param name 线程池名称
     * @param queueCapacity 队列容量
     * @param threadPoolStatusMonitors 自定义监控器列表
     * @return 线程池执行器
     */
    public static ThreadPoolExecutor createAndMonitor(int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        String name,
        int queueCapacity,
        List<ThreadPoolStatusMonitor> threadPoolStatusMonitors) {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) ThreadUtils.newThreadPoolExecutor(
            corePoolSize,
            maximumPoolSize,
            keepAliveTime,
            unit,
            new LinkedBlockingQueue<>(queueCapacity),
            new ThreadFactoryBuilder().setNameFormat(name + "-%d").build(),
            new ThreadPoolExecutor.DiscardOldestPolicy());
        List<ThreadPoolStatusMonitor> printers = Lists.newArrayList(new ThreadPoolQueueSizeMonitor(queueCapacity));
        printers.addAll(threadPoolStatusMonitors);

        MONITOR_EXECUTOR.add(ThreadPoolWrapper.builder()
            .name(name)
            .threadPoolExecutor(executor)
            .statusPrinters(printers)
            .build());
        return executor;
    }

    /**
     * 遍历所有线程池并打印监控水位
     */
    public static void logThreadPoolStatus() {
        for (ThreadPoolWrapper threadPoolWrapper : MONITOR_EXECUTOR) {
            List<ThreadPoolStatusMonitor> monitors = threadPoolWrapper.getStatusPrinters();
            for (ThreadPoolStatusMonitor monitor : monitors) {
                double value = monitor.value(threadPoolWrapper.getThreadPoolExecutor());
                String nameFormatted = String.format("%-40s", threadPoolWrapper.getName());
                String descFormatted = String.format("%-12s", monitor.describe());
                waterMarkLogger.info("{}{}{}", nameFormatted, descFormatted, value);
                if (enablePrintJstack) {
                    if (monitor.needPrintJstack(threadPoolWrapper.getThreadPoolExecutor(), value) &&
                        System.currentTimeMillis() - jstackTime > jstackPeriodTime) {
                        jstackTime = System.currentTimeMillis();
                        jstackLogger.warn("jstack start\n{}", UtilAll.jstack());
                    }
                }
            }
        }
    }

    /**
     * 启动线程池状态定时监控任务
     */
    public static void init() {
        MONITOR_SCHEDULED.scheduleAtFixedRate(ThreadPoolMonitor::logThreadPoolStatus, 20,
            threadPoolStatusPeriodTime, TimeUnit.MILLISECONDS);
    }

    /**
     * 关闭线程池监控调度任务
     */
    public static void shutdown() {
        MONITOR_SCHEDULED.shutdown();
    }
}
