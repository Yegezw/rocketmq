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

package org.apache.rocketmq.common;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

public class ThreadFactoryImpl implements ThreadFactory {

    /**
     * 公共日志记录器
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    /**
     * 线程序号生成器
     */
    private final AtomicLong threadIndex = new AtomicLong(0);
    /**
     * 线程名前缀
     */
    private final String threadNamePrefix;
    /**
     * 是否创建守护线程
     */
    private final boolean daemon;

    /**
     * 使用指定前缀创建线程工厂, 默认创建非守护线程
     *
     * @param threadNamePrefix 线程名前缀
     */
    public ThreadFactoryImpl(final String threadNamePrefix) {
        this(threadNamePrefix, false);
    }

    /**
     * 使用指定前缀与守护标识创建线程工厂
     *
     * @param threadNamePrefix 线程名前缀
     * @param daemon 是否为守护线程
     */
    public ThreadFactoryImpl(final String threadNamePrefix, boolean daemon) {
        this.threadNamePrefix = threadNamePrefix;
        this.daemon = daemon;
    }

    /**
     * 使用 Broker 身份构造线程工厂, 默认创建非守护线程
     *
     * @param threadNamePrefix 线程名前缀
     * @param brokerIdentity Broker 身份信息
     */
    public ThreadFactoryImpl(final String threadNamePrefix, BrokerIdentity brokerIdentity) {
        this(threadNamePrefix, false, brokerIdentity);
    }

    /**
     * 使用 Broker 身份构造线程工厂, 在容器模式下将身份标识拼接到线程名前缀
     *
     * @param threadNamePrefix 线程名前缀
     * @param daemon 是否为守护线程
     * @param brokerIdentity Broker 身份信息
     */
    public ThreadFactoryImpl(final String threadNamePrefix, boolean daemon, BrokerIdentity brokerIdentity) {
        this.daemon = daemon;
        if (brokerIdentity != null && brokerIdentity.isInBrokerContainer()) {
            this.threadNamePrefix = brokerIdentity.getIdentifier() + threadNamePrefix;
        } else {
            this.threadNamePrefix = threadNamePrefix;
        }
    }

    /**
     * 创建新线程并统一设置名称, 守护属性与异常处理器
     *
     * @param r 线程执行任务
     * @return 新建线程对象
     */
    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, threadNamePrefix + this.threadIndex.incrementAndGet());
        thread.setDaemon(daemon);

        // Log all uncaught exception
        // 统一记录线程未捕获异常, 便于定位并发故障
        thread.setUncaughtExceptionHandler((t, e) ->
            LOGGER.error("[BUG] Thread has an uncaught exception, threadId={}, threadName={}",
                t.getId(), t.getName(), e));

        return thread;
    }
}
