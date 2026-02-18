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

package org.apache.rocketmq.srvutil;

import org.apache.rocketmq.logging.org.slf4j.Logger;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link ShutdownHookThread} is the standard hook for filtersrv and namesrv modules.
 * Through {@link Callable} interface, this hook can customization operations in anywhere.
 * <br>
 * {@link ShutdownHookThread} 是 filtersrv 与 namesrv 模块的标准关闭钩子线程<br>
 * 通过 {@link Callable} 接口可在回调中自定义关闭动作
 */
public class ShutdownHookThread extends Thread {
    /**
     * 标识关闭流程是否已执行
     */
    private volatile boolean hasShutdown = false;
    /**
     * 记录关闭钩子触发次数
     */
    private AtomicInteger shutdownTimes = new AtomicInteger(0);
    /**
     * 关闭流程日志记录器
     */
    private final Logger log;
    /**
     * 关闭阶段回调函数
     */
    private final Callable callback;

    /**
     * Create the standard hook thread, with a call back, by using {@link Callable} interface.
     * <br>
     * 创建标准关闭钩子线程, 通过 {@link Callable} 回调执行自定义关闭逻辑
     *
     * @param log The log instance is used in hook thread, 用于输出关闭阶段日志的记录器
     * @param callback The call back function, 关闭阶段执行的回调函数
     */
    public ShutdownHookThread(Logger log, Callable callback) {
        super("ShutdownHook");
        this.log = log;
        this.callback = callback;
    }

    /**
     * Thread run method.<br>
     * Invoke when the jvm shutdown.<br>
     * 1. count the invocation times.<br>
     * 2. execute the {@link ShutdownHookThread#callback}, and time it.
     * <br>
     * 线程运行入口<br>
     * 在 JVM 关闭时触发<br>
     * 1. 统计钩子触发次数<br>
     * 2. 执行 {@link ShutdownHookThread#callback} 并统计耗时
     */
    @Override
    public void run() {
        synchronized (this) {
            log.info("shutdown hook was invoked, " + this.shutdownTimes.incrementAndGet() + " times.");
            if (!this.hasShutdown) {
                this.hasShutdown = true;
                long beginTime = System.currentTimeMillis();
                try {
                    this.callback.call();
                } catch (Exception e) {
                    log.error("shutdown hook callback invoked failure.", e);
                }
                long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                log.info("shutdown hook done, consuming time total(ms): " + consumingTimeTotal);
            }
        }
    }
}
