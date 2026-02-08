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
package org.apache.rocketmq.remoting.common;


import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * Base class for background thread
 * <br>
 * 后台线程基类
 */
public abstract class ServiceThread implements Runnable {
    /**
     * Remoting 日志对象
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);

    /**
     * 线程 Join 默认超时时间, 单位为毫秒
     */
    private static final long JOIN_TIME = 90 * 1000;
    /**
     * 服务线程对象
     */
    protected final Thread thread;
    /**
     * 通知标记, 表示是否已发送过唤醒通知
     */
    protected volatile boolean hasNotified = false;
    /**
     * 停止标记, 表示服务线程是否已停止
     */
    protected volatile boolean stopped = false;

    /**
     * 创建 ServiceThread 实例
     */
    public ServiceThread() {
        // 使用服务名创建线程, 便于排查线程状态
        this.thread = new Thread(this, this.getServiceName());
    }

    /**
     * 获取服务名称
     *
     * @return 服务名称
     */
    public abstract String getServiceName();

    /**
     * 启动服务线程
     */
    public void start() {
        this.thread.start();
    }

    /**
     * 关闭服务线程, 默认不发送中断
     */
    public void shutdown() {
        this.shutdown(false);
    }

    /**
     * 关闭服务线程
     *
     * @param interrupt 是否发送线程中断
     */
    public void shutdown(final boolean interrupt) {
        // 设置停止标记并尝试唤醒等待中的线程
        this.stopped = true;
        log.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);
        synchronized (this) {
            if (!this.hasNotified) {
                this.hasNotified = true;
                this.notify();
            }
        }

        try {
            // 按需中断线程
            if (interrupt) {
                this.thread.interrupt();
            }

            // 等待线程退出并记录耗时
            long beginTime = System.currentTimeMillis();
            this.thread.join(this.getJointime());
            long elapsedTime = System.currentTimeMillis() - beginTime;
            log.info("join thread " + this.getServiceName() + " elapsed time(ms) " + elapsedTime + " "
                + this.getJointime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    /**
     * 获取 Join 超时时间
     *
     * @return Join 超时时间, 单位为毫秒
     */
    public long getJointime() {
        return JOIN_TIME;
    }

    /**
     * 判断服务线程是否已停止
     *
     * @return true 表示已停止
     */
    public boolean isStopped() {
        return stopped;
    }
}
