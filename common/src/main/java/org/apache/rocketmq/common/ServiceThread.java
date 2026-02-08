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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 服务线程抽象基类<br>
 * 统一封装线程启动、停止、唤醒与等待流程, 供各类后台服务线程复用
 */
public abstract class ServiceThread implements Runnable {
    /**
     * 通用日志器<br>
     * 记录服务线程生命周期与异常信息
     */
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    /**
     * 默认线程等待退出时间, 单位毫秒
     */
    private static final long JOIN_TIME = 90 * 1000;

    /**
     * 实际运行线程对象
     */
    protected Thread thread;
    /**
     * 等待点<br>
     * 用于线程在等待状态下的阻塞与唤醒协作
     */
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);
    /**
     * 通知标记<br>
     * true 表示已发出唤醒通知, 可避免重复通知
     */
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);
    /**
     * 停止标记<br>
     * true 表示线程应尽快退出运行循环
     */
    protected volatile boolean stopped = false;
    /**
     * 守护线程开关<br>
     * true 时新建线程按守护线程模式启动
     */
    protected boolean isDaemon = false;

    //Make it able to restart the thread
    // 支持线程在停止后再次启动
    /**
     * 启动状态标记<br>
     * CAS 控制仅允许一次有效启动, 避免并发重复启动
     */
    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * 默认构造方法
     */
    public ServiceThread() {

    }

    public abstract String getServiceName();

    /**
     * 启动服务线程<br>
     * 通过 started 原子状态避免重复启动
     */
    public void start() {
        log.info("Try to start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        if (!started.compareAndSet(false, true)) {
            return;
        }
        stopped = false;
        this.thread = new Thread(this, getServiceName());
        this.thread.setDaemon(isDaemon);
        this.thread.start();
        log.info("Start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
    }

    /**
     * 关闭服务线程<br>
     * 默认不主动中断线程
     */
    public void shutdown() {
        this.shutdown(false);
    }

    /**
     * 关闭服务线程<br>
     * interrupt 为 true 时会在唤醒后触发线程中断
     */
    public void shutdown(final boolean interrupt) {
        log.info("Try to shutdown service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        if (!started.compareAndSet(true, false)) {
            return;
        }
        this.stopped = true;
        log.info("shutdown thread[{}] interrupt={} ", getServiceName(), interrupt);

        //if thead is waiting, wakeup it
        // 如果线程正在等待, 先唤醒线程
        wakeup();

        try {
            if (interrupt) {
                this.thread.interrupt();
            }

            long beginTime = System.currentTimeMillis();
            if (!this.thread.isDaemon()) {
                this.thread.join(this.getJoinTime());
            }
            long elapsedTime = System.currentTimeMillis() - beginTime;
            log.info("join thread[{}], elapsed time: {}ms, join time:{}ms", getServiceName(), elapsedTime, this.getJoinTime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    public long getJoinTime() {
        return JOIN_TIME;
    }

    /**
     * 标记线程停止<br>
     * 不等待线程退出, 由运行循环感知 stopped 后自行收敛
     */
    public void makeStop() {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("makestop thread[{}] ", this.getServiceName());
    }

    /**
     * 唤醒等待线程<br>
     * 仅首次通知有效, 后续重复通知会被 hasNotified 拦截
     */
    public void wakeup() {
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
            // notify, 通知处于等待状态的线程继续执行
        }
    }

    /**
     * 按间隔等待线程继续运行, 支持被显式唤醒或超时唤醒
     */
    protected void waitForRunning(long interval) {
        if (hasNotified.compareAndSet(true, false)) {
            this.onWaitEnd();
            return;
        }

        //entry to wait
        // 进入等待前重置等待点
        waitPoint.reset();

        try {
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        } finally {
            hasNotified.set(false);
            this.onWaitEnd();
        }
    }

    /**
     * 等待结束回调<br>
     * 子类可覆盖该方法执行等待后收尾逻辑
     */
    protected void onWaitEnd() {
    }

    public boolean isStopped() {
        return stopped;
    }

    public boolean isDaemon() {
        return isDaemon;
    }

    public void setDaemon(boolean daemon) {
        isDaemon = daemon;
    }
}
