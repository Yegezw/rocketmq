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
package org.apache.rocketmq.broker.latency;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.AbstractBrokerRunnable;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.future.FutureTaskExt;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * BrokerFastFailure will cover {@link BrokerController#getSendThreadPoolQueue()} and {@link BrokerController#getPullThreadPoolQueue()}
 * <br>
 * Broker 快速失败组件, 负责扫描发送拉取等请求队列, 并在排队超时时主动返回系统繁忙
 */
public class BrokerFastFailure {
    /**
     * Broker 日志记录器, 用于输出快速失败过程中的异常与诊断信息
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    /**
     * 定时调度器, 周期性触发过期请求清理任务
     */
    private final ScheduledExecutorService scheduledExecutorService;
    /**
     * Broker 控制器引用, 用于访问线程池队列与运行时配置
     */
    private final BrokerController brokerController;

    /**
     * 最近一次打印 jstack 的时间戳, 用于限制线程栈日志输出频率
     */
    private volatile long jstackTime = System.currentTimeMillis();

    /**
     * 待清理请求队列集合, 每个元素包含队列实例与对应超时时间提供器
     */
    private final List<Pair<BlockingQueue<Runnable>, Supplier<Long>>> cleanExpiredRequestQueueList = new ArrayList<>();

    /**
     * 构造快速失败组件并初始化默认清理队列
     *
     * @param brokerController Broker 控制器
     */
    public BrokerFastFailure(final BrokerController brokerController) {
        this.brokerController = brokerController;
        initCleanExpiredRequestQueueList();
        this.scheduledExecutorService = ThreadUtils.newScheduledThreadPool(1,
            new ThreadFactoryImpl("BrokerFastFailureScheduledThread", true,
                brokerController == null ? null : brokerController.getBrokerConfig()));
    }

    /**
     * 初始化内置线程池队列清理列表, 覆盖发送拉取心跳事务确认与管理请求
     */
    private void initCleanExpiredRequestQueueList() {
        cleanExpiredRequestQueueList.add(new Pair<>(this.brokerController.getSendThreadPoolQueue(), () -> this.brokerController.getBrokerConfig().getWaitTimeMillsInSendQueue()));
        cleanExpiredRequestQueueList.add(new Pair<>(this.brokerController.getPullThreadPoolQueue(), () -> this.brokerController.getBrokerConfig().getWaitTimeMillsInPullQueue()));
        cleanExpiredRequestQueueList.add(new Pair<>(this.brokerController.getLitePullThreadPoolQueue(), () -> this.brokerController.getBrokerConfig().getWaitTimeMillsInLitePullQueue()));
        cleanExpiredRequestQueueList.add(new Pair<>(this.brokerController.getHeartbeatThreadPoolQueue(), () -> this.brokerController.getBrokerConfig().getWaitTimeMillsInHeartbeatQueue()));
        cleanExpiredRequestQueueList.add(new Pair<>(this.brokerController.getEndTransactionThreadPoolQueue(), () -> this.brokerController.getBrokerConfig().getWaitTimeMillsInTransactionQueue()));
        cleanExpiredRequestQueueList.add(new Pair<>(this.brokerController.getAckThreadPoolQueue(), () -> this.brokerController.getBrokerConfig().getWaitTimeMillsInAckQueue()));
        cleanExpiredRequestQueueList.add(new Pair<>(this.brokerController.getAdminBrokerThreadPoolQueue(), () -> this.brokerController.getBrokerConfig().getWaitTimeMillsInAdminBrokerQueue()));
    }

    /**
     * 将线程池中的 Runnable 尝试转换为 RequestTask
     *
     * @param runnable 待转换任务
     * @return 转换成功返回 RequestTask, 否则返回 null
     */
    public static RequestTask castRunnable(final Runnable runnable) {
        try {
            if (runnable instanceof FutureTaskExt) {
                FutureTaskExt object = (FutureTaskExt) runnable;
                return (RequestTask) object.getRunnable();
            }
        } catch (Throwable e) {
            LOGGER.error(String.format("castRunnable exception, %s", runnable.getClass().getName()), e);
        }

        return null;
    }

    /**
     * 启动快速失败定时任务, 按固定周期清理等待超时的请求
     */
    public void start() {
        this.scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.brokerController.getBrokerConfig()) {
            /**
             * 执行周期清理逻辑, 仅在开启快速失败开关时生效
             */
            @Override
            public void run0() {
                if (brokerController.getBrokerConfig().isBrokerFastFailureEnable()) {
                    cleanExpiredRequest();
                }
            }
        }, 1000, 10, TimeUnit.MILLISECONDS);
    }

    /**
     * 清理所有需要快速失败处理的队列请求, 包括 PageCache 繁忙与普通超时两类场景
     */
    private void cleanExpiredRequest() {

        while (this.brokerController.getMessageStore().isOSPageCacheBusy()) {
            try {
                if (!this.brokerController.getSendThreadPoolQueue().isEmpty()) {
                    final Runnable runnable = this.brokerController.getSendThreadPoolQueue().poll(0, TimeUnit.SECONDS);
                    if (null == runnable) {
                        break;
                    }

                    final RequestTask rt = castRunnable(runnable);
                    if (rt != null) {
                        rt.returnResponse(RemotingSysResponseCode.SYSTEM_BUSY, String.format(
                            "[PCBUSY_CLEAN_QUEUE]broker busy, start flow control for a while, period in queue: %sms, "
                                + "size of queue: %d",
                            System.currentTimeMillis() - rt.getCreateTimestamp(),
                            this.brokerController.getSendThreadPoolQueue().size()));
                    }
                } else {
                    break;
                }
            } catch (Throwable ignored) {
            }
        }

        for (Pair<BlockingQueue<Runnable>, Supplier<Long>> pair : cleanExpiredRequestQueueList) {
            cleanExpiredRequestInQueue(pair.getObject1(), pair.getObject2().get());
        }
    }

    /**
     * 清理单个队列中的超时请求, 超时任务会被移除并直接返回系统繁忙响应
     *
     * @param blockingQueue 待清理任务队列
     * @param maxWaitTimeMillsInQueue 队列允许的最大等待时间, 单位毫秒
     */
    void cleanExpiredRequestInQueue(final BlockingQueue<Runnable> blockingQueue, final long maxWaitTimeMillsInQueue) {
        while (true) {
            try {
                if (!blockingQueue.isEmpty()) {
                    final Runnable runnable = blockingQueue.peek();
                    if (null == runnable) {
                        break;
                    }
                    final RequestTask rt = castRunnable(runnable);
                    if (rt == null || rt.isStopRun()) {
                        break;
                    }

                    final long behind = System.currentTimeMillis() - rt.getCreateTimestamp();
                    if (behind >= maxWaitTimeMillsInQueue) {
                        if (blockingQueue.remove(runnable)) {
                            rt.setStopRun(true);
                            rt.returnResponse(RemotingSysResponseCode.SYSTEM_BUSY, String.format("[TIMEOUT_CLEAN_QUEUE]broker busy, start flow control for a while, period in queue: %sms, size of queue: %d", behind, blockingQueue.size()));
                            if (System.currentTimeMillis() - jstackTime > 15000) {
                                jstackTime = System.currentTimeMillis();
                                LOGGER.warn("broker jstack \n " + UtilAll.jstack());
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            } catch (Throwable ignored) {
            }
        }
    }

    /**
     * 动态追加需要执行超时清理的队列
     *
     * @param cleanExpiredRequestQueue 新增队列
     * @param maxWaitTimeMillsInQueueSupplier 队列超时时间提供器
     */
    public synchronized void addCleanExpiredRequestQueue(BlockingQueue<Runnable> cleanExpiredRequestQueue,
        Supplier<Long> maxWaitTimeMillsInQueueSupplier) {
        cleanExpiredRequestQueueList.add(new Pair<>(cleanExpiredRequestQueue, maxWaitTimeMillsInQueueSupplier));
    }

    /**
     * 关闭快速失败定时任务调度器
     */
    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }
}
