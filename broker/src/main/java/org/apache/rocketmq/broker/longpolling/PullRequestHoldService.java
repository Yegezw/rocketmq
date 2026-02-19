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
package org.apache.rocketmq.broker.longpolling;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.exception.ConsumeQueueException;

/**
 * Pull 长轮询挂起服务, 负责缓存请求并在消息到达或超时后唤醒
 */
public class PullRequestHoldService extends ServiceThread {
    /**
     * Pull 挂起服务日志器
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    /**
     * 主题与队列键分隔符
     */
    protected static final String TOPIC_QUEUEID_SEPARATOR = "@";
    /**
     * broker 主控制器, 用于访问存储与请求处理组件
     */
    protected final BrokerController brokerController;
    /**
     * 轻量系统时钟, 降低高频时间调用开销
     */
    private final SystemClock systemClock = new SystemClock();
    /**
     * 挂起请求表, 键为 topic@queueId
     */
    protected ConcurrentMap<String/* topic@queueId, 主题与队列编号组合键 */, ManyPullRequest> pullRequestTable =
        new ConcurrentHashMap<>(1024);

    /**
     * 创建 Pull 挂起服务
     *
     * @param brokerController broker 控制器
     */
    public PullRequestHoldService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 将 pull 请求挂起到指定 topic 队列下, 等待消息到达或超时再执行
     *
     * @param topic 主题名
     * @param queueId 队列编号
     * @param pullRequest 待挂起请求
     */
    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }

        pullRequest.getRequestCommand().setSuspended(true);
        mpr.addPullRequest(pullRequest);
    }

    /**
     * 构建挂起请求映射键
     *
     * @param topic 主题名
     * @param queueId 队列编号
     * @return topic 与 queueId 拼接后的唯一键
     */
    private String buildKey(final String topic, final int queueId) {
        StringBuilder sb = new StringBuilder(topic.length() + 5);
        sb.append(topic);
        sb.append(TOPIC_QUEUEID_SEPARATOR);
        sb.append(queueId);
        return sb.toString();
    }

    /**
     * 周期性检查挂起请求, 根据长轮询或短轮询配置选择扫描间隔
     */
    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {
                if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                    this.waitForRunning(5 * 1000);
                } else {
                    this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
                }

                long beginLockTimestamp = this.systemClock.now();
                this.checkHoldRequest();
                long costTime = this.systemClock.now() - beginLockTimestamp;
                if (costTime > 5 * 1000) {
                    log.warn("PullRequestHoldService: check hold pull request cost {}ms", costTime);
                }
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info("{} service end", this.getServiceName());
    }

    @Override
    public String getServiceName() {
        if (brokerController != null && brokerController.getBrokerConfig().isInBrokerContainer()) {
            return this.brokerController.getBrokerIdentity().getIdentifier() + PullRequestHoldService.class.getSimpleName();
        }
        return PullRequestHoldService.class.getSimpleName();
    }

    /**
     * 扫描全部挂起键并以当前最大偏移量触发唤醒判断
     */
    protected void checkHoldRequest() {
        for (String key : this.pullRequestTable.keySet()) {
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (2 == kArray.length) {
                String topic = kArray[0];
                int queueId = Integer.parseInt(kArray[1]);
                try {
                    final long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                    this.notifyMessageArriving(topic, queueId, offset);
                } catch (Throwable e) {
                    log.error(
                        "PullRequestHoldService: failed to check hold request failed, topic={}, queueId={}", topic,
                        queueId, e);
                }
            }
        }
    }

    /**
     * 使用最大偏移量触发消息到达通知
     *
     * @param topic 主题名
     * @param queueId 队列编号
     * @param maxOffset 当前最大偏移量
     */
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset) {
        notifyMessageArriving(topic, queueId, maxOffset, null, 0, null, null);
    }

    /**
     * 按过滤规则唤醒符合条件的挂起请求, 并将未命中请求重新入队
     *
     * @param topic 主题名
     * @param queueId 队列编号
     * @param maxOffset 当前最大偏移量
     * @param tagsCode 标签码
     * @param msgStoreTime 消息存储时间
     * @param filterBitMap 过滤位图
     * @param properties 消息属性
     */
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset, final Long tagsCode,
        long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (mpr != null) {
            List<PullRequest> requestList = mpr.cloneListAndClear();
            if (requestList != null) {
                List<PullRequest> replayList = new ArrayList<>();

                for (PullRequest request : requestList) {
                    long newestOffset = maxOffset;
                    if (newestOffset <= request.getPullFromThisOffset()) {
                        try {
                            newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                        } catch (ConsumeQueueException e) {
                            log.error("Failed tp get max offset in queue", e);
                            continue;
                        }
                    }

                    if (newestOffset > request.getPullFromThisOffset()) {
                        boolean match = request.getMessageFilter().isMatchedByConsumeQueue(tagsCode,
                            new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
                        // match by bit map, need eval again when properties is not null, 先按位图匹配, 存在属性时再做二次判定
                        if (match && properties != null) {
                            match = request.getMessageFilter().isMatchedByCommitLog(null, properties);
                        }

                        if (match) {
                            try {
                                this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                    request.getRequestCommand());
                            } catch (Throwable e) {
                                log.error(
                                    "PullRequestHoldService#notifyMessageArriving: failed to execute request when "
                                        + "message matched, topic={}, queueId={}", topic, queueId, e);
                            }
                            continue;
                        }
                    }

                    if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                        try {
                            this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                request.getRequestCommand());
                        } catch (Throwable e) {
                            log.error(
                                "PullRequestHoldService#notifyMessageArriving: failed to execute request when time's "
                                    + "up, topic={}, queueId={}", topic, queueId, e);
                        }
                        continue;
                    }

                    replayList.add(request);
                }

                if (!replayList.isEmpty()) {
                    mpr.addPullRequest(replayList);
                }
            }
        }
    }

    /**
     * 主从切换为 master 后主动唤醒全部挂起请求, 避免消费者长时间等待
     */
    public void notifyMasterOnline() {
        for (ManyPullRequest mpr : this.pullRequestTable.values()) {
            if (mpr == null || mpr.isEmpty()) {
                continue;
            }
            for (PullRequest request : mpr.cloneListAndClear()) {
                try {
                    log.info("notify master online, wakeup {} {}", request.getClientChannel(), request.getRequestCommand());
                    this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                        request.getRequestCommand());
                } catch (Throwable e) {
                    log.error("execute request when master online failed.", e);
                }
            }
        }

    }
}
