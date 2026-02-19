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
package org.apache.rocketmq.broker.coldctr;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.longpolling.PullRequest;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * just requests are type of pull have the qualification to be put into this hold queue.
 * if the pull request is reading cold data and that request will be cold at the first time,
 * then the pull request will be cold in this @code pullRequestLinkedBlockingQueue,
 * in @code coldTimeoutMillis later the pull request will be warm and marked holded
 * <br>
 * 仅处理拉消息类型请求并进入该挂起队列, 用于冷数据限流场景<br>
 * 当请求首次命中冷读限流时先放入冷读挂起队列, 到达超时时间后再唤醒执行
 */
public class ColdDataPullRequestHoldService extends ServiceThread {

    /**
     * 冷读控制日志记录器, 输出挂起队列扫描与唤醒结果
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_COLDCTR_LOGGER_NAME);
    /**
     * 请求扩展字段键, 标记请求已通过冷挂起流程避免重复挂起
     */
    public static final String NO_SUSPEND_KEY = "_noSuspend_";

    /**
     * 冷读请求挂起超时时间, 到时后强制唤醒请求继续处理
     */
    private final long coldHoldTimeoutMillis = 3000;
    /**
     * 系统时钟封装, 用于统计定时扫描耗时
     */
    private final SystemClock systemClock = new SystemClock();
    /**
     * broker 控制器引用, 用于访问配置和唤醒请求处理器
     */
    private final BrokerController brokerController;
    /**
     * 冷读挂起请求队列, 保存等待超时后唤醒的 PullRequest
     */
    private final LinkedBlockingQueue<PullRequest> pullRequestColdHoldQueue = new LinkedBlockingQueue<>(10000);

    /**
     * 将冷读请求放入挂起队列, 仅在 broker 启用冷数据流控时生效
     *
     * @param pullRequest pull request, 需要挂起的拉消息请求
     */
    public void suspendColdDataReadRequest(PullRequest pullRequest) {
        if (this.brokerController.getMessageStoreConfig().isColdDataFlowControlEnable()) {
            pullRequestColdHoldQueue.offer(pullRequest);
        }
    }

    /**
     * 构造冷读挂起服务, 绑定 broker 控制器上下文
     *
     * @param brokerController broker controller, 当前 broker 控制器
     */
    public ColdDataPullRequestHoldService(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public String getServiceName() {
        return ColdDataPullRequestHoldService.class.getSimpleName();
    }

    /**
     * 后台循环扫描冷读挂起请求, 到期后唤醒并重新执行拉消息逻辑
     */
    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {
                if (!this.brokerController.getMessageStoreConfig().isColdDataFlowControlEnable()) {
                    this.waitForRunning(20 * 1000);
                } else {
                    this.waitForRunning(5 * 1000);
                }
                long beginClockTimestamp = this.systemClock.now();
                this.checkColdDataPullRequest();
                long costTime = this.systemClock.now() - beginClockTimestamp;
                log.info("[{}] checkColdDataPullRequest-cost {} ms.", costTime > 5 * 1000 ? "NOTIFYME" : "OK", costTime);
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception", e);
            }
        }
        log.info("{} service end", this.getServiceName());
    }

    /**
     * 检查挂起队列中的请求是否到期, 到期后标记免挂起并触发唤醒执行
     */
    private void checkColdDataPullRequest() {
        int succTotal = 0, errorTotal = 0, queueSize = pullRequestColdHoldQueue.size() ;
        Iterator<PullRequest> iterator = pullRequestColdHoldQueue.iterator();
        while (iterator.hasNext()) {
            PullRequest pullRequest = iterator.next();
            if (System.currentTimeMillis() >= pullRequest.getSuspendTimestamp() + coldHoldTimeoutMillis) {
                try {
                    pullRequest.getRequestCommand().addExtField(NO_SUSPEND_KEY, "1");
                    this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(
                        pullRequest.getClientChannel(), pullRequest.getRequestCommand());
                    succTotal++;
                } catch (Exception e) {
                    log.error("PullRequestColdHoldService checkColdDataPullRequest error", e);
                    errorTotal++;
                }
                //remove the timeout request from the iterator
                //从迭代器中移除已超时并完成唤醒的请求
                iterator.remove();
            }
        }
        log.info("checkColdPullRequest-info-finish, queueSize: {} successTotal: {} errorTotal: {}",
            queueSize, succTotal, errorTotal);
    }

}
