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

import com.alibaba.fastjson2.JSONObject;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.coldctr.AccAndTimeStamp;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * store the cg cold read ctr table and acc the size of the cold
 * reading msg, timing to clear the table and set acc to zero
 * <br>
 * 维护消费组冷读控制表与累计值, 周期性清理过期数据并重置统计窗口
 */
public class ColdDataCgCtrService extends ServiceThread {
    /**
     * 冷读控制日志记录器, 输出统计清理与策略调节过程
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_COLDCTR_LOGGER_NAME);
    /**
     * 系统时钟封装, 用于统计任务执行耗时
     */
    private final SystemClock systemClock = new SystemClock();
    /**
     * 消费组冷读累计驻留超时时间, 超时后移除运行时统计项
     */
    private final long cgColdAccResideTimeoutMills = 60 * 1000;
    /**
     * 当前统计窗口全局冷读累计值, 用于全局流控判定
     */
    private static final AtomicLong GLOBAL_ACC = new AtomicLong(0L);
    /**
     * 自适应阈值配置键后缀, 用于区分管理员配置与策略动态配置
     */
    private static final String ADAPTIVE = "||adaptive";
    /**
     * as soon as the consumerGroup read the cold data then it will be put into @code cgColdThresholdMapRuntime,
     * and it also will be removed when does not read cold data in @code cgColdAccResideTimeoutMills later;
     * <br>
     * 记录消费组运行时冷读累计与最后访问时间, 用于每轮窗口清理和限流判断
     */
    private final ConcurrentHashMap<String, AccAndTimeStamp> cgColdThresholdMapRuntime = new ConcurrentHashMap<>();
    /**
     * if the system admin wants to set the special cold read threshold for some consumerGroup, the configuration will
     * be putted into @code cgColdThresholdMapConfig
     * <br>
     * 保存管理员静态阈值与策略动态阈值, 键值通过 ADAPTIVE 后缀区分来源
     */
    private final ConcurrentHashMap<String, Long> cgColdThresholdMapConfig = new ConcurrentHashMap<>();
    /**
     * broker 配置引用, 提供冷读策略开关与阈值参数
     */
    private final BrokerConfig brokerConfig;
    /**
     * 存储层配置引用, 提供冷数据流控总开关
     */
    private final MessageStoreConfig messageStoreConfig;
    /**
     * 冷读调节策略实现, 负责自适应提升或降低消费组阈值
     */
    private final ColdCtrStrategy coldCtrStrategy;

    /**
     * 构造冷读控制服务并初始化策略实现, 根据 broker 配置选择 PID 或简单策略
     *
     * @param brokerController broker controller, 提供配置与运行时上下文
     */
    public ColdDataCgCtrService(BrokerController brokerController) {
        this.brokerConfig = brokerController.getBrokerConfig();
        this.messageStoreConfig = brokerController.getMessageStoreConfig();
        this.coldCtrStrategy = brokerConfig.isUsePIDColdCtrStrategy() ? new PIDAdaptiveColdCtrStrategy(this, (long)(brokerConfig.getGlobalColdReadThreshold() * 0.8)) : new SimpleColdCtrStrategy(this);
    }

    @Override
    public String getServiceName() {
        return ColdDataCgCtrService.class.getSimpleName();
    }

    /**
     * 定时执行冷读统计窗口清理与策略调节, 并根据开关动态调整轮询间隔
     */
    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {
                if (messageStoreConfig.isColdDataFlowControlEnable()) {
                    this.waitForRunning(5 * 1000);
                } else {
                    this.waitForRunning(180 * 1000);
                }
                long beginLockTimestamp = this.systemClock.now();
                clearDataAcc();
                if (!brokerConfig.isColdCtrStrategyEnable()) {
                    clearAdaptiveConfig();
                }
                long costTime = this.systemClock.now() - beginLockTimestamp;
                log.info("[{}] clearTheDataAcc-cost {} ms.", costTime > 3 * 1000 ? "NOTIFYME" : "OK", costTime);
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception", e);
            }
        }
        log.info("{} service end", this.getServiceName());
    }

    public String getColdDataFlowCtrInfo() {
        JSONObject result = new JSONObject();
        result.put("runtimeTable", this.cgColdThresholdMapRuntime);
        result.put("configTable", this.cgColdThresholdMapConfig);
        result.put("cgColdReadThreshold", this.brokerConfig.getCgColdReadThreshold());
        result.put("globalColdReadThreshold", this.brokerConfig.getGlobalColdReadThreshold());
        result.put("globalAcc", GLOBAL_ACC.get());
        return result.toJSONString();
    }

    /**
     * clear the long time no cold read cg in the table;
     * update the acc to zero for the cg in the table;
     * use the strategy to promote or decelerate the cg;
     * <br>
     * 清理长期未冷读的消费组并重置累计值, 按策略执行阈值升降调节
     */
    private void clearDataAcc() {
        log.info("clearDataAcc cgColdThresholdMapRuntime key size: {}", cgColdThresholdMapRuntime.size());
        if (brokerConfig.isColdCtrStrategyEnable()) {
            coldCtrStrategy.collect(GLOBAL_ACC.get());
        }
        Iterator<Entry<String, AccAndTimeStamp>> iterator = cgColdThresholdMapRuntime.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, AccAndTimeStamp> next = iterator.next();
            if (System.currentTimeMillis() >= cgColdAccResideTimeoutMills + next.getValue().getLastColdReadTimeMills()) {
                if (brokerConfig.isColdCtrStrategyEnable()) {
                    cgColdThresholdMapConfig.remove(buildAdaptiveKey(next.getKey()));
                }
                iterator.remove();
            } else if (next.getValue().getColdAcc().get() >= getThresholdByConsumerGroup(next.getKey())) {
                log.info("Coldctr consumerGroup: {}, acc: {}, threshold: {}", next.getKey(), next.getValue().getColdAcc().get(), getThresholdByConsumerGroup(next.getKey()));
                if (brokerConfig.isColdCtrStrategyEnable() && !isGlobalColdCtr() && !isAdminConfig(next.getKey())) {
                    coldCtrStrategy.promote(buildAdaptiveKey(next.getKey()), getThresholdByConsumerGroup(next.getKey()));
                }
            }
            next.getValue().getColdAcc().set(0L);
        }
        if (isGlobalColdCtr()) {
            log.info("Coldctr global acc: {}, threshold: {}", GLOBAL_ACC.get(), this.brokerConfig.getGlobalColdReadThreshold());
        }
        if (brokerConfig.isColdCtrStrategyEnable()) {
            sortAndDecelerate();
        }
        GLOBAL_ACC.set(0L);
    }

    /**
     * 按阈值从高到低选择部分消费组执行减速, 限制每轮减速数量避免波动过大
     */
    private void sortAndDecelerate() {
        List<Entry<String, Long>> configMapList = new ArrayList<Entry<String, Long>>(cgColdThresholdMapConfig.entrySet());
        configMapList.sort(new Comparator<Entry<String, Long>>() {
            @Override
            public int compare(Entry<String, Long> o1, Entry<String, Long> o2) {
                return (int)(o2.getValue() - o1.getValue());
            }
        });
        Iterator<Entry<String, Long>> iterator = configMapList.iterator();
        int maxDecelerate = 3;
        while (iterator.hasNext() && maxDecelerate > 0) {
            Entry<String, Long> next = iterator.next();
            if (!isAdminConfig(next.getKey())) {
                coldCtrStrategy.decelerate(next.getKey(), getThresholdByConsumerGroup(next.getKey()));
                maxDecelerate --;
            }
        }
    }

    /**
     * 累加消费组与全局冷读字节数, 并刷新该消费组最近冷读时间
     *
     * @param consumerGroup consumer group, 发生冷读的消费组
     * @param coldDataToAcc cold data bytes, 本次需要累计的冷读数据量
     */
    public void coldAcc(String consumerGroup, long coldDataToAcc) {
        if (coldDataToAcc <= 0) {
            return;
        }
        GLOBAL_ACC.addAndGet(coldDataToAcc);
        AccAndTimeStamp atomicAcc = cgColdThresholdMapRuntime.get(consumerGroup);
        if (null == atomicAcc) {
            atomicAcc = new AccAndTimeStamp(new AtomicLong(coldDataToAcc));
            atomicAcc = cgColdThresholdMapRuntime.putIfAbsent(consumerGroup, atomicAcc);
        }
        if (null != atomicAcc) {
            atomicAcc.getColdAcc().addAndGet(coldDataToAcc);
            atomicAcc.setLastColdReadTimeMills(System.currentTimeMillis());
        }
    }

    /**
     * 新增或更新消费组阈值配置, 用于管理员配置或策略动态调节
     *
     * @param consumerGroup consumer group, 目标消费组或自适应键
     * @param threshold threshold value, 需要写入的冷读阈值
     */
    public void addOrUpdateGroupConfig(String consumerGroup, Long threshold) {
        cgColdThresholdMapConfig.put(consumerGroup, threshold);
    }

    /**
     * 删除消费组阈值配置, 用于移除管理员配置或过期自适应配置
     *
     * @param consumerGroup consumer group, 需要移除的配置键
     */
    public void removeGroupConfig(String consumerGroup) {
        cgColdThresholdMapConfig.remove(consumerGroup);
    }

    public boolean isCgNeedColdDataFlowCtr(String consumerGroup) {
        if (!this.messageStoreConfig.isColdDataFlowControlEnable()) {
            return false;
        }
        if (MixAll.isSysConsumerGroupPullMessage(consumerGroup)) {
            return false;
        }
        AccAndTimeStamp accAndTimeStamp = cgColdThresholdMapRuntime.get(consumerGroup);
        if (null == accAndTimeStamp) {
            return false;
        }

        Long threshold = getThresholdByConsumerGroup(consumerGroup);
        if (accAndTimeStamp.getColdAcc().get() >= threshold) {
            return true;
        }
        return GLOBAL_ACC.get() >= this.brokerConfig.getGlobalColdReadThreshold();
    }

    public boolean isGlobalColdCtr() {
        return GLOBAL_ACC.get() > this.brokerConfig.getGlobalColdReadThreshold();
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    private Long getThresholdByConsumerGroup(String consumerGroup) {
        if (isAdminConfig(consumerGroup)) {
            if (consumerGroup.endsWith(ADAPTIVE)) {
                return cgColdThresholdMapConfig.get(consumerGroup.split(ADAPTIVE)[0]);
            }
            return cgColdThresholdMapConfig.get(consumerGroup);
        }
        Long threshold = null;
        if (brokerConfig.isColdCtrStrategyEnable()) {
            if (consumerGroup.endsWith(ADAPTIVE)) {
                threshold = cgColdThresholdMapConfig.get(consumerGroup);
            } else {
                threshold = cgColdThresholdMapConfig.get(buildAdaptiveKey(consumerGroup));
            }
        }
        if (null == threshold) {
            threshold = this.brokerConfig.getCgColdReadThreshold();
        }
        return threshold;
    }

    /**
     * 生成消费组的自适应配置键, 用于存储策略动态阈值
     *
     * @param consumerGroup consumer group, 原始消费组名
     * @return adaptive key, 带自适应后缀的配置键
     */
    private String buildAdaptiveKey(String consumerGroup) {
        return consumerGroup + ADAPTIVE;
    }

    private boolean isAdminConfig(String consumerGroup) {
        if (consumerGroup.endsWith(ADAPTIVE)) {
            consumerGroup = consumerGroup.split(ADAPTIVE)[0];
        }
        return cgColdThresholdMapConfig.containsKey(consumerGroup);
    }

    /**
     * 清理所有自适应阈值配置项, 保留管理员显式配置
     */
    private void clearAdaptiveConfig() {
        cgColdThresholdMapConfig.entrySet().removeIf(next -> next.getKey().endsWith(ADAPTIVE));
    }

}
