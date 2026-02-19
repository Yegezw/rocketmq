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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PIDAdaptiveColdCtrStrategy implements ColdCtrStrategy {
    /**
     * Stores the maximum number of recent et val
     * <br>
     * 保存最近误差值的最大样本数, 用于限制 PID 历史窗口大小
     */
    private static final int MAX_STORE_NUMS = 10;
    /**
     * The weights of the three modules of the PID formula
     * <br>
     * PID 三个分量的权重系数, 用于平衡比例积分微分项贡献
     */
    private static final Double KP = 0.5, KI = 0.3, KD = 0.2;
    /**
     * 历史误差值序列, 用于计算积分项和微分项
     */
    private final List<Long> historyEtValList = new ArrayList<>();
    /**
     * 冷读控制服务引用, 用于读取配置并回写消费组阈值
     */
    private final ColdDataCgCtrService coldDataCgCtrService;
    /**
     * 期望的全局冷读累计值, 作为误差计算的目标基线
     */
    private final Long expectGlobalVal;
    /**
     * 当前窗口误差值, 定义为期望值减去实际全局累计值
     */
    private long et = 0L;

    /**
     * 构造 PID 自适应冷读策略, 初始化目标值和控制服务
     *
     * @param coldDataCgCtrService cold ctr service, 冷读控制服务实例
     * @param expectGlobalVal expected global value, 期望全局冷读累计值
     */
    public PIDAdaptiveColdCtrStrategy(ColdDataCgCtrService coldDataCgCtrService, Long expectGlobalVal) {
        this.coldDataCgCtrService = coldDataCgCtrService;
        this.expectGlobalVal = expectGlobalVal;
    }

    /**
     * 基于 PID 公式计算当前决策因子, 正值倾向提升阈值, 负值倾向降低阈值
     *
     * @return pid factor, 当前窗口策略输出的 PID 决策因子
     */
    @Override
    public Double decisionFactor() {
        if (historyEtValList.size() < MAX_STORE_NUMS) {
            return 0.0;
        }
        Long et1 = historyEtValList.get(historyEtValList.size() - 1);
        Long et2 = historyEtValList.get(historyEtValList.size() - 2);
        Long differential = et1 - et2;
        Double integration = 0.0;
        for (Long item: historyEtValList) {
            integration += item;
        }
        return  KP * et + KI * integration + KD * differential;
    }

    /**
     * 当 PID 决策因子为正时提升消费组阈值, 从而放宽冷读限流
     *
     * @param consumerGroup consumer group, 需要提升阈值的消费组
     * @param currentThreshold current threshold, 当前生效阈值
     */
    @Override
    public void promote(String consumerGroup, Long currentThreshold) {
        if (decisionFactor() > 0) {
            coldDataCgCtrService.addOrUpdateGroupConfig(consumerGroup, (long)(currentThreshold * 1.5));
        }
    }

    /**
     * 当 PID 决策因子为负时降低消费组阈值, 并保证不低于 broker 最小阈值
     *
     * @param consumerGroup consumer group, 需要降低阈值的消费组
     * @param currentThreshold current threshold, 当前生效阈值
     */
    @Override
    public void decelerate(String consumerGroup, Long currentThreshold) {
        if (decisionFactor() < 0) {
            long changedThresholdVal = (long)(currentThreshold * 0.8);
            if (changedThresholdVal < coldDataCgCtrService.getBrokerConfig().getCgColdReadThreshold()) {
                changedThresholdVal = coldDataCgCtrService.getBrokerConfig().getCgColdReadThreshold();
            }
            coldDataCgCtrService.addOrUpdateGroupConfig(consumerGroup, changedThresholdVal);
        }
    }

    /**
     * 采集当前窗口全局冷读累计值并更新误差历史, 同时裁剪历史窗口长度
     *
     * @param globalAcc global accumulate value, 当前窗口系统级冷读累计值
     */
    @Override
    public void collect(Long globalAcc) {
        et = expectGlobalVal - globalAcc;
        historyEtValList.add(et);
        Iterator<Long> iterator = historyEtValList.iterator();
        while (historyEtValList.size() > MAX_STORE_NUMS && iterator.hasNext()) {
            iterator.next();
            iterator.remove();
        }
    }
}
