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

public class SimpleColdCtrStrategy implements ColdCtrStrategy {
    /**
     * 冷读控制服务引用, 用于更新消费组冷读阈值配置
     */
    private final ColdDataCgCtrService coldDataCgCtrService;

    /**
     * 构造简单冷读控制策略, 绑定阈值写入服务
     *
     * @param coldDataCgCtrService cold ctr service, 冷读控制服务实例
     */
    public SimpleColdCtrStrategy(ColdDataCgCtrService coldDataCgCtrService) {
        this.coldDataCgCtrService = coldDataCgCtrService;
    }

    /**
     * 简单策略不依赖 PID 决策因子, 固定返回空值
     *
     * @return null, 表示当前策略不输出决策因子
     */
    @Override
    public Double decisionFactor() {
        return null;
    }

    /**
     * 通过扩大阈值提升消费组冷读速率, 使其在下一窗口更难触发限流
     *
     * @param consumerGroup consumer group, 需要提升阈值的消费组
     * @param currentThreshold current threshold, 当前冷读阈值
     */
    @Override
    public void promote(String consumerGroup, Long currentThreshold) {
        coldDataCgCtrService.addOrUpdateGroupConfig(consumerGroup, (long)(currentThreshold * 1.5));
    }

    /**
     * 在全局冷读超限时降低消费组阈值, 让该消费组更早进入限流状态
     *
     * @param consumerGroup consumer group, 需要降低阈值的消费组
     * @param currentThreshold current threshold, 当前冷读阈值
     */
    @Override
    public void decelerate(String consumerGroup, Long currentThreshold) {
        if (!coldDataCgCtrService.isGlobalColdCtr()) {
            return;
        }
        long changedThresholdVal = (long)(currentThreshold * 0.8);
        if (changedThresholdVal < coldDataCgCtrService.getBrokerConfig().getCgColdReadThreshold()) {
            changedThresholdVal = coldDataCgCtrService.getBrokerConfig().getCgColdReadThreshold();
        }
        coldDataCgCtrService.addOrUpdateGroupConfig(consumerGroup, changedThresholdVal);
    }

    /**
     * 简单策略不维护历史统计, 该方法保留为空实现
     *
     * @param globalAcc global accumulate value, 当前窗口系统级冷读累计值
     */
    @Override
    public void collect(Long globalAcc) {
    }
}
