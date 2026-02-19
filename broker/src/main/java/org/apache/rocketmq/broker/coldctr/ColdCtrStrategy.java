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

public interface ColdCtrStrategy {
    /**
     * Calculate the determining factor about whether to accelerate or decelerate
     * <br>
     * 计算是否需要加速或减速的决策因子, 供控制策略判断调节方向
     *
     * @return decision factor, 当前控制策略输出的决策因子
     */
    Double decisionFactor();

    /**
     * Promote the speed for consumerGroup to read cold data
     * <br>
     * 提升 consumerGroup 的冷数据读取速率, 通过抬升阈值减少限流触发概率
     *
     * @param consumerGroup    consumer group, 需要提升阈值的消费组
     * @param currentThreshold current threshold, 当前生效的冷读阈值
     */
    void promote(String consumerGroup, Long currentThreshold);

    /**
     * Decelerate the speed for consumerGroup to read cold data
     * <br>
     * 降低 consumerGroup 的冷数据读取速率, 通过下调阈值提前触发限流
     *
     * @param consumerGroup    consumer group, 需要降低阈值的消费组
     * @param currentThreshold current threshold, 当前生效的冷读阈值
     */
    void decelerate(String consumerGroup, Long currentThreshold);

    /**
     * Collect the total number of cold read data in the system
     * <br>
     * 收集系统级冷读累计值, 为下一轮策略计算准备输入
     *
     * @param globalAcc global accumulate value, 当前窗口内系统级冷读累计值
     */
    void collect(Long globalAcc);
}
