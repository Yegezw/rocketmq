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

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.rocketmq.broker.metrics.ConsumerLagCalculator;
import org.apache.rocketmq.remoting.CommandCallback;

/**
 * POP 请求回调适配器, 将 Remoting 回调桥接到消费堆积统计流程
 */
public class PopCommandCallback implements CommandCallback {

    /**
     * 业务回调函数, 接收消费组上下文与 lag 记录器执行统计逻辑
     */
    private final BiConsumer<ConsumerLagCalculator.ProcessGroupInfo,
        Consumer<ConsumerLagCalculator.CalculateLagResult>> biConsumer;

    /**
     * 当前消费组处理上下文, 包含 lag 计算所需元数据
     */
    private final ConsumerLagCalculator.ProcessGroupInfo info;
    /**
     * lag 计算结果记录器, 用于输出统计结果
     */
    private final Consumer<ConsumerLagCalculator.CalculateLagResult> lagRecorder;


    /**
     * 创建 POP 回调实例
     *
     * @param biConsumer 业务回调函数
     * @param info 消费组上下文
     * @param lagRecorder lag 结果记录器
     */
    public PopCommandCallback(
        BiConsumer<ConsumerLagCalculator.ProcessGroupInfo,
                    Consumer<ConsumerLagCalculator.CalculateLagResult>> biConsumer,
        ConsumerLagCalculator.ProcessGroupInfo info,
        Consumer<ConsumerLagCalculator.CalculateLagResult> lagRecorder) {

        this.biConsumer = biConsumer;
        this.info = info;
        this.lagRecorder = lagRecorder;
    }

    /**
     * 执行回调逻辑, 触发消费堆积计算
     */
    @Override
    public void accept() {
        biConsumer.accept(info, lagRecorder);
    }
}
