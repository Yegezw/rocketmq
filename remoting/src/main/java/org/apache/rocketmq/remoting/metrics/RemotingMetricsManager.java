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
package org.apache.rocketmq.remoting.metrics;

import com.google.common.collect.Lists;
import io.netty.util.concurrent.Future;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.*;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.metrics.NopLongHistogram;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.*;

/**
 * Remoting 指标管理器, 负责指标初始化、视图配置和结果标签计算
 */
public class RemotingMetricsManager {
    /**
     * RPC 延迟直方图实例
     */
    public static LongHistogram rpcLatency = new NopLongHistogram();
    /**
     * 指标属性构建器供应器
     */
    public static Supplier<AttributesBuilder> attributesBuilderSupplier;

    /**
     * 创建新的属性构建器
     *
     * @return 属性构建器
     */
    public static AttributesBuilder newAttributesBuilder() {
        // 未初始化供应器时返回默认构建器
        if (attributesBuilderSupplier == null) {
            return Attributes.builder();
        }

        // 初始化后统一注入协议类型标签
        return attributesBuilderSupplier.get()
            .put(LABEL_PROTOCOL_TYPE, PROTOCOL_TYPE_REMOTING);
    }

    /**
     * 初始化 Remoting 指标
     *
     * @param meter                     OTel Meter 实例
     * @param attributesBuilderSupplier 属性构建器供应器
     */
    public static void initMetrics(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier) {
        // 保存属性构建器供应器
        RemotingMetricsManager.attributesBuilderSupplier = attributesBuilderSupplier;

        // 创建 RPC 延迟直方图
        rpcLatency = meter.histogramBuilder(HISTOGRAM_RPC_LATENCY)
            .setDescription("Rpc latency")
            .setUnit("milliseconds")
            .ofLongs()
            .build();
    }

    /**
     * 获取指标视图配置
     *
     * @return 指标选择器与视图构建器列表
     */
    public static List<Pair<InstrumentSelector, ViewBuilder>> getMetricsView() {
        // 定义 RPC 延迟分桶边界
        List<Double> rpcCostTimeBuckets = Arrays.asList(
            (double) Duration.ofMillis(1).toMillis(),
            (double) Duration.ofMillis(3).toMillis(),
            (double) Duration.ofMillis(5).toMillis(),
            (double) Duration.ofMillis(7).toMillis(),
            (double) Duration.ofMillis(10).toMillis(),
            (double) Duration.ofMillis(100).toMillis(),
            (double) Duration.ofSeconds(1).toMillis(),
            (double) Duration.ofSeconds(2).toMillis(),
            (double) Duration.ofSeconds(3).toMillis()
        );

        // 构建目标直方图选择器
        InstrumentSelector selector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM)
            .setName(HISTOGRAM_RPC_LATENCY)
            .build();

        // 构建显式分桶直方图视图
        ViewBuilder viewBuilder = View.builder()
            .setAggregation(Aggregation.explicitBucketHistogram(rpcCostTimeBuckets));
        return Lists.newArrayList(new Pair<>(selector, viewBuilder));
    }

    /**
     * 获取 writeAndFlush 结果标签值
     *
     * @param future Netty Future 对象
     * @return 结果标签值
     */
    public static String getWriteAndFlushResult(Future<?> future) {
        String result = RESULT_SUCCESS;

        // 优先判断取消状态
        if (future.isCancelled()) {
            result = RESULT_CANCELED;
        } else if (!future.isSuccess()) {
            // 未取消且失败时返回写通道失败
            result = RESULT_WRITE_CHANNEL_FAILED;
        }
        return result;
    }

}
