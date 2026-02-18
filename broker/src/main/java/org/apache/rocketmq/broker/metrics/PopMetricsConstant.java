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
package org.apache.rocketmq.broker.metrics;

/**
 * POP 指标常量定义
 */
public class PopMetricsConstant {
    /**
     * POP 缓冲扫描耗时直方图指标名
     */
    public static final String HISTOGRAM_POP_BUFFER_SCAN_TIME_CONSUME = "rocketmq_pop_buffer_scan_time_consume";
    /**
     * POP 回溯写入消息总量计数器指标名
     */
    public static final String COUNTER_POP_REVIVE_IN_MESSAGE_TOTAL = "rocketmq_pop_revive_in_message_total";
    /**
     * POP 回溯读取消息总量计数器指标名
     */
    public static final String COUNTER_POP_REVIVE_OUT_MESSAGE_TOTAL = "rocketmq_pop_revive_out_message_total";
    /**
     * POP 重试消息写入总量计数器指标名
     */
    public static final String COUNTER_POP_REVIVE_RETRY_MESSAGES_TOTAL = "rocketmq_pop_revive_retry_messages_total";

    /**
     * POP 回溯堆积量 Gauge 指标名
     */
    public static final String GAUGE_POP_REVIVE_LAG = "rocketmq_pop_revive_lag";
    /**
     * POP 回溯处理时延 Gauge 指标名
     */
    public static final String GAUGE_POP_REVIVE_LATENCY = "rocketmq_pop_revive_latency";
    /**
     * POP 偏移缓冲区大小 Gauge 指标名
     */
    public static final String GAUGE_POP_OFFSET_BUFFER_SIZE = "rocketmq_pop_offset_buffer_size";
    /**
     * POP CheckPoint 缓冲区大小 Gauge 指标名
     */
    public static final String GAUGE_POP_CHECKPOINT_BUFFER_SIZE = "rocketmq_pop_checkpoint_buffer_size";

    /**
     * 回溯消息类型标签名
     */
    public static final String LABEL_REVIVE_MESSAGE_TYPE = "revive_message_type";
    /**
     * 写入状态标签名
     */
    public static final String LABEL_PUT_STATUS = "put_status";
    /**
     * 队列标识标签名
     */
    public static final String LABEL_QUEUE_ID = "queue_id";
}
