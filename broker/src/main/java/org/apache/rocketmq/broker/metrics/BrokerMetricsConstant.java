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
 * Broker 指标名称与标签常量定义
 */
public class BrokerMetricsConstant {
    /**
     * OpenTelemetry Meter 名称
     */
    public static final String OPEN_TELEMETRY_METER_NAME = "broker-meter";

    /**
     * 请求处理器水位 Gauge 指标名
     */
    public static final String GAUGE_PROCESSOR_WATERMARK = "rocketmq_processor_watermark";
    /**
     * Broker 权限 Gauge 指标名
     */
    public static final String GAUGE_BROKER_PERMISSION = "rocketmq_broker_permission";
    /**
     * Topic 数量 Gauge 指标名
     */
    public static final String GAUGE_TOPIC_NUM = "rocketmq_topic_number";
    /**
     * 消费组数量 Gauge 指标名
     */
    public static final String GAUGE_CONSUMER_GROUP_NUM = "rocketmq_consumer_group_number";

    /**
     * 入站消息总量计数器指标名
     */
    public static final String COUNTER_MESSAGES_IN_TOTAL = "rocketmq_messages_in_total";
    /**
     * 出站消息总量计数器指标名
     */
    public static final String COUNTER_MESSAGES_OUT_TOTAL = "rocketmq_messages_out_total";
    /**
     * 入站吞吐总量计数器指标名
     */
    public static final String COUNTER_THROUGHPUT_IN_TOTAL = "rocketmq_throughput_in_total";
    /**
     * 出站吞吐总量计数器指标名
     */
    public static final String COUNTER_THROUGHPUT_OUT_TOTAL = "rocketmq_throughput_out_total";
    /**
     * 消息大小直方图指标名
     */
    public static final String HISTOGRAM_MESSAGE_SIZE = "rocketmq_message_size";
    /**
     * Topic 创建耗时直方图指标名
     */
    public static final String HISTOGRAM_TOPIC_CREATE_EXECUTE_TIME = "rocketmq_topic_create_execution_time";
    /**
     * 消费组创建耗时直方图指标名
     */
    public static final String HISTOGRAM_CONSUMER_GROUP_CREATE_EXECUTE_TIME = "rocketmq_consumer_group_create_execution_time";

    /**
     * 生产者连接数 Gauge 指标名
     */
    public static final String GAUGE_PRODUCER_CONNECTIONS = "rocketmq_producer_connections";
    /**
     * 消费者连接数 Gauge 指标名
     */
    public static final String GAUGE_CONSUMER_CONNECTIONS = "rocketmq_consumer_connections";

    /**
     * 消费堆积消息数 Gauge 指标名
     */
    public static final String GAUGE_CONSUMER_LAG_MESSAGES = "rocketmq_consumer_lag_messages";
    /**
     * 消费堆积时延 Gauge 指标名
     */
    public static final String GAUGE_CONSUMER_LAG_LATENCY = "rocketmq_consumer_lag_latency";
    /**
     * 消费飞行中消息数 Gauge 指标名
     */
    public static final String GAUGE_CONSUMER_INFLIGHT_MESSAGES = "rocketmq_consumer_inflight_messages";
    /**
     * 消费排队时延 Gauge 指标名
     */
    public static final String GAUGE_CONSUMER_QUEUEING_LATENCY = "rocketmq_consumer_queueing_latency";
    /**
     * 消费就绪消息数 Gauge 指标名
     */
    public static final String GAUGE_CONSUMER_READY_MESSAGES = "rocketmq_consumer_ready_messages";
    /**
     * 发送到死信队列总量计数器指标名
     */
    public static final String COUNTER_CONSUMER_SEND_TO_DLQ_MESSAGES_TOTAL = "rocketmq_send_to_dlq_messages_total";

    /**
     * 提交事务消息总量计数器指标名
     */
    public static final String COUNTER_COMMIT_MESSAGES_TOTAL = "rocketmq_commit_messages_total";
    /**
     * 回滚事务消息总量计数器指标名
     */
    public static final String COUNTER_ROLLBACK_MESSAGES_TOTAL = "rocketmq_rollback_messages_total";
    /**
     * 事务结束时延直方图指标名
     */
    public static final String HISTOGRAM_FINISH_MSG_LATENCY = "rocketmq_finish_message_latency";
    /**
     * 半消息数量 Gauge 指标名
     */
    public static final String GAUGE_HALF_MESSAGES = "rocketmq_half_messages";

    /**
     * 集群标签名
     */
    public static final String LABEL_CLUSTER_NAME = "cluster";
    /**
     * 节点类型标签名
     */
    public static final String LABEL_NODE_TYPE = "node_type";
    /**
     * Broker 节点类型标签值
     */
    public static final String NODE_TYPE_BROKER = "broker";
    /**
     * 节点标识标签名
     */
    public static final String LABEL_NODE_ID = "node_id";
    /**
     * 聚合方式标签名
     */
    public static final String LABEL_AGGREGATION = "aggregation";
    /**
     * Delta 聚合标签值
     */
    public static final String AGGREGATION_DELTA = "delta";
    /**
     * 处理器标签名
     */
    public static final String LABEL_PROCESSOR = "processor";

    /**
     * Topic 标签名
     */
    public static final String LABEL_TOPIC = "topic";
    /**
     * 调用状态标签名
     */
    public static final String LABEL_INVOCATION_STATUS = "invocation_status";
    /**
     * 是否重试标签名
     */
    public static final String LABEL_IS_RETRY = "is_retry";
    /**
     * 是否系统资源标签名
     */
    public static final String LABEL_IS_SYSTEM = "is_system";
    /**
     * 消费组标签名
     */
    public static final String LABEL_CONSUMER_GROUP = "consumer_group";
    /**
     * 消息类型标签名
     */
    public static final String LABEL_MESSAGE_TYPE = "message_type";
    /**
     * 客户端语言标签名
     */
    public static final String LABEL_LANGUAGE = "language";
    /**
     * 客户端版本标签名
     */
    public static final String LABEL_VERSION = "version";
    /**
     * 消费模式标签名
     */
    public static final String LABEL_CONSUME_MODE = "consume_mode";
}
