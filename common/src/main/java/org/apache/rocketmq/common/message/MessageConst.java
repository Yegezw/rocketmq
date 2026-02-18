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
package org.apache.rocketmq.common.message;

import java.util.HashSet;

/**
 * 消息属性常量定义
 */
public class MessageConst {
    /**
     * 消息业务键属性名
     */
    public static final String PROPERTY_KEYS = "KEYS";
    /**
     * 消息标签属性名
     */
    public static final String PROPERTY_TAGS = "TAGS";
    /**
     * 同步刷盘等待标记属性名
     */
    public static final String PROPERTY_WAIT_STORE_MSG_OK = "WAIT";
    /**
     * 延迟级别属性名
     */
    public static final String PROPERTY_DELAY_TIME_LEVEL = "DELAY";
    /**
     * 重试主题属性名
     */
    public static final String PROPERTY_RETRY_TOPIC = "RETRY_TOPIC";
    /**
     * 真实主题属性名
     */
    public static final String PROPERTY_REAL_TOPIC = "REAL_TOPIC";
    /**
     * 真实队列 ID 属性名
     */
    public static final String PROPERTY_REAL_QUEUE_ID = "REAL_QID";
    /**
     * 事务预处理标记属性名
     */
    public static final String PROPERTY_TRANSACTION_PREPARED = "TRAN_MSG";
    /**
     * 生产者组属性名
     */
    public static final String PROPERTY_PRODUCER_GROUP = "PGROUP";
    /**
     * 最小偏移量属性名
     */
    public static final String PROPERTY_MIN_OFFSET = "MIN_OFFSET";
    /**
     * 最大偏移量属性名
     */
    public static final String PROPERTY_MAX_OFFSET = "MAX_OFFSET";
    /**
     * 买家标识属性名
     */
    public static final String PROPERTY_BUYER_ID = "BUYER_ID";
    /**
     * 原始消息 ID 属性名
     */
    public static final String PROPERTY_ORIGIN_MESSAGE_ID = "ORIGIN_MESSAGE_ID";
    /**
     * 属性透传标记属性名
     */
    public static final String PROPERTY_TRANSFER_FLAG = "TRANSFER_FLAG";
    /**
     * 校正标记属性名
     */
    public static final String PROPERTY_CORRECTION_FLAG = "CORRECTION_FLAG";
    /**
     * MQ2 标记属性名
     */
    public static final String PROPERTY_MQ2_FLAG = "MQ2_FLAG";
    /**
     * 重试消费次数属性名
     */
    public static final String PROPERTY_RECONSUME_TIME = "RECONSUME_TIME";
    /**
     * 消息地域属性名
     */
    public static final String PROPERTY_MSG_REGION = "MSG_REGION";
    /**
     * 链路追踪开关属性名
     */
    public static final String PROPERTY_TRACE_SWITCH = "TRACE_ON";
    /**
     * 客户端唯一消息键属性名
     */
    public static final String PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX = "UNIQ_KEY";
    /**
     * 扩展唯一标识属性名
     */
    public static final String PROPERTY_EXTEND_UNIQ_INFO = "EXTEND_UNIQ_INFO";
    /**
     * 最大重试次数属性名
     */
    public static final String PROPERTY_MAX_RECONSUME_TIMES = "MAX_RECONSUME_TIMES";
    /**
     * 消费开始时间戳属性名
     */
    public static final String PROPERTY_CONSUME_START_TIMESTAMP = "CONSUME_START_TIME";
    /**
     * 批内消息数量属性名
     */
    public static final String PROPERTY_INNER_NUM = "INNER_NUM";
    /**
     * 批内消息基准偏移属性名
     */
    public static final String PROPERTY_INNER_BASE = "INNER_BASE";
    /**
     * 去重信息属性名
     */
    public static final String DUP_INFO = "DUP_INFO";
    /**
     * 事务检查免疫时间属性名 单位秒
     */
    public static final String PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS = "CHECK_IMMUNITY_TIME_IN_SECONDS";
    /**
     * 事务预处理消息队列偏移量属性名
     */
    public static final String PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET = "TRAN_PREPARED_QUEUE_OFFSET";
    /**
     * 事务 ID 属性名
     */
    public static final String PROPERTY_TRANSACTION_ID = "__transactionId__";
    /**
     * 事务回查次数属性名
     */
    public static final String PROPERTY_TRANSACTION_CHECK_TIMES = "TRANSACTION_CHECK_TIMES";
    /**
     * 实例 ID 属性名
     */
    public static final String PROPERTY_INSTANCE_ID = "INSTANCE_ID";
    /**
     * 请求关联 ID 属性名
     */
    public static final String PROPERTY_CORRELATION_ID = "CORRELATION_ID";
    /**
     * 回执目标客户端属性名
     */
    public static final String PROPERTY_MESSAGE_REPLY_TO_CLIENT = "REPLY_TO_CLIENT";
    /**
     * 回复消息生存时间属性名
     */
    public static final String PROPERTY_MESSAGE_TTL = "TTL";
    /**
     * 回复消息到达时间属性名
     */
    public static final String PROPERTY_REPLY_MESSAGE_ARRIVE_TIME = "ARRIVE_TIME";
    /**
     * 推送回复时间属性名
     */
    public static final String PROPERTY_PUSH_REPLY_TIME = "PUSH_REPLY_TIME";
    /**
     * 集群名属性名
     */
    public static final String PROPERTY_CLUSTER = "CLUSTER";
    /**
     * 消息类型属性名
     */
    public static final String PROPERTY_MESSAGE_TYPE = "MSG_TYPE";
    /**
     * POP 校验信息属性名
     */
    public static final String PROPERTY_POP_CK = "POP_CK";
    /**
     * POP 校验偏移量属性名
     */
    public static final String PROPERTY_POP_CK_OFFSET = "POP_CK_OFFSET";
    /**
     * 首次 POP 时间属性名
     */
    public static final String PROPERTY_FIRST_POP_TIME = "1ST_POP_TIME";
    /**
     * 顺序分片键属性名
     */
    public static final String PROPERTY_SHARDING_KEY = "__SHARDINGKEY";
    /**
     * 转发队列 ID 属性名
     */
    public static final String PROPERTY_FORWARD_QUEUE_ID = "PROPERTY_FORWARD_QUEUE_ID";
    /**
     * 重定向标记属性名
     */
    public static final String PROPERTY_REDIRECT = "REDIRECT";
    /**
     * 内部多路分发属性名
     */
    public static final String PROPERTY_INNER_MULTI_DISPATCH = "INNER_MULTI_DISPATCH";
    /**
     * 内部多路队列偏移属性名
     */
    public static final String PROPERTY_INNER_MULTI_QUEUE_OFFSET = "INNER_MULTI_QUEUE_OFFSET";
    /**
     * 链路追踪上下文属性名
     */
    public static final String PROPERTY_TRACE_CONTEXT = "TRACE_CONTEXT";
    /**
     * 定时延迟秒属性名
     */
    public static final String PROPERTY_TIMER_DELAY_SEC = "TIMER_DELAY_SEC";
    /**
     * 定时投递时间属性名
     */
    public static final String PROPERTY_TIMER_DELIVER_MS = "TIMER_DELIVER_MS";
    /**
     * 消息生效主机属性名
     */
    public static final String PROPERTY_BORN_HOST = "__BORNHOST";
    /**
     * 消息生效时间戳属性名
     */
    public static final String PROPERTY_BORN_TIMESTAMP = "BORN_TIMESTAMP";

    /**
     * property which name starts with "__RMQ.TRANSIENT." is called transient one that will not stored in broker disks.
     * <br>
     * 以 "__RMQ.TRANSIENT." 开头的属性为瞬态属性 不会落盘
     */
    public static final String PROPERTY_TRANSIENT_PREFIX = "__RMQ.TRANSIENT.";

    /**
     * the transient property key of topicSysFlag (set by client when pulling messages)
     * <br>
     * 拉消息时客户端设置的 topicSysFlag 瞬态属性键
     */
    public static final String PROPERTY_TRANSIENT_TOPIC_CONFIG = PROPERTY_TRANSIENT_PREFIX + "TOPIC_SYS_FLAG";

    /**
     * the transient property key of groupSysFlag (set by client when pulling messages)
     * <br>
     * 拉消息时客户端设置的 groupSysFlag 瞬态属性键
     */
    public static final String PROPERTY_TRANSIENT_GROUP_CONFIG = PROPERTY_TRANSIENT_PREFIX + "GROUP_SYS_FLAG";

    /**
     * 多个键分隔符
     */
    public static final String KEY_SEPARATOR = " ";

    /**
     * 系统保留属性集合
     */
    public static final HashSet<String> STRING_HASH_SET = new HashSet<>(64);

    /**
     * 定时消息入队时间属性名
     */
    public static final String PROPERTY_TIMER_ENQUEUE_MS = "TIMER_ENQUEUE_MS";
    /**
     * 定时消息出队时间属性名
     */
    public static final String PROPERTY_TIMER_DEQUEUE_MS = "TIMER_DEQUEUE_MS";
    /**
     * 定时轮转次数属性名
     */
    public static final String PROPERTY_TIMER_ROLL_TIMES = "TIMER_ROLL_TIMES";
    /**
     * 定时超时时间属性名
     */
    public static final String PROPERTY_TIMER_OUT_MS = "TIMER_OUT_MS";
    /**
     * 定时删除唯一键属性名
     */
    public static final String PROPERTY_TIMER_DEL_UNIQKEY = "TIMER_DEL_UNIQKEY";
    /**
     * 定时延迟级别属性名
     */
    public static final String PROPERTY_TIMER_DELAY_LEVEL = "TIMER_DELAY_LEVEL";
    /**
     * 定时延迟毫秒属性名
     */
    public static final String PROPERTY_TIMER_DELAY_MS = "TIMER_DELAY_MS";
    /**
     * CRC32 校验值属性名
     */
    public static final String PROPERTY_CRC32 = "__CRC32#";

    /**
     * properties for DLQ
     * <br>
     * 死信消息附加属性、死信来源主题属性名
     */
    public static final String PROPERTY_DLQ_ORIGIN_TOPIC = "DLQ_ORIGIN_TOPIC";
    /**
     * 死信来源消息 ID 属性名
     */
    public static final String PROPERTY_DLQ_ORIGIN_MESSAGE_ID = "DLQ_ORIGIN_MESSAGE_ID";

    // 初始化系统保留属性集合
    static {
        STRING_HASH_SET.add(PROPERTY_TRACE_SWITCH);
        STRING_HASH_SET.add(PROPERTY_MSG_REGION);
        STRING_HASH_SET.add(PROPERTY_KEYS);
        STRING_HASH_SET.add(PROPERTY_TAGS);
        STRING_HASH_SET.add(PROPERTY_WAIT_STORE_MSG_OK);
        STRING_HASH_SET.add(PROPERTY_DELAY_TIME_LEVEL);
        STRING_HASH_SET.add(PROPERTY_RETRY_TOPIC);
        STRING_HASH_SET.add(PROPERTY_REAL_TOPIC);
        STRING_HASH_SET.add(PROPERTY_REAL_QUEUE_ID);
        STRING_HASH_SET.add(PROPERTY_TRANSACTION_PREPARED);
        STRING_HASH_SET.add(PROPERTY_PRODUCER_GROUP);
        STRING_HASH_SET.add(PROPERTY_MIN_OFFSET);
        STRING_HASH_SET.add(PROPERTY_MAX_OFFSET);
        STRING_HASH_SET.add(PROPERTY_BUYER_ID);
        STRING_HASH_SET.add(PROPERTY_ORIGIN_MESSAGE_ID);
        STRING_HASH_SET.add(PROPERTY_TRANSFER_FLAG);
        STRING_HASH_SET.add(PROPERTY_CORRECTION_FLAG);
        STRING_HASH_SET.add(PROPERTY_MQ2_FLAG);
        STRING_HASH_SET.add(PROPERTY_RECONSUME_TIME);
        STRING_HASH_SET.add(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        STRING_HASH_SET.add(PROPERTY_MAX_RECONSUME_TIMES);
        STRING_HASH_SET.add(PROPERTY_CONSUME_START_TIMESTAMP);
        STRING_HASH_SET.add(PROPERTY_POP_CK);
        STRING_HASH_SET.add(PROPERTY_POP_CK_OFFSET);
        STRING_HASH_SET.add(PROPERTY_FIRST_POP_TIME);
        STRING_HASH_SET.add(PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        STRING_HASH_SET.add(DUP_INFO);
        STRING_HASH_SET.add(PROPERTY_EXTEND_UNIQ_INFO);
        STRING_HASH_SET.add(PROPERTY_INSTANCE_ID);
        STRING_HASH_SET.add(PROPERTY_CORRELATION_ID);
        STRING_HASH_SET.add(PROPERTY_MESSAGE_REPLY_TO_CLIENT);
        STRING_HASH_SET.add(PROPERTY_MESSAGE_TTL);
        STRING_HASH_SET.add(PROPERTY_REPLY_MESSAGE_ARRIVE_TIME);
        STRING_HASH_SET.add(PROPERTY_PUSH_REPLY_TIME);
        STRING_HASH_SET.add(PROPERTY_CLUSTER);
        STRING_HASH_SET.add(PROPERTY_MESSAGE_TYPE);
        STRING_HASH_SET.add(PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        STRING_HASH_SET.add(PROPERTY_TIMER_DELAY_MS);
        STRING_HASH_SET.add(PROPERTY_TIMER_DELAY_SEC);
        STRING_HASH_SET.add(PROPERTY_TIMER_DELIVER_MS);
        STRING_HASH_SET.add(PROPERTY_TIMER_ENQUEUE_MS);
        STRING_HASH_SET.add(PROPERTY_TIMER_DEQUEUE_MS);
        STRING_HASH_SET.add(PROPERTY_TIMER_ROLL_TIMES);
        STRING_HASH_SET.add(PROPERTY_TIMER_OUT_MS);
        STRING_HASH_SET.add(PROPERTY_TIMER_DEL_UNIQKEY);
        STRING_HASH_SET.add(PROPERTY_TIMER_DELAY_LEVEL);
        STRING_HASH_SET.add(PROPERTY_BORN_HOST);
        STRING_HASH_SET.add(PROPERTY_BORN_TIMESTAMP);
        STRING_HASH_SET.add(PROPERTY_DLQ_ORIGIN_TOPIC);
        STRING_HASH_SET.add(PROPERTY_DLQ_ORIGIN_MESSAGE_ID);
        STRING_HASH_SET.add(PROPERTY_CRC32);
    }
}
