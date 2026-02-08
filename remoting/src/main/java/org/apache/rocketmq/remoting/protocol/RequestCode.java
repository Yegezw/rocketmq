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

package org.apache.rocketmq.remoting.protocol;

/**
 * 请求类型
 */
public class RequestCode {

    /**
     * 发送消息请求码
     */
    public static final int SEND_MESSAGE = 10;

    /**
     * 拉取消息请求码
     */
    public static final int PULL_MESSAGE = 11;

    /**
     * 查询消息请求码
     */
    public static final int QUERY_MESSAGE = 12;
    /**
     * 查询 Broker 位点请求码
     */
    public static final int QUERY_BROKER_OFFSET = 13;
    /**
     * 查询消费者位点请求码
     */
    public static final int QUERY_CONSUMER_OFFSET = 14;
    /**
     * 更新消费者位点请求码
     */
    public static final int UPDATE_CONSUMER_OFFSET = 15;
    /**
     * 更新并创建 Topic 请求码
     */
    public static final int UPDATE_AND_CREATE_TOPIC = 17;
    /**
     * 批量更新并创建 Topic 请求码
     */
    public static final int UPDATE_AND_CREATE_TOPIC_LIST = 18;
    /**
     * 获取全部 Topic 配置请求码
     */
    public static final int GET_ALL_TOPIC_CONFIG = 21;
    /**
     * 获取 Topic 配置列表请求码
     */
    public static final int GET_TOPIC_CONFIG_LIST = 22;

    /**
     * 获取 Topic 名称列表请求码
     */
    public static final int GET_TOPIC_NAME_LIST = 23;

    /**
     * 更新 Broker 配置请求码
     */
    public static final int UPDATE_BROKER_CONFIG = 25;

    /**
     * 获取 Broker 配置请求码
     */
    public static final int GET_BROKER_CONFIG = 26;

    /**
     * 触发删除文件请求码
     */
    public static final int TRIGGER_DELETE_FILES = 27;

    /**
     * 获取 Broker 运行信息请求码
     */
    public static final int GET_BROKER_RUNTIME_INFO = 28;
    /**
     * 按时间戳搜索位点请求码
     */
    public static final int SEARCH_OFFSET_BY_TIMESTAMP = 29;
    /**
     * 获取最大位点请求码
     */
    public static final int GET_MAX_OFFSET = 30;
    /**
     * 获取最小位点请求码
     */
    public static final int GET_MIN_OFFSET = 31;

    /**
     * 获取最早消息存储时间请求码
     */
    public static final int GET_EARLIEST_MSG_STORETIME = 32;

    /**
     * 按消息 ID 查看消息请求码
     */
    public static final int VIEW_MESSAGE_BY_ID = 33;

    /**
     * 心跳请求码
     */
    public static final int HEART_BEAT = 34;

    /**
     * 客户端注销请求码
     */
    public static final int UNREGISTER_CLIENT = 35;

    /**
     * 消费者回退消息请求码
     */
    public static final int CONSUMER_SEND_MSG_BACK = 36;

    /**
     * 结束事务请求码
     */
    public static final int END_TRANSACTION = 37;
    /**
     * 按消费组获取消费者列表请求码
     */
    public static final int GET_CONSUMER_LIST_BY_GROUP = 38;

    /**
     * 检查事务状态请求码
     */
    public static final int CHECK_TRANSACTION_STATE = 39;

    /**
     * 通知消费者 ID 变更请求码
     */
    public static final int NOTIFY_CONSUMER_IDS_CHANGED = 40;

    /**
     * 批量锁定消息队列请求码
     */
    public static final int LOCK_BATCH_MQ = 41;

    /**
     * 批量解锁消息队列请求码
     */
    public static final int UNLOCK_BATCH_MQ = 42;
    /**
     * 获取全部消费者位点请求码
     */
    public static final int GET_ALL_CONSUMER_OFFSET = 43;

    /**
     * 获取全部延迟位点请求码
     */
    public static final int GET_ALL_DELAY_OFFSET = 45;

    /**
     * 校验客户端配置请求码
     */
    public static final int CHECK_CLIENT_CONFIG = 46;

    /**
     * 获取客户端配置请求码
     */
    public static final int GET_CLIENT_CONFIG = 47;

    /**
     * 获取定时器检查点请求码
     */
    public static final int GET_TIMER_CHECK_POINT = 60;

    /**
     * 获取定时器指标请求码
     */
    public static final int GET_TIMER_METRICS = 61;

    /**
     * POP 消息请求码
     */
    public static final int POP_MESSAGE = 200050;
    /**
     * ACK 消息请求码
     */
    public static final int ACK_MESSAGE = 200051;
    /**
     * 批量 ACK 消息请求码
     */
    public static final int BATCH_ACK_MESSAGE = 200151;
    /**
     * 窥探消息请求码
     */
    public static final int PEEK_MESSAGE = 200052;
    /**
     * 修改消息不可见时间请求码
     */
    public static final int CHANGE_MESSAGE_INVISIBLETIME = 200053;
    /**
     * 通知请求码
     */
    public static final int NOTIFICATION = 200054;
    /**
     * 轮询信息请求码
     */
    public static final int POLLING_INFO = 200055;
    /**
     * POP 回滚请求码
     */
    public static final int POP_ROLLBACK = 200056;

    /**
     * 写入 KV 配置请求码
     */
    public static final int PUT_KV_CONFIG = 100;

    /**
     * 获取 KV 配置请求码
     */
    public static final int GET_KV_CONFIG = 101;

    /**
     * 删除 KV 配置请求码
     */
    public static final int DELETE_KV_CONFIG = 102;

    /**
     * 注册 Broker 信息
     */
    public static final int REGISTER_BROKER = 103;

    /**
     * 注销 Broker 信息
     */
    public static final int UNREGISTER_BROKER = 104;
    /**
     * 根据主题获取路由信息
     */
    public static final int GET_ROUTEINFO_BY_TOPIC = 105;

    /**
     * 获取 Broker 集群信息请求码
     */
    public static final int GET_BROKER_CLUSTER_INFO = 106;
    /**
     * 更新并创建订阅组请求码
     */
    public static final int UPDATE_AND_CREATE_SUBSCRIPTIONGROUP = 200;
    /**
     * 获取全部订阅组配置请求码
     */
    public static final int GET_ALL_SUBSCRIPTIONGROUP_CONFIG = 201;
    /**
     * 获取 Topic 统计信息请求码
     */
    public static final int GET_TOPIC_STATS_INFO = 202;
    /**
     * 获取消费者连接列表请求码
     */
    public static final int GET_CONSUMER_CONNECTION_LIST = 203;
    /**
     * 获取生产者连接列表请求码
     */
    public static final int GET_PRODUCER_CONNECTION_LIST = 204;
    /**
     * 清除 Broker 写权限请求码
     */
    public static final int WIPE_WRITE_PERM_OF_BROKER = 205;

    /**
     * 从 NameServer 获取全部 Topic 列表请求码
     */
    public static final int GET_ALL_TOPIC_LIST_FROM_NAMESERVER = 206;

    /**
     * 删除订阅组请求码
     */
    public static final int DELETE_SUBSCRIPTIONGROUP = 207;
    /**
     * 获取消费统计请求码
     */
    public static final int GET_CONSUME_STATS = 208;

    /**
     * 暂停消费者请求码
     */
    public static final int SUSPEND_CONSUMER = 209;

    /**
     * 恢复消费者请求码
     */
    public static final int RESUME_CONSUMER = 210;
    /**
     * 在消费者端重置位点请求码
     */
    public static final int RESET_CONSUMER_OFFSET_IN_CONSUMER = 211;
    /**
     * 在 Broker 端重置位点请求码
     */
    public static final int RESET_CONSUMER_OFFSET_IN_BROKER = 212;

    /**
     * 调整消费者线程池请求码
     */
    public static final int ADJUST_CONSUMER_THREAD_POOL = 213;

    /**
     * 查询消息被谁消费请求码
     */
    public static final int WHO_CONSUME_THE_MESSAGE = 214;

    /**
     * 在 Broker 删除 Topic 请求码
     */
    public static final int DELETE_TOPIC_IN_BROKER = 215;

    /**
     * 在 NameServer 删除 Topic 请求码
     */
    public static final int DELETE_TOPIC_IN_NAMESRV = 216;
    /**
     * 在 NameServer 注册 Topic 请求码
     */
    public static final int REGISTER_TOPIC_IN_NAMESRV = 217;
    /**
     * 按命名空间获取 KV 列表请求码
     */
    public static final int GET_KVLIST_BY_NAMESPACE = 219;

    /**
     * 重置消费者客户端位点请求码
     */
    public static final int RESET_CONSUMER_CLIENT_OFFSET = 220;

    /**
     * 从客户端获取消费者状态请求码
     */
    public static final int GET_CONSUMER_STATUS_FROM_CLIENT = 221;

    /**
     * 调用 Broker 重置位点请求码
     */
    public static final int INVOKE_BROKER_TO_RESET_OFFSET = 222;

    /**
     * 调用 Broker 获取消费者状态请求码
     */
    public static final int INVOKE_BROKER_TO_GET_CONSUMER_STATUS = 223;

    /**
     * 查询 Topic 被谁消费请求码
     */
    public static final int QUERY_TOPIC_CONSUME_BY_WHO = 300;

    /**
     * 按集群获取 Topic 列表请求码
     */
    public static final int GET_TOPICS_BY_CLUSTER = 224;

    /**
     * 批量更新并创建订阅组请求码
     */
    public static final int UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST = 225;

    /**
     * 按消费者查询 Topic 请求码
     */
    public static final int QUERY_TOPICS_BY_CONSUMER = 343;
    /**
     * 按消费者查询订阅请求码
     */
    public static final int QUERY_SUBSCRIPTION_BY_CONSUMER = 345;

    /**
     * 注册过滤服务请求码
     */
    public static final int REGISTER_FILTER_SERVER = 301;
    /**
     * 注册消息过滤类请求码
     */
    public static final int REGISTER_MESSAGE_FILTER_CLASS = 302;

    /**
     * 查询消费时间跨度请求码
     */
    public static final int QUERY_CONSUME_TIME_SPAN = 303;

    /**
     * 从 NameServer 获取系统 Topic 列表请求码
     */
    public static final int GET_SYSTEM_TOPIC_LIST_FROM_NS = 304;
    /**
     * 从 Broker 获取系统 Topic 列表请求码
     */
    public static final int GET_SYSTEM_TOPIC_LIST_FROM_BROKER = 305;

    /**
     * 清理过期消费队列请求码
     */
    public static final int CLEAN_EXPIRED_CONSUMEQUEUE = 306;

    /**
     * 获取消费者运行信息请求码
     */
    public static final int GET_CONSUMER_RUNNING_INFO = 307;

    /**
     * 查询纠正位点请求码
     */
    public static final int QUERY_CORRECTION_OFFSET = 308;
    /**
     * 直接消费消息请求码
     */
    public static final int CONSUME_MESSAGE_DIRECTLY = 309;

    /**
     * 发送消息 V2 请求码
     */
    public static final int SEND_MESSAGE_V2 = 310;

    /**
     * 获取单元 Topic 列表请求码
     */
    public static final int GET_UNIT_TOPIC_LIST = 311;

    /**
     * 获取含单元订阅 Topic 列表请求码
     */
    public static final int GET_HAS_UNIT_SUB_TOPIC_LIST = 312;

    /**
     * 获取含单元订阅且非单元 Topic 列表请求码
     */
    public static final int GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST = 313;

    /**
     * 克隆消费组位点请求码
     */
    public static final int CLONE_GROUP_OFFSET = 314;

    /**
     * 查看 Broker 统计数据请求码
     */
    public static final int VIEW_BROKER_STATS_DATA = 315;

    /**
     * 清理未使用 Topic 请求码
     */
    public static final int CLEAN_UNUSED_TOPIC = 316;

    /**
     * 获取 Broker 消费统计请求码
     */
    public static final int GET_BROKER_CONSUME_STATS = 317;

    /**
     * update the config of name server<br>
     * 更新 NameServer 配置请求码
     */
    public static final int UPDATE_NAMESRV_CONFIG = 318;

    /**
     * get config from name server<br>
     * 从 NameServer 获取配置请求码
     */
    public static final int GET_NAMESRV_CONFIG = 319;

    /**
     * 发送批量消息请求码
     */
    public static final int SEND_BATCH_MESSAGE = 320;

    /**
     * 查询消费队列请求码
     */
    public static final int QUERY_CONSUME_QUEUE = 321;

    /**
     * 查询数据版本请求码
     */
    public static final int QUERY_DATA_VERSION = 322;

    /**
     * resume logic of checking half messages that have been put in TRANS_CHECK_MAXTIME_TOPIC before<br>
     * 恢复检查半消息逻辑请求码
     */
    public static final int RESUME_CHECK_HALF_MESSAGE = 323;

    /**
     * 发送回复消息请求码
     */
    public static final int SEND_REPLY_MESSAGE = 324;

    /**
     * 发送回复消息 V2 请求码
     */
    public static final int SEND_REPLY_MESSAGE_V2 = 325;

    /**
     * 推送回复消息到客户端请求码
     */
    public static final int PUSH_REPLY_MESSAGE_TO_CLIENT = 326;

    /**
     * 添加 Broker 写权限请求码
     */
    public static final int ADD_WRITE_PERM_OF_BROKER = 327;
    
    /**
     * 获取全部生产者信息请求码
     */
    public static final int GET_ALL_PRODUCER_INFO = 328;
    
    /**
     * 删除过期 CommitLog 请求码
     */
    public static final int DELETE_EXPIRED_COMMITLOG = 329;

    /**
     * 获取 Topic 配置请求码
     */
    public static final int GET_TOPIC_CONFIG = 351;

    /**
     * 获取订阅组配置请求码
     */
    public static final int GET_SUBSCRIPTIONGROUP_CONFIG = 352;
    /**
     * 更新并获取消费组禁用配置请求码
     */
    public static final int UPDATE_AND_GET_GROUP_FORBIDDEN = 353;
    /**
     * 检查 RocksDB CQ 写入进度请求码
     */
    public static final int CHECK_ROCKSDB_CQ_WRITE_PROGRESS = 354;
    /**
     * 导出 RocksDB 配置到 JSON 请求码
     */
    public static final int EXPORT_ROCKSDB_CONFIG_TO_JSON = 355;

    /**
     * Lite 拉取消息请求码
     */
    public static final int LITE_PULL_MESSAGE = 361;
    /**
     * 消息撤回请求码
     */
    public static final int RECALL_MESSAGE = 370;

    /**
     * 查询分配结果请求码
     */
    public static final int QUERY_ASSIGNMENT = 400;
    /**
     * 设置消息请求模式请求码
     */
    public static final int SET_MESSAGE_REQUEST_MODE = 401;
    /**
     * 获取全部消息请求模式请求码
     */
    public static final int GET_ALL_MESSAGE_REQUEST_MODE = 402;

    /**
     * 更新并创建静态 Topic 请求码
     */
    public static final int UPDATE_AND_CREATE_STATIC_TOPIC = 513;

    /**
     * 获取 Broker 成员组请求码
     */
    public static final int GET_BROKER_MEMBER_GROUP = 901;

    /**
     * 添加 Broker 请求码
     */
    public static final int ADD_BROKER = 902;

    /**
     * 移除 Broker 请求码
     */
    public static final int REMOVE_BROKER = 903;

    /**
     * Broker 心跳检测
     */
    public static final int BROKER_HEARTBEAT = 904;

    /**
     * 通知最小 BrokerId 变更请求码
     */
    public static final int NOTIFY_MIN_BROKER_ID_CHANGE = 905;

    /**
     * 交换 Broker HA 信息请求码
     */
    public static final int EXCHANGE_BROKER_HA_INFO = 906;

    /**
     * 获取 Broker HA 状态请求码
     */
    public static final int GET_BROKER_HA_STATUS = 907;

    /**
     * 重置主节点刷盘位点请求码
     */
    public static final int RESET_MASTER_FLUSH_OFFSET = 908;

    /**
     * Controller code
     * Controller 请求码段
     */
    public static final int CONTROLLER_ALTER_SYNC_STATE_SET = 1001;

    /**
     * Controller 选举主节点请求码
     */
    public static final int CONTROLLER_ELECT_MASTER = 1002;

    /**
     * Controller 注册 Broker 请求码
     */
    public static final int CONTROLLER_REGISTER_BROKER = 1003;

    /**
     * Controller 获取副本信息请求码
     */
    public static final int CONTROLLER_GET_REPLICA_INFO = 1004;

    /**
     * Controller 获取元数据请求码
     */
    public static final int CONTROLLER_GET_METADATA_INFO = 1005;

    /**
     * Controller 获取同步状态数据请求码
     */
    public static final int CONTROLLER_GET_SYNC_STATE_DATA = 1006;

    /**
     * 获取 Broker epoch 缓存请求码
     */
    public static final int GET_BROKER_EPOCH_CACHE = 1007;

    /**
     * 通知 Broker 角色变更请求码
     */
    public static final int NOTIFY_BROKER_ROLE_CHANGED = 1008;

    /**
     * update the config of controller
     * 更新 Controller 配置请求码
     */
    public static final int UPDATE_CONTROLLER_CONFIG = 1009;

    /**
     * get config from controller
     * 从 Controller 获取配置请求码
     */
    public static final int GET_CONTROLLER_CONFIG = 1010;

    /**
     * clean broker data
     * 清理 Broker 数据请求码
     */
    public static final int CLEAN_BROKER_DATA = 1011;
    /**
     * Controller 获取下一个 BrokerId 请求码
     */
    public static final int CONTROLLER_GET_NEXT_BROKER_ID = 1012;

    /**
     * Controller 申请 BrokerId 请求码
     */
    public static final int CONTROLLER_APPLY_BROKER_ID = 1013;
    /**
     * Broker 关闭通道请求码
     */
    public static final short BROKER_CLOSE_CHANNEL_REQUEST = 1014;
    /**
     * 检查非活跃 Broker 请求码
     */
    public static final short CHECK_NOT_ACTIVE_BROKER_REQUEST = 1015;
    /**
     * 获取 Broker 存活信息请求码
     */
    public static final short GET_BROKER_LIVE_INFO_REQUEST = 1016;
    /**
     * 获取同步状态数据请求码
     */
    public static final short GET_SYNC_STATE_DATA_REQUEST = 1017;
    /**
     * Raft Broker 心跳事件请求码
     */
    public static final short RAFT_BROKER_HEART_BEAT_EVENT_REQUEST = 1018;

    /**
     * 更新冷数据流控配置请求码
     */
    public static final int UPDATE_COLD_DATA_FLOW_CTR_CONFIG = 2001;
    /**
     * 移除冷数据流控配置请求码
     */
    public static final int REMOVE_COLD_DATA_FLOW_CTR_CONFIG = 2002;
    /**
     * 获取冷数据流控信息请求码
     */
    public static final int GET_COLD_DATA_FLOW_CTR_INFO = 2003;
    /**
     * 设置 CommitLog 读取模式请求码
     */
    public static final int SET_COMMITLOG_READ_MODE = 2004;

    /**
     * 鉴权创建用户请求码
     */
    public static final int AUTH_CREATE_USER = 3001;
    /**
     * 鉴权更新用户请求码
     */
    public static final int AUTH_UPDATE_USER = 3002;
    /**
     * 鉴权删除用户请求码
     */
    public static final int AUTH_DELETE_USER = 3003;
    /**
     * 鉴权获取用户请求码
     */
    public static final int AUTH_GET_USER = 3004;
    /**
     * 鉴权列出用户请求码
     */
    public static final int AUTH_LIST_USER = 3005;

    /**
     * 鉴权创建 ACL 请求码
     */
    public static final int AUTH_CREATE_ACL = 3006;
    /**
     * 鉴权更新 ACL 请求码
     */
    public static final int AUTH_UPDATE_ACL = 3007;
    /**
     * 鉴权删除 ACL 请求码
     */
    public static final int AUTH_DELETE_ACL = 3008;
    /**
     * 鉴权获取 ACL 请求码
     */
    public static final int AUTH_GET_ACL = 3009;
    /**
     * 鉴权列出 ACL 请求码
     */
    public static final int AUTH_LIST_ACL = 3010;
}
