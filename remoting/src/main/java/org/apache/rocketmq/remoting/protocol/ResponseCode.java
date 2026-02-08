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
 * 响应类型
 */
public class ResponseCode extends RemotingSysResponseCode {

    /**
     * 刷盘超时
     */
    public static final int FLUSH_DISK_TIMEOUT = 10;

    /**
     * 从节点不可用
     */
    public static final int SLAVE_NOT_AVAILABLE = 11;

    /**
     * 同步到从节点超时
     */
    public static final int FLUSH_SLAVE_TIMEOUT = 12;

    /**
     * 消息非法
     */
    public static final int MESSAGE_ILLEGAL = 13;

    /**
     * 服务不可用
     */
    public static final int SERVICE_NOT_AVAILABLE = 14;

    /**
     * 版本不支持
     */
    public static final int VERSION_NOT_SUPPORTED = 15;

    /**
     * 权限不足
     */
    public static final int NO_PERMISSION = 16;

    /**
     * Topic 不存在
     */
    public static final int TOPIC_NOT_EXIST = 17;
    /**
     * Topic 已存在
     */
    public static final int TOPIC_EXIST_ALREADY = 18;
    /**
     * 拉取未命中消息
     */
    public static final int PULL_NOT_FOUND = 19;

    /**
     * 需要立即重试拉取
     */
    public static final int PULL_RETRY_IMMEDIATELY = 20;

    /**
     * 拉取位点已移动
     */
    public static final int PULL_OFFSET_MOVED = 21;

    /**
     * 查询结果不存在
     */
    public static final int QUERY_NOT_FOUND = 22;

    /**
     * 订阅表达式解析失败
     */
    public static final int SUBSCRIPTION_PARSE_FAILED = 23;

    /**
     * 订阅不存在
     */
    public static final int SUBSCRIPTION_NOT_EXIST = 24;

    /**
     * 订阅版本非最新
     */
    public static final int SUBSCRIPTION_NOT_LATEST = 25;

    /**
     * 订阅组不存在
     */
    public static final int SUBSCRIPTION_GROUP_NOT_EXIST = 26;

    /**
     * 过滤数据不存在
     */
    public static final int FILTER_DATA_NOT_EXIST = 27;

    /**
     * 过滤数据非最新
     */
    public static final int FILTER_DATA_NOT_LATEST = 28;

    /**
     * 参数非法
     */
    public static final int INVALID_PARAMETER = 29;

    /**
     * 事务应提交
     */
    public static final int TRANSACTION_SHOULD_COMMIT = 200;

    /**
     * 事务应回滚
     */
    public static final int TRANSACTION_SHOULD_ROLLBACK = 201;

    /**
     * 事务状态未知
     */
    public static final int TRANSACTION_STATE_UNKNOW = 202;

    /**
     * 事务状态所属组错误
     */
    public static final int TRANSACTION_STATE_GROUP_WRONG = 203;
    /**
     * 缺少买家标识
     */
    public static final int NO_BUYER_ID = 204;

    /**
     * 不在当前单元
     */
    public static final int NOT_IN_CURRENT_UNIT = 205;

    /**
     * 消费者不在线
     */
    public static final int CONSUMER_NOT_ONLINE = 206;

    /**
     * 消费超时
     */
    public static final int CONSUME_MSG_TIMEOUT = 207;

    /**
     * 无消息
     */
    public static final int NO_MESSAGE = 208;

    /**
     * 轮询队列已满
     */
    public static final int POLLING_FULL = 209;

    /**
     * 轮询超时
     */
    public static final int POLLING_TIMEOUT = 210;

    /**
     * Broker 不存在
     */
    public static final int BROKER_NOT_EXIST = 211;

    /**
     * Broker 分发未完成
     */
    public static final int BROKER_DISPATCH_NOT_COMPLETE = 212;

    /**
     * 广播消费限制
     */
    public static final int BROADCAST_CONSUMPTION = 213;

    /**
     * 触发流控
     */
    public static final int FLOW_CONTROL = 215;

    /**
     * 队列 Leader 非当前节点
     */
    public static final int NOT_LEADER_FOR_QUEUE = 501;

    /**
     * 非法操作
     */
    public static final int ILLEGAL_OPERATION = 604;

    /**
     * RPC 未知异常
     */
    public static final int RPC_UNKNOWN = -1000;
    /**
     * RPC 地址为空
     */
    public static final int RPC_ADDR_IS_NULL = -1002;
    /**
     * RPC 发送到通道失败
     */
    public static final int RPC_SEND_TO_CHANNEL_FAILED = -1004;
    /**
     * RPC 超时
     */
    public static final int RPC_TIME_OUT = -1006;

    /**
     * 连接需主动关闭
     */
    public static final int GO_AWAY = 1500;

    /**
     * Controller response code<br>
     * Controller 响应码段, Master epoch 被围栏拒绝
     */
    public static final int CONTROLLER_FENCED_MASTER_EPOCH = 2000;
    /**
     * 同步状态集 epoch 被围栏拒绝
     */
    public static final int CONTROLLER_FENCED_SYNC_STATE_SET_EPOCH = 2001;
    /**
     * Master 非法
     */
    public static final int CONTROLLER_INVALID_MASTER = 2002;
    /**
     * 副本集合非法
     */
    public static final int CONTROLLER_INVALID_REPLICAS = 2003;
    /**
     * Master 不可用
     */
    public static final int CONTROLLER_MASTER_NOT_AVAILABLE = 2004;
    /**
     * Controller 请求非法
     */
    public static final int CONTROLLER_INVALID_REQUEST = 2005;
    /**
     * Broker 不存活
     */
    public static final int CONTROLLER_BROKER_NOT_ALIVE = 2006;
    /**
     * 当前节点不是 Controller Leader
     */
    public static final int CONTROLLER_NOT_LEADER = 2007;

    /**
     * Broker 元数据不存在
     */
    public static final int CONTROLLER_BROKER_METADATA_NOT_EXIST = 2008;

    /**
     * 清理 Broker 元数据请求非法
     */
    public static final int CONTROLLER_INVALID_CLEAN_BROKER_METADATA = 2009;

    /**
     * Broker 需要先注册
     */
    public static final int CONTROLLER_BROKER_NEED_TO_BE_REGISTERED = 2010;

    /**
     * Master 仍然存在
     */
    public static final int CONTROLLER_MASTER_STILL_EXIST = 2011;

    /**
     * 选举 Master 失败
     */
    public static final int CONTROLLER_ELECT_MASTER_FAILED = 2012;
    
    /**
     * 变更同步状态集失败
     */
    public static final int CONTROLLER_ALTER_SYNC_STATE_SET_FAILED = 2013;

    /**
     * BrokerId 非法
     */
    public static final int CONTROLLER_BROKER_ID_INVALID = 2014;

    /**
     * JRaft 内部异常
     */
    public static final int CONTROLLER_JRAFT_INTERNAL_ERROR = 2015;

    /**
     * Broker 存活信息不存在
     */
    public static final int CONTROLLER_BROKER_LIVE_INFO_NOT_EXISTS = 2016;

    /**
     * 用户不存在
     */
    public static final int USER_NOT_EXIST = 3001;

    /**
     * 策略不存在
     */
    public static final int POLICY_NOT_EXIST = 3002;
}
