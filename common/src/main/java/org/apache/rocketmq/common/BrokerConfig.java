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
package org.apache.rocketmq.common;

import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.common.config.ConfigManagerVersion;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.common.metrics.MetricsExporterType;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.NetworkUtil;

import java.util.concurrent.TimeUnit;

public class BrokerConfig extends BrokerIdentity {

    /**
     * Broker 配置文件路径
     */
    private String brokerConfigPath = null;

    /**
     * RocketMQ 安装目录
     */
    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));
    /**
     * NameServer 地址列表
     */
    @ImportantField
    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));

    /**
     * Listen port for single broker
     * <br>
     * Broker 监听端口
     */
    @ImportantField
    private int listenPort = 6888;

    /**
     * Broker 主通信地址
     */
    @ImportantField
    private String brokerIP1 = NetworkUtil.getLocalAddress();
    /**
     * Broker 备用通信地址
     */
    private String brokerIP2 = NetworkUtil.getLocalAddress();

    /**
     * 是否并发执行恢复流程
     */
    @ImportantField
    private boolean recoverConcurrently = false;

    /**
     * Broker 读写权限位
     */
    private int brokerPermission = PermName.PERM_READ | PermName.PERM_WRITE;
    /**
     * 默认 Topic 队列数量
     */
    private int defaultTopicQueueNums = 8;
    /**
     * 是否自动创建 Topic
     */
    @ImportantField
    private boolean autoCreateTopicEnable = true;

    /**
     * 是否允许集群 Topic
     */
    private boolean clusterTopicEnable = true;

    /**
     * 是否允许 Broker 内置 Topic
     */
    private boolean brokerTopicEnable = true;
    /**
     * 是否自动创建订阅组
     */
    @ImportantField
    private boolean autoCreateSubscriptionGroup = true;
    /**
     * 消息存储插件名称
     */
    private String messageStorePlugIn = "";

    /**
     * 本机 CPU 核心数量
     */
    private static final int PROCESSOR_NUMBER = Runtime.getRuntime().availableProcessors();
    /**
     * 消息轨迹 Topic 名称
     */
    @ImportantField
    private String msgTraceTopicName = TopicValidator.RMQ_SYS_TRACE_TOPIC;
    /**
     * 是否启用消息轨迹 Topic
     */
    @ImportantField
    private boolean traceTopicEnable = false;
    /**
     * thread numbers for send message thread pool.
     * <br>
     * 发送消息线程池线程数量
     */
    private int sendMessageThreadPoolNums = Math.min(PROCESSOR_NUMBER, 4);
    /**
     * 异步写入消息线程池线程数量
     */
    private int putMessageFutureThreadPoolNums = Math.min(PROCESSOR_NUMBER, 4);
    /**
     * 拉取消息线程池线程数量
     */
    private int pullMessageThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;
    /**
     * 轻量拉取线程池线程数量
     */
    private int litePullMessageThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;
    /**
     * 确认消息线程池线程数量
     */
    private int ackMessageThreadPoolNums = 16;
    /**
     * 应答消息处理线程池线程数量
     */
    private int processReplyMessageThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;
    /**
     * 查询消息线程池线程数量
     */
    private int queryMessageThreadPoolNums = 8 + PROCESSOR_NUMBER;

    /**
     * Broker 管理线程池线程数量
     */
    private int adminBrokerThreadPoolNums = 16;
    /**
     * 客户端管理线程池线程数量
     */
    private int clientManageThreadPoolNums = 32;
    /**
     * 消费者管理线程池线程数量
     */
    private int consumerManageThreadPoolNums = 32;
    /**
     * 负载均衡线程池线程数量
     */
    private int loadBalanceProcessorThreadPoolNums = 32;
    /**
     * 心跳处理线程池线程数量
     */
    private int heartbeatThreadPoolNums = Math.min(32, PROCESSOR_NUMBER);
    /**
     * 恢复线程池线程数量
     */
    private int recoverThreadPoolNums = 32;

    /**
     * Thread numbers for EndTransactionProcessor
     * <br>
     * 结束事务线程池线程数量
     */
    private int endTransactionThreadPoolNums = Math.max(8 + PROCESSOR_NUMBER * 2,
            sendMessageThreadPoolNums * 4);

    /**
     * 刷写消费位点间隔, 单位毫秒
     */
    private int flushConsumerOffsetInterval = 1000 * 5;

    /**
     * 刷写历史消费位点间隔, 单位毫秒
     */
    private int flushConsumerOffsetHistoryInterval = 1000 * 60;

    /**
     * 是否拒绝事务消息
     */
    @ImportantField
    private boolean rejectTransactionMessage = false;

    /**
     * 是否通过 DNS 解析 NameServer 地址
     */
    @ImportantField
    private boolean fetchNameSrvAddrByDnsLookup = false;

    /**
     * 是否通过地址服务获取 NameServer 地址
     */
    @ImportantField
    private boolean fetchNamesrvAddrByAddressServer = false;

    /**
     * 发送线程池队列容量
     */
    private int sendThreadPoolQueueCapacity = 10000;
    /**
     * 写入线程池队列容量
     */
    private int putThreadPoolQueueCapacity = 10000;
    /**
     * 拉取线程池队列容量
     */
    private int pullThreadPoolQueueCapacity = 100000;
    /**
     * 轻量拉取线程池队列容量
     */
    private int litePullThreadPoolQueueCapacity = 100000;
    /**
     * 确认线程池队列容量
     */
    private int ackThreadPoolQueueCapacity = 100000;
    /**
     * 应答线程池队列容量
     */
    private int replyThreadPoolQueueCapacity = 10000;
    /**
     * 查询线程池队列容量
     */
    private int queryThreadPoolQueueCapacity = 20000;
    /**
     * 客户端管理线程池队列容量
     */
    private int clientManagerThreadPoolQueueCapacity = 1000000;
    /**
     * 消费者管理线程池队列容量
     */
    private int consumerManagerThreadPoolQueueCapacity = 1000000;
    /**
     * 心跳线程池队列容量
     */
    private int heartbeatThreadPoolQueueCapacity = 50000;
    /**
     * 结束事务线程池队列容量
     */
    private int endTransactionPoolQueueCapacity = 100000;
    /**
     * Broker 管理线程池队列容量
     */
    private int adminBrokerThreadPoolQueueCapacity = 10000;
    /**
     * 负载均衡线程池队列容量
     */
    private int loadBalanceThreadPoolQueueCapacity = 100000;

    /**
     * 是否启用长轮询
     */
    private boolean longPollingEnable = true;

    /**
     * 短轮询间隔, 单位毫秒
     */
    private long shortPollingTimeMills = 1000;

    /**
     * 是否通知消费者列表变更
     */
    private boolean notifyConsumerIdsChangedEnable = true;

    /**
     * 是否启用高速模式
     */
    private boolean highSpeedMode = false;

    /**
     * 商业统计基础计数
     */
    private int commercialBaseCount = 1;

    /**
     * 商业统计单条消息大小, 单位字节
     */
    private int commercialSizePerMsg = 4 * 1024;

    /**
     * 是否启用账号统计
     */
    private boolean accountStatsEnable = true;
    /**
     * 是否输出零值统计项
     */
    private boolean accountStatsPrintZeroValues = true;

    /**
     * 统计项最大空闲时间, 单位分钟
     */
    private int maxStatsIdleTimeInMinutes = -1;

    /**
     * 是否通过堆内存传输消息
     */
    private boolean transferMsgByHeap = true;

    /**
     * 地域标识
     */
    private String regionId = MixAll.DEFAULT_TRACE_REGION_ID;
    /**
     * 注册 Broker 超时时间, 单位毫秒
     */
    private int registerBrokerTimeoutMills = 24000;

    /**
     * 发送心跳超时时间, 单位毫秒
     */
    private int sendHeartbeatTimeoutMillis = 1000;

    /**
     * 是否允许从节点读取
     */
    private boolean slaveReadEnable = false;

    /**
     * 是否在消费者慢读时禁用消费
     */
    private boolean disableConsumeIfConsumerReadSlowly = false;
    /**
     * 消费者堆积阈值, 单位字节
     */
    private long consumerFallbehindThreshold = 1024L * 1024 * 1024 * 16;

    /**
     * 是否启用 Broker 快速失败机制
     */
    private boolean brokerFastFailureEnable = true;
    /**
     * 发送队列最大等待时间, 单位毫秒
     */
    private long waitTimeMillsInSendQueue = 200;
    /**
     * 拉取队列最大等待时间, 单位毫秒
     */
    private long waitTimeMillsInPullQueue = 5 * 1000;
    /**
     * 轻量拉取队列最大等待时间, 单位毫秒
     */
    private long waitTimeMillsInLitePullQueue = 5 * 1000;
    /**
     * 心跳队列最大等待时间, 单位毫秒
     */
    private long waitTimeMillsInHeartbeatQueue = 31 * 1000;
    /**
     * 事务队列最大等待时间, 单位毫秒
     */
    private long waitTimeMillsInTransactionQueue = 3 * 1000;
    /**
     * 确认队列最大等待时间, 单位毫秒
     */
    private long waitTimeMillsInAckQueue = 3000;
    /**
     * 管理队列最大等待时间, 单位毫秒
     */
    private long waitTimeMillsInAdminBrokerQueue = 5 * 1000;
    /**
     * 开始接受发送请求时间戳
     */
    private long startAcceptSendRequestTimeStamp = 0L;

    /**
     * 是否启用消息轨迹
     */
    private boolean traceOn = true;

    // Switch of filter bit map calculation.
    // If switch on:
    // 1. Calculate filter bit map when construct queue.
    // 2. Filter bit map will be saved to consume queue extend file if allowed.
    // 是否启用过滤位图计算
    private boolean enableCalcFilterBitMap = false;

    //Reject the pull consumer instance to pull messages from broker.
    // 是否拒绝拉取消费者请求
    private boolean rejectPullConsumerEnable = false;

    // Expect num of consumers will use filter.
    // 预期使用过滤器的消费者数量
    private int expectConsumerNumUseFilter = 32;

    // Error rate of bloom filter, 1~100.
    // 布隆过滤器最大误判率
    private int maxErrorRateOfBloomFilter = 20;

    //how long to clean filter data after dead.Default: 24h
    // 过滤数据清理时长, 单位毫秒
    private long filterDataCleanTimeSpan = 24 * 3600 * 1000;

    // whether do filter when retry.
    // 是否在重试时启用过滤
    private boolean filterSupportRetry = false;
    /**
     * 是否启用属性过滤
     */
    private boolean enablePropertyFilter = false;

    /**
     * 是否启用注册压缩
     */
    private boolean compressedRegister = false;

    /**
     * 是否强制注册
     */
    private boolean forceRegister = true;

    /**
     * This configurable item defines interval of topics registration of broker to name server. Allowing values are
     * between 10,000 and 60,000 milliseconds.
     * <br>
     * 向 NameServer 注册间隔, 单位毫秒
     */
    private int registerNameServerPeriod = 1000 * 30;

    /**
     * This configurable item defines interval of update name server address. Default: 120 * 1000 milliseconds
     * <br>
     * 更新 NameServer 地址间隔, 单位毫秒
     */
    private int updateNameServerAddrPeriod = 1000 * 120;

    /**
     * the interval to send heartbeat to name server for liveness detection.
     * <br>
     * Broker 心跳间隔, 单位毫秒
     */
    private int brokerHeartbeatInterval = 1000;

    /**
     * How long the broker will be considered as inactive by nameserver since last heartbeat. Effective only if
     * enableSlaveActingMaster is true
     * <br>
     * Broker 不活跃判定超时, 单位毫秒
     */
    private long brokerNotActiveTimeoutMillis = 10 * 1000;

    /**
     * 是否启用网络流控
     */
    private boolean enableNetWorkFlowControl = false;

    /**
     * 是否启用广播消费位点存储
     */
    private boolean enableBroadcastOffsetStore = true;

    /**
     * 广播消费位点过期时间, 单位秒
     */
    private long broadcastOffsetExpireSecond = 2 * 60;

    /**
     * 广播消费位点最大过期时间, 单位秒
     */
    private long broadcastOffsetExpireMaxSecond = 5 * 60;

    /**
     * Pop 轮询槽位大小
     */
    private int popPollingSize = 1024;
    /**
     * Pop 轮询映射容量
     */
    private int popPollingMapSize = 100000;
    // 20w cost 200M heap memory.
    // Pop 轮询最大容量
    private long maxPopPollingSize = 100000;
    /**
     * Revive 队列数量
     */
    private int reviveQueueNum = 8;
    /**
     * Revive 执行间隔, 单位毫秒
     */
    private long reviveInterval = 1000;
    /**
     * Revive 最大慢处理阈值
     */
    private long reviveMaxSlow = 3;
    /**
     * Revive 扫描间隔, 单位毫秒
     */
    private long reviveScanTime = 10000;
    /**
     * 是否跳过长等待 Ack 消息
     */
    private boolean enableSkipLongAwaitingAck = false;
    /**
     * Revive Ack 等待时间, 单位毫秒
     */
    private long reviveAckWaitMs = TimeUnit.MINUTES.toMillis(3);
    /**
     * 是否启用 Pop 日志
     */
    private boolean enablePopLog = false;
    /**
     * 是否启用 Pop Buffer 合并
     */
    private boolean enablePopBufferMerge = false;
    /**
     * Pop Ck 在 Buffer 停留时间, 单位毫秒
     */
    private int popCkStayBufferTime = 10 * 1000;
    /**
     * Pop Ck Buffer 超时时间, 单位毫秒
     */
    private int popCkStayBufferTimeOut = 3 * 1000;
    /**
     * Pop Ck Buffer 最大容量
     */
    private int popCkMaxBufferSize = 200000;
    /**
     * Pop Ck 位点队列最大长度
     */
    private int popCkOffsetMaxQueueSize = 20000;
    /**
     * 是否启用 Pop 批量 Ack
     */
    private boolean enablePopBatchAck = false;
    // set the interval to the maxFilterMessageSize in MessageStoreConfig divided by the cq unit size
    // Pop 长轮询强制通知间隔, 单位毫秒
    private long popLongPollingForceNotifyInterval = 800;
    /**
     * 是否在 Pop 计算堆积前通知
     */
    private boolean enableNotifyBeforePopCalculateLag = true;
    /**
     * 是否在 Pop 顺序锁释放后通知
     */
    private boolean enableNotifyAfterPopOrderLockRelease = true;
    /**
     * 是否通过内存消息检查初始化 Pop 位点
     */
    private boolean initPopOffsetByCheckMsgInMem = true;
    // read message from pop retry topic v1, for the compatibility, will be removed in the future version
    // 是否从 Pop Retry Topic V1 读取消息
    private boolean retrieveMessageFromPopRetryTopicV1 = true;
    /**
     * 是否启用 Retry Topic V2
     */
    private boolean enableRetryTopicV2 = false;
    /**
     * 从重试 Topic 拉取概率, 单位百分比
     */
    private int popFromRetryProbability = 20;
    /**
     * 是否初始化 Pop 消费者 FS 服务
     */
    private boolean popConsumerFSServiceInit = true;
    /**
     * 是否开启 Pop 消费者 KV 服务日志
     */
    private boolean popConsumerKVServiceLog = false;
    /**
     * 是否初始化 Pop 消费者 KV 服务
     */
    private boolean popConsumerKVServiceInit = false;
    /**
     * 是否启用 Pop 消费者 KV 服务
     */
    private boolean popConsumerKVServiceEnable = false;
    /**
     * Pop Revive 单次读取最大返回大小, 单位字节
     */
    private int popReviveMaxReturnSizePerRead = 16 * 1024;
    /**
     * Pop Revive 最大尝试次数
     */
    private int popReviveMaxAttemptTimes = 16;

    /**
     * 是否实时通知消费者变更
     */
    private boolean realTimeNotifyConsumerChange = true;

    /**
     * 是否启用轻量拉取
     */
    private boolean litePullMessageEnable = true;

    // The period to sync broker member group from namesrv, default value is 1 second
    // 同步 Broker 成员组间隔, 单位毫秒
    private int syncBrokerMemberGroupPeriod = 1000;

    /**
     * the interval of pulling topic information from the named server
     * <br>
     * 负载均衡轮询 NameServer 间隔, 单位毫秒
     */
    private long loadBalancePollNameServerInterval = 1000 * 30;

    /**
     * the interval of cleaning
     * <br>
     * 清理离线 Broker 间隔, 单位毫秒
     */
    private int cleanOfflineBrokerInterval = 1000 * 30;

    /**
     * 是否启用服务端负载均衡
     */
    private boolean serverLoadBalancerEnable = true;

    /**
     * 默认消息请求模式
     */
    private MessageRequestMode defaultMessageRequestMode = MessageRequestMode.PULL;

    /**
     * 默认 Pop 共享队列数量
     */
    private int defaultPopShareQueueNum = -1;

    /**
     * The minimum time of the transactional message  to be checked firstly, one message only exceed this time interval
     * that can be checked.
     * <br>
     * 事务消息首次回查超时, 单位毫秒
     */
    @ImportantField
    private long transactionTimeOut = 6 * 1000;

    /**
     * The maximum number of times the message was checked, if exceed this value, this message will be discarded.
     * <br>
     * 事务消息最大回查次数
     */
    @ImportantField
    private int transactionCheckMax = 15;

    /**
     * Transaction message check interval.
     * <br>
     * 事务消息回查间隔, 单位毫秒
     */
    @ImportantField
    private long transactionCheckInterval = 30 * 1000;

    /**
     * 事务指标刷写间隔, 单位毫秒
     */
    private long transactionMetricFlushInterval = 3 * 1000;

    /**
     * transaction batch op message
     * <br>
     * 事务操作消息最大大小, 单位字节
     */
    private int transactionOpMsgMaxSize = 4096;

    /**
     * 事务操作批处理间隔, 单位毫秒
     */
    private int transactionOpBatchInterval = 3000;

    /**
     * Acl feature switch
     * <br>
     * 是否启用 ACL
     */
    @ImportantField
    private boolean aclEnable = false;

    /**
     * 是否存储应答消息
     */
    private boolean storeReplyMessageEnable = true;

    /**
     * 是否启用明细统计
     */
    private boolean enableDetailStat = true;

    /**
     * 是否自动删除未使用统计项
     */
    private boolean autoDeleteUnusedStats = true;

    /**
     * Whether to distinguish log paths when multiple brokers are deployed on the same machine
     * <br>
     * 是否隔离多 Broker 日志路径
     */
    private boolean isolateLogEnable = false;

    /**
     * 转发请求超时时间, 单位毫秒
     */
    private long forwardTimeout = 3 * 1000;

    /**
     * Slave will act master when failover. For example, if master down, timer or transaction message which is expire in slave will
     * put to master (master of the same process in broker container mode or other masters in cluster when enableFailoverRemotingActing is true)
     * when enableSlaveActingMaster is true
     * <br>
     * 是否启用 Slave Acting Master
     */
    private boolean enableSlaveActingMaster = false;

    /**
     * 是否启用远程逃逸
     */
    private boolean enableRemoteEscape = false;

    /**
     * 是否跳过预上线流程
     */
    private boolean skipPreOnline = false;

    /**
     * 是否启用异步发送
     */
    private boolean asyncSendEnable = true;

    /**
     * 是否使用服务端重置位点
     */
    private boolean useServerSideResetOffset = true;

    /**
     * 消费位点版本步长
     */
    private long consumerOffsetUpdateVersionStep = 500;

    /**
     * 延迟位点版本步长
     */
    private long delayOffsetUpdateVersionStep = 200;

    /**
     * Whether to lock quorum replicas.
     * <br>
     * True: need to lock quorum replicas succeed. False: only need to lock one replica succeed.
     * <br>
     * 是否启用严格锁定模式
     */
    private boolean lockInStrictMode = false;

    /**
     * 是否兼容旧版 NameServer
     */
    private boolean compatibleWithOldNameSrv = true;

    /**
     * Is startup controller mode, which support auto switch broker's role.
     * <br>
     * 是否启用 Controller 模式
     */
    private boolean enableControllerMode = false;

    /**
     * Controller 地址
     */
    private String controllerAddr = "";

    /**
     * 是否通过 DNS 解析 Controller 地址
     */
    private boolean fetchControllerAddrByDnsLookup = false;

    /**
     * 同步 Broker 元数据间隔, 单位毫秒
     */
    private long syncBrokerMetadataPeriod = 5 * 1000;

    /**
     * 检查 SyncStateSet 间隔, 单位毫秒
     */
    private long checkSyncStateSetPeriod = 5 * 1000;

    /**
     * 同步 Controller 元数据间隔, 单位毫秒
     */
    private long syncControllerMetadataPeriod = 10 * 1000;

    /**
     * Controller 心跳超时时间, 单位毫秒
     */
    private long controllerHeartBeatTimeoutMills = 10 * 1000;

    /**
     * 更新 Topic 时是否校验系统 Topic
     */
    private boolean validateSystemTopicWhenUpdateTopic = true;

    /**
     * It is an important basis for the controller to choose the broker master.
     * The lower the value of brokerElectionPriority, the higher the priority of the broker being selected as the master.
     * You can set a lower priority for the broker with better machine conditions.
     * <br>
     * Broker 选举优先级
     */
    private int brokerElectionPriority = Integer.MAX_VALUE;

    /**
     * 是否启用静态订阅
     */
    private boolean useStaticSubscription = false;

    /**
     * 指标导出器类型
     */
    private MetricsExporterType metricsExporterType = MetricsExporterType.DISABLE;

    /**
     * OpenTelemetry 指标基数上限
     */
    private int metricsOtelCardinalityLimit = 50 * 1000;
    /**
     * gRPC 指标导出目标地址
     */
    private String metricsGrpcExporterTarget = "";
    /**
     * gRPC 指标导出请求头
     */
    private String metricsGrpcExporterHeader = "";
    /**
     * gRPC 指标导出超时时间, 单位毫秒
     */
    private long metricGrpcExporterTimeOutInMills = 3 * 1000;
    /**
     * gRPC 指标导出间隔, 单位毫秒
     */
    private long metricGrpcExporterIntervalInMills = 60 * 1000;
    /**
     * 日志指标导出间隔, 单位毫秒
     */
    private long metricLoggingExporterIntervalInMills = 10 * 1000;

    /**
     * Prometheus 指标导出端口
     */
    private int metricsPromExporterPort = 5557;
    /**
     * Prometheus 指标导出主机地址
     */
    private String metricsPromExporterHost = "";

    // Label pairs in CSV. Each label follows pattern of Key:Value. eg: instance_id:xxx,uid:xxx
    // 指标标签列表
    private String metricsLabel = "";

    /**
     * 是否按增量导出指标
     */
    private boolean metricsInDelta = false;

    /**
     * 通道过期时间, 单位毫秒
     */
    private long channelExpiredTimeout = 1000 * 120;
    /**
     * 订阅过期时间, 单位毫秒
     */
    private long subscriptionExpiredTimeout = 1000 * 60 * 10;

    /**
     * Estimate accumulation or not when subscription filter type is tag and is not SUB_ALL.
     * <br>
     * 是否估算消息堆积
     */
    private boolean estimateAccumulation = true;

    /**
     * 是否启用冷读控制策略
     */
    private boolean coldCtrStrategyEnable = false;
    /**
     * 是否使用 PID 冷读控制策略
     */
    private boolean usePIDColdCtrStrategy = true;
    /**
     * 消费组冷读阈值, 单位字节
     */
    private long cgColdReadThreshold = 3 * 1024 * 1024;
    /**
     * 全局冷读阈值, 单位字节
     */
    private long globalColdReadThreshold = 100 * 1024 * 1024;
    
    /**
     * The interval to fetch namesrv addr, default value is 10 second
     * <br>
     * 拉取 NameServer 地址间隔, 单位毫秒
     */
    private long fetchNamesrvAddrInterval = 10 * 1000;

    /**
     * Pop response returns the actual retry topic rather than tampering with the original topic
     * <br>
     * Pop 响应是否返回真实重试 Topic
     */
    private boolean popResponseReturnActualRetryTopic = false;

    /**
     * If both the deleteTopicWithBrokerRegistration flag in the NameServer configuration and this flag are set to true,
     * it guarantees the ultimate consistency of data between the broker and the nameserver during topic deletion.
     * <br>
     * 是否启用单 Topic 注册
     */
    private boolean enableSingleTopicRegister = false;

    /**
     * 是否启用混合消息类型
     */
    private boolean enableMixedMessageType = false;

    /**
     * This flag and deleteTopicWithBrokerRegistration flag in the NameServer cannot be set to true at the same time,
     * otherwise there will be a loss of routing
     * <br>
     * 是否启用拆分注册
     */
    private boolean enableSplitRegistration = false;

    /**
     * Pop Inflight 消息阈值
     */
    private long popInflightMessageThreshold = 10000;
    /**
     * 是否启用 Pop 消息阈值控制
     */
    private boolean enablePopMessageThreshold = false;

    /**
     * 是否启用快速通道事件处理
     */
    private boolean enableFastChannelEventProcess = false;
    /**
     * 是否打印通道分组信息
     */
    private boolean printChannelGroups = false;
    /**
     * 打印通道分组最小数量
     */
    private int printChannelGroupsMinNum = 5;

    /**
     * 拆分注册批次大小
     */
    private int splitRegistrationSize = 800;

    /**
     * Config in this black list will be not allowed to update by command.
     * Try to update this config black list by restart process.
     * Try to update configures in black list by restart process.
     * <br>
     * 禁止动态修改的配置黑名单
     */
    private String configBlackList = "configBlackList;brokerConfigPath";

    // if false, will still rewrite ck after max times 17
    // CK 重投达到上限时是否跳过
    private boolean skipWhenCKRePutReachMaxTimes = false;

    /**
     * 是否异步追加 Ack
     */
    private boolean appendAckAsync = false;

    /**
     * 是否异步追加 Ck
     */
    private boolean appendCkAsync = false;

    /**
     * 删除 Topic 时是否清理重试 Topic
     */
    private boolean clearRetryTopicWhenDeleteTopic = true;

    /**
     * 是否启用 LMQ 统计
     */
    private boolean enableLmqStats = false;

    /**
     * V2 is recommended in cases where LMQ feature is extensively used.
     * <br>
     * 配置管理器版本
     */
    private String configManagerVersion = ConfigManagerVersion.V1.getVersion();

    /**
     * Broker 不可写时是否允许撤回
     */
    private boolean allowRecallWhenBrokerNotWriteable = true;

    /**
     * 是否启用消息撤回
     */
    private boolean recallMessageEnable = false;

    /**
     * 是否启用生产者注册
     */
    private boolean enableRegisterProducer = true;

    /**
     * 是否允许创建系统消费组
     */
    private boolean enableCreateSysGroup = true;

    public String getConfigBlackList() {
        return configBlackList;
    }

    public void setConfigBlackList(String configBlackList) {
        this.configBlackList = configBlackList;
    }

    public long getMaxPopPollingSize() {
        return maxPopPollingSize;
    }

    public void setMaxPopPollingSize(long maxPopPollingSize) {
        this.maxPopPollingSize = maxPopPollingSize;
    }

    public int getReviveQueueNum() {
        return reviveQueueNum;
    }

    public void setReviveQueueNum(int reviveQueueNum) {
        this.reviveQueueNum = reviveQueueNum;
    }

    public long getReviveInterval() {
        return reviveInterval;
    }

    public void setReviveInterval(long reviveInterval) {
        this.reviveInterval = reviveInterval;
    }

    public int getPopCkStayBufferTime() {
        return popCkStayBufferTime;
    }

    public void setPopCkStayBufferTime(int popCkStayBufferTime) {
        this.popCkStayBufferTime = popCkStayBufferTime;
    }

    public int getPopCkStayBufferTimeOut() {
        return popCkStayBufferTimeOut;
    }

    public void setPopCkStayBufferTimeOut(int popCkStayBufferTimeOut) {
        this.popCkStayBufferTimeOut = popCkStayBufferTimeOut;
    }

    public int getPopPollingMapSize() {
        return popPollingMapSize;
    }

    public void setPopPollingMapSize(int popPollingMapSize) {
        this.popPollingMapSize = popPollingMapSize;
    }

    public long getReviveScanTime() {
        return reviveScanTime;
    }

    public void setReviveScanTime(long reviveScanTime) {
        this.reviveScanTime = reviveScanTime;
    }

    public long getReviveMaxSlow() {
        return reviveMaxSlow;
    }

    public void setReviveMaxSlow(long reviveMaxSlow) {
        this.reviveMaxSlow = reviveMaxSlow;
    }

    public int getPopPollingSize() {
        return popPollingSize;
    }

    public void setPopPollingSize(int popPollingSize) {
        this.popPollingSize = popPollingSize;
    }

    public boolean isEnablePopBufferMerge() {
        return enablePopBufferMerge;
    }

    public void setEnablePopBufferMerge(boolean enablePopBufferMerge) {
        this.enablePopBufferMerge = enablePopBufferMerge;
    }

    public int getPopCkMaxBufferSize() {
        return popCkMaxBufferSize;
    }

    public void setPopCkMaxBufferSize(int popCkMaxBufferSize) {
        this.popCkMaxBufferSize = popCkMaxBufferSize;
    }

    public int getPopCkOffsetMaxQueueSize() {
        return popCkOffsetMaxQueueSize;
    }

    public void setPopCkOffsetMaxQueueSize(int popCkOffsetMaxQueueSize) {
        this.popCkOffsetMaxQueueSize = popCkOffsetMaxQueueSize;
    }

    public boolean isEnablePopBatchAck() {
        return enablePopBatchAck;
    }

    public void setEnablePopBatchAck(boolean enablePopBatchAck) {
        this.enablePopBatchAck = enablePopBatchAck;
    }

    public boolean isEnableSkipLongAwaitingAck() {
        return enableSkipLongAwaitingAck;
    }

    public void setEnableSkipLongAwaitingAck(boolean enableSkipLongAwaitingAck) {
        this.enableSkipLongAwaitingAck = enableSkipLongAwaitingAck;
    }

    public long getReviveAckWaitMs() {
        return reviveAckWaitMs;
    }

    public void setReviveAckWaitMs(long reviveAckWaitMs) {
        this.reviveAckWaitMs = reviveAckWaitMs;
    }

    public boolean isEnablePopLog() {
        return enablePopLog;
    }

    public void setEnablePopLog(boolean enablePopLog) {
        this.enablePopLog = enablePopLog;
    }

    public int getPopFromRetryProbability() {
        return popFromRetryProbability;
    }

    public void setPopFromRetryProbability(int popFromRetryProbability) {
        this.popFromRetryProbability = popFromRetryProbability;
    }

    public boolean isPopConsumerFSServiceInit() {
        return popConsumerFSServiceInit;
    }

    public void setPopConsumerFSServiceInit(boolean popConsumerFSServiceInit) {
        this.popConsumerFSServiceInit = popConsumerFSServiceInit;
    }

    public boolean isPopConsumerKVServiceLog() {
        return popConsumerKVServiceLog;
    }

    public void setPopConsumerKVServiceLog(boolean popConsumerKVServiceLog) {
        this.popConsumerKVServiceLog = popConsumerKVServiceLog;
    }

    public boolean isPopConsumerKVServiceInit() {
        return popConsumerKVServiceInit;
    }

    public void setPopConsumerKVServiceInit(boolean popConsumerKVServiceInit) {
        this.popConsumerKVServiceInit = popConsumerKVServiceInit;
    }

    public boolean isPopConsumerKVServiceEnable() {
        return popConsumerKVServiceEnable;
    }

    public void setPopConsumerKVServiceEnable(boolean popConsumerKVServiceEnable) {
        this.popConsumerKVServiceEnable = popConsumerKVServiceEnable;
    }

    public int getPopReviveMaxReturnSizePerRead() {
        return popReviveMaxReturnSizePerRead;
    }

    public void setPopReviveMaxReturnSizePerRead(int popReviveMaxReturnSizePerRead) {
        this.popReviveMaxReturnSizePerRead = popReviveMaxReturnSizePerRead;
    }

    public int getPopReviveMaxAttemptTimes() {
        return popReviveMaxAttemptTimes;
    }

    public void setPopReviveMaxAttemptTimes(int popReviveMaxAttemptTimes) {
        this.popReviveMaxAttemptTimes = popReviveMaxAttemptTimes;
    }

    public boolean isTraceOn() {
        return traceOn;
    }

    public void setTraceOn(final boolean traceOn) {
        this.traceOn = traceOn;
    }

    public long getStartAcceptSendRequestTimeStamp() {
        return startAcceptSendRequestTimeStamp;
    }

    public void setStartAcceptSendRequestTimeStamp(final long startAcceptSendRequestTimeStamp) {
        this.startAcceptSendRequestTimeStamp = startAcceptSendRequestTimeStamp;
    }

    public long getWaitTimeMillsInSendQueue() {
        return waitTimeMillsInSendQueue;
    }

    public void setWaitTimeMillsInSendQueue(final long waitTimeMillsInSendQueue) {
        this.waitTimeMillsInSendQueue = waitTimeMillsInSendQueue;
    }

    public long getConsumerFallbehindThreshold() {
        return consumerFallbehindThreshold;
    }

    public void setConsumerFallbehindThreshold(final long consumerFallbehindThreshold) {
        this.consumerFallbehindThreshold = consumerFallbehindThreshold;
    }

    public boolean isBrokerFastFailureEnable() {
        return brokerFastFailureEnable;
    }

    public void setBrokerFastFailureEnable(final boolean brokerFastFailureEnable) {
        this.brokerFastFailureEnable = brokerFastFailureEnable;
    }

    public long getWaitTimeMillsInPullQueue() {
        return waitTimeMillsInPullQueue;
    }

    public void setWaitTimeMillsInPullQueue(final long waitTimeMillsInPullQueue) {
        this.waitTimeMillsInPullQueue = waitTimeMillsInPullQueue;
    }

    public boolean isDisableConsumeIfConsumerReadSlowly() {
        return disableConsumeIfConsumerReadSlowly;
    }

    public void setDisableConsumeIfConsumerReadSlowly(final boolean disableConsumeIfConsumerReadSlowly) {
        this.disableConsumeIfConsumerReadSlowly = disableConsumeIfConsumerReadSlowly;
    }

    public boolean isSlaveReadEnable() {
        return slaveReadEnable;
    }

    public void setSlaveReadEnable(final boolean slaveReadEnable) {
        this.slaveReadEnable = slaveReadEnable;
    }

    public int getRegisterBrokerTimeoutMills() {
        return registerBrokerTimeoutMills;
    }

    public void setRegisterBrokerTimeoutMills(final int registerBrokerTimeoutMills) {
        this.registerBrokerTimeoutMills = registerBrokerTimeoutMills;
    }

    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(final String regionId) {
        this.regionId = regionId;
    }

    public boolean isTransferMsgByHeap() {
        return transferMsgByHeap;
    }

    public void setTransferMsgByHeap(final boolean transferMsgByHeap) {
        this.transferMsgByHeap = transferMsgByHeap;
    }

    public String getMessageStorePlugIn() {
        return messageStorePlugIn;
    }

    public void setMessageStorePlugIn(String messageStorePlugIn) {
        this.messageStorePlugIn = messageStorePlugIn;
    }

    public boolean isHighSpeedMode() {
        return highSpeedMode;
    }

    public void setHighSpeedMode(final boolean highSpeedMode) {
        this.highSpeedMode = highSpeedMode;
    }

    public int getBrokerPermission() {
        return brokerPermission;
    }

    public void setBrokerPermission(int brokerPermission) {
        this.brokerPermission = brokerPermission;
    }

    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }

    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }

    public boolean isAutoCreateTopicEnable() {
        return autoCreateTopicEnable;
    }

    public void setAutoCreateTopicEnable(boolean autoCreateTopic) {
        this.autoCreateTopicEnable = autoCreateTopic;
    }

    public String getBrokerIP1() {
        return brokerIP1;
    }

    public void setBrokerIP1(String brokerIP1) {
        this.brokerIP1 = brokerIP1;
    }

    public String getBrokerIP2() {
        return brokerIP2;
    }

    public void setBrokerIP2(String brokerIP2) {
        this.brokerIP2 = brokerIP2;
    }

    public int getSendMessageThreadPoolNums() {
        return sendMessageThreadPoolNums;
    }

    public void setSendMessageThreadPoolNums(int sendMessageThreadPoolNums) {
        this.sendMessageThreadPoolNums = sendMessageThreadPoolNums;
    }

    public int getPutMessageFutureThreadPoolNums() {
        return putMessageFutureThreadPoolNums;
    }

    public void setPutMessageFutureThreadPoolNums(int putMessageFutureThreadPoolNums) {
        this.putMessageFutureThreadPoolNums = putMessageFutureThreadPoolNums;
    }

    public int getPullMessageThreadPoolNums() {
        return pullMessageThreadPoolNums;
    }

    public void setPullMessageThreadPoolNums(int pullMessageThreadPoolNums) {
        this.pullMessageThreadPoolNums = pullMessageThreadPoolNums;
    }

    public int getAckMessageThreadPoolNums() {
        return ackMessageThreadPoolNums;
    }

    public void setAckMessageThreadPoolNums(int ackMessageThreadPoolNums) {
        this.ackMessageThreadPoolNums = ackMessageThreadPoolNums;
    }

    public int getProcessReplyMessageThreadPoolNums() {
        return processReplyMessageThreadPoolNums;
    }

    public void setProcessReplyMessageThreadPoolNums(int processReplyMessageThreadPoolNums) {
        this.processReplyMessageThreadPoolNums = processReplyMessageThreadPoolNums;
    }

    public int getQueryMessageThreadPoolNums() {
        return queryMessageThreadPoolNums;
    }

    public void setQueryMessageThreadPoolNums(final int queryMessageThreadPoolNums) {
        this.queryMessageThreadPoolNums = queryMessageThreadPoolNums;
    }

    public int getAdminBrokerThreadPoolNums() {
        return adminBrokerThreadPoolNums;
    }

    public void setAdminBrokerThreadPoolNums(int adminBrokerThreadPoolNums) {
        this.adminBrokerThreadPoolNums = adminBrokerThreadPoolNums;
    }

    public int getFlushConsumerOffsetInterval() {
        return flushConsumerOffsetInterval;
    }

    public void setFlushConsumerOffsetInterval(int flushConsumerOffsetInterval) {
        this.flushConsumerOffsetInterval = flushConsumerOffsetInterval;
    }

    public int getFlushConsumerOffsetHistoryInterval() {
        return flushConsumerOffsetHistoryInterval;
    }

    public void setFlushConsumerOffsetHistoryInterval(int flushConsumerOffsetHistoryInterval) {
        this.flushConsumerOffsetHistoryInterval = flushConsumerOffsetHistoryInterval;
    }

    public boolean isClusterTopicEnable() {
        return clusterTopicEnable;
    }

    public void setClusterTopicEnable(boolean clusterTopicEnable) {
        this.clusterTopicEnable = clusterTopicEnable;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public boolean isAutoCreateSubscriptionGroup() {
        return autoCreateSubscriptionGroup;
    }

    public void setAutoCreateSubscriptionGroup(boolean autoCreateSubscriptionGroup) {
        this.autoCreateSubscriptionGroup = autoCreateSubscriptionGroup;
    }

    public String getBrokerConfigPath() {
        return brokerConfigPath;
    }

    public void setBrokerConfigPath(String brokerConfigPath) {
        this.brokerConfigPath = brokerConfigPath;
    }

    public String getRocketmqHome() {
        return rocketmqHome;
    }

    public void setRocketmqHome(String rocketmqHome) {
        this.rocketmqHome = rocketmqHome;
    }

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public int getLitePullMessageThreadPoolNums() {
        return litePullMessageThreadPoolNums;
    }

    public void setLitePullMessageThreadPoolNums(int litePullMessageThreadPoolNums) {
        this.litePullMessageThreadPoolNums = litePullMessageThreadPoolNums;
    }

    public int getLitePullThreadPoolQueueCapacity() {
        return litePullThreadPoolQueueCapacity;
    }

    public void setLitePullThreadPoolQueueCapacity(int litePullThreadPoolQueueCapacity) {
        this.litePullThreadPoolQueueCapacity = litePullThreadPoolQueueCapacity;
    }

    public int getAdminBrokerThreadPoolQueueCapacity() {
        return adminBrokerThreadPoolQueueCapacity;
    }

    public void setAdminBrokerThreadPoolQueueCapacity(int adminBrokerThreadPoolQueueCapacity) {
        this.adminBrokerThreadPoolQueueCapacity = adminBrokerThreadPoolQueueCapacity;
    }

    public int getLoadBalanceThreadPoolQueueCapacity() {
        return loadBalanceThreadPoolQueueCapacity;
    }

    public void setLoadBalanceThreadPoolQueueCapacity(int loadBalanceThreadPoolQueueCapacity) {
        this.loadBalanceThreadPoolQueueCapacity = loadBalanceThreadPoolQueueCapacity;
    }

    public int getSendHeartbeatTimeoutMillis() {
        return sendHeartbeatTimeoutMillis;
    }

    public void setSendHeartbeatTimeoutMillis(int sendHeartbeatTimeoutMillis) {
        this.sendHeartbeatTimeoutMillis = sendHeartbeatTimeoutMillis;
    }

    public long getWaitTimeMillsInLitePullQueue() {
        return waitTimeMillsInLitePullQueue;
    }

    public void setWaitTimeMillsInLitePullQueue(long waitTimeMillsInLitePullQueue) {
        this.waitTimeMillsInLitePullQueue = waitTimeMillsInLitePullQueue;
    }

    public boolean isLitePullMessageEnable() {
        return litePullMessageEnable;
    }

    public void setLitePullMessageEnable(boolean litePullMessageEnable) {
        this.litePullMessageEnable = litePullMessageEnable;
    }

    public int getSyncBrokerMemberGroupPeriod() {
        return syncBrokerMemberGroupPeriod;
    }

    public void setSyncBrokerMemberGroupPeriod(int syncBrokerMemberGroupPeriod) {
        this.syncBrokerMemberGroupPeriod = syncBrokerMemberGroupPeriod;
    }

    public boolean isRejectTransactionMessage() {
        return rejectTransactionMessage;
    }

    public void setRejectTransactionMessage(boolean rejectTransactionMessage) {
        this.rejectTransactionMessage = rejectTransactionMessage;
    }

    public boolean isFetchNamesrvAddrByAddressServer() {
        return fetchNamesrvAddrByAddressServer;
    }

    public void setFetchNamesrvAddrByAddressServer(boolean fetchNamesrvAddrByAddressServer) {
        this.fetchNamesrvAddrByAddressServer = fetchNamesrvAddrByAddressServer;
    }

    public int getSendThreadPoolQueueCapacity() {
        return sendThreadPoolQueueCapacity;
    }

    public void setSendThreadPoolQueueCapacity(int sendThreadPoolQueueCapacity) {
        this.sendThreadPoolQueueCapacity = sendThreadPoolQueueCapacity;
    }

    public int getPutThreadPoolQueueCapacity() {
        return putThreadPoolQueueCapacity;
    }

    public void setPutThreadPoolQueueCapacity(int putThreadPoolQueueCapacity) {
        this.putThreadPoolQueueCapacity = putThreadPoolQueueCapacity;
    }

    public int getPullThreadPoolQueueCapacity() {
        return pullThreadPoolQueueCapacity;
    }

    public void setPullThreadPoolQueueCapacity(int pullThreadPoolQueueCapacity) {
        this.pullThreadPoolQueueCapacity = pullThreadPoolQueueCapacity;
    }

    public int getAckThreadPoolQueueCapacity() {
        return ackThreadPoolQueueCapacity;
    }

    public void setAckThreadPoolQueueCapacity(int ackThreadPoolQueueCapacity) {
        this.ackThreadPoolQueueCapacity = ackThreadPoolQueueCapacity;
    }

    public int getReplyThreadPoolQueueCapacity() {
        return replyThreadPoolQueueCapacity;
    }

    public void setReplyThreadPoolQueueCapacity(int replyThreadPoolQueueCapacity) {
        this.replyThreadPoolQueueCapacity = replyThreadPoolQueueCapacity;
    }

    public int getQueryThreadPoolQueueCapacity() {
        return queryThreadPoolQueueCapacity;
    }

    public void setQueryThreadPoolQueueCapacity(final int queryThreadPoolQueueCapacity) {
        this.queryThreadPoolQueueCapacity = queryThreadPoolQueueCapacity;
    }

    public boolean isBrokerTopicEnable() {
        return brokerTopicEnable;
    }

    public void setBrokerTopicEnable(boolean brokerTopicEnable) {
        this.brokerTopicEnable = brokerTopicEnable;
    }

    public boolean isLongPollingEnable() {
        return longPollingEnable;
    }

    public void setLongPollingEnable(boolean longPollingEnable) {
        this.longPollingEnable = longPollingEnable;
    }

    public boolean isNotifyConsumerIdsChangedEnable() {
        return notifyConsumerIdsChangedEnable;
    }

    public void setNotifyConsumerIdsChangedEnable(boolean notifyConsumerIdsChangedEnable) {
        this.notifyConsumerIdsChangedEnable = notifyConsumerIdsChangedEnable;
    }

    public long getShortPollingTimeMills() {
        return shortPollingTimeMills;
    }

    public void setShortPollingTimeMills(long shortPollingTimeMills) {
        this.shortPollingTimeMills = shortPollingTimeMills;
    }

    public int getClientManageThreadPoolNums() {
        return clientManageThreadPoolNums;
    }

    public void setClientManageThreadPoolNums(int clientManageThreadPoolNums) {
        this.clientManageThreadPoolNums = clientManageThreadPoolNums;
    }

    public int getClientManagerThreadPoolQueueCapacity() {
        return clientManagerThreadPoolQueueCapacity;
    }

    public void setClientManagerThreadPoolQueueCapacity(int clientManagerThreadPoolQueueCapacity) {
        this.clientManagerThreadPoolQueueCapacity = clientManagerThreadPoolQueueCapacity;
    }

    public int getConsumerManagerThreadPoolQueueCapacity() {
        return consumerManagerThreadPoolQueueCapacity;
    }

    public void setConsumerManagerThreadPoolQueueCapacity(int consumerManagerThreadPoolQueueCapacity) {
        this.consumerManagerThreadPoolQueueCapacity = consumerManagerThreadPoolQueueCapacity;
    }

    public int getConsumerManageThreadPoolNums() {
        return consumerManageThreadPoolNums;
    }

    public void setConsumerManageThreadPoolNums(int consumerManageThreadPoolNums) {
        this.consumerManageThreadPoolNums = consumerManageThreadPoolNums;
    }

    public int getCommercialBaseCount() {
        return commercialBaseCount;
    }

    public void setCommercialBaseCount(int commercialBaseCount) {
        this.commercialBaseCount = commercialBaseCount;
    }

    public boolean isEnableCalcFilterBitMap() {
        return enableCalcFilterBitMap;
    }

    public void setEnableCalcFilterBitMap(boolean enableCalcFilterBitMap) {
        this.enableCalcFilterBitMap = enableCalcFilterBitMap;
    }

    public int getExpectConsumerNumUseFilter() {
        return expectConsumerNumUseFilter;
    }

    public void setExpectConsumerNumUseFilter(int expectConsumerNumUseFilter) {
        this.expectConsumerNumUseFilter = expectConsumerNumUseFilter;
    }

    public int getMaxErrorRateOfBloomFilter() {
        return maxErrorRateOfBloomFilter;
    }

    public void setMaxErrorRateOfBloomFilter(int maxErrorRateOfBloomFilter) {
        this.maxErrorRateOfBloomFilter = maxErrorRateOfBloomFilter;
    }

    public long getFilterDataCleanTimeSpan() {
        return filterDataCleanTimeSpan;
    }

    public void setFilterDataCleanTimeSpan(long filterDataCleanTimeSpan) {
        this.filterDataCleanTimeSpan = filterDataCleanTimeSpan;
    }

    public boolean isFilterSupportRetry() {
        return filterSupportRetry;
    }

    public void setFilterSupportRetry(boolean filterSupportRetry) {
        this.filterSupportRetry = filterSupportRetry;
    }

    public boolean isEnablePropertyFilter() {
        return enablePropertyFilter;
    }

    public void setEnablePropertyFilter(boolean enablePropertyFilter) {
        this.enablePropertyFilter = enablePropertyFilter;
    }

    public boolean isCompressedRegister() {
        return compressedRegister;
    }

    public void setCompressedRegister(boolean compressedRegister) {
        this.compressedRegister = compressedRegister;
    }

    public boolean isForceRegister() {
        return forceRegister;
    }

    public void setForceRegister(boolean forceRegister) {
        this.forceRegister = forceRegister;
    }

    public int getHeartbeatThreadPoolQueueCapacity() {
        return heartbeatThreadPoolQueueCapacity;
    }

    public void setHeartbeatThreadPoolQueueCapacity(int heartbeatThreadPoolQueueCapacity) {
        this.heartbeatThreadPoolQueueCapacity = heartbeatThreadPoolQueueCapacity;
    }

    public int getHeartbeatThreadPoolNums() {
        return heartbeatThreadPoolNums;
    }

    public void setHeartbeatThreadPoolNums(int heartbeatThreadPoolNums) {
        this.heartbeatThreadPoolNums = heartbeatThreadPoolNums;
    }

    public long getWaitTimeMillsInHeartbeatQueue() {
        return waitTimeMillsInHeartbeatQueue;
    }

    public void setWaitTimeMillsInHeartbeatQueue(long waitTimeMillsInHeartbeatQueue) {
        this.waitTimeMillsInHeartbeatQueue = waitTimeMillsInHeartbeatQueue;
    }

    public int getRegisterNameServerPeriod() {
        return registerNameServerPeriod;
    }

    public void setRegisterNameServerPeriod(int registerNameServerPeriod) {
        this.registerNameServerPeriod = registerNameServerPeriod;
    }

    public long getTransactionTimeOut() {
        return transactionTimeOut;
    }

    public void setTransactionTimeOut(long transactionTimeOut) {
        this.transactionTimeOut = transactionTimeOut;
    }

    public int getTransactionCheckMax() {
        return transactionCheckMax;
    }

    public void setTransactionCheckMax(int transactionCheckMax) {
        this.transactionCheckMax = transactionCheckMax;
    }

    public long getTransactionCheckInterval() {
        return transactionCheckInterval;
    }

    public void setTransactionCheckInterval(long transactionCheckInterval) {
        this.transactionCheckInterval = transactionCheckInterval;
    }

    public int getEndTransactionThreadPoolNums() {
        return endTransactionThreadPoolNums;
    }

    public void setEndTransactionThreadPoolNums(int endTransactionThreadPoolNums) {
        this.endTransactionThreadPoolNums = endTransactionThreadPoolNums;
    }

    public int getEndTransactionPoolQueueCapacity() {
        return endTransactionPoolQueueCapacity;
    }

    public void setEndTransactionPoolQueueCapacity(int endTransactionPoolQueueCapacity) {
        this.endTransactionPoolQueueCapacity = endTransactionPoolQueueCapacity;
    }

    public long getWaitTimeMillsInTransactionQueue() {
        return waitTimeMillsInTransactionQueue;
    }

    public void setWaitTimeMillsInTransactionQueue(long waitTimeMillsInTransactionQueue) {
        this.waitTimeMillsInTransactionQueue = waitTimeMillsInTransactionQueue;
    }

    public String getMsgTraceTopicName() {
        return msgTraceTopicName;
    }

    public long getWaitTimeMillsInAdminBrokerQueue() {
        return waitTimeMillsInAdminBrokerQueue;
    }

    public void setWaitTimeMillsInAdminBrokerQueue(long waitTimeMillsInAdminBrokerQueue) {
        this.waitTimeMillsInAdminBrokerQueue = waitTimeMillsInAdminBrokerQueue;
    }

    public void setMsgTraceTopicName(String msgTraceTopicName) {
        this.msgTraceTopicName = msgTraceTopicName;
    }

    public boolean isTraceTopicEnable() {
        return traceTopicEnable;
    }

    public void setTraceTopicEnable(boolean traceTopicEnable) {
        this.traceTopicEnable = traceTopicEnable;
    }

    public void setAclEnable(boolean aclEnable) {
        this.aclEnable = aclEnable;
    }

    public boolean isStoreReplyMessageEnable() {
        return storeReplyMessageEnable;
    }

    public void setStoreReplyMessageEnable(boolean storeReplyMessageEnable) {
        this.storeReplyMessageEnable = storeReplyMessageEnable;
    }

    public boolean isEnableDetailStat() {
        return enableDetailStat;
    }

    public void setEnableDetailStat(boolean enableDetailStat) {
        this.enableDetailStat = enableDetailStat;
    }

    public boolean isAutoDeleteUnusedStats() {
        return autoDeleteUnusedStats;
    }

    public void setAutoDeleteUnusedStats(boolean autoDeleteUnusedStats) {
        this.autoDeleteUnusedStats = autoDeleteUnusedStats;
    }

    public long getLoadBalancePollNameServerInterval() {
        return loadBalancePollNameServerInterval;
    }

    public void setLoadBalancePollNameServerInterval(long loadBalancePollNameServerInterval) {
        this.loadBalancePollNameServerInterval = loadBalancePollNameServerInterval;
    }

    public int getCleanOfflineBrokerInterval() {
        return cleanOfflineBrokerInterval;
    }

    public void setCleanOfflineBrokerInterval(int cleanOfflineBrokerInterval) {
        this.cleanOfflineBrokerInterval = cleanOfflineBrokerInterval;
    }

    public int getLoadBalanceProcessorThreadPoolNums() {
        return loadBalanceProcessorThreadPoolNums;
    }

    public void setLoadBalanceProcessorThreadPoolNums(int loadBalanceProcessorThreadPoolNums) {
        this.loadBalanceProcessorThreadPoolNums = loadBalanceProcessorThreadPoolNums;
    }

    public boolean isServerLoadBalancerEnable() {
        return serverLoadBalancerEnable;
    }

    public void setServerLoadBalancerEnable(boolean serverLoadBalancerEnable) {
        this.serverLoadBalancerEnable = serverLoadBalancerEnable;
    }

    public MessageRequestMode getDefaultMessageRequestMode() {
        return defaultMessageRequestMode;
    }

    public void setDefaultMessageRequestMode(String defaultMessageRequestMode) {
        this.defaultMessageRequestMode = MessageRequestMode.valueOf(defaultMessageRequestMode);
    }

    public int getDefaultPopShareQueueNum() {
        return defaultPopShareQueueNum;
    }

    public void setDefaultPopShareQueueNum(int defaultPopShareQueueNum) {
        this.defaultPopShareQueueNum = defaultPopShareQueueNum;
    }

    public long getForwardTimeout() {
        return forwardTimeout;
    }

    public void setForwardTimeout(long timeout) {
        this.forwardTimeout = timeout;
    }

    public int getBrokerHeartbeatInterval() {
        return brokerHeartbeatInterval;
    }

    public void setBrokerHeartbeatInterval(int brokerHeartbeatInterval) {
        this.brokerHeartbeatInterval = brokerHeartbeatInterval;
    }

    public long getBrokerNotActiveTimeoutMillis() {
        return brokerNotActiveTimeoutMillis;
    }

    public void setBrokerNotActiveTimeoutMillis(long brokerNotActiveTimeoutMillis) {
        this.brokerNotActiveTimeoutMillis = brokerNotActiveTimeoutMillis;
    }

    public boolean isEnableNetWorkFlowControl() {
        return enableNetWorkFlowControl;
    }

    public void setEnableNetWorkFlowControl(boolean enableNetWorkFlowControl) {
        this.enableNetWorkFlowControl = enableNetWorkFlowControl;
    }

    public long getPopLongPollingForceNotifyInterval() {
        return popLongPollingForceNotifyInterval;
    }

    public void setPopLongPollingForceNotifyInterval(long popLongPollingForceNotifyInterval) {
        this.popLongPollingForceNotifyInterval = popLongPollingForceNotifyInterval;
    }

    public boolean isEnableNotifyBeforePopCalculateLag() {
        return enableNotifyBeforePopCalculateLag;
    }

    public void setEnableNotifyBeforePopCalculateLag(boolean enableNotifyBeforePopCalculateLag) {
        this.enableNotifyBeforePopCalculateLag = enableNotifyBeforePopCalculateLag;
    }

    public boolean isEnableNotifyAfterPopOrderLockRelease() {
        return enableNotifyAfterPopOrderLockRelease;
    }

    public void setEnableNotifyAfterPopOrderLockRelease(boolean enableNotifyAfterPopOrderLockRelease) {
        this.enableNotifyAfterPopOrderLockRelease = enableNotifyAfterPopOrderLockRelease;
    }

    public boolean isInitPopOffsetByCheckMsgInMem() {
        return initPopOffsetByCheckMsgInMem;
    }

    public void setInitPopOffsetByCheckMsgInMem(boolean initPopOffsetByCheckMsgInMem) {
        this.initPopOffsetByCheckMsgInMem = initPopOffsetByCheckMsgInMem;
    }

    public boolean isRetrieveMessageFromPopRetryTopicV1() {
        return retrieveMessageFromPopRetryTopicV1;
    }

    public void setRetrieveMessageFromPopRetryTopicV1(boolean retrieveMessageFromPopRetryTopicV1) {
        this.retrieveMessageFromPopRetryTopicV1 = retrieveMessageFromPopRetryTopicV1;
    }

    public boolean isEnableRetryTopicV2() {
        return enableRetryTopicV2;
    }

    public void setEnableRetryTopicV2(boolean enableRetryTopicV2) {
        this.enableRetryTopicV2 = enableRetryTopicV2;
    }

    public boolean isRealTimeNotifyConsumerChange() {
        return realTimeNotifyConsumerChange;
    }

    public void setRealTimeNotifyConsumerChange(boolean realTimeNotifyConsumerChange) {
        this.realTimeNotifyConsumerChange = realTimeNotifyConsumerChange;
    }

    public boolean isEnableSlaveActingMaster() {
        return enableSlaveActingMaster;
    }

    public void setEnableSlaveActingMaster(boolean enableSlaveActingMaster) {
        this.enableSlaveActingMaster = enableSlaveActingMaster;
    }

    public boolean isEnableRemoteEscape() {
        return enableRemoteEscape;
    }

    public void setEnableRemoteEscape(boolean enableRemoteEscape) {
        this.enableRemoteEscape = enableRemoteEscape;
    }

    public boolean isSkipPreOnline() {
        return skipPreOnline;
    }

    public void setSkipPreOnline(boolean skipPreOnline) {
        this.skipPreOnline = skipPreOnline;
    }

    public boolean isAsyncSendEnable() {
        return asyncSendEnable;
    }

    public void setAsyncSendEnable(boolean asyncSendEnable) {
        this.asyncSendEnable = asyncSendEnable;
    }

    public long getConsumerOffsetUpdateVersionStep() {
        return consumerOffsetUpdateVersionStep;
    }

    public void setConsumerOffsetUpdateVersionStep(long consumerOffsetUpdateVersionStep) {
        this.consumerOffsetUpdateVersionStep = consumerOffsetUpdateVersionStep;
    }

    public long getDelayOffsetUpdateVersionStep() {
        return delayOffsetUpdateVersionStep;
    }

    public void setDelayOffsetUpdateVersionStep(long delayOffsetUpdateVersionStep) {
        this.delayOffsetUpdateVersionStep = delayOffsetUpdateVersionStep;
    }

    public int getCommercialSizePerMsg() {
        return commercialSizePerMsg;
    }

    public void setCommercialSizePerMsg(int commercialSizePerMsg) {
        this.commercialSizePerMsg = commercialSizePerMsg;
    }

    public long getWaitTimeMillsInAckQueue() {
        return waitTimeMillsInAckQueue;
    }

    public void setWaitTimeMillsInAckQueue(long waitTimeMillsInAckQueue) {
        this.waitTimeMillsInAckQueue = waitTimeMillsInAckQueue;
    }

    public boolean isRejectPullConsumerEnable() {
        return rejectPullConsumerEnable;
    }

    public void setRejectPullConsumerEnable(boolean rejectPullConsumerEnable) {
        this.rejectPullConsumerEnable = rejectPullConsumerEnable;
    }

    public boolean isAccountStatsEnable() {
        return accountStatsEnable;
    }

    public void setAccountStatsEnable(boolean accountStatsEnable) {
        this.accountStatsEnable = accountStatsEnable;
    }

    public boolean isAccountStatsPrintZeroValues() {
        return accountStatsPrintZeroValues;
    }

    public void setAccountStatsPrintZeroValues(boolean accountStatsPrintZeroValues) {
        this.accountStatsPrintZeroValues = accountStatsPrintZeroValues;
    }

    public int getMaxStatsIdleTimeInMinutes() {
        return maxStatsIdleTimeInMinutes;
    }

    public void setMaxStatsIdleTimeInMinutes(int maxStatsIdleTimeInMinutes) {
        this.maxStatsIdleTimeInMinutes = maxStatsIdleTimeInMinutes;
    }

    public boolean isLockInStrictMode() {
        return lockInStrictMode;
    }

    public void setLockInStrictMode(boolean lockInStrictMode) {
        this.lockInStrictMode = lockInStrictMode;
    }

    public boolean isIsolateLogEnable() {
        return isolateLogEnable;
    }

    public void setIsolateLogEnable(boolean isolateLogEnable) {
        this.isolateLogEnable = isolateLogEnable;
    }

    public boolean isCompatibleWithOldNameSrv() {
        return compatibleWithOldNameSrv;
    }

    public void setCompatibleWithOldNameSrv(boolean compatibleWithOldNameSrv) {
        this.compatibleWithOldNameSrv = compatibleWithOldNameSrv;
    }

    public boolean isEnableControllerMode() {
        return enableControllerMode;
    }

    public void setEnableControllerMode(boolean enableControllerMode) {
        this.enableControllerMode = enableControllerMode;
    }

    public String getControllerAddr() {
        return controllerAddr;
    }

    public void setControllerAddr(String controllerAddr) {
        this.controllerAddr = controllerAddr;
    }

    public boolean isFetchControllerAddrByDnsLookup() {
        return fetchControllerAddrByDnsLookup;
    }

    public void setFetchControllerAddrByDnsLookup(boolean fetchControllerAddrByDnsLookup) {
        this.fetchControllerAddrByDnsLookup = fetchControllerAddrByDnsLookup;
    }

    public long getSyncBrokerMetadataPeriod() {
        return syncBrokerMetadataPeriod;
    }

    public void setSyncBrokerMetadataPeriod(long syncBrokerMetadataPeriod) {
        this.syncBrokerMetadataPeriod = syncBrokerMetadataPeriod;
    }

    public long getCheckSyncStateSetPeriod() {
        return checkSyncStateSetPeriod;
    }

    public void setCheckSyncStateSetPeriod(long checkSyncStateSetPeriod) {
        this.checkSyncStateSetPeriod = checkSyncStateSetPeriod;
    }

    public long getSyncControllerMetadataPeriod() {
        return syncControllerMetadataPeriod;
    }

    public void setSyncControllerMetadataPeriod(long syncControllerMetadataPeriod) {
        this.syncControllerMetadataPeriod = syncControllerMetadataPeriod;
    }

    public int getBrokerElectionPriority() {
        return brokerElectionPriority;
    }

    public void setBrokerElectionPriority(int brokerElectionPriority) {
        this.brokerElectionPriority = brokerElectionPriority;
    }

    public long getControllerHeartBeatTimeoutMills() {
        return controllerHeartBeatTimeoutMills;
    }

    public void setControllerHeartBeatTimeoutMills(long controllerHeartBeatTimeoutMills) {
        this.controllerHeartBeatTimeoutMills = controllerHeartBeatTimeoutMills;
    }

    public boolean isRecoverConcurrently() {
        return recoverConcurrently;
    }

    public void setRecoverConcurrently(boolean recoverConcurrently) {
        this.recoverConcurrently = recoverConcurrently;
    }

    public int getRecoverThreadPoolNums() {
        return recoverThreadPoolNums;
    }

    public void setRecoverThreadPoolNums(int recoverThreadPoolNums) {
        this.recoverThreadPoolNums = recoverThreadPoolNums;
    }

    public boolean isFetchNameSrvAddrByDnsLookup() {
        return fetchNameSrvAddrByDnsLookup;
    }

    public void setFetchNameSrvAddrByDnsLookup(boolean fetchNameSrvAddrByDnsLookup) {
        this.fetchNameSrvAddrByDnsLookup = fetchNameSrvAddrByDnsLookup;
    }

    public boolean isUseServerSideResetOffset() {
        return useServerSideResetOffset;
    }

    public void setUseServerSideResetOffset(boolean useServerSideResetOffset) {
        this.useServerSideResetOffset = useServerSideResetOffset;
    }

    public boolean isEnableBroadcastOffsetStore() {
        return enableBroadcastOffsetStore;
    }

    public void setEnableBroadcastOffsetStore(boolean enableBroadcastOffsetStore) {
        this.enableBroadcastOffsetStore = enableBroadcastOffsetStore;
    }

    public long getBroadcastOffsetExpireSecond() {
        return broadcastOffsetExpireSecond;
    }

    public void setBroadcastOffsetExpireSecond(long broadcastOffsetExpireSecond) {
        this.broadcastOffsetExpireSecond = broadcastOffsetExpireSecond;
    }

    public long getBroadcastOffsetExpireMaxSecond() {
        return broadcastOffsetExpireMaxSecond;
    }

    public void setBroadcastOffsetExpireMaxSecond(long broadcastOffsetExpireMaxSecond) {
        this.broadcastOffsetExpireMaxSecond = broadcastOffsetExpireMaxSecond;
    }

    public MetricsExporterType getMetricsExporterType() {
        return metricsExporterType;
    }

    public void setMetricsExporterType(MetricsExporterType metricsExporterType) {
        this.metricsExporterType = metricsExporterType;
    }

    public void setMetricsExporterType(int metricsExporterType) {
        this.metricsExporterType = MetricsExporterType.valueOf(metricsExporterType);
    }

    public void setMetricsExporterType(String metricsExporterType) {
        this.metricsExporterType = MetricsExporterType.valueOf(metricsExporterType);
    }

    public int getMetricsOtelCardinalityLimit() {
        return metricsOtelCardinalityLimit;
    }

    public void setMetricsOtelCardinalityLimit(int metricsOtelCardinalityLimit) {
        this.metricsOtelCardinalityLimit = metricsOtelCardinalityLimit;
    }

    public String getMetricsGrpcExporterTarget() {
        return metricsGrpcExporterTarget;
    }

    public void setMetricsGrpcExporterTarget(String metricsGrpcExporterTarget) {
        this.metricsGrpcExporterTarget = metricsGrpcExporterTarget;
    }

    public String getMetricsGrpcExporterHeader() {
        return metricsGrpcExporterHeader;
    }

    public void setMetricsGrpcExporterHeader(String metricsGrpcExporterHeader) {
        this.metricsGrpcExporterHeader = metricsGrpcExporterHeader;
    }

    public long getMetricGrpcExporterTimeOutInMills() {
        return metricGrpcExporterTimeOutInMills;
    }

    public void setMetricGrpcExporterTimeOutInMills(long metricGrpcExporterTimeOutInMills) {
        this.metricGrpcExporterTimeOutInMills = metricGrpcExporterTimeOutInMills;
    }

    public long getMetricGrpcExporterIntervalInMills() {
        return metricGrpcExporterIntervalInMills;
    }

    public void setMetricGrpcExporterIntervalInMills(long metricGrpcExporterIntervalInMills) {
        this.metricGrpcExporterIntervalInMills = metricGrpcExporterIntervalInMills;
    }

    public long getMetricLoggingExporterIntervalInMills() {
        return metricLoggingExporterIntervalInMills;
    }

    public void setMetricLoggingExporterIntervalInMills(long metricLoggingExporterIntervalInMills) {
        this.metricLoggingExporterIntervalInMills = metricLoggingExporterIntervalInMills;
    }

    public String getMetricsLabel() {
        return metricsLabel;
    }

    public void setMetricsLabel(String metricsLabel) {
        this.metricsLabel = metricsLabel;
    }

    public boolean isMetricsInDelta() {
        return metricsInDelta;
    }

    public void setMetricsInDelta(boolean metricsInDelta) {
        this.metricsInDelta = metricsInDelta;
    }

    public int getMetricsPromExporterPort() {
        return metricsPromExporterPort;
    }

    public void setMetricsPromExporterPort(int metricsPromExporterPort) {
        this.metricsPromExporterPort = metricsPromExporterPort;
    }

    public String getMetricsPromExporterHost() {
        return metricsPromExporterHost;
    }

    public void setMetricsPromExporterHost(String metricsPromExporterHost) {
        this.metricsPromExporterHost = metricsPromExporterHost;
    }

    public int getTransactionOpMsgMaxSize() {
        return transactionOpMsgMaxSize;
    }

    public void setTransactionOpMsgMaxSize(int transactionOpMsgMaxSize) {
        this.transactionOpMsgMaxSize = transactionOpMsgMaxSize;
    }

    public int getTransactionOpBatchInterval() {
        return transactionOpBatchInterval;
    }

    public void setTransactionOpBatchInterval(int transactionOpBatchInterval) {
        this.transactionOpBatchInterval = transactionOpBatchInterval;
    }

    public long getChannelExpiredTimeout() {
        return channelExpiredTimeout;
    }

    public void setChannelExpiredTimeout(long channelExpiredTimeout) {
        this.channelExpiredTimeout = channelExpiredTimeout;
    }

    public long getSubscriptionExpiredTimeout() {
        return subscriptionExpiredTimeout;
    }

    public void setSubscriptionExpiredTimeout(long subscriptionExpiredTimeout) {
        this.subscriptionExpiredTimeout = subscriptionExpiredTimeout;
    }

    public boolean isValidateSystemTopicWhenUpdateTopic() {
        return validateSystemTopicWhenUpdateTopic;
    }

    public void setValidateSystemTopicWhenUpdateTopic(boolean validateSystemTopicWhenUpdateTopic) {
        this.validateSystemTopicWhenUpdateTopic = validateSystemTopicWhenUpdateTopic;
    }

    public boolean isEstimateAccumulation() {
        return estimateAccumulation;
    }

    public void setEstimateAccumulation(boolean estimateAccumulation) {
        this.estimateAccumulation = estimateAccumulation;
    }

    public boolean isColdCtrStrategyEnable() {
        return coldCtrStrategyEnable;
    }

    public void setColdCtrStrategyEnable(boolean coldCtrStrategyEnable) {
        this.coldCtrStrategyEnable = coldCtrStrategyEnable;
    }

    public boolean isUsePIDColdCtrStrategy() {
        return usePIDColdCtrStrategy;
    }

    public void setUsePIDColdCtrStrategy(boolean usePIDColdCtrStrategy) {
        this.usePIDColdCtrStrategy = usePIDColdCtrStrategy;
    }

    public long getCgColdReadThreshold() {
        return cgColdReadThreshold;
    }

    public void setCgColdReadThreshold(long cgColdReadThreshold) {
        this.cgColdReadThreshold = cgColdReadThreshold;
    }

    public long getGlobalColdReadThreshold() {
        return globalColdReadThreshold;
    }

    public void setGlobalColdReadThreshold(long globalColdReadThreshold) {
        this.globalColdReadThreshold = globalColdReadThreshold;
    }

    public boolean isUseStaticSubscription() {
        return useStaticSubscription;
    }

    public void setUseStaticSubscription(boolean useStaticSubscription) {
        this.useStaticSubscription = useStaticSubscription;
    }
    
    public long getFetchNamesrvAddrInterval() {
        return fetchNamesrvAddrInterval;
    }
    
    public void setFetchNamesrvAddrInterval(final long fetchNamesrvAddrInterval) {
        this.fetchNamesrvAddrInterval = fetchNamesrvAddrInterval;
    }

    public boolean isPopResponseReturnActualRetryTopic() {
        return popResponseReturnActualRetryTopic;
    }

    public void setPopResponseReturnActualRetryTopic(boolean popResponseReturnActualRetryTopic) {
        this.popResponseReturnActualRetryTopic = popResponseReturnActualRetryTopic;
    }

    public boolean isEnableSingleTopicRegister() {
        return enableSingleTopicRegister;
    }

    public void setEnableSingleTopicRegister(boolean enableSingleTopicRegister) {
        this.enableSingleTopicRegister = enableSingleTopicRegister;
    }

    public boolean isEnableMixedMessageType() {
        return enableMixedMessageType;
    }

    public void setEnableMixedMessageType(boolean enableMixedMessageType) {
        this.enableMixedMessageType = enableMixedMessageType;
    }

    public boolean isEnableSplitRegistration() {
        return enableSplitRegistration;
    }

    public void setEnableSplitRegistration(boolean enableSplitRegistration) {
        this.enableSplitRegistration = enableSplitRegistration;
    }

    public boolean isEnableFastChannelEventProcess() {
        return enableFastChannelEventProcess;
    }

    public void setEnableFastChannelEventProcess(boolean enableFastChannelEventProcess) {
        this.enableFastChannelEventProcess = enableFastChannelEventProcess;
    }

    public boolean isPrintChannelGroups() {
        return printChannelGroups;
    }

    public void setPrintChannelGroups(boolean printChannelGroups) {
        this.printChannelGroups = printChannelGroups;
    }

    public int getPrintChannelGroupsMinNum() {
        return printChannelGroupsMinNum;
    }

    public void setPrintChannelGroupsMinNum(int printChannelGroupsMinNum) {
        this.printChannelGroupsMinNum = printChannelGroupsMinNum;
    }

    public int getSplitRegistrationSize() {
        return splitRegistrationSize;
    }

    public void setSplitRegistrationSize(int splitRegistrationSize) {
        this.splitRegistrationSize = splitRegistrationSize;
    }

    public long getTransactionMetricFlushInterval() {
        return transactionMetricFlushInterval;
    }

    public void setTransactionMetricFlushInterval(long transactionMetricFlushInterval) {
        this.transactionMetricFlushInterval = transactionMetricFlushInterval;
    }

    public long getPopInflightMessageThreshold() {
        return popInflightMessageThreshold;
    }

    public void setPopInflightMessageThreshold(long popInflightMessageThreshold) {
        this.popInflightMessageThreshold = popInflightMessageThreshold;
    }

    public boolean isEnablePopMessageThreshold() {
        return enablePopMessageThreshold;
    }

    public void setEnablePopMessageThreshold(boolean enablePopMessageThreshold) {
        this.enablePopMessageThreshold = enablePopMessageThreshold;
    }

    public boolean isSkipWhenCKRePutReachMaxTimes() {
        return skipWhenCKRePutReachMaxTimes;
    }

    public void setSkipWhenCKRePutReachMaxTimes(boolean skipWhenCKRePutReachMaxTimes) {
        this.skipWhenCKRePutReachMaxTimes = skipWhenCKRePutReachMaxTimes;
    }

    public int getUpdateNameServerAddrPeriod() {
        return updateNameServerAddrPeriod;
    }

    public void setUpdateNameServerAddrPeriod(int updateNameServerAddrPeriod) {
        this.updateNameServerAddrPeriod = updateNameServerAddrPeriod;
    }

    public boolean isAppendAckAsync() {
        return appendAckAsync;
    }

    public void setAppendAckAsync(boolean appendAckAsync) {
        this.appendAckAsync = appendAckAsync;
    }

    public boolean isAppendCkAsync() {
        return appendCkAsync;
    }

    public void setAppendCkAsync(boolean appendCkAsync) {
        this.appendCkAsync = appendCkAsync;
    }

    public boolean isClearRetryTopicWhenDeleteTopic() {
        return clearRetryTopicWhenDeleteTopic;
    }

    public void setClearRetryTopicWhenDeleteTopic(boolean clearRetryTopicWhenDeleteTopic) {
        this.clearRetryTopicWhenDeleteTopic = clearRetryTopicWhenDeleteTopic;
    }

    public boolean isEnableLmqStats() {
        return enableLmqStats;
    }

    public void setEnableLmqStats(boolean enableLmqStats) {
        this.enableLmqStats = enableLmqStats;
    }

    public String getConfigManagerVersion() {
        return configManagerVersion;
    }

    public void setConfigManagerVersion(String configManagerVersion) {
        this.configManagerVersion = configManagerVersion;
    }

    public boolean isAllowRecallWhenBrokerNotWriteable() {
        return allowRecallWhenBrokerNotWriteable;
    }

    public void setAllowRecallWhenBrokerNotWriteable(boolean allowRecallWhenBrokerNotWriteable) {
        this.allowRecallWhenBrokerNotWriteable = allowRecallWhenBrokerNotWriteable;
    }

    public boolean isRecallMessageEnable() {
        return recallMessageEnable;
    }

    public void setRecallMessageEnable(boolean recallMessageEnable) {
        this.recallMessageEnable = recallMessageEnable;
    }

    public boolean isEnableRegisterProducer() {
        return enableRegisterProducer;
    }

    public void setEnableRegisterProducer(boolean enableRegisterProducer) {
        this.enableRegisterProducer = enableRegisterProducer;
    }

    public boolean isEnableCreateSysGroup() {
        return enableCreateSysGroup;
    }

    public void setEnableCreateSysGroup(boolean enableCreateSysGroup) {
        this.enableCreateSysGroup = enableCreateSysGroup;
    }
}
