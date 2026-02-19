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
package org.apache.rocketmq.broker;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.manager.AuthenticationMetadataManager;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.authorization.manager.AuthorizationMetadataManager;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.auth.migration.AuthMigrator;
import org.apache.rocketmq.broker.auth.pipeline.AuthenticationPipeline;
import org.apache.rocketmq.broker.auth.pipeline.AuthorizationPipeline;
import org.apache.rocketmq.broker.client.ClientHousekeepingService;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.DefaultConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.broker.client.net.Broker2Client;
import org.apache.rocketmq.broker.client.rebalance.RebalanceLockManager;
import org.apache.rocketmq.broker.coldctr.ColdDataCgCtrService;
import org.apache.rocketmq.broker.coldctr.ColdDataPullRequestHoldService;
import org.apache.rocketmq.broker.config.v1.RocksDBConsumerOffsetManager;
import org.apache.rocketmq.broker.config.v1.RocksDBLmqSubscriptionGroupManager;
import org.apache.rocketmq.broker.config.v1.RocksDBLmqTopicConfigManager;
import org.apache.rocketmq.broker.config.v1.RocksDBSubscriptionGroupManager;
import org.apache.rocketmq.broker.config.v1.RocksDBTopicConfigManager;
import org.apache.rocketmq.broker.config.v2.ConfigStorage;
import org.apache.rocketmq.broker.config.v2.ConsumerOffsetManagerV2;
import org.apache.rocketmq.broker.config.v2.SubscriptionGroupManagerV2;
import org.apache.rocketmq.broker.config.v2.TopicConfigManagerV2;
import org.apache.rocketmq.broker.controller.ReplicasManager;
import org.apache.rocketmq.broker.dledger.DLedgerRoleChangeHandler;
import org.apache.rocketmq.broker.failover.EscapeBridge;
import org.apache.rocketmq.broker.filter.CommitLogDispatcherCalcBitMap;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.latency.BrokerFastFailure;
import org.apache.rocketmq.broker.longpolling.LmqPullRequestHoldService;
import org.apache.rocketmq.broker.longpolling.NotifyMessageArrivingListener;
import org.apache.rocketmq.broker.longpolling.PullRequestHoldService;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.mqtrace.SendMessageHook;
import org.apache.rocketmq.broker.offset.BroadcastOffsetManager;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.offset.ConsumerOrderInfoManager;
import org.apache.rocketmq.broker.offset.LmqConsumerOffsetManager;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.broker.plugin.BrokerAttachedPlugin;
import org.apache.rocketmq.broker.pop.PopConsumerService;
import org.apache.rocketmq.broker.processor.AckMessageProcessor;
import org.apache.rocketmq.broker.processor.AdminBrokerProcessor;
import org.apache.rocketmq.broker.processor.ChangeInvisibleTimeProcessor;
import org.apache.rocketmq.broker.processor.ClientManageProcessor;
import org.apache.rocketmq.broker.processor.ConsumerManageProcessor;
import org.apache.rocketmq.broker.processor.EndTransactionProcessor;
import org.apache.rocketmq.broker.processor.NotificationProcessor;
import org.apache.rocketmq.broker.processor.PeekMessageProcessor;
import org.apache.rocketmq.broker.processor.PollingInfoProcessor;
import org.apache.rocketmq.broker.processor.PopInflightMessageCounter;
import org.apache.rocketmq.broker.processor.PopMessageProcessor;
import org.apache.rocketmq.broker.processor.PullMessageProcessor;
import org.apache.rocketmq.broker.processor.QueryAssignmentProcessor;
import org.apache.rocketmq.broker.processor.QueryMessageProcessor;
import org.apache.rocketmq.broker.processor.RecallMessageProcessor;
import org.apache.rocketmq.broker.processor.ReplyMessageProcessor;
import org.apache.rocketmq.broker.processor.SendMessageProcessor;
import org.apache.rocketmq.broker.schedule.ScheduleMessageService;
import org.apache.rocketmq.broker.slave.SlaveSynchronize;
import org.apache.rocketmq.broker.subscription.LmqSubscriptionGroupManager;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.LmqTopicConfigManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.broker.topic.TopicQueueMappingCleanService;
import org.apache.rocketmq.broker.topic.TopicQueueMappingManager;
import org.apache.rocketmq.broker.topic.TopicRouteInfoManager;
import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.TransactionMetricsFlushService;
import org.apache.rocketmq.broker.transaction.TransactionalMessageCheckService;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.broker.transaction.queue.DefaultTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageBridge;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageServiceImpl;
import org.apache.rocketmq.broker.util.HookUtils;
import org.apache.rocketmq.common.AbstractBrokerRunnable;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.BrokerIdentity;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.config.ConfigManagerVersion;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.stats.MomentStatsItem;
import org.apache.rocketmq.common.utils.ServiceProvider;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.Configuration;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.protocol.BrokerSyncInfo;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.RequestHeaderRegistry;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigAndMappingSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo;
import org.apache.rocketmq.srvutil.FileWatchService;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.RocksDBMessageStore;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;
import org.apache.rocketmq.store.hook.PutMessageHook;
import org.apache.rocketmq.store.hook.SendMessageBackHook;
import org.apache.rocketmq.store.plugin.MessageStoreFactory;
import org.apache.rocketmq.store.plugin.MessageStorePluginContext;
import org.apache.rocketmq.store.stats.BrokerStats;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.stats.LmqBrokerStatsManager;
import org.apache.rocketmq.store.timer.TimerCheckpoint;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.store.timer.TimerMetrics;

import java.net.InetSocketAddress;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Broker 控制器<br>
 * 负责 Broker 生命周期管理与核心组件协同
 */
public class BrokerController {
    /**
     * Broker 日志记录器
     */
    protected static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    /**
     * 保护日志记录器
     */
    private static final Logger LOG_PROTECTION = LoggerFactory.getLogger(LoggerName.PROTECTION_LOGGER_NAME);
    /**
     * 水位日志记录器
     */
    private static final Logger LOG_WATER_MARK = LoggerFactory.getLogger(LoggerName.WATER_MARK_LOGGER_NAME);
    /**
     * HA 地址最小长度常量
     */
    protected static final int HA_ADDRESS_MIN_LENGTH = 6;

    /**
     * Broker 配置对象
     */
    protected final BrokerConfig brokerConfig;
    /**
     * Netty 服务端配置
     */
    private final NettyServerConfig nettyServerConfig;
    /**
     * Netty 客户端配置
     */
    private final NettyClientConfig nettyClientConfig;
    /**
     * 消息存储配置
     */
    protected final MessageStoreConfig messageStoreConfig;
    /**
     * 认证授权配置
     */
    private final AuthConfig authConfig;
    /**
     * 消费位点管理器
     */
    protected ConsumerOffsetManager consumerOffsetManager;
    /**
     * 广播位点管理器
     */
    protected final BroadcastOffsetManager broadcastOffsetManager;
    /**
     * 消费者管理器
     */
    protected final ConsumerManager consumerManager;
    /**
     * 消费者过滤管理器
     */
    protected final ConsumerFilterManager consumerFilterManager;
    /**
     * 顺序消费信息管理器
     */
    protected final ConsumerOrderInfoManager consumerOrderInfoManager;
    /**
     * POP 在途消息计数器
     */
    protected final PopInflightMessageCounter popInflightMessageCounter;
    /**
     * POP 消费服务
     */
    protected final PopConsumerService popConsumerService;
    /**
     * 生产者管理器
     */
    protected final ProducerManager producerManager;
    /**
     * 定时消息服务
     */
    protected final ScheduleMessageService scheduleMessageService;
    /**
     * 客户端连接管理服务
     */
    protected final ClientHousekeepingService clientHousekeepingService;
    /**
     * 拉消息处理器
     */
    protected final PullMessageProcessor pullMessageProcessor;
    /**
     * 窥探消息处理器
     */
    protected final PeekMessageProcessor peekMessageProcessor;
    /**
     * POP 消息处理器
     */
    protected final PopMessageProcessor popMessageProcessor;
    /**
     * ACK 消息处理器
     */
    protected final AckMessageProcessor ackMessageProcessor;
    /**
     * 修改不可见时间处理器
     */
    protected final ChangeInvisibleTimeProcessor changeInvisibleTimeProcessor;
    /**
     * 通知处理器
     */
    protected final NotificationProcessor notificationProcessor;
    /**
     * 轮询信息处理器
     */
    protected final PollingInfoProcessor pollingInfoProcessor;
    /**
     * 查询分配处理器
     */
    protected final QueryAssignmentProcessor queryAssignmentProcessor;
    /**
     * 客户端管理处理器
     */
    protected final ClientManageProcessor clientManageProcessor;
    /**
     * 发送消息处理器
     */
    protected final SendMessageProcessor sendMessageProcessor;
    /**
     * 撤回消息处理器
     */
    protected final RecallMessageProcessor recallMessageProcessor;
    /**
     * 回复消息处理器
     */
    protected final ReplyMessageProcessor replyMessageProcessor;
    /**
     * 拉请求挂起服务
     */
    protected final PullRequestHoldService pullRequestHoldService;
    /**
     * 消息到达监听器
     */
    protected final MessageArrivingListener messageArrivingListener;
    /**
     * Broker 到客户端服务
     */
    protected final Broker2Client broker2Client;
    /**
     * 消费者 ID 变更监听器
     */
    protected final ConsumerIdsChangeListener consumerIdsChangeListener;
    /**
     * 结束事务处理器
     */
    protected final EndTransactionProcessor endTransactionProcessor;
    /**
     * 重平衡锁管理器
     */
    private final RebalanceLockManager rebalanceLockManager = new RebalanceLockManager();
    /**
     * 主题路由信息管理器
     */
    private final TopicRouteInfoManager topicRouteInfoManager;
    /**
     * Broker 外部通信 API
     */
    protected BrokerOuterAPI brokerOuterAPI;
    /**
     * 定时任务执行器
     */
    protected ScheduledExecutorService scheduledExecutorService;
    /**
     * 成员组同步执行器
     */
    protected ScheduledExecutorService syncBrokerMemberGroupExecutorService;
    /**
     * 心跳执行器
     */
    protected ScheduledExecutorService brokerHeartbeatExecutorService;
    /**
     * 从节点同步服务
     */
    protected final SlaveSynchronize slaveSynchronize;
    /**
     * 发送线程池队列
     */
    protected final BlockingQueue<Runnable> sendThreadPoolQueue;
    /**
     * 存储线程池队列
     */
    protected final BlockingQueue<Runnable> putThreadPoolQueue;
    /**
     * ACK 线程池队列
     */
    protected final BlockingQueue<Runnable> ackThreadPoolQueue;
    /**
     * 拉取线程池队列
     */
    protected final BlockingQueue<Runnable> pullThreadPoolQueue;
    /**
     * 轻量拉取线程池队列
     */
    protected final BlockingQueue<Runnable> litePullThreadPoolQueue;
    /**
     * 回复线程池队列
     */
    protected final BlockingQueue<Runnable> replyThreadPoolQueue;
    /**
     * 查询线程池队列
     */
    protected final BlockingQueue<Runnable> queryThreadPoolQueue;
    /**
     * 客户端管理线程池队列
     */
    protected final BlockingQueue<Runnable> clientManagerThreadPoolQueue;
    /**
     * 心跳线程池队列
     */
    protected final BlockingQueue<Runnable> heartbeatThreadPoolQueue;
    /**
     * 消费者管理线程池队列
     */
    protected final BlockingQueue<Runnable> consumerManagerThreadPoolQueue;
    /**
     * 结束事务线程池队列
     */
    protected final BlockingQueue<Runnable> endTransactionThreadPoolQueue;
    /**
     * 管理线程池队列
     */
    protected final BlockingQueue<Runnable> adminBrokerThreadPoolQueue;
    /**
     * 负载均衡线程池队列
     */
    protected final BlockingQueue<Runnable> loadBalanceThreadPoolQueue;
    /**
     * Broker 统计管理器
     */
    protected BrokerStatsManager brokerStatsManager;
    /**
     * 发送消息 Hook 列表
     */
    protected final List<SendMessageHook> sendMessageHookList = new ArrayList<>();
    /**
     * 消费消息 Hook 列表
     */
    protected final List<ConsumeMessageHook> consumeMessageHookList = new ArrayList<>();
    /**
     * 消息存储实现
     */
    protected MessageStore messageStore;
    /**
     * TCP remoting 服务标识
     */
    protected static final String TCP_REMOTING_SERVER = "TCP_REMOTING_SERVER";
    /**
     * FAST remoting 服务标识
     */
    protected static final String FAST_REMOTING_SERVER = "FAST_REMOTING_SERVER";
    /**
     * remoting 服务映射
     */
    protected final Map<String, RemotingServer> remotingServerMap = new ConcurrentHashMap<>();
    /**
     * remoting 启动闭锁
     */
    protected CountDownLatch remotingServerStartLatch;
    /**
     * If {Topic, SubscriptionGroup, Offset}ManagerV2 are used, config entries are stored in RocksDB.
     * <br>
     * 若使用 {Topic, SubscriptionGroup, Offset} ManagerV2, 配置项会存储在 RocksDB 中
     */
    protected ConfigStorage configStorage;
    /**
     * 主题配置管理器
     */
    protected TopicConfigManager topicConfigManager;
    /**
     * 订阅组管理器
     */
    protected SubscriptionGroupManager subscriptionGroupManager;
    /**
     * 主题队列映射管理器
     */
    protected TopicQueueMappingManager topicQueueMappingManager;
    /**
     * 发送消息执行器
     */
    protected ExecutorService sendMessageExecutor;
    /**
     * 拉消息执行器
     */
    protected ExecutorService pullMessageExecutor;
    /**
     * 轻量拉消息执行器
     */
    protected ExecutorService litePullMessageExecutor;
    /**
     * 存储 Future 执行器
     */
    protected ExecutorService putMessageFutureExecutor;
    /**
     * ACK 消息执行器
     */
    protected ExecutorService ackMessageExecutor;
    /**
     * 回复消息执行器
     */
    protected ExecutorService replyMessageExecutor;
    /**
     * 查询消息执行器
     */
    protected ExecutorService queryMessageExecutor;
    /**
     * 管理请求执行器
     */
    protected ExecutorService adminBrokerExecutor;
    /**
     * 客户端管理执行器
     */
    protected ExecutorService clientManageExecutor;
    /**
     * 心跳执行器
     */
    protected ExecutorService heartbeatExecutor;
    /**
     * 消费者管理执行器
     */
    protected ExecutorService consumerManageExecutor;
    /**
     * 负载均衡执行器
     */
    protected ExecutorService loadBalanceExecutor;
    /**
     * 结束事务执行器
     */
    protected ExecutorService endTransactionExecutor;
    /**
     * 是否周期更新主节点 HA 地址
     */
    protected boolean updateMasterHAServerAddrPeriodically = false;
    /**
     * Broker 统计对象
     */
    private BrokerStats brokerStats;
    /**
     * 存储地址
     */
    private InetSocketAddress storeHost;
    /**
     * 定时消息存储
     */
    private TimerMessageStore timerMessageStore;
    /**
     * 定时器检查点
     */
    private TimerCheckpoint timerCheckpoint;
    /**
     * 快速失败组件
     */
    protected BrokerFastFailure brokerFastFailure;
    /**
     * 配置管理器
     */
    private Configuration configuration;
    /**
     * 主题队列映射清理服务
     */
    protected TopicQueueMappingCleanService topicQueueMappingCleanService;
    /**
     * 文件监听服务
     */
    protected FileWatchService fileWatchService;
    /**
     * 事务消息回查服务
     */
    protected TransactionalMessageCheckService transactionalMessageCheckService;
    /**
     * 事务消息服务
     */
    protected TransactionalMessageService transactionalMessageService;
    /**
     * 事务回查监听器
     */
    protected AbstractTransactionalMessageCheckListener transactionalMessageCheckListener;
    /**
     * 关闭状态标记
     */
    protected volatile boolean shutdown = false;
    /**
     * 关闭钩子
     */
    protected ShutdownHook shutdownHook;
    /**
     * 定时服务启动标记
     */
    private volatile boolean isScheduleServiceStart = false;
    /**
     * 事务回查服务启动标记
     */
    private volatile boolean isTransactionCheckServiceStart = false;
    /**
     * Broker 成员组信息
     */
    protected volatile BrokerMemberGroup brokerMemberGroup;
    /**
     * 逃逸桥接服务
     */
    protected EscapeBridge escapeBridge;
    /**
     * 附加插件列表
     */
    protected List<BrokerAttachedPlugin> brokerAttachedPlugins = new ArrayList<>();
    /**
     * 应启动时间戳
     */
    protected volatile long shouldStartTime;
    /**
     * Broker 预上线服务
     */
    private BrokerPreOnlineService brokerPreOnlineService;
    /**
     * 隔离状态标记
     */
    protected volatile boolean isIsolated = false;
    /**
     * 组内最小 brokerId
     */
    protected volatile long minBrokerIdInGroup = 0;
    /**
     * 组内最小 broker 地址
     */
    protected volatile String minBrokerAddrInGroup = null;
    /**
     * 并发控制锁
     */
    private final Lock lock = new ReentrantLock();
    /**
     * 定时任务 Future 列表
     */
    protected final List<ScheduledFuture<?>> scheduledFutures = new ArrayList<>();
    /**
     * 副本管理器
     */
    protected ReplicasManager replicasManager;
    /**
     * 最近同步时间戳
     */
    private long lastSyncTimeMs = System.currentTimeMillis();
    /**
     * Broker 指标管理器
     */
    private BrokerMetricsManager brokerMetricsManager;
    /**
     * 冷数据拉请求挂起服务
     */
    private ColdDataPullRequestHoldService coldDataPullRequestHoldService;
    /**
     * 冷数据消费组控制服务
     */
    private ColdDataCgCtrService coldDataCgCtrService;
    /**
     * 事务指标刷新服务
     */
    private TransactionMetricsFlushService transactionMetricsFlushService;
    /**
     * 认证元数据管理器
     */
    private AuthenticationMetadataManager authenticationMetadataManager;
    /**
     * 鉴权元数据管理器
     */
    private AuthorizationMetadataManager authorizationMetadataManager;

    /**
     * 构造 Broker 控制器
     *
     * @param brokerConfig Broker 配置
     * @param nettyServerConfig Netty 服务端配置
     * @param nettyClientConfig Netty 客户端配置
     * @param messageStoreConfig 消息存储配置
     * @param authConfig 认证授权配置
     * @param shutdownHook 关闭钩子
     */
    public BrokerController(
        final BrokerConfig brokerConfig,
        final NettyServerConfig nettyServerConfig,
        final NettyClientConfig nettyClientConfig,
        final MessageStoreConfig messageStoreConfig,
        final AuthConfig authConfig,
        final ShutdownHook shutdownHook
    ) {
        this(brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig, authConfig);
        this.shutdownHook = shutdownHook;
    }

    /**
     * 构造 Broker 控制器
     *
     * @param brokerConfig Broker 配置
     * @param messageStoreConfig 消息存储配置
     */
    public BrokerController(
        final BrokerConfig brokerConfig,
        final MessageStoreConfig messageStoreConfig
    ) {
        this(brokerConfig, null, null, messageStoreConfig, null);
    }

    /**
     * 构造 Broker 控制器
     *
     * @param brokerConfig Broker 配置
     * @param nettyServerConfig Netty 服务端配置
     * @param nettyClientConfig Netty 客户端配置
     * @param messageStoreConfig 消息存储配置
     */
    public BrokerController(
        final BrokerConfig brokerConfig,
        final NettyServerConfig nettyServerConfig,
        final NettyClientConfig nettyClientConfig,
        final MessageStoreConfig messageStoreConfig
    ) {
        this(brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig, null);
    }

    /**
     * 构造 Broker 控制器
     *
     * @param brokerConfig Broker 配置
     * @param nettyServerConfig Netty 服务端配置
     * @param nettyClientConfig Netty 客户端配置
     * @param messageStoreConfig 消息存储配置
     * @param authConfig 认证授权配置
     */
    public BrokerController(
        final BrokerConfig brokerConfig,
        final NettyServerConfig nettyServerConfig,
        final NettyClientConfig nettyClientConfig,
        final MessageStoreConfig messageStoreConfig,
        final AuthConfig authConfig
    ) {
        this.brokerConfig = brokerConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.authConfig = authConfig;
        this.setStoreHost(new InetSocketAddress(this.getBrokerConfig().getBrokerIP1(), getListenPort()));

        this.brokerStatsManager = messageStoreConfig.isEnableLmq() ? new LmqBrokerStatsManager(this.brokerConfig) : new BrokerStatsManager(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.isEnableDetailStat());
        this.broadcastOffsetManager = new BroadcastOffsetManager(this);
        if (ConfigManagerVersion.V2.getVersion().equals(brokerConfig.getConfigManagerVersion())) {
            this.configStorage = new ConfigStorage(messageStoreConfig);
            this.topicConfigManager = new TopicConfigManagerV2(this, configStorage);
            this.subscriptionGroupManager = new SubscriptionGroupManagerV2(this, configStorage);
            this.consumerOffsetManager = new ConsumerOffsetManagerV2(this, configStorage);
        } else if (this.messageStoreConfig.isEnableRocksDBStore()) {
            this.topicConfigManager = messageStoreConfig.isEnableLmq() ? new RocksDBLmqTopicConfigManager(this) : new RocksDBTopicConfigManager(this);
            this.subscriptionGroupManager = messageStoreConfig.isEnableLmq() ? new RocksDBLmqSubscriptionGroupManager(this) : new RocksDBSubscriptionGroupManager(this);
            this.consumerOffsetManager = new RocksDBConsumerOffsetManager(this);
        } else {
            this.topicConfigManager = messageStoreConfig.isEnableLmq() ? new LmqTopicConfigManager(this) : new TopicConfigManager(this);
            this.subscriptionGroupManager = messageStoreConfig.isEnableLmq() ? new LmqSubscriptionGroupManager(this) : new SubscriptionGroupManager(this);
            this.consumerOffsetManager = messageStoreConfig.isEnableLmq() ? new LmqConsumerOffsetManager(this) : new ConsumerOffsetManager(this);
        }
        this.topicQueueMappingManager = new TopicQueueMappingManager(this);
        this.authenticationMetadataManager = AuthenticationFactory.getMetadataManager(this.authConfig);
        this.authorizationMetadataManager = AuthorizationFactory.getMetadataManager(this.authConfig);
        this.pullMessageProcessor = new PullMessageProcessor(this);
        this.peekMessageProcessor = new PeekMessageProcessor(this);
        this.pullRequestHoldService = messageStoreConfig.isEnableLmq() ? new LmqPullRequestHoldService(this) : new PullRequestHoldService(this);
        this.popMessageProcessor = new PopMessageProcessor(this);
        this.notificationProcessor = new NotificationProcessor(this);
        this.pollingInfoProcessor = new PollingInfoProcessor(this);
        this.ackMessageProcessor = new AckMessageProcessor(this);
        this.changeInvisibleTimeProcessor = new ChangeInvisibleTimeProcessor(this);
        this.sendMessageProcessor = new SendMessageProcessor(this);
        this.recallMessageProcessor = new RecallMessageProcessor(this);
        this.replyMessageProcessor = new ReplyMessageProcessor(this);
        this.messageArrivingListener = new NotifyMessageArrivingListener(this.pullRequestHoldService, this.popMessageProcessor, this.notificationProcessor);
        this.consumerIdsChangeListener = new DefaultConsumerIdsChangeListener(this);
        this.consumerManager = new ConsumerManager(this.consumerIdsChangeListener, this.brokerStatsManager, this.brokerConfig);
        this.producerManager = new ProducerManager(this.brokerStatsManager);
        this.consumerFilterManager = new ConsumerFilterManager(this);
        this.consumerOrderInfoManager = new ConsumerOrderInfoManager(this);
        this.popInflightMessageCounter = new PopInflightMessageCounter(this);
        this.popConsumerService = brokerConfig.isPopConsumerKVServiceInit() ? new PopConsumerService(this) : null;
        this.clientHousekeepingService = new ClientHousekeepingService(this);
        this.broker2Client = new Broker2Client(this);
        this.scheduleMessageService = new ScheduleMessageService(this);
        this.coldDataPullRequestHoldService = new ColdDataPullRequestHoldService(this);
        this.coldDataCgCtrService = new ColdDataCgCtrService(this);

        if (nettyClientConfig != null) {
            this.brokerOuterAPI = new BrokerOuterAPI(nettyClientConfig, authConfig);
        }

        this.queryAssignmentProcessor = new QueryAssignmentProcessor(this);
        this.clientManageProcessor = new ClientManageProcessor(this);
        this.slaveSynchronize = new SlaveSynchronize(this);
        this.endTransactionProcessor = new EndTransactionProcessor(this);

        this.sendThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getSendThreadPoolQueueCapacity());
        this.putThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getPutThreadPoolQueueCapacity());
        this.pullThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getPullThreadPoolQueueCapacity());
        this.litePullThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getLitePullThreadPoolQueueCapacity());

        this.ackThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getAckThreadPoolQueueCapacity());
        this.replyThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getReplyThreadPoolQueueCapacity());
        this.queryThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getQueryThreadPoolQueueCapacity());
        this.clientManagerThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getClientManagerThreadPoolQueueCapacity());
        this.consumerManagerThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getConsumerManagerThreadPoolQueueCapacity());
        this.heartbeatThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getHeartbeatThreadPoolQueueCapacity());
        this.endTransactionThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getEndTransactionPoolQueueCapacity());
        this.adminBrokerThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getAdminBrokerThreadPoolQueueCapacity());
        this.loadBalanceThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getLoadBalanceThreadPoolQueueCapacity());

        this.brokerFastFailure = new BrokerFastFailure(this);

        String brokerConfigPath;
        if (brokerConfig.getBrokerConfigPath() != null && !brokerConfig.getBrokerConfigPath().isEmpty()) {
            brokerConfigPath = brokerConfig.getBrokerConfigPath();
        } else {
            brokerConfigPath = BrokerPathConfigHelper.getBrokerConfigPath();
        }
        this.configuration = new Configuration(
            LOG,
            brokerConfigPath,
            this.brokerConfig, this.nettyServerConfig, this.nettyClientConfig, this.messageStoreConfig
        );

        this.brokerStatsManager.setProducerStateGetter(new BrokerStatsManager.StateGetter() {
            @Override
            public boolean online(String instanceId, String group, String topic) {
                if (getTopicConfigManager().getTopicConfigTable().containsKey(NamespaceUtil.wrapNamespace(instanceId, topic))) {
                    return getProducerManager().groupOnline(NamespaceUtil.wrapNamespace(instanceId, group));
                } else {
                    return getProducerManager().groupOnline(group);
                }
            }
        });
        this.brokerStatsManager.setConsumerStateGetter(new BrokerStatsManager.StateGetter() {
            @Override
            public boolean online(String instanceId, String group, String topic) {
                String topicFullName = NamespaceUtil.wrapNamespace(instanceId, topic);
                if (getTopicConfigManager().getTopicConfigTable().containsKey(topicFullName)) {
                    return getConsumerManager().findSubscriptionData(NamespaceUtil.wrapNamespace(instanceId, group), topicFullName) != null;
                } else {
                    return getConsumerManager().findSubscriptionData(group, topic) != null;
                }
            }
        });

        this.brokerMemberGroup = new BrokerMemberGroup(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.getBrokerName());
        this.brokerMemberGroup.getBrokerAddrs().put(this.brokerConfig.getBrokerId(), this.getBrokerAddr());

        this.escapeBridge = new EscapeBridge(this);

        this.topicRouteInfoManager = new TopicRouteInfoManager(this);

        if (this.brokerConfig.isEnableSlaveActingMaster() && !this.brokerConfig.isSkipPreOnline()) {
            this.brokerPreOnlineService = new BrokerPreOnlineService(this);
        }

        if (this.authConfig != null && this.authConfig.isMigrateAuthFromV1Enabled()) {
            new AuthMigrator(this.authConfig).migrate();
        }
    }

    public AuthConfig getAuthConfig() {
        return authConfig;
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }

    public BlockingQueue<Runnable> getPullThreadPoolQueue() {
        return pullThreadPoolQueue;
    }

    public BlockingQueue<Runnable> getQueryThreadPoolQueue() {
        return queryThreadPoolQueue;
    }

    public BrokerMetricsManager getBrokerMetricsManager() {
        return brokerMetricsManager;
    }

    /**
     * 初始化 remoting 服务端
     */
    protected void initializeRemotingServer() throws CloneNotSupportedException {
        RemotingServer tcpRemotingServer = new NettyRemotingServer(this.nettyServerConfig, this.clientHousekeepingService);
        NettyServerConfig fastConfig = (NettyServerConfig) this.nettyServerConfig.clone();

        int listeningPort = nettyServerConfig.getListenPort() - 2;
        if (listeningPort < 0) {
            listeningPort = 0;
        }
        fastConfig.setListenPort(listeningPort);

        RemotingServer fastRemotingServer = new NettyRemotingServer(fastConfig, this.clientHousekeepingService);

        remotingServerMap.put(TCP_REMOTING_SERVER, tcpRemotingServer);   // 10911
        remotingServerMap.put(FAST_REMOTING_SERVER, fastRemotingServer); // 10909
    }

    /**
     * Initialize resources including remoting server and thread executors.
     * <br>
     * 初始化资源, 包括 remoting 服务端与线程执行器
     */
    protected void initializeResources() {
        this.scheduledExecutorService = ThreadUtils.newScheduledThreadPool(1,
            new ThreadFactoryImpl("BrokerControllerScheduledThread", true, getBrokerIdentity()));

        this.sendMessageExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getSendMessageThreadPoolNums(),
            this.brokerConfig.getSendMessageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.sendThreadPoolQueue,
            new ThreadFactoryImpl("SendMessageThread_", getBrokerIdentity()));

        this.pullMessageExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getPullMessageThreadPoolNums(),
            this.brokerConfig.getPullMessageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.pullThreadPoolQueue,
            new ThreadFactoryImpl("PullMessageThread_", getBrokerIdentity()));

        this.litePullMessageExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getLitePullMessageThreadPoolNums(),
            this.brokerConfig.getLitePullMessageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.litePullThreadPoolQueue,
            new ThreadFactoryImpl("LitePullMessageThread_", getBrokerIdentity()));

        this.putMessageFutureExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getPutMessageFutureThreadPoolNums(),
            this.brokerConfig.getPutMessageFutureThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.putThreadPoolQueue,
            new ThreadFactoryImpl("PutMessageThread_", getBrokerIdentity()));

        this.ackMessageExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getAckMessageThreadPoolNums(),
            this.brokerConfig.getAckMessageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.ackThreadPoolQueue,
            new ThreadFactoryImpl("AckMessageThread_", getBrokerIdentity()));

        this.queryMessageExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getQueryMessageThreadPoolNums(),
            this.brokerConfig.getQueryMessageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.queryThreadPoolQueue,
            new ThreadFactoryImpl("QueryMessageThread_", getBrokerIdentity()));

        this.adminBrokerExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getAdminBrokerThreadPoolNums(),
            this.brokerConfig.getAdminBrokerThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.adminBrokerThreadPoolQueue,
            new ThreadFactoryImpl("AdminBrokerThread_", getBrokerIdentity()));

        this.clientManageExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getClientManageThreadPoolNums(),
            this.brokerConfig.getClientManageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.clientManagerThreadPoolQueue,
            new ThreadFactoryImpl("ClientManageThread_", getBrokerIdentity()));

        this.heartbeatExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getHeartbeatThreadPoolNums(),
            this.brokerConfig.getHeartbeatThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.heartbeatThreadPoolQueue,
            new ThreadFactoryImpl("HeartbeatThread_", true, getBrokerIdentity()));

        this.consumerManageExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getConsumerManageThreadPoolNums(),
            this.brokerConfig.getConsumerManageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.consumerManagerThreadPoolQueue,
            new ThreadFactoryImpl("ConsumerManageThread_", true, getBrokerIdentity()));

        this.replyMessageExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getProcessReplyMessageThreadPoolNums(),
            this.brokerConfig.getProcessReplyMessageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.replyThreadPoolQueue,
            new ThreadFactoryImpl("ProcessReplyMessageThread_", getBrokerIdentity()));

        this.endTransactionExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getEndTransactionThreadPoolNums(),
            this.brokerConfig.getEndTransactionThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.endTransactionThreadPoolQueue,
            new ThreadFactoryImpl("EndTransactionThread_", getBrokerIdentity()));

        this.loadBalanceExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getLoadBalanceProcessorThreadPoolNums(),
            this.brokerConfig.getLoadBalanceProcessorThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.loadBalanceThreadPoolQueue,
            new ThreadFactoryImpl("LoadBalanceProcessorThread_", getBrokerIdentity()));

        this.syncBrokerMemberGroupExecutorService = ThreadUtils.newScheduledThreadPool(1,
            new ThreadFactoryImpl("BrokerControllerSyncBrokerScheduledThread", getBrokerIdentity()));
        this.brokerHeartbeatExecutorService = ThreadUtils.newScheduledThreadPool(1,
            new ThreadFactoryImpl("BrokerControllerHeartbeatScheduledThread", getBrokerIdentity()));

        this.topicQueueMappingCleanService = new TopicQueueMappingCleanService(this);
    }

    /**
     * 初始化 Broker 定时任务
     */
    protected void initializeBrokerScheduledTasks() {
        // 每日统计任务: 延迟到次日 0 点触发, 之后按 24 小时固定频率执行
        final long initialDelay = UtilAll.computeNextMorningTimeMillis() - System.currentTimeMillis();
        final long period = TimeUnit.DAYS.toMillis(1);

        // 任务 1: 周期记录 Broker 统计快照, 供运维观测日维度指标
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    BrokerController.this.getBrokerStats().record();
                } catch (Throwable e) {
                    LOG.error("BrokerController: failed to record broker stats", e);
                }
            }
        }, initialDelay, period, TimeUnit.MILLISECONDS);

        // 任务 2: 周期落盘消费进度, 控制重启后位点恢复误差
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    BrokerController.this.consumerOffsetManager.persist();
                } catch (Throwable e) {
                    LOG.error(
                        "BrokerController: failed to persist config file of consumerOffset", e);
                }
            }
        }, 1000 * 10, this.brokerConfig.getFlushConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

        // 任务 3: 周期落盘过滤规则与顺序消费信息, 避免内存态丢失
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    BrokerController.this.consumerFilterManager.persist();
                    BrokerController.this.consumerOrderInfoManager.persist();
                } catch (Throwable e) {
                    LOG.error(
                        "BrokerController: failed to persist config file of consumerFilter or consumerOrderInfo",
                        e);
                }
            }
        }, 1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);

        // 任务 4: 执行慢消费保护逻辑, 按阈值禁用异常消费组
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    BrokerController.this.protectBroker();
                } catch (Throwable e) {
                    LOG.error("BrokerController: failed to protectBroker", e);
                }
            }
        }, 3, 3, TimeUnit.MINUTES);

        // 任务 5: 高频打印线程池水位, 便于快速定位堆积
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    BrokerController.this.printWaterMark();
                } catch (Throwable e) {
                    LOG.error("BrokerController: failed to print broker watermark", e);
                }
            }
        }, 10, 1, TimeUnit.SECONDS);

        // 任务 6: 清理已删除主题对应的 Timer 指标, 防止指标集合无界增长
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    BrokerController.this.messageStore.getTimerMessageStore().getTimerMetrics()
                            .cleanMetrics(BrokerController.this.topicConfigManager.getTopicConfigTable().keySet());
                } catch (Throwable e) {
                    LOG.error("BrokerController: failed to clean unused timer metrics.", e);
                }
            }
        }, 3, 3, TimeUnit.MINUTES);

        // 任务 7: 输出分发落后字节数, 用于评估 CommitLog 到消费队列分发延迟
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    LOG.info("Dispatch task fall behind commit log {}bytes",
                        BrokerController.this.getMessageStore().dispatchBehindBytes());
                } catch (Throwable e) {
                    LOG.error("Failed to print dispatchBehindBytes", e);
                }
            }
        }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);

        // 经典主从 HA 场景才执行以下逻辑
        // DLedger, Duplication, Controller 模式均由各自链路管理主从关系
        if (!messageStoreConfig.isEnableDLegerCommitLog() && !messageStoreConfig.isDuplicationEnable() && !brokerConfig.isEnableControllerMode()) {
            if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
                // 若配置了固定主节点地址则直接使用, 无需再周期刷新
                if (this.messageStoreConfig.getHaMasterAddress() != null && this.messageStoreConfig.getHaMasterAddress().length() >= HA_ADDRESS_MIN_LENGTH) {
                    this.messageStore.updateHaMasterAddress(this.messageStoreConfig.getHaMasterAddress());
                    this.updateMasterHAServerAddrPeriodically = false;
                } else {
                    // 未配置固定主节点地址时, 后续通过注册返回结果动态刷新主节点地址
                    this.updateMasterHAServerAddrPeriodically = true;
                }

                // 从节点同步任务
                // 1. syncAll 频率控制在 60 秒级, 避免配置全量同步过于频繁
                // 2. TimerCheckPoint 对延迟敏感, 保持 3 秒级高频同步
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            if (System.currentTimeMillis() - lastSyncTimeMs > 60 * 1000) {
                                BrokerController.this.getSlaveSynchronize().syncAll();
                                lastSyncTimeMs = System.currentTimeMillis();
                            }

                            //timer checkpoint, latency-sensitive, so sync it more frequently
                            // timer 检查点对延迟敏感, 需要更高同步频率
                            if (messageStoreConfig.isTimerWheelEnable()) {
                                BrokerController.this.getSlaveSynchronize().syncTimerCheckPoint();
                            }
                        } catch (Throwable e) {
                            LOG.error("Failed to sync all config for slave.", e);
                        }
                    }
                }, 1000 * 10, 3 * 1000, TimeUnit.MILLISECONDS);

            } else {
                // 主节点周期打印主从落后量, 便于观察复制健康度
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            BrokerController.this.printMasterAndSlaveDiff();
                        } catch (Throwable e) {
                            LOG.error("Failed to print diff of master and slave.", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
            }
        }

        // Controller 模式下主节点切换由 controller 协调, 需开启主节点地址周期更新
        if (this.brokerConfig.isEnableControllerMode()) {
            this.updateMasterHAServerAddrPeriodically = true;
        }
    }

    /**
     * 初始化调度任务
     */
    protected void initializeScheduledTasks() {

        initializeBrokerScheduledTasks();

        // 更新 NettyRemotingClient 维护的 namesrv 地址列表
        // 1. 若已指定 namesrv 地址, 则定期更新 namesrv 地址列表
        // 2. 否则若配置了通过地址服务器获取 namesrv 地址, 则定期从地址服务器拉取 namesrv 地址
        if (this.brokerConfig.getNamesrvAddr() != null) {
            this.updateNamesrvAddr();
            LOG.info("Set user specified name server address: {}", this.brokerConfig.getNamesrvAddr());
            // also auto update namesrv if specify
            // 若已指定也自动更新 namesrv 地址
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.updateNamesrvAddr();
                    } catch (Throwable e) {
                        LOG.error("Failed to update nameServer address list", e);
                    }
                }
            }, 1000 * 10, this.brokerConfig.getUpdateNameServerAddrPeriod(), TimeUnit.MILLISECONDS);
        } else if (this.brokerConfig.isFetchNamesrvAddrByAddressServer()) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.brokerOuterAPI.fetchNameServerAddr();
                    } catch (Throwable e) {
                        LOG.error("Failed to fetch nameServer address", e);
                    }
                }
            }, 1000 * 10, this.brokerConfig.getFetchNamesrvAddrInterval(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 更新 NameServer 地址
     */
    private void updateNamesrvAddr() {
        if (this.brokerConfig.isFetchNameSrvAddrByDnsLookup()) {
            // 先通过 DNS 解析域名再更新 NameServer 地址列表
            this.brokerOuterAPI.updateNameServerAddressListByDnsLookup(this.brokerConfig.getNamesrvAddr());
        } else {
            // 按分号分隔地址字符串并更新 NameServer 地址列表
            this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
        }
    }

    /**
     * 初始化元数据组件<br>
     * 启动配置存储并加载主题配置, 队列映射, 消费进度, 订阅组, 过滤规则与顺序消费元数据<br>
     * 任一子组件加载失败都返回 false, 调用方据此中止 Broker 启动流程
     *
     * @return true 表示元数据全部加载成功
     */
    public boolean initializeMetadata() {
        boolean result = true;
        if (null != configStorage) {
            result = configStorage.start();
        }
        // 尝试从 json 配置文件或者 bak 备份文件中加载 json 字符串, 然后反序列化转换为自身内部的属性
        result = result && this.topicConfigManager.load();
        result = result && this.topicQueueMappingManager.load();
        result = result && this.consumerOffsetManager.load();
        result = result && this.subscriptionGroupManager.load();
        result = result && this.consumerFilterManager.load();
        result = result && this.consumerOrderInfoManager.load();
        return result;
    }

    /**
     * 初始化消息存储<br>
     * 根据存储配置创建默认存储实现, 按需挂载 DLedger 角色变更处理器与存储插件链<br>
     * 同时注册位图分发器并在定时消息开启时初始化 Timer 相关组件
     *
     * @return true 表示消息存储初始化成功
     */
    public boolean initializeMessageStore() {
        boolean result = true;
        try {
            DefaultMessageStore defaultMessageStore;
            if (this.messageStoreConfig.isEnableRocksDBStore()) {
                defaultMessageStore = new RocksDBMessageStore(this.messageStoreConfig, this.brokerStatsManager, this.messageArrivingListener, this.brokerConfig, topicConfigManager.getTopicConfigTable());
            } else {
                defaultMessageStore = new DefaultMessageStore(this.messageStoreConfig, this.brokerStatsManager, this.messageArrivingListener, this.brokerConfig, topicConfigManager.getTopicConfigTable());
                if (messageStoreConfig.isRocksdbCQDoubleWriteEnable()) {
                    defaultMessageStore.enableRocksdbCQWrite();
                }
            }

            if (messageStoreConfig.isEnableDLegerCommitLog()) {
                DLedgerRoleChangeHandler roleChangeHandler =
                    new DLedgerRoleChangeHandler(this, defaultMessageStore);
                ((DLedgerCommitLog) defaultMessageStore.getCommitLog())
                    .getdLedgerServer().getDLedgerLeaderElector().addRoleChangeHandler(roleChangeHandler);
            }

            this.brokerStats = new BrokerStats(defaultMessageStore);

            // Load store plugin
            // 加载存储插件
            MessageStorePluginContext context = new MessageStorePluginContext(
                messageStoreConfig, brokerStatsManager, messageArrivingListener, brokerConfig, configuration);
            this.messageStore = MessageStoreFactory.build(context, defaultMessageStore);
            this.messageStore.getDispatcherList().addFirst(new CommitLogDispatcherCalcBitMap(this.brokerConfig, this.consumerFilterManager));
            if (messageStoreConfig.isTimerWheelEnable()) {
                this.timerCheckpoint = new TimerCheckpoint(BrokerPathConfigHelper.getTimerCheckPath(messageStoreConfig.getStorePathRootDir()));
                TimerMetrics timerMetrics = new TimerMetrics(BrokerPathConfigHelper.getTimerMetricsPath(messageStoreConfig.getStorePathRootDir()));
                this.timerMessageStore = new TimerMessageStore(messageStore, messageStoreConfig, timerCheckpoint, timerMetrics, brokerStatsManager);
                this.timerMessageStore.registerEscapeBridgeHook(msg -> escapeBridge.putMessage(msg));
                this.messageStore.setTimerMessageStore(this.timerMessageStore);
            }
        } catch (Exception e) {
            result = false;
            LOG.error("BrokerController#initialize: unexpected error occurs", e);
        }
        return result;
    }

    /**
     * 初始化 Broker 控制器<br>
     * 按元数据, 存储, 服务恢复三个阶段串行执行, 保证失败快速返回
     *
     * @return true 表示 Broker 初始化完成
     * @throws CloneNotSupportedException remoting 配置克隆失败时抛出
     */
    public boolean initialize() throws CloneNotSupportedException {
        // Broker 启动时, 默认会监听 3 个端口: 10909、10911、10912
        // remotingServer     监听 10911 端口, 可以用于处理客户端的所有请求
        // fastRemotingServer 监听 10909 端口, 对应可以处理客户端除了拉取消息之外的所有请求, 所谓的 VIP 端口
        // haListenPort       监听 10912 端口, 用于 Broker 的主从同步, 即高可用服务
        // 客户端生产者发送消息是默认请求 fastRemotingServer, 即所谓的 VIP 通道, 但可以通过关闭 VIP 通道配置为使用 remotingServer
        // 消费者拉取消息只能请求 remotingServer

        // 加载配置文件
        boolean result = this.initializeMetadata();
        if (!result) {
            return false;
        }

        // 初始化消息存储对象 DefaultMessageStore
        result = this.initializeMessageStore();
        if (!result) {
            return false;
        }

        // 恢复并初始化核心服务
        return this.recoverAndInitService();
    }

    /**
     * 恢复并初始化核心服务<br>
     * 负责加载消息存储与插件状态, 构建 remoting 与线程池资源, 注册处理器与定时任务<br>
     * 同时完成事务, RPC Hook, 认证流水线与 TLS 热加载监听初始化
     *
     * @return true 表示核心服务恢复并初始化成功
     * @throws CloneNotSupportedException remoting 快速通道配置克隆失败时抛出
     */
    public boolean recoverAndInitService() throws CloneNotSupportedException {

        boolean result = true;

        if (this.brokerConfig.isEnableControllerMode()) {
            this.replicasManager = new ReplicasManager(this);
            this.replicasManager.setFenced(true);
        }

        if (messageStore != null) {
            registerMessageStoreHook();
            result = this.messageStore.load();
        }

        if (messageStoreConfig.isTimerWheelEnable()) {
            result = result && this.timerMessageStore.load();
        }

        //scheduleMessageService load after messageStore load success
        // 在 messageStore 加载成功后再加载 scheduleMessageService
        result = result && this.scheduleMessageService.load();

        for (BrokerAttachedPlugin brokerAttachedPlugin : brokerAttachedPlugins) {
            if (brokerAttachedPlugin != null) {
                result = result && brokerAttachedPlugin.load();
            }
        }

        this.brokerMetricsManager = new BrokerMetricsManager(this);

        if (result) {

            initializeRemotingServer(); // 初始化 remoting 服务端

            initializeResources();      // 初始化资源, 包括 remoting 服务端与线程执行器

            registerProcessor();        // 注册请求处理器

            initializeScheduledTasks(); // 初始化调度任务

            initialTransaction();       // 初始化事务能力

            initialRpcHooks();          // 初始化 RPC Hook

            initialRequestPipeline();   // 初始化请求处理流水线

            if (TlsSystemConfig.tlsMode != TlsMode.DISABLED) {
                // Register a listener to reload SslContext
                // 注册监听器用于重新加载 SslContext
                try {
                    fileWatchService = new FileWatchService(
                        new String[] {
                            TlsSystemConfig.tlsServerCertPath,
                            TlsSystemConfig.tlsServerKeyPath,
                            TlsSystemConfig.tlsServerTrustCertPath
                        },
                        new FileWatchService.Listener() {
                            boolean certChanged, keyChanged = false;

                            @Override
                            public void onChanged(String path) {
                                if (path.equals(TlsSystemConfig.tlsServerTrustCertPath)) {
                                    LOG.info("The trust certificate changed, reload the ssl context");
                                    reloadServerSslContext();
                                }
                                if (path.equals(TlsSystemConfig.tlsServerCertPath)) {
                                    certChanged = true;
                                }
                                if (path.equals(TlsSystemConfig.tlsServerKeyPath)) {
                                    keyChanged = true;
                                }
                                if (certChanged && keyChanged) {
                                    LOG.info("The certificate and private key changed, reload the ssl context");
                                    certChanged = keyChanged = false;
                                    reloadServerSslContext();
                                }
                            }

                            private void reloadServerSslContext() {
                                for (Map.Entry<String, RemotingServer> entry : remotingServerMap.entrySet()) {
                                    RemotingServer remotingServer = entry.getValue();
                                    if (remotingServer instanceof NettyRemotingServer) {
                                        ((NettyRemotingServer) remotingServer).loadSslContext();
                                    }
                                }
                            }
                        });
                } catch (Exception e) {
                    result = false;
                    LOG.warn("FileWatchService created error, can't load the certificate dynamically");
                }
            }
        }

        return result;
    }

    /**
     * 注册消息存储钩子<br>
     * 在写入前挂载基础校验, 批量消息校验, 定时消息转换等钩子<br>
     * 同时设置 SendMessageBack 回调, 用于消费失败回退消息场景
     */
    public void registerMessageStoreHook() {
        List<PutMessageHook> putMessageHookList = messageStore.getPutMessageHookList();

        putMessageHookList.add(new PutMessageHook() {
            @Override
            public String hookName() {
                return "checkBeforePutMessage";
            }

            @Override
            public PutMessageResult executeBeforePutMessage(MessageExt msg) {
                return HookUtils.checkBeforePutMessage(BrokerController.this, msg);
            }
        });

        putMessageHookList.add(new PutMessageHook() {
            @Override
            public String hookName() {
                return "innerBatchChecker";
            }

            @Override
            public PutMessageResult executeBeforePutMessage(MessageExt msg) {
                if (msg instanceof MessageExtBrokerInner) {
                    return HookUtils.checkInnerBatch(BrokerController.this, msg);
                }
                return null;
            }
        });

        putMessageHookList.add(new PutMessageHook() {
            @Override
            public String hookName() {
                return "handleScheduleMessage";
            }

            @Override
            public PutMessageResult executeBeforePutMessage(MessageExt msg) {
                if (msg instanceof MessageExtBrokerInner) {
                    return HookUtils.handleScheduleMessage(BrokerController.this, (MessageExtBrokerInner) msg);
                }
                return null;
            }
        });

        SendMessageBackHook sendMessageBackHook = new SendMessageBackHook() {
            @Override
            public boolean executeSendMessageBack(List<MessageExt> msgList, String brokerName, String brokerAddr) {
                return HookUtils.sendMessageBack(BrokerController.this, msgList, brokerName, brokerAddr);
            }
        };

        if (messageStore != null) {
            messageStore.setSendMessageBackHook(sendMessageBackHook);
        }
    }

    /**
     * 初始化 broker 事务消息基础设施, 包含事务消息服务, 回查监听器和指标刷新组件<br>
     * 初始化顺序: 先加载事务服务, 再加载回查监听器, 然后注入 BrokerController 并创建后台服务<br>
     * SPI 未提供实现时回退默认事务实现, 保证事务半消息和回查链路可用<br>
     * 方法返回后事务指标刷新线程已启动, 后续事务流程可直接复用该能力
     */
    private void initialTransaction() {
        // 优先通过 SPI 加载事务消息服务, 允许业务侧替换事务存储或处理逻辑
        this.transactionalMessageService = ServiceProvider.loadClass(TransactionalMessageService.class);
        if (null == this.transactionalMessageService) {
            // 未配置 SPI 实现时回退默认事务服务, 默认实现通过 bridge 访问消息存储
            this.transactionalMessageService = new TransactionalMessageServiceImpl(
                new TransactionalMessageBridge(this, this.getMessageStore()));
            LOG.warn("Load default transaction message hook service: {}",
                TransactionalMessageServiceImpl.class.getSimpleName());
        }
        // 加载事务回查监听器, 用于处理长时间未决事务消息的状态回查
        this.transactionalMessageCheckListener = ServiceProvider.loadClass(
            AbstractTransactionalMessageCheckListener.class);
        if (null == this.transactionalMessageCheckListener) {
            // 未提供自定义监听器时使用默认监听器, 保证回查链路可运行
            this.transactionalMessageCheckListener = new DefaultTransactionalMessageCheckListener();
            LOG.warn("Load default discard message hook service: {}",
                DefaultTransactionalMessageCheckListener.class.getSimpleName());
        }
        // 注入 broker 上下文, 使回查监听器可访问 broker 运行时组件
        this.transactionalMessageCheckListener.setBrokerController(this);
        // 构建事务状态检查服务, 由后续启动阶段负责调度执行
        this.transactionalMessageCheckService = new TransactionalMessageCheckService(this);
        // 构建并启动事务指标刷新服务, 定期落盘事务统计数据
        this.transactionMetricsFlushService = new TransactionMetricsFlushService(this);
        this.transactionMetricsFlushService.start();

    }

    /**
     * 初始化 RPC Hook 组件, 用于在 remoting 请求前后执行自定义逻辑<br>
     * 处理流程: 从 SPI 拉取 RPCHook 扩展, 判空后逐个注册到服务端链路<br>
     * 未加载到扩展时直接返回, 保持 broker 默认请求处理路径
     */
    private void initialRpcHooks() {

        // 从 SPI 发现 RPCHook 扩展实现, 支持安全审计和链路追踪等增强逻辑
        List<RPCHook> rpcHooks = ServiceProvider.load(RPCHook.class);
        // 无扩展实现时直接退出, 避免空集合遍历
        if (rpcHooks == null || rpcHooks.isEmpty()) {
            return;
        }
        // 逐个注册到服务端 RPCHook 链路, 使所有请求统一经过 hook 回调
        for (RPCHook rpcHook : rpcHooks) {
            this.registerServerRPCHook(rpcHook);
        }
    }

    /**
     * 初始化请求处理流水线<br>
     * 在启用认证配置时构建鉴权与认证管道, 统一拦截后续请求
     */
    private void initialRequestPipeline() {
        if (this.authConfig == null) {
            return;
        }
        RequestPipeline pipeline = (ctx, request) -> {
        };
        // add pipeline
        // the last pipe add will execute at the first
        // 添加 pipeline
        // 最后添加的管道最先执行
        try {
            pipeline = pipeline.pipe(new AuthorizationPipeline(authConfig))
                .pipe(new AuthenticationPipeline(authConfig));
            this.setRequestPipeline(pipeline);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 注册请求处理器<br>
     * 将各类 RequestCode 绑定到对应处理器与线程池<br>
     * 同步完成 TCP 通道与 Fast 通道注册, 并初始化请求头映射表
     */
    public void registerProcessor() {
        RemotingServer remotingServer = remotingServerMap.get(TCP_REMOTING_SERVER);
        RemotingServer fastRemotingServer = remotingServerMap.get(FAST_REMOTING_SERVER);

        // SendMessageProcessor 发送消息处理器
        sendMessageProcessor.registerSendMessageHook(sendMessageHookList);
        sendMessageProcessor.registerConsumeMessageHook(consumeMessageHookList);

        remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendMessageProcessor, this.sendMessageExecutor);
        remotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendMessageProcessor, this.sendMessageExecutor);
        remotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, sendMessageProcessor, this.sendMessageExecutor);
        remotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendMessageProcessor, this.sendMessageExecutor);
        remotingServer.registerProcessor(RequestCode.RECALL_MESSAGE, recallMessageProcessor, this.sendMessageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendMessageProcessor, this.sendMessageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendMessageProcessor, this.sendMessageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, sendMessageProcessor, this.sendMessageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendMessageProcessor, this.sendMessageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.RECALL_MESSAGE, recallMessageProcessor, this.sendMessageExecutor);

        // PullMessageProcessor 拉消息处理器
        remotingServer.registerProcessor(RequestCode.PULL_MESSAGE, this.pullMessageProcessor, this.pullMessageExecutor);
        remotingServer.registerProcessor(RequestCode.LITE_PULL_MESSAGE, this.pullMessageProcessor, this.litePullMessageExecutor);
        this.pullMessageProcessor.registerConsumeMessageHook(consumeMessageHookList);
        // PeekMessageProcessor 窥探消息处理器
        remotingServer.registerProcessor(RequestCode.PEEK_MESSAGE, this.peekMessageProcessor, this.pullMessageExecutor);
        // PopMessageProcessor POP 消息处理器
        remotingServer.registerProcessor(RequestCode.POP_MESSAGE, this.popMessageProcessor, this.pullMessageExecutor);
        // fastRemotingServer 未注册 PullMessageProcessor, PeekMessageProcessor, PopMessageProcessor

        // AckMessageProcessor ACK 消息处理器
        remotingServer.registerProcessor(RequestCode.ACK_MESSAGE, this.ackMessageProcessor, this.ackMessageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.ACK_MESSAGE, this.ackMessageProcessor, this.ackMessageExecutor);

        remotingServer.registerProcessor(RequestCode.BATCH_ACK_MESSAGE, this.ackMessageProcessor, this.ackMessageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.BATCH_ACK_MESSAGE, this.ackMessageProcessor, this.ackMessageExecutor);
        // ChangeInvisibleTimeProcessor 修改不可见时间处理器
        remotingServer.registerProcessor(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, this.changeInvisibleTimeProcessor, this.ackMessageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, this.changeInvisibleTimeProcessor, this.ackMessageExecutor);
        // notificationProcessor 通知处理器
        remotingServer.registerProcessor(RequestCode.NOTIFICATION, this.notificationProcessor, this.pullMessageExecutor);

        // pollingInfoProcessor 轮询信息处理器
        remotingServer.registerProcessor(RequestCode.POLLING_INFO, this.pollingInfoProcessor, this.pullMessageExecutor);

        // ReplyMessageProcessor 回复消息处理器
        replyMessageProcessor.registerSendMessageHook(sendMessageHookList);

        remotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE, replyMessageProcessor, replyMessageExecutor);
        remotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE_V2, replyMessageProcessor, replyMessageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE, replyMessageProcessor, replyMessageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE_V2, replyMessageProcessor, replyMessageExecutor);

        // QueryMessageProcessor 查询消息处理器
        NettyRequestProcessor queryProcessor = new QueryMessageProcessor(this);
        remotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, this.queryMessageExecutor);
        remotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, this.queryMessageExecutor);

        fastRemotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, this.queryMessageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, this.queryMessageExecutor);

        // ClientManageProcessor 客户端管理处理器
        remotingServer.registerProcessor(RequestCode.HEART_BEAT, clientManageProcessor, this.heartbeatExecutor);
        remotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientManageProcessor, this.clientManageExecutor);
        remotingServer.registerProcessor(RequestCode.CHECK_CLIENT_CONFIG, clientManageProcessor, this.clientManageExecutor);

        fastRemotingServer.registerProcessor(RequestCode.HEART_BEAT, clientManageProcessor, this.heartbeatExecutor);
        fastRemotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientManageProcessor, this.clientManageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.CHECK_CLIENT_CONFIG, clientManageProcessor, this.clientManageExecutor);

        // ConsumerManageProcessor 消费者管理处理器
        ConsumerManageProcessor consumerManageProcessor = new ConsumerManageProcessor(this);
        remotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerManageProcessor, this.consumerManageExecutor);
        remotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);
        remotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);

        fastRemotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerManageProcessor, this.consumerManageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);

        // QueryAssignmentProcessor 查询分配处理器
        remotingServer.registerProcessor(RequestCode.QUERY_ASSIGNMENT, queryAssignmentProcessor, loadBalanceExecutor);
        fastRemotingServer.registerProcessor(RequestCode.QUERY_ASSIGNMENT, queryAssignmentProcessor, loadBalanceExecutor);
        remotingServer.registerProcessor(RequestCode.SET_MESSAGE_REQUEST_MODE, queryAssignmentProcessor, loadBalanceExecutor);
        fastRemotingServer.registerProcessor(RequestCode.SET_MESSAGE_REQUEST_MODE, queryAssignmentProcessor, loadBalanceExecutor);

        // EndTransactionProcessor 结束事务处理器
        remotingServer.registerProcessor(RequestCode.END_TRANSACTION, endTransactionProcessor, this.endTransactionExecutor);
        fastRemotingServer.registerProcessor(RequestCode.END_TRANSACTION, endTransactionProcessor, this.endTransactionExecutor);

        // Default 默认处理器
        AdminBrokerProcessor adminProcessor = new AdminBrokerProcessor(this);
        remotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);
        fastRemotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);

        // Initialize the mapping of request codes to request headers. 初始化请求码与请求头映射
        RequestHeaderRegistry.getInstance().initialize();
    }

    public BrokerStats getBrokerStats() {
        return brokerStats;
    }

    public void setBrokerStats(BrokerStats brokerStats) {
        this.brokerStats = brokerStats;
    }

    /**
     * 执行 Broker 保护检查
     */
    public void protectBroker() {
        if (this.brokerConfig.isDisableConsumeIfConsumerReadSlowly()) {
            for (Map.Entry<String, MomentStatsItem> next : this.brokerStatsManager.getMomentStatsItemSetFallSize().getStatsItemTable().entrySet()) {
                final long fallBehindBytes = next.getValue().getValue().get();
                if (fallBehindBytes > this.brokerConfig.getConsumerFallbehindThreshold()) {
                    final String[] split = next.getValue().getStatsKey().split("@");
                    final String group = split[2];
                    LOG_PROTECTION.info("[PROTECT_BROKER] the consumer[{}] consume slowly, {} bytes, disable it", group, fallBehindBytes);
                    this.subscriptionGroupManager.disableConsume(group);
                }
            }
        }
    }

    /**
     * 计算队列头任务等待时长
     */
    public long headSlowTimeMills(BlockingQueue<Runnable> q) {
        long slowTimeMills = 0;
        final Runnable peek = q.peek();
        if (peek != null) {
            RequestTask rt = BrokerFastFailure.castRunnable(peek);
            slowTimeMills = rt == null ? 0 : this.messageStore.now() - rt.getCreateTimestamp();
        }

        if (slowTimeMills < 0) {
            slowTimeMills = 0;
        }

        return slowTimeMills;
    }

    /**
     * 计算发送队列头等待时长
     */
    public long headSlowTimeMills4SendThreadPoolQueue() {
        return this.headSlowTimeMills(this.sendThreadPoolQueue);
    }

    /**
     * 计算拉取队列头等待时长
     */
    public long headSlowTimeMills4PullThreadPoolQueue() {
        return this.headSlowTimeMills(this.pullThreadPoolQueue);
    }

    /**
     * 计算轻量拉取队列头等待时长
     */
    public long headSlowTimeMills4LitePullThreadPoolQueue() {
        return this.headSlowTimeMills(this.litePullThreadPoolQueue);
    }

    /**
     * 计算查询队列头等待时长
     */
    public long headSlowTimeMills4QueryThreadPoolQueue() {
        return this.headSlowTimeMills(this.queryThreadPoolQueue);
    }

    /**
     * 计算 ACK 队列头等待时长
     */
    public long headSlowTimeMills4AckThreadPoolQueue() {
        return this.headSlowTimeMills(this.ackThreadPoolQueue);
    }

    /**
     * 计算结束事务队列头等待时长
     */
    public long headSlowTimeMills4EndTransactionThreadPoolQueue() {
        return this.headSlowTimeMills(this.endTransactionThreadPoolQueue);
    }

    /**
     * 计算客户端管理队列头等待时长
     */
    public long headSlowTimeMills4ClientManagerThreadPoolQueue() {
        return this.headSlowTimeMills(this.clientManagerThreadPoolQueue);
    }

    /**
     * 计算心跳队列头等待时长
     */
    public long headSlowTimeMills4HeartbeatThreadPoolQueue() {
        return this.headSlowTimeMills(this.heartbeatThreadPoolQueue);
    }

    /**
     * 计算管理队列头等待时长
     */
    public long headSlowTimeMills4AdminBrokerThreadPoolQueue() {
        return this.headSlowTimeMills(this.adminBrokerThreadPoolQueue);
    }

    /**
     * 打印线程池水位信息
     */
    public void printWaterMark() {
        logWaterMarkQueueInfo("Send", this.sendThreadPoolQueue, this::headSlowTimeMills4SendThreadPoolQueue);
        logWaterMarkQueueInfo("Pull", this.pullThreadPoolQueue, this::headSlowTimeMills4PullThreadPoolQueue);
        logWaterMarkQueueInfo("Query", this.queryThreadPoolQueue, this::headSlowTimeMills4QueryThreadPoolQueue);
        logWaterMarkQueueInfo("Lite Pull", this.litePullThreadPoolQueue, this::headSlowTimeMills4LitePullThreadPoolQueue);
        logWaterMarkQueueInfo("Transaction", this.endTransactionThreadPoolQueue, this::headSlowTimeMills4EndTransactionThreadPoolQueue);
        logWaterMarkQueueInfo("ClientManager", this.clientManagerThreadPoolQueue, this::headSlowTimeMills4ClientManagerThreadPoolQueue);
        logWaterMarkQueueInfo("Heartbeat", this.heartbeatThreadPoolQueue, this::headSlowTimeMills4HeartbeatThreadPoolQueue);
        logWaterMarkQueueInfo("Ack", this.ackThreadPoolQueue, this::headSlowTimeMills4AckThreadPoolQueue);
        logWaterMarkQueueInfo("Admin", this.adminBrokerThreadPoolQueue, this::headSlowTimeMills4AdminBrokerThreadPoolQueue);
    }

    /**
     * 记录队列水位信息
     */
    private void logWaterMarkQueueInfo(String queueName, BlockingQueue<?> queue, Supplier<Long> slowTimeSupplier) {
        LOG_WATER_MARK.info("[WATERMARK] {} Queue Size: {} SlowTimeMills: {}", queueName, queue.size(), slowTimeSupplier.get());
    }

    public MessageStore getMessageStore() {
        return messageStore;
    }

    public void setMessageStore(MessageStore messageStore) {
        this.messageStore = messageStore;
    }

    /**
     * 打印主从差值
     */
    protected void printMasterAndSlaveDiff() {
        if (messageStore.getHaService() != null && messageStore.getHaService().getConnectionCount().get() > 0) {
            long diff = this.messageStore.slaveFallBehindMuch();
            LOG.info("CommitLog: slave fall behind master {}bytes", diff);
        }
    }

    public Broker2Client getBroker2Client() {
        return broker2Client;
    }

    public ConsumerManager getConsumerManager() {
        return consumerManager;
    }

    public ConsumerFilterManager getConsumerFilterManager() {
        return consumerFilterManager;
    }

    public ConsumerOrderInfoManager getConsumerOrderInfoManager() {
        return consumerOrderInfoManager;
    }

    public PopInflightMessageCounter getPopInflightMessageCounter() {
        return popInflightMessageCounter;
    }

    public PopConsumerService getPopConsumerService() {
        return popConsumerService;
    }

    public ConsumerOffsetManager getConsumerOffsetManager() {
        return consumerOffsetManager;
    }

    public void setConsumerOffsetManager(ConsumerOffsetManager consumerOffsetManager) {
        this.consumerOffsetManager = consumerOffsetManager;
    }


    public BroadcastOffsetManager getBroadcastOffsetManager() {
        return broadcastOffsetManager;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public ProducerManager getProducerManager() {
        return producerManager;
    }

    public PullMessageProcessor getPullMessageProcessor() {
        return pullMessageProcessor;
    }

    public PullRequestHoldService getPullRequestHoldService() {
        return pullRequestHoldService;
    }

    public void setSubscriptionGroupManager(SubscriptionGroupManager subscriptionGroupManager) {
        this.subscriptionGroupManager = subscriptionGroupManager;
    }

    public SubscriptionGroupManager getSubscriptionGroupManager() {
        return subscriptionGroupManager;
    }

    public PopMessageProcessor getPopMessageProcessor() {
        return popMessageProcessor;
    }

    public NotificationProcessor getNotificationProcessor() {
        return notificationProcessor;
    }

    public TimerMessageStore getTimerMessageStore() {
        return timerMessageStore;
    }

    public void setTimerMessageStore(TimerMessageStore timerMessageStore) {
        this.timerMessageStore = timerMessageStore;
    }

    public AckMessageProcessor getAckMessageProcessor() {
        return ackMessageProcessor;
    }

    public ChangeInvisibleTimeProcessor getChangeInvisibleTimeProcessor() {
        return changeInvisibleTimeProcessor;
    }

    /**
     * 关闭基础服务<br>
     * 先下线路由再按依赖顺序关闭网络, 存储, 线程池与后台服务<br>
     * 关闭过程中持久化关键元数据, 保证下次启动可恢复
     */
    protected void shutdownBasicService() {

        shutdown = true;

        this.unregisterBrokerAll();

        if (this.shutdownHook != null) {
            this.shutdownHook.beforeShutdown(this);
        }

        for (Map.Entry<String, RemotingServer> entry : remotingServerMap.entrySet()) {
            RemotingServer remotingServer = entry.getValue();
            if (remotingServer != null) {
                remotingServer.shutdown();
            }
        }

        if (this.brokerMetricsManager != null) {
            this.brokerMetricsManager.shutdown();
        }

        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.shutdown();
        }

        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.shutdown();
        }

        if (this.pullRequestHoldService != null) {
            this.pullRequestHoldService.shutdown();
        }

        if (this.popConsumerService != null) {
            this.popConsumerService.shutdown();
        }

        if (this.popMessageProcessor.getPopLongPollingService() != null) {
            this.popMessageProcessor.getPopLongPollingService().shutdown();
        }

        if (this.popMessageProcessor.getQueueLockManager() != null) {
            this.popMessageProcessor.getQueueLockManager().shutdown();
        }

        if (this.popMessageProcessor.getPopBufferMergeService() != null) {
            this.popMessageProcessor.getPopBufferMergeService().shutdown();
        }

        if (this.ackMessageProcessor.getPopReviveServices() != null) {
            this.ackMessageProcessor.shutdownPopReviveService();
        }

        if (this.transactionalMessageService != null) {
            this.transactionalMessageService.close();
        }

        if (this.notificationProcessor != null) {
            this.notificationProcessor.getPopLongPollingService().shutdown();
        }

        if (this.consumerIdsChangeListener != null) {
            this.consumerIdsChangeListener.shutdown();
        }

        if (this.topicQueueMappingCleanService != null) {
            this.topicQueueMappingCleanService.shutdown();
        }
        //it is better to make sure the timerMessageStore shutdown firstly
        // 最好优先确保 timerMessageStore 先关闭
        if (this.timerMessageStore != null) {
            this.timerMessageStore.shutdown();
        }
        if (this.fileWatchService != null) {
            this.fileWatchService.shutdown();
        }

        if (this.broadcastOffsetManager != null) {
            this.broadcastOffsetManager.shutdown();
        }

        if (this.messageStore != null) {
            this.messageStore.shutdown();
        }

        if (this.replicasManager != null) {
            this.replicasManager.shutdown();
        }

        shutdownScheduledExecutorService(this.scheduledExecutorService);

        if (this.sendMessageExecutor != null) {
            this.sendMessageExecutor.shutdown();
        }

        if (this.litePullMessageExecutor != null) {
            this.litePullMessageExecutor.shutdown();
        }

        if (this.pullMessageExecutor != null) {
            this.pullMessageExecutor.shutdown();
        }

        if (this.replyMessageExecutor != null) {
            this.replyMessageExecutor.shutdown();
        }

        if (this.putMessageFutureExecutor != null) {
            this.putMessageFutureExecutor.shutdown();
        }

        if (this.ackMessageExecutor != null) {
            this.ackMessageExecutor.shutdown();
        }

        if (this.adminBrokerExecutor != null) {
            this.adminBrokerExecutor.shutdown();
        }

        if (this.brokerFastFailure != null) {
            this.brokerFastFailure.shutdown();
        }

        if (this.consumerFilterManager != null) {
            this.consumerFilterManager.persist();
        }

        if (this.scheduleMessageService != null) {
            this.scheduleMessageService.persist();
            this.scheduleMessageService.shutdown();
        }

        if (this.clientManageExecutor != null) {
            this.clientManageExecutor.shutdown();
        }

        if (this.queryMessageExecutor != null) {
            this.queryMessageExecutor.shutdown();
        }

        if (this.heartbeatExecutor != null) {
            this.heartbeatExecutor.shutdown();
        }

        if (this.consumerManageExecutor != null) {
            this.consumerManageExecutor.shutdown();
        }

        if (this.transactionalMessageCheckService != null) {
            this.transactionalMessageCheckService.shutdown(false);
        }

        if (this.endTransactionExecutor != null) {
            this.endTransactionExecutor.shutdown();
        }

        if (this.transactionMetricsFlushService != null) {
            this.transactionMetricsFlushService.shutdown();
        }

        if (this.escapeBridge != null) {
            this.escapeBridge.shutdown();
        }

        if (this.topicRouteInfoManager != null) {
            this.topicRouteInfoManager.shutdown();
        }

        if (this.brokerPreOnlineService != null && !this.brokerPreOnlineService.isStopped()) {
            this.brokerPreOnlineService.shutdown();
        }

        if (this.coldDataPullRequestHoldService != null) {
            this.coldDataPullRequestHoldService.shutdown();
        }

        if (this.coldDataCgCtrService != null) {
            this.coldDataCgCtrService.shutdown();
        }

        shutdownScheduledExecutorService(this.syncBrokerMemberGroupExecutorService);
        shutdownScheduledExecutorService(this.brokerHeartbeatExecutorService);

        if (this.topicConfigManager != null) {
            this.topicConfigManager.persist();
            this.topicConfigManager.stop();
        }

        if (this.subscriptionGroupManager != null) {
            this.subscriptionGroupManager.persist();
            this.subscriptionGroupManager.stop();
        }

        if (this.consumerOffsetManager != null) {
            this.consumerOffsetManager.persist();
            this.consumerOffsetManager.stop();
        }

        if (this.consumerOrderInfoManager != null) {
            this.consumerOrderInfoManager.persist();
            this.consumerOrderInfoManager.shutdown();
        }

        if (this.configStorage != null) {
            this.configStorage.shutdown();
        }

        if (this.authenticationMetadataManager != null) {
            this.authenticationMetadataManager.shutdown();
        }

        if (this.authorizationMetadataManager != null) {
            this.authorizationMetadataManager.shutdown();
        }

        for (BrokerAttachedPlugin brokerAttachedPlugin : brokerAttachedPlugins) {
            if (brokerAttachedPlugin != null) {
                brokerAttachedPlugin.shutdown();
            }
        }
    }

    /**
     * 关闭 Broker 控制器
     */
    public void shutdown() {

        shutdownBasicService();

        for (ScheduledFuture<?> scheduledFuture : scheduledFutures) {
            scheduledFuture.cancel(true);
        }

        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.shutdown();
        }
    }

    /**
     * 关闭定时执行器
     */
    protected void shutdownScheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
        if (scheduledExecutorService == null) {
            return;
        }
        scheduledExecutorService.shutdown();
        try {
            scheduledExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignore) {
            BrokerController.LOG.warn("shutdown ScheduledExecutorService was Interrupted!  ", ignore);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 下线全部 broker 注册信息
     */
    protected void unregisterBrokerAll() {
        this.brokerOuterAPI.unregisterBrokerAll(
            this.brokerConfig.getBrokerClusterName(),
            this.getBrokerAddr(),
            this.brokerConfig.getBrokerName(),
            this.brokerConfig.getBrokerId());
    }

    public String getBrokerAddr() {
        return this.brokerConfig.getBrokerIP1() + ":" + this.nettyServerConfig.getListenPort();
    }

    /**
     * 启动基础服务<br>
     * 按依赖顺序启动存储, remoting, 插件与后台任务, 并刷新 storeHost 信息
     *
     * @throws Exception 任一核心组件启动失败时抛出
     */
    protected void startBasicService() throws Exception {

        if (this.messageStore != null) {
            this.messageStore.start();
        }

        if (this.timerMessageStore != null) {
            this.timerMessageStore.start();
        }

        if (this.replicasManager != null) {
            this.replicasManager.start();
        }

        if (remotingServerStartLatch != null) {
            remotingServerStartLatch.await();
        }

        for (Map.Entry<String, RemotingServer> entry : remotingServerMap.entrySet()) {
            RemotingServer remotingServer = entry.getValue();
            if (remotingServer != null) {
                remotingServer.start();

                if (TCP_REMOTING_SERVER.equals(entry.getKey())) {
                    // In test scenarios where it is up to OS to pick up an available port, set the listening port back to config
                    // 在测试场景中由 OS 分配可用端口时, 需要回写监听端口到配置
                    if (null != nettyServerConfig && 0 == nettyServerConfig.getListenPort()) {
                        nettyServerConfig.setListenPort(remotingServer.localListenPort());
                    }
                }
            }
        }

        this.storeHost = new InetSocketAddress(this.getBrokerConfig().getBrokerIP1(), this.getNettyServerConfig().getListenPort());

        for (BrokerAttachedPlugin brokerAttachedPlugin : brokerAttachedPlugins) {
            if (brokerAttachedPlugin != null) {
                brokerAttachedPlugin.start();
            }
        }

        if (this.popMessageProcessor != null) {
            this.popMessageProcessor.getPopLongPollingService().start();
            if (brokerConfig.isPopConsumerFSServiceInit()) {
                this.popMessageProcessor.getPopBufferMergeService().start();
            }
            this.popMessageProcessor.getQueueLockManager().start();
        }

        if (this.ackMessageProcessor != null) {
            if (brokerConfig.isPopConsumerFSServiceInit()) {
                this.ackMessageProcessor.startPopReviveService();
            }
        }

        if (this.notificationProcessor != null) {
            this.notificationProcessor.getPopLongPollingService().start();
        }

        if (this.popConsumerService != null) {
            this.popConsumerService.start();
        }

        if (this.topicQueueMappingCleanService != null) {
            this.topicQueueMappingCleanService.start();
        }

        if (this.fileWatchService != null) {
            this.fileWatchService.start();
        }

        if (this.pullRequestHoldService != null) {
            this.pullRequestHoldService.start();
        }

        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.start();
        }

        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.start();
        }

        if (this.brokerFastFailure != null) {
            this.brokerFastFailure.start();
        }

        if (this.broadcastOffsetManager != null) {
            this.broadcastOffsetManager.start();
        }

        if (this.escapeBridge != null) {
            this.escapeBridge.start();
        }

        if (this.topicRouteInfoManager != null) {
            this.topicRouteInfoManager.start();
        }

        if (this.brokerPreOnlineService != null) {
            this.brokerPreOnlineService.start();
        }

        if (this.coldDataPullRequestHoldService != null) {
            this.coldDataPullRequestHoldService.start();
        }

        if (this.coldDataCgCtrService != null) {
            this.coldDataCgCtrService.start();
        }
    }

    /**
     * 启动 Broker 控制器<br>
     * 启动基础组件后建立周期注册, 心跳, 成员同步与元数据刷新任务<br>
     * 在主从切换模式下按隔离状态决定是否立即对外注册
     *
     * @throws Exception 基础服务启动失败时抛出
     */
    public void start() throws Exception {

        // 计算可对外注册时间点, 启动保护期内先不向 NameServer 暴露路由
        this.shouldStartTime = System.currentTimeMillis() + messageStoreConfig.getDisappearTimeAfterStart();

        // 多副本 slave acting master 模式下先进入隔离态, 等待成员组选主完成
        if (messageStoreConfig.getTotalReplicas() > 1 && this.brokerConfig.isEnableSlaveActingMaster()) {
            isIsolated = true;
        }

        // 后续注册与心跳都依赖 brokerOuterAPI, 因此优先启动
        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.start();
        }

        // 启动存储, remoting, 线程池等基础能力
        startBasicService();

        // 非隔离且非 DLedger 与非双写复制场景下, 启动时立即强制注册一次
        if (!isIsolated && !this.messageStoreConfig.isEnableDLegerCommitLog() && !this.messageStoreConfig.isDuplicationEnable()) {
            changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == MixAll.MASTER_ID);
            this.registerBrokerAll(true, false, true);
        }

        /*
         * 周期注册任务
         * 启动保护期内延后注册, 隔离状态下跳过注册, 否则按周期刷新路由数据
         */
        scheduledFutures.add(this.scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
            @Override
            public void run0() {
                try {
                    // 启动保护期内不注册, 避免冷启动阶段路由抖动
                    if (System.currentTimeMillis() < shouldStartTime) {
                        BrokerController.LOG.info("Register to namesrv after {}", shouldStartTime);
                        return;
                    }
                    // 隔离态 broker 不对外注册, 等待角色收敛后再暴露路由
                    if (isIsolated) {
                        BrokerController.LOG.info("Skip register for broker is isolated");
                        return;
                    }
                    // 执行周期注册, 是否强制注册由配置控制
                    BrokerController.this.registerBrokerAll(true, false, brokerConfig.isForceRegister());
                } catch (Throwable e) {
                    BrokerController.LOG.error("registerBrokerAll Exception", e);
                }
            }
        }, 1000 * 10, Math.max(10000, Math.min(brokerConfig.getRegisterNameServerPeriod(), 60000)), TimeUnit.MILLISECONDS));

        /*
         * slave acting master 模式下需要并行维护
         * 1. 发送心跳维持在线状态
         * 2. 同步成员组驱动最小 broker 切换
         */
        if (this.brokerConfig.isEnableSlaveActingMaster()) {
            scheduleSendHeartbeat();

            scheduledFutures.add(this.syncBrokerMemberGroupExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
                @Override
                public void run0() {
                    try {
                        // 同步成员信息后将触发最小 broker 更新与角色切换逻辑
                        BrokerController.this.syncBrokerMemberGroup();
                    } catch (Throwable e) {
                        BrokerController.LOG.error("sync BrokerMemberGroup error. ", e);
                    }
                }
            }, 1000, this.brokerConfig.getSyncBrokerMemberGroupPeriod(), TimeUnit.MILLISECONDS));
        }

        // controller 模式同样需要独立心跳任务, 用于向 controller 报活
        if (this.brokerConfig.isEnableControllerMode()) {
            scheduleSendHeartbeat();
        }

        // 跳过预上线时直接启动特殊服务并完成一次注册
        if (brokerConfig.isSkipPreOnline()) {
            startServiceWithoutCondition();
        }

        // 周期刷新元数据, 降低鉴权与路由缓存陈旧风险
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    BrokerController.this.brokerOuterAPI.refreshMetadata();
                } catch (Exception e) {
                    LOG.error("ScheduledTask refresh metadata exception", e);
                }
            }
        }, 10, 5, TimeUnit.SECONDS);
    }

    /**
     * 调度心跳发送任务<br>
     * 按固定间隔发送 broker 心跳, 隔离状态下自动跳过发送
     */
    protected void scheduleSendHeartbeat() {
        scheduledFutures.add(this.brokerHeartbeatExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
            @Override
            public void run0() {
                if (isIsolated) {
                    return;
                }
                try {
                    BrokerController.this.sendHeartbeat();
                } catch (Exception e) {
                    BrokerController.LOG.error("sendHeartbeat Exception", e);
                }

            }
        }, 1000, brokerConfig.getBrokerHeartbeatInterval(), TimeUnit.MILLISECONDS));
    }

    /**
     * 注册单主题路由
     */
    public synchronized void registerSingleTopicAll(final TopicConfig topicConfig) {
        TopicConfig tmpTopic = topicConfig;
        if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
            || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {
            // Copy the topic config and modify the perm
            // 复制主题配置并修改权限
            tmpTopic = new TopicConfig(topicConfig);
            tmpTopic.setPerm(topicConfig.getPerm() & this.brokerConfig.getBrokerPermission());
        }
        this.brokerOuterAPI.registerSingleTopicAll(this.brokerConfig.getBrokerName(), tmpTopic, 3000);
    }

    /**
     * 增量注册 broker 数据
     */
    public synchronized void registerIncrementBrokerData(TopicConfig topicConfig, DataVersion dataVersion) {
        this.registerIncrementBrokerData(Collections.singletonList(topicConfig), dataVersion);
    }

    /**
     * 增量注册 broker 数据
     */
    public synchronized void registerIncrementBrokerData(List<TopicConfig> topicConfigList, DataVersion dataVersion) {
        if (topicConfigList == null || topicConfigList.isEmpty()) {
            return;
        }

        TopicConfigAndMappingSerializeWrapper topicConfigSerializeWrapper = new TopicConfigAndMappingSerializeWrapper();
        topicConfigSerializeWrapper.setDataVersion(dataVersion);

        ConcurrentMap<String, TopicConfig> topicConfigTable = topicConfigList.stream()
            .map(topicConfig -> {
                TopicConfig registerTopicConfig;
                if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
                    || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {
                    registerTopicConfig =
                        new TopicConfig(topicConfig.getTopicName(),
                            topicConfig.getReadQueueNums(),
                            topicConfig.getWriteQueueNums(),
                                topicConfig.getPerm()
                                        & this.brokerConfig.getBrokerPermission(), topicConfig.getTopicSysFlag());
                } else {
                    registerTopicConfig = new TopicConfig(topicConfig);
                }
                return registerTopicConfig;
            })
            .collect(Collectors.toConcurrentMap(TopicConfig::getTopicName, Function.identity()));
        topicConfigSerializeWrapper.setTopicConfigTable(topicConfigTable);

        Map<String, TopicQueueMappingInfo> topicQueueMappingInfoMap = topicConfigList.stream()
            .map(TopicConfig::getTopicName)
            .map(topicName -> Optional.ofNullable(this.topicQueueMappingManager.getTopicQueueMapping(topicName))
                .map(info -> new AbstractMap.SimpleImmutableEntry<>(topicName, TopicQueueMappingDetail.cloneAsMappingInfo(info)))
                .orElse(null))
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (!topicQueueMappingInfoMap.isEmpty()) {
            topicConfigSerializeWrapper.setTopicQueueMappingInfoMap(topicQueueMappingInfoMap);
        }

        doRegisterBrokerAll(true, false, topicConfigSerializeWrapper);
    }

    /**
     * 注册全部 broker 数据<br>
     * 将当前主题配置与队列映射打包后上报到 NameServer<br>
     * 在开启分片注册时按批次拆分发送, 否则依据 needRegister 判定是否发送全量数据
     *
     * @param checkOrderConfig true 表示注册后同步顺序主题配置
     * @param oneway true 表示单向注册, 不等待返回结果
     * @param forceRegister true 表示忽略变更检测强制注册
     */
    public synchronized void registerBrokerAll(final boolean checkOrderConfig, boolean oneway, boolean forceRegister) {
        ConcurrentMap<String, TopicConfig> topicConfigMap = this.getTopicConfigManager().getTopicConfigTable();
        ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();

        for (TopicConfig topicConfig : topicConfigMap.values()) {
            if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
                || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {
                topicConfigTable.put(topicConfig.getTopicName(),
                    new TopicConfig(topicConfig.getTopicName(), topicConfig.getReadQueueNums(), topicConfig.getWriteQueueNums(),
                        topicConfig.getPerm() & getBrokerConfig().getBrokerPermission()));
            } else {
                topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
            }

            if (this.brokerConfig.isEnableSplitRegistration()
                && topicConfigTable.size() >= this.brokerConfig.getSplitRegistrationSize()) {
                TopicConfigAndMappingSerializeWrapper topicConfigWrapper = this.getTopicConfigManager().buildSerializeWrapper(topicConfigTable);
                doRegisterBrokerAll(checkOrderConfig, oneway, topicConfigWrapper);
                topicConfigTable.clear();
            }
        }

        Map<String, TopicQueueMappingInfo> topicQueueMappingInfoMap = this.getTopicQueueMappingManager().getTopicQueueMappingTable().entrySet().stream()
            .map(entry -> new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), TopicQueueMappingDetail.cloneAsMappingInfo(entry.getValue())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        TopicConfigAndMappingSerializeWrapper topicConfigWrapper = this.getTopicConfigManager().
            buildSerializeWrapper(topicConfigTable, topicQueueMappingInfoMap);
        if (this.brokerConfig.isEnableSplitRegistration() || forceRegister || needRegister(this.brokerConfig.getBrokerClusterName(),
            this.getBrokerAddr(),
            this.brokerConfig.getBrokerName(),
            this.brokerConfig.getBrokerId(),
            this.brokerConfig.getRegisterBrokerTimeoutMills(),
            this.brokerConfig.isInBrokerContainer())) {
            doRegisterBrokerAll(checkOrderConfig, oneway, topicConfigWrapper);
        }
    }

    /**
     * 执行 broker 注册调用<br>
     * 组装注册参数并调用 BrokerOuterAPI 完成远端注册, 随后处理注册返回结果
     *
     * @param checkOrderConfig true 表示需要更新顺序主题配置
     * @param oneway true 表示单向注册
     * @param topicConfigWrapper 待注册的主题配置快照
     */
    protected void doRegisterBrokerAll(boolean checkOrderConfig, boolean oneway,
        TopicConfigSerializeWrapper topicConfigWrapper) {

        if (shutdown) {
            BrokerController.LOG.info("BrokerController#doRegisterBrokerAll: broker has shutdown, no need to register any more.");
            return;
        }
        List<RegisterBrokerResult> registerBrokerResultList = this.brokerOuterAPI.registerBrokerAll(
            this.brokerConfig.getBrokerClusterName(),
            this.getBrokerAddr(),
            this.brokerConfig.getBrokerName(),
            this.brokerConfig.getBrokerId(),
            this.getHAServerAddr(),
            topicConfigWrapper,
            Lists.newArrayList(),
            oneway,
            this.brokerConfig.getRegisterBrokerTimeoutMills(),
            this.brokerConfig.isEnableSlaveActingMaster(),
            this.brokerConfig.isCompressedRegister(),
            this.brokerConfig.isEnableSlaveActingMaster() ? this.brokerConfig.getBrokerNotActiveTimeoutMillis() : null,
            this.getBrokerIdentity());

        handleRegisterBrokerResult(registerBrokerResultList, checkOrderConfig);
    }

    /**
     * 发送 broker 心跳<br>
     * controller 模式下先向 controller 上报存活信息<br>
     * 从节点代理主节点模式下再向 NameServer 发送心跳或数据版本心跳
     */
    protected void sendHeartbeat() {
        if (this.brokerConfig.isEnableControllerMode()) {
            this.replicasManager.sendHeartbeatToController();
        }

        if (this.brokerConfig.isEnableSlaveActingMaster()) {
            if (this.brokerConfig.isCompatibleWithOldNameSrv()) {
                this.brokerOuterAPI.sendHeartbeatViaDataVersion(
                    this.brokerConfig.getBrokerClusterName(),
                    this.getBrokerAddr(),
                    this.brokerConfig.getBrokerName(),
                    this.brokerConfig.getBrokerId(),
                    this.brokerConfig.getSendHeartbeatTimeoutMillis(),
                    this.getTopicConfigManager().getDataVersion(),
                    this.brokerConfig.isInBrokerContainer());
            } else {
                this.brokerOuterAPI.sendHeartbeat(
                    this.brokerConfig.getBrokerClusterName(),
                    this.getBrokerAddr(),
                    this.brokerConfig.getBrokerName(),
                    this.brokerConfig.getBrokerId(),
                    this.brokerConfig.getSendHeartbeatTimeoutMillis(),
                    this.brokerConfig.isInBrokerContainer());
            }
        }
    }

    /**
     * 同步 broker 成员组信息<br>
     * 从 NameServer 拉取同名 broker 组成员后更新存活副本数<br>
     * 非隔离状态下根据最小 brokerId 触发服务角色切换
     */
    protected void syncBrokerMemberGroup() {
        try {
            brokerMemberGroup = this.getBrokerOuterAPI()
                .syncBrokerMemberGroup(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.getBrokerName(), this.brokerConfig.isCompatibleWithOldNameSrv());
        } catch (Exception e) {
            BrokerController.LOG.error("syncBrokerMemberGroup from namesrv failed, ", e);
            return;
        }
        if (brokerMemberGroup == null || brokerMemberGroup.getBrokerAddrs().size() == 0) {
            BrokerController.LOG.warn("Couldn't find any broker member from namesrv in {}/{}", this.brokerConfig.getBrokerClusterName(), this.brokerConfig.getBrokerName());
            return;
        }
        this.messageStore.setAliveReplicaNumInGroup(calcAliveBrokerNumInGroup(brokerMemberGroup.getBrokerAddrs()));

        if (!this.isIsolated) {
            long minBrokerId = brokerMemberGroup.minimumBrokerId();
            this.updateMinBroker(minBrokerId, brokerMemberGroup.getBrokerAddrs().get(minBrokerId));
        }
    }

    /**
     * 计算组内存活 broker 数量
     */
    private int calcAliveBrokerNumInGroup(Map<Long, String> brokerAddrTable) {
        if (brokerAddrTable.containsKey(this.brokerConfig.getBrokerId())) {
            return brokerAddrTable.size();
        } else {
            return brokerAddrTable.size() + 1;
        }
    }

    /**
     * 处理 broker 注册结果<br>
     * 根据 NameServer 返回结果更新主节点地址, 同步主从状态并按需刷新顺序主题配置
     *
     * @param registerBrokerResultList 注册返回结果列表
     * @param checkOrderConfig true 表示需要更新顺序主题配置
     */
    protected void handleRegisterBrokerResult(List<RegisterBrokerResult> registerBrokerResultList,
        boolean checkOrderConfig) {
        for (RegisterBrokerResult registerBrokerResult : registerBrokerResultList) {
            if (registerBrokerResult != null) {
                if (this.updateMasterHAServerAddrPeriodically && registerBrokerResult.getHaServerAddr() != null) {
                    this.messageStore.updateHaMasterAddress(registerBrokerResult.getHaServerAddr());
                    this.messageStore.updateMasterAddress(registerBrokerResult.getMasterAddr());
                }

                this.slaveSynchronize.setMasterAddr(registerBrokerResult.getMasterAddr());
                if (checkOrderConfig) {
                    this.getTopicConfigManager().updateOrderTopicConfig(registerBrokerResult.getKvTable());
                }
                break;
            }
        }
    }

    /**
     * 判断是否需要执行全量注册<br>
     * 通过 NameServer 返回的变更标记判断本地主题配置是否需要重新上报
     *
     * @param clusterName 集群名称
     * @param brokerAddr broker 地址
     * @param brokerName broker 名称
     * @param brokerId broker 标识
     * @param timeoutMills 请求超时时间, 单位毫秒
     * @param isInBrokerContainer broker 是否运行在容器模式
     * @return true 表示至少一个 NameServer 认为配置发生变更
     */
    private boolean needRegister(final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId,
        final int timeoutMills,
        final boolean isInBrokerContainer) {

        TopicConfigSerializeWrapper topicConfigWrapper = this.getTopicConfigManager().buildTopicConfigSerializeWrapper();
        List<Boolean> changeList = brokerOuterAPI.needRegister(clusterName, brokerAddr, brokerName, brokerId, topicConfigWrapper, timeoutMills, isInBrokerContainer);
        boolean needRegister = false;
        for (Boolean changed : changeList) {
            if (changed) {
                needRegister = true;
                break;
            }
        }
        return needRegister;
    }

    /**
     * 按条件启动服务
     */
    public void startService(long minBrokerId, String minBrokerAddr) {
        BrokerController.LOG.info("{} start service, min broker id is {}, min broker addr: {}",
            this.brokerConfig.getCanonicalName(), minBrokerId, minBrokerAddr);
        this.minBrokerIdInGroup = minBrokerId;
        this.minBrokerAddrInGroup = minBrokerAddr;

        this.changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == minBrokerId);
        this.registerBrokerAll(true, false, brokerConfig.isForceRegister());

        isIsolated = false;
    }

    /**
     * 无条件启动服务
     */
    public void startServiceWithoutCondition() {
        BrokerController.LOG.info("{} start service", this.brokerConfig.getCanonicalName());

        this.changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == MixAll.MASTER_ID);
        this.registerBrokerAll(true, false, brokerConfig.isForceRegister());

        isIsolated = false;
    }

    /**
     * 停止服务
     */
    public void stopService() {
        BrokerController.LOG.info("{} stop service", this.getBrokerConfig().getCanonicalName());
        isIsolated = true;
        this.changeSpecialServiceStatus(false);
    }

    public boolean isSpecialServiceRunning() {
        if (isScheduleServiceStart() && isTransactionCheckServiceStart()) {
            return true;
        }

        return this.ackMessageProcessor != null && this.ackMessageProcessor.isPopReviveServiceRunning();
    }

    /**
     * 处理主节点下线
     */
    private void onMasterOffline() {
        // close channels with master broker
        // 关闭与主 broker 的连接
        String masterAddr = this.slaveSynchronize.getMasterAddr();
        if (masterAddr != null) {
            this.brokerOuterAPI.getRemotingClient().closeChannels(
                Arrays.asList(masterAddr, MixAll.brokerVIPChannel(true, masterAddr)));
        }
        // master not available, stop sync
        // 主节点不可用时停止同步
        this.slaveSynchronize.setMasterAddr(null);
        this.messageStore.updateHaMasterAddress(null);
    }

    /**
     * 处理主节点上线
     */
    private void onMasterOnline(String masterAddr, String masterHaAddr) {
        boolean needSyncMasterFlushOffset = this.messageStore.getMasterFlushedOffset() == 0
            && this.messageStoreConfig.isSyncMasterFlushOffsetWhenStartup();
        if (masterHaAddr == null || needSyncMasterFlushOffset) {
            try {
                BrokerSyncInfo brokerSyncInfo = this.brokerOuterAPI.retrieveBrokerHaInfo(masterAddr);

                if (needSyncMasterFlushOffset) {
                    LOG.info("Set master flush offset in slave to {}", brokerSyncInfo.getMasterFlushOffset());
                    this.messageStore.setMasterFlushedOffset(brokerSyncInfo.getMasterFlushOffset());
                }

                if (masterHaAddr == null) {
                    this.messageStore.updateHaMasterAddress(brokerSyncInfo.getMasterHaAddress());
                    this.messageStore.updateMasterAddress(brokerSyncInfo.getMasterAddress());
                }
            } catch (Exception e) {
                LOG.error("retrieve master ha info exception, {}", e);
            }
        }

        // set master HA address.
        // 设置主节点 HA 地址
        if (masterHaAddr != null) {
            this.messageStore.updateHaMasterAddress(masterHaAddr);
        }

        // wakeup HAClient
        // 唤醒 HAClient
        this.messageStore.wakeupHAClient();
    }

    /**
     * 处理最小 broker 变更事件<br>
     * 更新本地最小节点视图后切换特殊服务状态, 并根据上下线信息调整主从同步链路
     *
     * @param minBrokerId 变更后的最小 brokerId
     * @param minBrokerAddr 变更后的最小 broker 地址
     * @param offlineBrokerAddr 本轮检测判定下线的 broker 地址, 可为 null
     * @param masterHaAddr 主节点 HA 地址, 可为 null
     */
    private void onMinBrokerChange(long minBrokerId, String minBrokerAddr, String offlineBrokerAddr,
        String masterHaAddr) {
        LOG.info("Min broker changed, old: {}-{}, new {}-{}",
            this.minBrokerIdInGroup, this.minBrokerAddrInGroup, minBrokerId, minBrokerAddr);

        this.minBrokerIdInGroup = minBrokerId;
        this.minBrokerAddrInGroup = minBrokerAddr;

        this.changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == this.minBrokerIdInGroup);

        if (offlineBrokerAddr != null && offlineBrokerAddr.equals(this.slaveSynchronize.getMasterAddr())) {
            // master offline
            // 主节点下线
            onMasterOffline();
        }

        if (minBrokerId == MixAll.MASTER_ID && minBrokerAddr != null) {
            // master online
            // 主节点上线
            onMasterOnline(minBrokerAddr, masterHaAddr);
        }

        // notify PullRequest on hold to pull from master.
        // 通知挂起 PullRequest 从主节点拉取
        if (this.minBrokerIdInGroup == MixAll.MASTER_ID) {
            this.pullRequestHoldService.notifyMasterOnline();
        }
    }

    /**
     * 更新组内最小 broker 信息<br>
     * 使用非阻塞锁快速更新本地最小节点视图, 适用于周期同步场景
     *
     * @param minBrokerId 当前组内最小 brokerId
     * @param minBrokerAddr 最小 broker 对应地址
     */
    public void updateMinBroker(long minBrokerId, String minBrokerAddr) {
        if (brokerConfig.isEnableSlaveActingMaster() && brokerConfig.getBrokerId() != MixAll.MASTER_ID) {
            if (lock.tryLock()) {
                try {
                    if (minBrokerId != this.minBrokerIdInGroup) {
                        String offlineBrokerAddr = null;
                        if (minBrokerId > this.minBrokerIdInGroup) {
                            offlineBrokerAddr = this.minBrokerAddrInGroup;
                        }
                        onMinBrokerChange(minBrokerId, minBrokerAddr, offlineBrokerAddr, null);
                    }
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    /**
     * 更新组内最小 broker 信息<br>
     * 使用带超时锁执行角色切换, 支持显式传入下线节点与主节点 HA 地址
     *
     * @param minBrokerId 当前组内最小 brokerId
     * @param minBrokerAddr 最小 broker 对应地址
     * @param offlineBrokerAddr 已下线 broker 地址, 可为 null
     * @param masterHaAddr 主节点 HA 地址, 可为 null
     */
    public void updateMinBroker(long minBrokerId, String minBrokerAddr, String offlineBrokerAddr,
        String masterHaAddr) {
        if (brokerConfig.isEnableSlaveActingMaster() && brokerConfig.getBrokerId() != MixAll.MASTER_ID) {
            try {
                if (lock.tryLock(3000, TimeUnit.MILLISECONDS)) {
                    try {
                        if (minBrokerId != this.minBrokerIdInGroup) {
                            onMinBrokerChange(minBrokerId, minBrokerAddr, offlineBrokerAddr, masterHaAddr);
                        }
                    } finally {
                        lock.unlock();
                    }

                }
            } catch (InterruptedException e) {
                LOG.error("Update min broker error, {}", e);
            }
        }
    }

    /**
     * 变更特殊服务状态<br>
     * 统一切换仅主节点可运行的服务状态, 包括定时消息, 事务回查与 PopRevive
     *
     * @param shouldStart true 表示启动特殊服务, false 表示关闭
     */
    public void changeSpecialServiceStatus(boolean shouldStart) {

        for (BrokerAttachedPlugin brokerAttachedPlugin : brokerAttachedPlugins) {
            if (brokerAttachedPlugin != null) {
                brokerAttachedPlugin.statusChanged(shouldStart);
            }
        }

        changeScheduleServiceStatus(shouldStart);

        changeTransactionCheckServiceStatus(shouldStart);

        if (this.ackMessageProcessor != null) {
            LOG.info("Set PopReviveService Status to {}", shouldStart);
            this.ackMessageProcessor.setPopReviveServiceStatus(shouldStart);
        }
    }

    /**
     * 变更事务回查服务状态<br>
     * 状态发生变化时再执行启动或关闭, 避免重复切换
     *
     * @param shouldStart true 表示启动事务回查服务
     */
    private synchronized void changeTransactionCheckServiceStatus(boolean shouldStart) {
        if (isTransactionCheckServiceStart != shouldStart) {
            LOG.info("TransactionCheckService status changed to {}", shouldStart);
            if (shouldStart) {
                this.transactionalMessageCheckService.start();
            } else {
                this.transactionalMessageCheckService.shutdown(true);
            }
            isTransactionCheckServiceStart = shouldStart;
        }
    }

    /**
     * 变更定时服务状态<br>
     * 同步维护 scheduleMessageService 与 timerMessageStore 的运行状态
     *
     * @param shouldStart true 表示启动定时服务
     */
    public synchronized void changeScheduleServiceStatus(boolean shouldStart) {
        if (isScheduleServiceStart != shouldStart) {
            LOG.info("ScheduleServiceStatus changed to {}", shouldStart);
            if (shouldStart) {
                this.scheduleMessageService.start();
            } else {
                this.scheduleMessageService.stop();
            }
            isScheduleServiceStart = shouldStart;

            if (timerMessageStore != null) {
                timerMessageStore.syncLastReadTimeMs();
                timerMessageStore.setShouldRunningDequeue(shouldStart);
            }
        }
    }

    public MessageStore getMessageStoreByBrokerName(String brokerName) {
        if (this.brokerConfig.getBrokerName().equals(brokerName)) {
            return this.getMessageStore();
        }
        return null;
    }

    /**
     * 获取节点身份信息<br>
     * 包含集群名称, broker 名称, brokerId 与容器模式标识, 供注册与心跳使用<br>
     * 在开启 DLegerCommitLog 时会从 dLegerSelfId 派生节点标识, 启动后再由角色切换流程维护 brokerId 的主从语义
     */
    public BrokerIdentity getBrokerIdentity() {
        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            return new BrokerIdentity(
                brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(),
                Integer.parseInt(messageStoreConfig.getdLegerSelfId().substring(1)), brokerConfig.isInBrokerContainer());
        } else {
            return new BrokerIdentity(
                brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(),
                brokerConfig.getBrokerId(), brokerConfig.isInBrokerContainer());
        }
    }

    public TopicConfigManager getTopicConfigManager() {
        return topicConfigManager;
    }

    public void setTopicConfigManager(TopicConfigManager topicConfigManager) {
        this.topicConfigManager = topicConfigManager;
    }

    public TopicQueueMappingManager getTopicQueueMappingManager() {
        return topicQueueMappingManager;
    }

    public AuthenticationMetadataManager getAuthenticationMetadataManager() {
        return authenticationMetadataManager;
    }

    @VisibleForTesting
    public void setAuthenticationMetadataManager(
        AuthenticationMetadataManager authenticationMetadataManager) {
        this.authenticationMetadataManager = authenticationMetadataManager;
    }

    public AuthorizationMetadataManager getAuthorizationMetadataManager() {
        return authorizationMetadataManager;
    }

    @VisibleForTesting
    public void setAuthorizationMetadataManager(
        AuthorizationMetadataManager authorizationMetadataManager) {
        this.authorizationMetadataManager = authorizationMetadataManager;
    }

    public String getHAServerAddr() {
        return this.brokerConfig.getBrokerIP2() + ":" + this.messageStoreConfig.getHaListenPort();
    }

    public RebalanceLockManager getRebalanceLockManager() {
        return rebalanceLockManager;
    }

    public SlaveSynchronize getSlaveSynchronize() {
        return slaveSynchronize;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public ExecutorService getPullMessageExecutor() {
        return pullMessageExecutor;
    }

    public ExecutorService getPutMessageFutureExecutor() {
        return putMessageFutureExecutor;
    }

    public void setPullMessageExecutor(ExecutorService pullMessageExecutor) {
        this.pullMessageExecutor = pullMessageExecutor;
    }

    public BlockingQueue<Runnable> getSendThreadPoolQueue() {
        return sendThreadPoolQueue;
    }

    public BlockingQueue<Runnable> getAckThreadPoolQueue() {
        return ackThreadPoolQueue;
    }

    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    public void setBrokerStatsManager(BrokerStatsManager brokerStatsManager) {
        this.brokerStatsManager = brokerStatsManager;
    }

    public List<SendMessageHook> getSendMessageHookList() {
        return sendMessageHookList;
    }

    /**
     * 注册发送消息 Hook
     */
    public void registerSendMessageHook(final SendMessageHook hook) {
        this.sendMessageHookList.add(hook);
        LOG.info("register SendMessageHook Hook, {}", hook.hookName());
    }

    public List<ConsumeMessageHook> getConsumeMessageHookList() {
        return consumeMessageHookList;
    }

    /**
     * 注册消费消息 Hook
     */
    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        LOG.info("register ConsumeMessageHook Hook, {}", hook.hookName());
    }

    /**
     * 注册服务端 RPC Hook
     */
    public void registerServerRPCHook(RPCHook rpcHook) {
        for (Map.Entry<String, RemotingServer> entry : remotingServerMap.entrySet()) {
            RemotingServer remotingServer = entry.getValue();
            if (remotingServer != null) {
                remotingServer.registerRPCHook(rpcHook);
            }
        }
    }

    public void setRequestPipeline(RequestPipeline pipeline) {
        for (Map.Entry<String, RemotingServer> entry : remotingServerMap.entrySet()) {
            RemotingServer remotingServer = entry.getValue();
            if (remotingServer != null) {
                remotingServer.setRequestPipeline(pipeline);
            }
        }
    }

    public RemotingServer getRemotingServer() {
        return remotingServerMap.get(TCP_REMOTING_SERVER);
    }

    public void setRemotingServer(RemotingServer remotingServer) {
        remotingServerMap.put(TCP_REMOTING_SERVER, remotingServer);
    }

    public RemotingServer getFastRemotingServer() {
        return remotingServerMap.get(FAST_REMOTING_SERVER);
    }

    public void setFastRemotingServer(RemotingServer fastRemotingServer) {
        remotingServerMap.put(FAST_REMOTING_SERVER, fastRemotingServer);
    }

    public RemotingServer getRemotingServerByName(String name) {
        return remotingServerMap.get(name);
    }

    public void setRemotingServerByName(String name, RemotingServer remotingServer) {
        remotingServerMap.put(name, remotingServer);
    }

    public ClientHousekeepingService getClientHousekeepingService() {
        return clientHousekeepingService;
    }

    public CountDownLatch getRemotingServerStartLatch() {
        return remotingServerStartLatch;
    }

    public void setRemotingServerStartLatch(CountDownLatch remotingServerStartLatch) {
        this.remotingServerStartLatch = remotingServerStartLatch;
    }

    /**
     * 注册客户端 RPC Hook
     */
    public void registerClientRPCHook(RPCHook rpcHook) {
        this.getBrokerOuterAPI().registerRPCHook(rpcHook);
    }

    public BrokerOuterAPI getBrokerOuterAPI() {
        return brokerOuterAPI;
    }

    public InetSocketAddress getStoreHost() {
        return storeHost;
    }

    public void setStoreHost(InetSocketAddress storeHost) {
        this.storeHost = storeHost;
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

    public BlockingQueue<Runnable> getHeartbeatThreadPoolQueue() {
        return heartbeatThreadPoolQueue;
    }

    public TransactionalMessageCheckService getTransactionalMessageCheckService() {
        return transactionalMessageCheckService;
    }

    public void setTransactionalMessageCheckService(
        TransactionalMessageCheckService transactionalMessageCheckService) {
        this.transactionalMessageCheckService = transactionalMessageCheckService;
    }

    public TransactionalMessageService getTransactionalMessageService() {
        return transactionalMessageService;
    }

    public void setTransactionalMessageService(TransactionalMessageService transactionalMessageService) {
        this.transactionalMessageService = transactionalMessageService;
    }

    public AbstractTransactionalMessageCheckListener getTransactionalMessageCheckListener() {
        return transactionalMessageCheckListener;
    }

    public void setTransactionalMessageCheckListener(
        AbstractTransactionalMessageCheckListener transactionalMessageCheckListener) {
        this.transactionalMessageCheckListener = transactionalMessageCheckListener;
    }

    public BlockingQueue<Runnable> getEndTransactionThreadPoolQueue() {
        return endTransactionThreadPoolQueue;

    }

    public ExecutorService getSendMessageExecutor() {
        return sendMessageExecutor;
    }

    public SendMessageProcessor getSendMessageProcessor() {
        return sendMessageProcessor;
    }

    public RecallMessageProcessor getRecallMessageProcessor() {
        return recallMessageProcessor;
    }

    public QueryAssignmentProcessor getQueryAssignmentProcessor() {
        return queryAssignmentProcessor;
    }

    public TopicQueueMappingCleanService getTopicQueueMappingCleanService() {
        return topicQueueMappingCleanService;
    }

    public ExecutorService getAdminBrokerExecutor() {
        return adminBrokerExecutor;
    }

    public BlockingQueue<Runnable> getLitePullThreadPoolQueue() {
        return litePullThreadPoolQueue;
    }

    public ShutdownHook getShutdownHook() {
        return shutdownHook;
    }

    public void setShutdownHook(ShutdownHook shutdownHook) {
        this.shutdownHook = shutdownHook;
    }

    public long getMinBrokerIdInGroup() {
        return this.brokerConfig.getBrokerId();
    }

    /**
     * 选择主 broker 地址
     */
    public BrokerController peekMasterBroker() {
        return brokerConfig.getBrokerId() == MixAll.MASTER_ID ? this : null;
    }

    public BrokerMemberGroup getBrokerMemberGroup() {
        return this.brokerMemberGroup;
    }

    public int getListenPort() {
        return this.nettyServerConfig.getListenPort();
    }

    public List<BrokerAttachedPlugin> getBrokerAttachedPlugins() {
        return brokerAttachedPlugins;
    }

    public EscapeBridge getEscapeBridge() {
        return escapeBridge;
    }

    public long getShouldStartTime() {
        return shouldStartTime;
    }

    public BrokerPreOnlineService getBrokerPreOnlineService() {
        return brokerPreOnlineService;
    }

    public EndTransactionProcessor getEndTransactionProcessor() {
        return endTransactionProcessor;
    }

    public boolean isScheduleServiceStart() {
        return isScheduleServiceStart;
    }

    public boolean isTransactionCheckServiceStart() {
        return isTransactionCheckServiceStart;
    }

    public ScheduleMessageService getScheduleMessageService() {
        return scheduleMessageService;
    }

    public ReplicasManager getReplicasManager() {
        return replicasManager;
    }

    public void setIsolated(boolean isolated) {
        isIsolated = isolated;
    }

    public boolean isIsolated() {
        return this.isIsolated;
    }

    public TimerCheckpoint getTimerCheckpoint() {
        return timerCheckpoint;
    }

    public TopicRouteInfoManager getTopicRouteInfoManager() {
        return this.topicRouteInfoManager;
    }

    public BlockingQueue<Runnable> getClientManagerThreadPoolQueue() {
        return clientManagerThreadPoolQueue;
    }

    public BlockingQueue<Runnable> getConsumerManagerThreadPoolQueue() {
        return consumerManagerThreadPoolQueue;
    }

    public BlockingQueue<Runnable> getAsyncPutThreadPoolQueue() {
        return putThreadPoolQueue;
    }

    public BlockingQueue<Runnable> getReplyThreadPoolQueue() {
        return replyThreadPoolQueue;
    }

    public BlockingQueue<Runnable> getAdminBrokerThreadPoolQueue() {
        return adminBrokerThreadPoolQueue;
    }

    public ColdDataPullRequestHoldService getColdDataPullRequestHoldService() {
        return coldDataPullRequestHoldService;
    }

    public void setColdDataPullRequestHoldService(
        ColdDataPullRequestHoldService coldDataPullRequestHoldService) {
        this.coldDataPullRequestHoldService = coldDataPullRequestHoldService;
    }

    public ColdDataCgCtrService getColdDataCgCtrService() {
        return coldDataCgCtrService;
    }

    public void setColdDataCgCtrService(ColdDataCgCtrService coldDataCgCtrService) {
        this.coldDataCgCtrService = coldDataCgCtrService;
    }


}
