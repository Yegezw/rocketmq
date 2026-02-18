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

package org.apache.rocketmq.proxy.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.metrics.MetricsExporterType;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.ProxyMode;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Proxy 核心配置对象
 */
public class ProxyConfig implements ConfigFile {
    /**
     * 配置日志记录器
     */
    private final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    /**
     * 默认配置文件名
     */
    public final static String DEFAULT_CONFIG_FILE_NAME = "rmq-proxy.json";
    /**
     * 当前进程可用处理器核数
     */
    private static final int PROCESSOR_NUMBER = Runtime.getRuntime().availableProcessors();
    /**
     * 默认集群名称
     */
    private static final String DEFAULT_CLUSTER_NAME = "DefaultCluster";

    /**
     * 本地主机名
     */
    private static String localHostName;

    // 初始化本机主机名
    static {
        try {
            localHostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.error("Failed to obtain the host name", e);
        }
    }

    /**
     * RocketMQ 集群名称
     */
    private String rocketMQClusterName = DEFAULT_CLUSTER_NAME;
    /**
     * Proxy 集群名称
     */
    private String proxyClusterName = DEFAULT_CLUSTER_NAME;
    /**
     * Proxy 节点名称
     */
    private String proxyName = StringUtils.isEmpty(localHostName) ? "DEFAULT_PROXY" : localHostName;

    /**
     * 本地服务地址
     */
    private String localServeAddr = "";

    /**
     * 心跳同步主题所属集群名
     */
    private String heartbeatSyncerTopicClusterName = "";
    /**
     * 心跳同步线程池线程数
     */
    private int heartbeatSyncerThreadPoolNums = 4;
    /**
     * 心跳同步线程池队列容量
     */
    private int heartbeatSyncerThreadPoolQueueCapacity = 100;

    /**
     * 心跳同步主题名称
     */
    private String heartbeatSyncerTopicName = "DefaultHeartBeatSyncerTopic";

    /**
     * configuration for ThreadPoolMonitor
     * <br>
     * ThreadPoolMonitor 相关配置
     */
    private boolean enablePrintJstack = true;
    /**
     * 线程栈打印周期毫秒值
     */
    private long printJstackInMillis = Duration.ofSeconds(60).toMillis();
    /**
     * 线程池状态打印周期毫秒值
     */
    private long printThreadPoolStatusInMillis = Duration.ofSeconds(3).toMillis();

    /**
     * NameServer 地址
     */
    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));
    /**
     * NameServer 域名
     */
    private String namesrvDomain = "";
    /**
     * NameServer 域名子分组
     */
    private String namesrvDomainSubgroup = "";
    /**
     * TLS
     * TLS 相关配置
     */
    private boolean tlsTestModeEnable = true;
    /**
     * TLS 私钥文件路径
     */
    private String tlsKeyPath = ConfigurationManager.getProxyHome() + "/conf/tls/rocketmq.key";
    /**
     * TLS 证书文件路径
     */
    private String tlsCertPath = ConfigurationManager.getProxyHome() + "/conf/tls/rocketmq.crt";
    /**
     * gRPC
     * gRPC 相关配置
     */
    private String proxyMode = ProxyMode.CLUSTER.name();
    /**
     * gRPC 服务端口
     */
    private Integer grpcServerPort = 8081;
    /**
     * gRPC 关闭等待秒数
     */
    private long grpcShutdownTimeSeconds = 30;
    /**
     * gRPC Boss 事件循环线程数
     */
    private int grpcBossLoopNum = 1;
    /**
     * gRPC Worker 事件循环线程数
     */
    private int grpcWorkerLoopNum = PROCESSOR_NUMBER * 2;
    /**
     * 是否启用 gRPC Epoll
     */
    private boolean enableGrpcEpoll = false;
    /**
     * gRPC 业务线程池线程数
     */
    private int grpcThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;
    /**
     * gRPC 业务线程池队列容量
     */
    private int grpcThreadPoolQueueCapacity = 100000;
    /**
     * Broker 配置文件路径
     */
    private String brokerConfigPath = ConfigurationManager.getProxyHome() + "/conf/broker.conf";
    /**
     * gRPC max message size<br>
     * 130M = 4M * 32 messages + 2M attributes<br>
     * gRPC 最大消息大小<br>
     * 130M = 4M * 32 消息 + 2M 属性
     */
    private int grpcMaxInboundMessageSize = 130 * 1024 * 1024;
    /**
     * max message body size, 0 or negative number means no limit for proxy
     * <br>
     * 最大消息体大小, 0 或负数表示 Proxy 不限制
     */
    private int maxMessageSize = 4 * 1024 * 1024;
    /**
     * if true, proxy will check message body size and reject msg if it's body is empty
     * <br>
     * 为 true 时 Proxy 会校验消息体大小, 并拒绝空消息体
     */
    private boolean enableMessageBodyEmptyCheck = true;
    /**
     * max user property size, 0 or negative number means no limit for proxy
     * <br>
     * 最大用户属性大小, 0 或负数表示 Proxy 不限制
     */
    private int maxUserPropertySize = 16 * 1024;
    /**
     * 用户属性最大个数
     */
    private int userPropertyMaxNum = 128;

    /**
     * max message group size, 0 or negative number means no limit for proxy
     * <br>
     * 最大消息组长度, 0 或负数表示 Proxy 不限制
     */
    private int maxMessageGroupSize = 64;

    /**
     * When a message pops, the message is invisible by default
     * <br>
     * 消息 POP 后默认进入不可见状态
     */
    private long defaultInvisibleTimeMills = Duration.ofSeconds(60).toMillis();
    /**
     * 接收侧最小不可见时长毫秒值
     */
    private long minInvisibleTimeMillsForRecv = Duration.ofSeconds(10).toMillis();
    /**
     * 最大不可见时长毫秒值
     */
    private long maxInvisibleTimeMills = Duration.ofHours(12).toMillis();
    /**
     * 最大延迟时长毫秒值
     */
    private long maxDelayTimeMills = Duration.ofDays(1).toMillis();
    /**
     * 事务恢复最大等待秒数
     */
    private long maxTransactionRecoverySecond = Duration.ofHours(1).getSeconds();
    /**
     * 是否启用主题消息类型校验
     */
    private boolean enableTopicMessageTypeCheck = true;

    /**
     * gRPC 客户端生产者最大重试次数
     */
    private int grpcClientProducerMaxAttempts = 3;
    /**
     * gRPC 客户端生产者退避初始毫秒值
     */
    private long grpcClientProducerBackoffInitialMillis = 10;
    /**
     * gRPC 客户端生产者退避最大毫秒值
     */
    private long grpcClientProducerBackoffMaxMillis = 1000;
    /**
     * gRPC 客户端生产者退避倍率
     */
    private int grpcClientProducerBackoffMultiplier = 2;
    /**
     * gRPC 客户端消费者长轮询最小超时毫秒值
     */
    private long grpcClientConsumerMinLongPollingTimeoutMillis = Duration.ofSeconds(5).toMillis();
    /**
     * gRPC 客户端消费者长轮询最大超时毫秒值
     */
    private long grpcClientConsumerMaxLongPollingTimeoutMillis = Duration.ofSeconds(20).toMillis();
    /**
     * gRPC 客户端消费者长轮询批量大小
     */
    private int grpcClientConsumerLongPollingBatchSize = 32;
    /**
     * gRPC 客户端空闲时长毫秒值
     */
    private long grpcClientIdleTimeMills = Duration.ofSeconds(120).toMillis();

    /**
     * 通道过期秒数
     */
    private int channelExpiredInSeconds = 60;
    /**
     * 上下文过期秒数
     */
    private int contextExpiredInSeconds = 30;

    /**
     * RocketMQ MQClient 实例数
     */
    private int rocketmqMQClientNum = 6;

    /**
     * gRPC Proxy 中继请求超时秒数
     */
    private long grpcProxyRelayRequestTimeoutInSeconds = 5;
    /**
     * gRPC 生产者线程池线程数
     */
    private int grpcProducerThreadPoolNums = PROCESSOR_NUMBER;
    /**
     * gRPC 生产者线程池队列容量
     */
    private int grpcProducerThreadQueueCapacity = 10000;
    /**
     * gRPC 消费者线程池线程数
     */
    private int grpcConsumerThreadPoolNums = PROCESSOR_NUMBER;
    /**
     * gRPC 消费者线程池队列容量
     */
    private int grpcConsumerThreadQueueCapacity = 10000;
    /**
     * gRPC 路由线程池线程数
     */
    private int grpcRouteThreadPoolNums = PROCESSOR_NUMBER;
    /**
     * gRPC 路由线程池队列容量
     */
    private int grpcRouteThreadQueueCapacity = 10000;
    /**
     * gRPC 客户端管理线程池线程数
     */
    private int grpcClientManagerThreadPoolNums = PROCESSOR_NUMBER;
    /**
     * gRPC 客户端管理线程池队列容量
     */
    private int grpcClientManagerThreadQueueCapacity = 10000;
    /**
     * gRPC 事务线程池线程数
     */
    private int grpcTransactionThreadPoolNums = PROCESSOR_NUMBER;
    /**
     * gRPC 事务线程池队列容量
     */
    private int grpcTransactionThreadQueueCapacity = 10000;

    /**
     * 生产处理线程池线程数
     */
    private int producerProcessorThreadPoolNums = PROCESSOR_NUMBER;
    /**
     * 生产处理线程池队列容量
     */
    private int producerProcessorThreadPoolQueueCapacity = 10000;
    /**
     * 消费处理线程池线程数
     */
    private int consumerProcessorThreadPoolNums = PROCESSOR_NUMBER;
    /**
     * 消费处理线程池队列容量
     */
    private int consumerProcessorThreadPoolQueueCapacity = 10000;

    /**
     * 是否使用请求中端口作为 Endpoint 端口
     */
    private boolean useEndpointPortFromRequest = false;

    /**
     * 主题路由服务缓存过期秒数
     */
    private int topicRouteServiceCacheExpiredSeconds = 300;
    /**
     * 主题路由服务缓存刷新秒数
     */
    private int topicRouteServiceCacheRefreshSeconds = 20;
    /**
     * 主题路由服务缓存最大数量
     */
    private int topicRouteServiceCacheMaxNum = 20000;
    /**
     * 主题路由服务线程池线程数
     */
    private int topicRouteServiceThreadPoolNums = PROCESSOR_NUMBER;
    /**
     * 主题路由服务线程池队列容量
     */
    private int topicRouteServiceThreadPoolQueueCapacity = 5000;
    /**
     * 主题配置缓存过期秒数
     */
    private int topicConfigCacheExpiredSeconds = 300;
    /**
     * 主题配置缓存刷新秒数
     */
    private int topicConfigCacheRefreshSeconds = 20;
    /**
     * 主题配置缓存最大数量
     */
    private int topicConfigCacheMaxNum = 20000;
    /**
     * 订阅组配置缓存过期秒数
     */
    private int subscriptionGroupConfigCacheExpiredSeconds = 300;
    /**
     * 订阅组配置缓存刷新秒数
     */
    private int subscriptionGroupConfigCacheRefreshSeconds = 20;
    /**
     * 订阅组配置缓存最大数量
     */
    private int subscriptionGroupConfigCacheMaxNum = 20000;
    /**
     * 用户缓存过期秒数
     */
    private int userCacheExpiredSeconds = 300;
    /**
     * 用户缓存刷新秒数
     */
    private int userCacheRefreshSeconds = 20;
    /**
     * 用户缓存最大数量
     */
    private int userCacheMaxNum = 20000;
    /**
     * ACL缓存过期秒数
     */
    private int aclCacheExpiredSeconds = 300;
    /**
     * ACL缓存刷新秒数
     */
    private int aclCacheRefreshSeconds = 20;
    /**
     * ACL缓存最大数量
     */
    private int aclCacheMaxNum = 20000;
    /**
     * 元数据线程池线程数
     */
    private int metadataThreadPoolNums = 3;
    /**
     * 元数据线程池队列容量
     */
    private int metadataThreadPoolQueueCapacity = 100000;

    /**
     * 事务心跳线程池线程数
     */
    private int transactionHeartbeatThreadPoolNums = 20;
    /**
     * 事务心跳线程池队列容量
     */
    private int transactionHeartbeatThreadPoolQueueCapacity = 200;
    /**
     * 事务心跳周期秒数
     */
    private int transactionHeartbeatPeriodSecond = 20;
    /**
     * 事务心跳批量数量
     */
    private int transactionHeartbeatBatchNum = 100;
    /**
     * 事务数据过期扫描周期毫秒值
     */
    private long transactionDataExpireScanPeriodMillis = Duration.ofSeconds(10).toMillis();
    /**
     * 事务数据最大等待清理毫秒值
     */
    private long transactionDataMaxWaitClearMillis = Duration.ofSeconds(30).toMillis();
    /**
     * 事务数据过期毫秒值
     */
    private long transactionDataExpireMillis = Duration.ofSeconds(30).toMillis();
    /**
     * 事务数据最大数量
     */
    private int transactionDataMaxNum = 15;

    /**
     * 长轮询预留时间毫秒值
     */
    private long longPollingReserveTimeInMillis = 100;

    /**
     * 清理时不可见时长毫秒值
     */
    private long invisibleTimeMillisWhenClear = 1000L;
    /**
     * 是否启用 Proxy 自动续期
     */
    private boolean enableProxyAutoRenew = true;
    /**
     * 最大续期重试次数
     */
    private int maxRenewRetryTimes = 3;
    /**
     * 续期线程线程池线程数
     */
    private int renewThreadPoolNums = 2;
    /**
     * 续期最大线程线程池线程数
     */
    private int renewMaxThreadPoolNums = 4;
    /**
     * 续期线程线程池队列容量
     */
    private int renewThreadPoolQueueCapacity = 300;
    /**
     * Handle 组加锁超时毫秒值
     */
    private long lockTimeoutMsInHandleGroup = TimeUnit.SECONDS.toMillis(3);
    /**
     * 续期提前时间毫秒值
     */
    private long renewAheadTimeMillis = TimeUnit.SECONDS.toMillis(10);
    /**
     * 续期最大时间毫秒值
     */
    private long renewMaxTimeMillis = TimeUnit.HOURS.toMillis(3);
    /**
     * 续期调度周期毫秒值
     */
    private long renewSchedulePeriodMillis = TimeUnit.SECONDS.toMillis(5);

    /**
     * 集群模式是否启用 ACL RPC Hook
     */
    private boolean enableAclRpcHookForClusterMode = false;

    /**
     * 是否使用延迟级别
     */
    private boolean useDelayLevel = false;
    /**
     * 延迟级别定义字符串
     */
    private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
    /**
     * 延迟级别与毫秒值映射表
     */
    private transient ConcurrentSkipListMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable = new ConcurrentSkipListMap<>();

    /**
     * 指标采集模式
     */
    private String metricCollectorMode = MetricCollectorMode.OFF.getModeString();
    // Example address: 127.0.0.1:1234
    // 示例地址: 127.0.0.1:1234
    /**
     * 指标采集地址
     */
    private String metricCollectorAddress = "";

    /**
     * 区域标识
     */
    private String regionId = "";

    /**
     * 是否开启消息轨迹
     */
    private boolean traceOn = false;

    /**
     * 指标导出类型
     */
    private MetricsExporterType metricsExporterType = MetricsExporterType.DISABLE;

    /**
     * gRPC 指标导出目标地址
     */
    private String metricsGrpcExporterTarget = "";
    /**
     * gRPC 指标导出请求头
     */
    private String metricsGrpcExporterHeader = "";
    /**
     * gRPC 指标导出超时毫秒值
     */
    private long metricGrpcExporterTimeOutInMills = 3 * 1000;
    /**
     * gRPC 指标导出间隔毫秒值
     */
    private long metricGrpcExporterIntervalInMills = 60 * 1000;
    /**
     * 日志指标导出间隔毫秒值
     */
    private long metricLoggingExporterIntervalInMills = 10 * 1000;

    /**
     * Prometheus 指标导出端口
     */
    private int metricsPromExporterPort = 5557;
    /**
     * Prometheus 指标导出主机
     */
    private String metricsPromExporterHost = "";

    // Label pairs in CSV. Each label follows pattern of Key:Value. eg: instance_id:xxx,uid:xxx
    // CSV 标签键值对, 每个标签格式为 Key:Value, 例如 instance_id:xxx, uid:xxx
    /**
     * 指标标签定义字符串
     */
    private String metricsLabel = "";

    /**
     * 是否使用差量指标
     */
    private boolean metricsInDelta = false;

    /**
     * 通道过期超时毫秒值
     */
    private long channelExpiredTimeout = 1000 * 120;

    // remoting
    // remoting 相关配置
    /**
     * 是否启用 Remoting 本地 Proxy gRPC
     */
    private boolean enableRemotingLocalProxyGrpc = true;
    /**
     * 本地 Proxy 连接超时毫秒值
     */
    private int localProxyConnectTimeoutMs = 3000;
    /**
     * Remoting 对外访问地址
     */
    private String remotingAccessAddr = "";
    /**
     * Remoting 监听端口
     */
    private int remotingListenPort = 8080;

    // related to proxy's send strategy in cluster mode.
    // 集群模式下 Proxy 发送策略相关配置
    /**
     * 是否启用发送延迟容错
     */
    private boolean sendLatencyEnable = false;
    /**
     * 是否启用探测器
     */
    private boolean startDetectorEnable = false;
    /**
     * 探测超时毫秒值
     */
    private int detectTimeout = 200;
    /**
     * 探测间隔毫秒值
     */
    private int detectInterval = 2 * 1000;

    /**
     * Remoting 心跳线程池线程数
     */
    private int remotingHeartbeatThreadPoolNums = 2 * PROCESSOR_NUMBER;
    /**
     * Remoting 主题路由线程池线程数
     */
    private int remotingTopicRouteThreadPoolNums = 2 * PROCESSOR_NUMBER;
    /**
     * Remoting 发送消息线程池线程数
     */
    private int remotingSendMessageThreadPoolNums = 4 * PROCESSOR_NUMBER;
    /**
     * Remoting 拉消息线程池线程数
     */
    private int remotingPullMessageThreadPoolNums = 4 * PROCESSOR_NUMBER;
    /**
     * Remoting 位点更新线程池线程数
     */
    private int remotingUpdateOffsetThreadPoolNums = 4 * PROCESSOR_NUMBER;
    /**
     * Remoting 默认线程池线程数
     */
    private int remotingDefaultThreadPoolNums = 4 * PROCESSOR_NUMBER;

    /**
     * Remoting 心跳线程池队列容量
     */
    private int remotingHeartbeatThreadPoolQueueCapacity = 50000;
    /**
     * Remoting 主题路由线程池队列容量
     */
    private int remotingTopicRouteThreadPoolQueueCapacity = 50000;
    /**
     * Remoting 发送线程池队列容量
     */
    private int remotingSendThreadPoolQueueCapacity = 10000;
    /**
     * Remoting 拉消息线程池队列容量
     */
    private int remotingPullThreadPoolQueueCapacity = 50000;
    /**
     * Remoting 位点更新线程池队列容量
     */
    private int remotingUpdateOffsetThreadPoolQueueCapacity = 10000;
    /**
     * Remoting 默认线程池队列容量
     */
    private int remotingDefaultThreadPoolQueueCapacity = 50000;

    /**
     * Remoting 发送队列最大等待毫秒值
     */
    private long remotingWaitTimeMillsInSendQueue = 3 * 1000;
    /**
     * Remoting 拉消息队列最大等待毫秒值
     */
    private long remotingWaitTimeMillsInPullQueue = 5 * 1000;
    /**
     * Remoting 心跳队列最大等待毫秒值
     */
    private long remotingWaitTimeMillsInHeartbeatQueue = 31 * 1000;
    /**
     * Remoting 位点更新队列最大等待毫秒值
     */
    private long remotingWaitTimeMillsInUpdateOffsetQueue = 3 * 1000;
    /**
     * Remoting 主题路由队列最大等待毫秒值
     */
    private long remotingWaitTimeMillsInTopicRouteQueue = 3 * 1000;
    /**
     * Remoting 默认队列最大等待毫秒值
     */
    private long remotingWaitTimeMillsInDefaultQueue = 3 * 1000;

    /**
     * 是否启用批量 ACK
     */
    private boolean enableBatchAck = false;

    /**
     * 初始化派生配置与默认值
     */
    @Override
    public void initData() {
        parseDelayLevel();
        if (StringUtils.isEmpty(localServeAddr)) {
            this.localServeAddr = NetworkUtil.getLocalAddress();
        }
        if (StringUtils.isBlank(localServeAddr)) {
            throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, "get local serve ip failed");
        }
        if (StringUtils.isBlank(remotingAccessAddr)) {
            this.remotingAccessAddr = this.localServeAddr;
        }
        if (StringUtils.isBlank(heartbeatSyncerTopicClusterName)) {
            this.heartbeatSyncerTopicClusterName = this.rocketMQClusterName;
        }
    }

    /**
     * 根据目标时间戳计算延迟级别
     *
     * @param timeMillis 目标时间戳
     * @return 对应延迟级别
     */
    public int computeDelayLevel(long timeMillis) {
        long intervalMillis = timeMillis - System.currentTimeMillis();
        List<Map.Entry<Integer, Long>> sortedLevels = delayLevelTable.entrySet().stream().sorted(Comparator.comparingLong(Map.Entry::getValue)).collect(Collectors.toList());
        for (Map.Entry<Integer, Long> entry : sortedLevels) {
            if (entry.getValue() > intervalMillis) {
                return entry.getKey();
            }
        }
        return sortedLevels.get(sortedLevels.size() - 1).getKey();
    }

    /**
     * 解析消息延迟级别配置并填充延迟级别表
     */
    public void parseDelayLevel() {
        this.delayLevelTable = new ConcurrentSkipListMap<>();
        Map<String, Long> timeUnitTable = new HashMap<>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        String levelString = this.getMessageDelayLevel();
        try {
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                String ch = value.substring(value.length() - 1);
                Long tu = timeUnitTable.get(ch);

                int level = i + 1;
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                long delayTimeMillis = tu * num;
                this.delayLevelTable.put(level, delayTimeMillis);
            }
        } catch (Exception e) {
            log.error("parse delay level failed. messageDelayLevel:{}", messageDelayLevel, e);
        }
    }

    public String getRocketMQClusterName() {
        return rocketMQClusterName;
    }

    public void setRocketMQClusterName(String rocketMQClusterName) {
        this.rocketMQClusterName = rocketMQClusterName;
    }

    public String getProxyClusterName() {
        return proxyClusterName;
    }

    public void setProxyClusterName(String proxyClusterName) {
        this.proxyClusterName = proxyClusterName;
    }

    public String getProxyName() {
        return proxyName;
    }

    public void setProxyName(String proxyName) {
        this.proxyName = proxyName;
    }

    public String getLocalServeAddr() {
        return localServeAddr;
    }

    public void setLocalServeAddr(String localServeAddr) {
        this.localServeAddr = localServeAddr;
    }

    public String getHeartbeatSyncerTopicClusterName() {
        return heartbeatSyncerTopicClusterName;
    }

    public void setHeartbeatSyncerTopicClusterName(String heartbeatSyncerTopicClusterName) {
        this.heartbeatSyncerTopicClusterName = heartbeatSyncerTopicClusterName;
    }

    public int getHeartbeatSyncerThreadPoolNums() {
        return heartbeatSyncerThreadPoolNums;
    }

    public void setHeartbeatSyncerThreadPoolNums(int heartbeatSyncerThreadPoolNums) {
        this.heartbeatSyncerThreadPoolNums = heartbeatSyncerThreadPoolNums;
    }

    public int getHeartbeatSyncerThreadPoolQueueCapacity() {
        return heartbeatSyncerThreadPoolQueueCapacity;
    }

    public void setHeartbeatSyncerThreadPoolQueueCapacity(int heartbeatSyncerThreadPoolQueueCapacity) {
        this.heartbeatSyncerThreadPoolQueueCapacity = heartbeatSyncerThreadPoolQueueCapacity;
    }

    public String getHeartbeatSyncerTopicName() {
        return heartbeatSyncerTopicName;
    }

    public void setHeartbeatSyncerTopicName(String heartbeatSyncerTopicName) {
        this.heartbeatSyncerTopicName = heartbeatSyncerTopicName;
    }

    public boolean isEnablePrintJstack() {
        return enablePrintJstack;
    }

    public void setEnablePrintJstack(boolean enablePrintJstack) {
        this.enablePrintJstack = enablePrintJstack;
    }

    public long getPrintJstackInMillis() {
        return printJstackInMillis;
    }

    public void setPrintJstackInMillis(long printJstackInMillis) {
        this.printJstackInMillis = printJstackInMillis;
    }

    public long getPrintThreadPoolStatusInMillis() {
        return printThreadPoolStatusInMillis;
    }

    public void setPrintThreadPoolStatusInMillis(long printThreadPoolStatusInMillis) {
        this.printThreadPoolStatusInMillis = printThreadPoolStatusInMillis;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public String getNamesrvDomain() {
        return namesrvDomain;
    }

    public void setNamesrvDomain(String namesrvDomain) {
        this.namesrvDomain = namesrvDomain;
    }

    public String getNamesrvDomainSubgroup() {
        return namesrvDomainSubgroup;
    }

    public void setNamesrvDomainSubgroup(String namesrvDomainSubgroup) {
        this.namesrvDomainSubgroup = namesrvDomainSubgroup;
    }

    public String getProxyMode() {
        return proxyMode;
    }

    public void setProxyMode(String proxyMode) {
        this.proxyMode = proxyMode;
    }

    public Integer getGrpcServerPort() {
        return grpcServerPort;
    }

    public void setGrpcServerPort(Integer grpcServerPort) {
        this.grpcServerPort = grpcServerPort;
    }

    public long getGrpcShutdownTimeSeconds() {
        return grpcShutdownTimeSeconds;
    }

    public void setGrpcShutdownTimeSeconds(long grpcShutdownTimeSeconds) {
        this.grpcShutdownTimeSeconds = grpcShutdownTimeSeconds;
    }

    public boolean isUseEndpointPortFromRequest() {
        return useEndpointPortFromRequest;
    }

    public void setUseEndpointPortFromRequest(boolean useEndpointPortFromRequest) {
        this.useEndpointPortFromRequest = useEndpointPortFromRequest;
    }

    public boolean isTlsTestModeEnable() {
        return tlsTestModeEnable;
    }

    public void setTlsTestModeEnable(boolean tlsTestModeEnable) {
        this.tlsTestModeEnable = tlsTestModeEnable;
    }

    public String getTlsKeyPath() {
        return tlsKeyPath;
    }

    public void setTlsKeyPath(String tlsKeyPath) {
        this.tlsKeyPath = tlsKeyPath;
    }

    public String getTlsCertPath() {
        return tlsCertPath;
    }

    public void setTlsCertPath(String tlsCertPath) {
        this.tlsCertPath = tlsCertPath;
    }

    public int getGrpcBossLoopNum() {
        return grpcBossLoopNum;
    }

    public void setGrpcBossLoopNum(int grpcBossLoopNum) {
        this.grpcBossLoopNum = grpcBossLoopNum;
    }

    public int getGrpcWorkerLoopNum() {
        return grpcWorkerLoopNum;
    }

    public void setGrpcWorkerLoopNum(int grpcWorkerLoopNum) {
        this.grpcWorkerLoopNum = grpcWorkerLoopNum;
    }

    public boolean isEnableGrpcEpoll() {
        return enableGrpcEpoll;
    }

    public void setEnableGrpcEpoll(boolean enableGrpcEpoll) {
        this.enableGrpcEpoll = enableGrpcEpoll;
    }

    public int getGrpcThreadPoolNums() {
        return grpcThreadPoolNums;
    }

    public void setGrpcThreadPoolNums(int grpcThreadPoolNums) {
        this.grpcThreadPoolNums = grpcThreadPoolNums;
    }

    public int getGrpcThreadPoolQueueCapacity() {
        return grpcThreadPoolQueueCapacity;
    }

    public void setGrpcThreadPoolQueueCapacity(int grpcThreadPoolQueueCapacity) {
        this.grpcThreadPoolQueueCapacity = grpcThreadPoolQueueCapacity;
    }

    public String getBrokerConfigPath() {
        return brokerConfigPath;
    }

    public void setBrokerConfigPath(String brokerConfigPath) {
        this.brokerConfigPath = brokerConfigPath;
    }

    public int getGrpcMaxInboundMessageSize() {
        return grpcMaxInboundMessageSize;
    }

    public void setGrpcMaxInboundMessageSize(int grpcMaxInboundMessageSize) {
        this.grpcMaxInboundMessageSize = grpcMaxInboundMessageSize;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public int getMaxUserPropertySize() {
        return maxUserPropertySize;
    }

    public void setMaxUserPropertySize(int maxUserPropertySize) {
        this.maxUserPropertySize = maxUserPropertySize;
    }

    public int getUserPropertyMaxNum() {
        return userPropertyMaxNum;
    }

    public void setUserPropertyMaxNum(int userPropertyMaxNum) {
        this.userPropertyMaxNum = userPropertyMaxNum;
    }

    public int getMaxMessageGroupSize() {
        return maxMessageGroupSize;
    }

    public void setMaxMessageGroupSize(int maxMessageGroupSize) {
        this.maxMessageGroupSize = maxMessageGroupSize;
    }

    public long getMinInvisibleTimeMillsForRecv() {
        return minInvisibleTimeMillsForRecv;
    }

    public void setMinInvisibleTimeMillsForRecv(long minInvisibleTimeMillsForRecv) {
        this.minInvisibleTimeMillsForRecv = minInvisibleTimeMillsForRecv;
    }

    public long getDefaultInvisibleTimeMills() {
        return defaultInvisibleTimeMills;
    }

    public void setDefaultInvisibleTimeMills(long defaultInvisibleTimeMills) {
        this.defaultInvisibleTimeMills = defaultInvisibleTimeMills;
    }

    public long getMaxInvisibleTimeMills() {
        return maxInvisibleTimeMills;
    }

    public void setMaxInvisibleTimeMills(long maxInvisibleTimeMills) {
        this.maxInvisibleTimeMills = maxInvisibleTimeMills;
    }

    public long getMaxDelayTimeMills() {
        return maxDelayTimeMills;
    }

    public void setMaxDelayTimeMills(long maxDelayTimeMills) {
        this.maxDelayTimeMills = maxDelayTimeMills;
    }

    public long getMaxTransactionRecoverySecond() {
        return maxTransactionRecoverySecond;
    }

    public void setMaxTransactionRecoverySecond(long maxTransactionRecoverySecond) {
        this.maxTransactionRecoverySecond = maxTransactionRecoverySecond;
    }

    public int getGrpcClientProducerMaxAttempts() {
        return grpcClientProducerMaxAttempts;
    }

    public void setGrpcClientProducerMaxAttempts(int grpcClientProducerMaxAttempts) {
        this.grpcClientProducerMaxAttempts = grpcClientProducerMaxAttempts;
    }

    public long getGrpcClientProducerBackoffInitialMillis() {
        return grpcClientProducerBackoffInitialMillis;
    }

    public void setGrpcClientProducerBackoffInitialMillis(long grpcClientProducerBackoffInitialMillis) {
        this.grpcClientProducerBackoffInitialMillis = grpcClientProducerBackoffInitialMillis;
    }

    public long getGrpcClientProducerBackoffMaxMillis() {
        return grpcClientProducerBackoffMaxMillis;
    }

    public void setGrpcClientProducerBackoffMaxMillis(long grpcClientProducerBackoffMaxMillis) {
        this.grpcClientProducerBackoffMaxMillis = grpcClientProducerBackoffMaxMillis;
    }

    public int getGrpcClientProducerBackoffMultiplier() {
        return grpcClientProducerBackoffMultiplier;
    }

    public void setGrpcClientProducerBackoffMultiplier(int grpcClientProducerBackoffMultiplier) {
        this.grpcClientProducerBackoffMultiplier = grpcClientProducerBackoffMultiplier;
    }

    public long getGrpcClientConsumerMinLongPollingTimeoutMillis() {
        return grpcClientConsumerMinLongPollingTimeoutMillis;
    }

    public void setGrpcClientConsumerMinLongPollingTimeoutMillis(long grpcClientConsumerMinLongPollingTimeoutMillis) {
        this.grpcClientConsumerMinLongPollingTimeoutMillis = grpcClientConsumerMinLongPollingTimeoutMillis;
    }

    public long getGrpcClientConsumerMaxLongPollingTimeoutMillis() {
        return grpcClientConsumerMaxLongPollingTimeoutMillis;
    }

    public void setGrpcClientConsumerMaxLongPollingTimeoutMillis(long grpcClientConsumerMaxLongPollingTimeoutMillis) {
        this.grpcClientConsumerMaxLongPollingTimeoutMillis = grpcClientConsumerMaxLongPollingTimeoutMillis;
    }

    public int getGrpcClientConsumerLongPollingBatchSize() {
        return grpcClientConsumerLongPollingBatchSize;
    }

    public void setGrpcClientConsumerLongPollingBatchSize(int grpcClientConsumerLongPollingBatchSize) {
        this.grpcClientConsumerLongPollingBatchSize = grpcClientConsumerLongPollingBatchSize;
    }

    public int getChannelExpiredInSeconds() {
        return channelExpiredInSeconds;
    }

    public void setChannelExpiredInSeconds(int channelExpiredInSeconds) {
        this.channelExpiredInSeconds = channelExpiredInSeconds;
    }

    public int getContextExpiredInSeconds() {
        return contextExpiredInSeconds;
    }

    public void setContextExpiredInSeconds(int contextExpiredInSeconds) {
        this.contextExpiredInSeconds = contextExpiredInSeconds;
    }

    public int getRocketmqMQClientNum() {
        return rocketmqMQClientNum;
    }

    public void setRocketmqMQClientNum(int rocketmqMQClientNum) {
        this.rocketmqMQClientNum = rocketmqMQClientNum;
    }

    public long getGrpcProxyRelayRequestTimeoutInSeconds() {
        return grpcProxyRelayRequestTimeoutInSeconds;
    }

    public void setGrpcProxyRelayRequestTimeoutInSeconds(long grpcProxyRelayRequestTimeoutInSeconds) {
        this.grpcProxyRelayRequestTimeoutInSeconds = grpcProxyRelayRequestTimeoutInSeconds;
    }

    public int getGrpcProducerThreadPoolNums() {
        return grpcProducerThreadPoolNums;
    }

    public void setGrpcProducerThreadPoolNums(int grpcProducerThreadPoolNums) {
        this.grpcProducerThreadPoolNums = grpcProducerThreadPoolNums;
    }

    public int getGrpcProducerThreadQueueCapacity() {
        return grpcProducerThreadQueueCapacity;
    }

    public void setGrpcProducerThreadQueueCapacity(int grpcProducerThreadQueueCapacity) {
        this.grpcProducerThreadQueueCapacity = grpcProducerThreadQueueCapacity;
    }

    public int getGrpcConsumerThreadPoolNums() {
        return grpcConsumerThreadPoolNums;
    }

    public void setGrpcConsumerThreadPoolNums(int grpcConsumerThreadPoolNums) {
        this.grpcConsumerThreadPoolNums = grpcConsumerThreadPoolNums;
    }

    public int getGrpcConsumerThreadQueueCapacity() {
        return grpcConsumerThreadQueueCapacity;
    }

    public void setGrpcConsumerThreadQueueCapacity(int grpcConsumerThreadQueueCapacity) {
        this.grpcConsumerThreadQueueCapacity = grpcConsumerThreadQueueCapacity;
    }

    public int getGrpcRouteThreadPoolNums() {
        return grpcRouteThreadPoolNums;
    }

    public void setGrpcRouteThreadPoolNums(int grpcRouteThreadPoolNums) {
        this.grpcRouteThreadPoolNums = grpcRouteThreadPoolNums;
    }

    public int getGrpcRouteThreadQueueCapacity() {
        return grpcRouteThreadQueueCapacity;
    }

    public void setGrpcRouteThreadQueueCapacity(int grpcRouteThreadQueueCapacity) {
        this.grpcRouteThreadQueueCapacity = grpcRouteThreadQueueCapacity;
    }

    public int getGrpcClientManagerThreadPoolNums() {
        return grpcClientManagerThreadPoolNums;
    }

    public void setGrpcClientManagerThreadPoolNums(int grpcClientManagerThreadPoolNums) {
        this.grpcClientManagerThreadPoolNums = grpcClientManagerThreadPoolNums;
    }

    public int getGrpcClientManagerThreadQueueCapacity() {
        return grpcClientManagerThreadQueueCapacity;
    }

    public void setGrpcClientManagerThreadQueueCapacity(int grpcClientManagerThreadQueueCapacity) {
        this.grpcClientManagerThreadQueueCapacity = grpcClientManagerThreadQueueCapacity;
    }

    public int getGrpcTransactionThreadPoolNums() {
        return grpcTransactionThreadPoolNums;
    }

    public void setGrpcTransactionThreadPoolNums(int grpcTransactionThreadPoolNums) {
        this.grpcTransactionThreadPoolNums = grpcTransactionThreadPoolNums;
    }

    public int getGrpcTransactionThreadQueueCapacity() {
        return grpcTransactionThreadQueueCapacity;
    }

    public void setGrpcTransactionThreadQueueCapacity(int grpcTransactionThreadQueueCapacity) {
        this.grpcTransactionThreadQueueCapacity = grpcTransactionThreadQueueCapacity;
    }

    public int getProducerProcessorThreadPoolNums() {
        return producerProcessorThreadPoolNums;
    }

    public void setProducerProcessorThreadPoolNums(int producerProcessorThreadPoolNums) {
        this.producerProcessorThreadPoolNums = producerProcessorThreadPoolNums;
    }

    public int getProducerProcessorThreadPoolQueueCapacity() {
        return producerProcessorThreadPoolQueueCapacity;
    }

    public void setProducerProcessorThreadPoolQueueCapacity(int producerProcessorThreadPoolQueueCapacity) {
        this.producerProcessorThreadPoolQueueCapacity = producerProcessorThreadPoolQueueCapacity;
    }

    public int getConsumerProcessorThreadPoolNums() {
        return consumerProcessorThreadPoolNums;
    }

    public void setConsumerProcessorThreadPoolNums(int consumerProcessorThreadPoolNums) {
        this.consumerProcessorThreadPoolNums = consumerProcessorThreadPoolNums;
    }

    public int getConsumerProcessorThreadPoolQueueCapacity() {
        return consumerProcessorThreadPoolQueueCapacity;
    }

    public void setConsumerProcessorThreadPoolQueueCapacity(int consumerProcessorThreadPoolQueueCapacity) {
        this.consumerProcessorThreadPoolQueueCapacity = consumerProcessorThreadPoolQueueCapacity;
    }

    public int getTopicRouteServiceCacheExpiredSeconds() {
        return topicRouteServiceCacheExpiredSeconds;
    }

    public void setTopicRouteServiceCacheExpiredSeconds(int topicRouteServiceCacheExpiredSeconds) {
        this.topicRouteServiceCacheExpiredSeconds = topicRouteServiceCacheExpiredSeconds;
    }

    public int getTopicRouteServiceCacheRefreshSeconds() {
        return topicRouteServiceCacheRefreshSeconds;
    }

    public void setTopicRouteServiceCacheRefreshSeconds(int topicRouteServiceCacheRefreshSeconds) {
        this.topicRouteServiceCacheRefreshSeconds = topicRouteServiceCacheRefreshSeconds;
    }

    public int getTopicRouteServiceCacheMaxNum() {
        return topicRouteServiceCacheMaxNum;
    }

    public void setTopicRouteServiceCacheMaxNum(int topicRouteServiceCacheMaxNum) {
        this.topicRouteServiceCacheMaxNum = topicRouteServiceCacheMaxNum;
    }

    public int getTopicRouteServiceThreadPoolNums() {
        return topicRouteServiceThreadPoolNums;
    }

    public void setTopicRouteServiceThreadPoolNums(int topicRouteServiceThreadPoolNums) {
        this.topicRouteServiceThreadPoolNums = topicRouteServiceThreadPoolNums;
    }

    public int getTopicRouteServiceThreadPoolQueueCapacity() {
        return topicRouteServiceThreadPoolQueueCapacity;
    }

    public void setTopicRouteServiceThreadPoolQueueCapacity(int topicRouteServiceThreadPoolQueueCapacity) {
        this.topicRouteServiceThreadPoolQueueCapacity = topicRouteServiceThreadPoolQueueCapacity;
    }

    public int getTopicConfigCacheRefreshSeconds() {
        return topicConfigCacheRefreshSeconds;
    }

    public void setTopicConfigCacheRefreshSeconds(int topicConfigCacheRefreshSeconds) {
        this.topicConfigCacheRefreshSeconds = topicConfigCacheRefreshSeconds;
    }

    public int getTopicConfigCacheExpiredSeconds() {
        return topicConfigCacheExpiredSeconds;
    }

    public void setTopicConfigCacheExpiredSeconds(int topicConfigCacheExpiredSeconds) {
        this.topicConfigCacheExpiredSeconds = topicConfigCacheExpiredSeconds;
    }

    public int getTopicConfigCacheMaxNum() {
        return topicConfigCacheMaxNum;
    }

    public void setTopicConfigCacheMaxNum(int topicConfigCacheMaxNum) {
        this.topicConfigCacheMaxNum = topicConfigCacheMaxNum;
    }

    public int getSubscriptionGroupConfigCacheRefreshSeconds() {
        return subscriptionGroupConfigCacheRefreshSeconds;
    }

    public void setSubscriptionGroupConfigCacheRefreshSeconds(int subscriptionGroupConfigCacheRefreshSeconds) {
        this.subscriptionGroupConfigCacheRefreshSeconds = subscriptionGroupConfigCacheRefreshSeconds;
    }

    public int getSubscriptionGroupConfigCacheExpiredSeconds() {
        return subscriptionGroupConfigCacheExpiredSeconds;
    }

    public void setSubscriptionGroupConfigCacheExpiredSeconds(int subscriptionGroupConfigCacheExpiredSeconds) {
        this.subscriptionGroupConfigCacheExpiredSeconds = subscriptionGroupConfigCacheExpiredSeconds;
    }

    public int getSubscriptionGroupConfigCacheMaxNum() {
        return subscriptionGroupConfigCacheMaxNum;
    }

    public void setSubscriptionGroupConfigCacheMaxNum(int subscriptionGroupConfigCacheMaxNum) {
        this.subscriptionGroupConfigCacheMaxNum = subscriptionGroupConfigCacheMaxNum;
    }

    public int getUserCacheExpiredSeconds() {
        return userCacheExpiredSeconds;
    }

    public void setUserCacheExpiredSeconds(int userCacheExpiredSeconds) {
        this.userCacheExpiredSeconds = userCacheExpiredSeconds;
    }

    public int getUserCacheRefreshSeconds() {
        return userCacheRefreshSeconds;
    }

    public void setUserCacheRefreshSeconds(int userCacheRefreshSeconds) {
        this.userCacheRefreshSeconds = userCacheRefreshSeconds;
    }

    public int getUserCacheMaxNum() {
        return userCacheMaxNum;
    }

    public void setUserCacheMaxNum(int userCacheMaxNum) {
        this.userCacheMaxNum = userCacheMaxNum;
    }

    public int getAclCacheExpiredSeconds() {
        return aclCacheExpiredSeconds;
    }

    public void setAclCacheExpiredSeconds(int aclCacheExpiredSeconds) {
        this.aclCacheExpiredSeconds = aclCacheExpiredSeconds;
    }

    public int getAclCacheRefreshSeconds() {
        return aclCacheRefreshSeconds;
    }

    public void setAclCacheRefreshSeconds(int aclCacheRefreshSeconds) {
        this.aclCacheRefreshSeconds = aclCacheRefreshSeconds;
    }

    public int getAclCacheMaxNum() {
        return aclCacheMaxNum;
    }

    public void setAclCacheMaxNum(int aclCacheMaxNum) {
        this.aclCacheMaxNum = aclCacheMaxNum;
    }

    public int getMetadataThreadPoolNums() {
        return metadataThreadPoolNums;
    }

    public void setMetadataThreadPoolNums(int metadataThreadPoolNums) {
        this.metadataThreadPoolNums = metadataThreadPoolNums;
    }

    public int getMetadataThreadPoolQueueCapacity() {
        return metadataThreadPoolQueueCapacity;
    }

    public void setMetadataThreadPoolQueueCapacity(int metadataThreadPoolQueueCapacity) {
        this.metadataThreadPoolQueueCapacity = metadataThreadPoolQueueCapacity;
    }

    public int getTransactionHeartbeatThreadPoolNums() {
        return transactionHeartbeatThreadPoolNums;
    }

    public void setTransactionHeartbeatThreadPoolNums(int transactionHeartbeatThreadPoolNums) {
        this.transactionHeartbeatThreadPoolNums = transactionHeartbeatThreadPoolNums;
    }

    public int getTransactionHeartbeatThreadPoolQueueCapacity() {
        return transactionHeartbeatThreadPoolQueueCapacity;
    }

    public void setTransactionHeartbeatThreadPoolQueueCapacity(int transactionHeartbeatThreadPoolQueueCapacity) {
        this.transactionHeartbeatThreadPoolQueueCapacity = transactionHeartbeatThreadPoolQueueCapacity;
    }

    public int getTransactionHeartbeatPeriodSecond() {
        return transactionHeartbeatPeriodSecond;
    }

    public void setTransactionHeartbeatPeriodSecond(int transactionHeartbeatPeriodSecond) {
        this.transactionHeartbeatPeriodSecond = transactionHeartbeatPeriodSecond;
    }

    public int getTransactionHeartbeatBatchNum() {
        return transactionHeartbeatBatchNum;
    }

    public void setTransactionHeartbeatBatchNum(int transactionHeartbeatBatchNum) {
        this.transactionHeartbeatBatchNum = transactionHeartbeatBatchNum;
    }

    public long getTransactionDataExpireScanPeriodMillis() {
        return transactionDataExpireScanPeriodMillis;
    }

    public void setTransactionDataExpireScanPeriodMillis(long transactionDataExpireScanPeriodMillis) {
        this.transactionDataExpireScanPeriodMillis = transactionDataExpireScanPeriodMillis;
    }

    public long getTransactionDataMaxWaitClearMillis() {
        return transactionDataMaxWaitClearMillis;
    }

    public void setTransactionDataMaxWaitClearMillis(long transactionDataMaxWaitClearMillis) {
        this.transactionDataMaxWaitClearMillis = transactionDataMaxWaitClearMillis;
    }

    public long getTransactionDataExpireMillis() {
        return transactionDataExpireMillis;
    }

    public void setTransactionDataExpireMillis(long transactionDataExpireMillis) {
        this.transactionDataExpireMillis = transactionDataExpireMillis;
    }

    public int getTransactionDataMaxNum() {
        return transactionDataMaxNum;
    }

    public void setTransactionDataMaxNum(int transactionDataMaxNum) {
        this.transactionDataMaxNum = transactionDataMaxNum;
    }

    public long getLongPollingReserveTimeInMillis() {
        return longPollingReserveTimeInMillis;
    }

    public void setLongPollingReserveTimeInMillis(long longPollingReserveTimeInMillis) {
        this.longPollingReserveTimeInMillis = longPollingReserveTimeInMillis;
    }

    public boolean isEnableAclRpcHookForClusterMode() {
        return enableAclRpcHookForClusterMode;
    }

    public void setEnableAclRpcHookForClusterMode(boolean enableAclRpcHookForClusterMode) {
        this.enableAclRpcHookForClusterMode = enableAclRpcHookForClusterMode;
    }

    public boolean isEnableTopicMessageTypeCheck() {
        return enableTopicMessageTypeCheck;
    }

    public void setEnableTopicMessageTypeCheck(boolean enableTopicMessageTypeCheck) {
        this.enableTopicMessageTypeCheck = enableTopicMessageTypeCheck;
    }

    public long getInvisibleTimeMillisWhenClear() {
        return invisibleTimeMillisWhenClear;
    }

    public void setInvisibleTimeMillisWhenClear(long invisibleTimeMillisWhenClear) {
        this.invisibleTimeMillisWhenClear = invisibleTimeMillisWhenClear;
    }

    public boolean isEnableProxyAutoRenew() {
        return enableProxyAutoRenew;
    }

    public void setEnableProxyAutoRenew(boolean enableProxyAutoRenew) {
        this.enableProxyAutoRenew = enableProxyAutoRenew;
    }

    public int getMaxRenewRetryTimes() {
        return maxRenewRetryTimes;
    }

    public void setMaxRenewRetryTimes(int maxRenewRetryTimes) {
        this.maxRenewRetryTimes = maxRenewRetryTimes;
    }

    public int getRenewThreadPoolNums() {
        return renewThreadPoolNums;
    }

    public void setRenewThreadPoolNums(int renewThreadPoolNums) {
        this.renewThreadPoolNums = renewThreadPoolNums;
    }

    public int getRenewMaxThreadPoolNums() {
        return renewMaxThreadPoolNums;
    }

    public void setRenewMaxThreadPoolNums(int renewMaxThreadPoolNums) {
        this.renewMaxThreadPoolNums = renewMaxThreadPoolNums;
    }

    public int getRenewThreadPoolQueueCapacity() {
        return renewThreadPoolQueueCapacity;
    }

    public void setRenewThreadPoolQueueCapacity(int renewThreadPoolQueueCapacity) {
        this.renewThreadPoolQueueCapacity = renewThreadPoolQueueCapacity;
    }

    public long getLockTimeoutMsInHandleGroup() {
        return lockTimeoutMsInHandleGroup;
    }

    public void setLockTimeoutMsInHandleGroup(long lockTimeoutMsInHandleGroup) {
        this.lockTimeoutMsInHandleGroup = lockTimeoutMsInHandleGroup;
    }

    public long getRenewAheadTimeMillis() {
        return renewAheadTimeMillis;
    }

    public void setRenewAheadTimeMillis(long renewAheadTimeMillis) {
        this.renewAheadTimeMillis = renewAheadTimeMillis;
    }

    public long getRenewMaxTimeMillis() {
        return renewMaxTimeMillis;
    }

    public void setRenewMaxTimeMillis(long renewMaxTimeMillis) {
        this.renewMaxTimeMillis = renewMaxTimeMillis;
    }

    public long getRenewSchedulePeriodMillis() {
        return renewSchedulePeriodMillis;
    }

    public void setRenewSchedulePeriodMillis(long renewSchedulePeriodMillis) {
        this.renewSchedulePeriodMillis = renewSchedulePeriodMillis;
    }

    public String getMetricCollectorMode() {
        return metricCollectorMode;
    }

    public void setMetricCollectorMode(String metricCollectorMode) {
        this.metricCollectorMode = metricCollectorMode;
    }

    public String getMetricCollectorAddress() {
        return metricCollectorAddress;
    }

    public void setMetricCollectorAddress(String metricCollectorAddress) {
        this.metricCollectorAddress = metricCollectorAddress;
    }

    public boolean isUseDelayLevel() {
        return useDelayLevel;
    }

    public void setUseDelayLevel(boolean useDelayLevel) {
        this.useDelayLevel = useDelayLevel;
    }

    public String getMessageDelayLevel() {
        return messageDelayLevel;
    }

    public void setMessageDelayLevel(String messageDelayLevel) {
        this.messageDelayLevel = messageDelayLevel;
    }

    public ConcurrentSkipListMap<Integer, Long> getDelayLevelTable() {
        return delayLevelTable;
    }

    public long getGrpcClientIdleTimeMills() {
        return grpcClientIdleTimeMills;
    }

    public void setGrpcClientIdleTimeMills(final long grpcClientIdleTimeMills) {
        this.grpcClientIdleTimeMills = grpcClientIdleTimeMills;
    }

    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }

    public boolean isTraceOn() {
        return traceOn;
    }

    public void setTraceOn(boolean traceOn) {
        this.traceOn = traceOn;
    }

    public String getRemotingAccessAddr() {
        return remotingAccessAddr;
    }

    public void setRemotingAccessAddr(String remotingAccessAddr) {
        this.remotingAccessAddr = remotingAccessAddr;
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

    public long getChannelExpiredTimeout() {
        return channelExpiredTimeout;
    }

    public boolean isEnableRemotingLocalProxyGrpc() {
        return enableRemotingLocalProxyGrpc;
    }

    public void setChannelExpiredTimeout(long channelExpiredTimeout) {
        this.channelExpiredTimeout = channelExpiredTimeout;
    }

    public void setEnableRemotingLocalProxyGrpc(boolean enableRemotingLocalProxyGrpc) {
        this.enableRemotingLocalProxyGrpc = enableRemotingLocalProxyGrpc;
    }

    public int getLocalProxyConnectTimeoutMs() {
        return localProxyConnectTimeoutMs;
    }

    public void setLocalProxyConnectTimeoutMs(int localProxyConnectTimeoutMs) {
        this.localProxyConnectTimeoutMs = localProxyConnectTimeoutMs;
    }

    public int getRemotingListenPort() {
        return remotingListenPort;
    }

    public void setRemotingListenPort(int remotingListenPort) {
        this.remotingListenPort = remotingListenPort;
    }

    public int getRemotingHeartbeatThreadPoolNums() {
        return remotingHeartbeatThreadPoolNums;
    }

    public void setRemotingHeartbeatThreadPoolNums(int remotingHeartbeatThreadPoolNums) {
        this.remotingHeartbeatThreadPoolNums = remotingHeartbeatThreadPoolNums;
    }

    public int getRemotingTopicRouteThreadPoolNums() {
        return remotingTopicRouteThreadPoolNums;
    }

    public void setRemotingTopicRouteThreadPoolNums(int remotingTopicRouteThreadPoolNums) {
        this.remotingTopicRouteThreadPoolNums = remotingTopicRouteThreadPoolNums;
    }

    public int getRemotingSendMessageThreadPoolNums() {
        return remotingSendMessageThreadPoolNums;
    }

    public void setRemotingSendMessageThreadPoolNums(int remotingSendMessageThreadPoolNums) {
        this.remotingSendMessageThreadPoolNums = remotingSendMessageThreadPoolNums;
    }

    public int getRemotingPullMessageThreadPoolNums() {
        return remotingPullMessageThreadPoolNums;
    }

    public void setRemotingPullMessageThreadPoolNums(int remotingPullMessageThreadPoolNums) {
        this.remotingPullMessageThreadPoolNums = remotingPullMessageThreadPoolNums;
    }

    public int getRemotingUpdateOffsetThreadPoolNums() {
        return remotingUpdateOffsetThreadPoolNums;
    }

    public void setRemotingUpdateOffsetThreadPoolNums(int remotingUpdateOffsetThreadPoolNums) {
        this.remotingUpdateOffsetThreadPoolNums = remotingUpdateOffsetThreadPoolNums;
    }

    public int getRemotingDefaultThreadPoolNums() {
        return remotingDefaultThreadPoolNums;
    }

    public void setRemotingDefaultThreadPoolNums(int remotingDefaultThreadPoolNums) {
        this.remotingDefaultThreadPoolNums = remotingDefaultThreadPoolNums;
    }

    public int getRemotingHeartbeatThreadPoolQueueCapacity() {
        return remotingHeartbeatThreadPoolQueueCapacity;
    }

    public void setRemotingHeartbeatThreadPoolQueueCapacity(int remotingHeartbeatThreadPoolQueueCapacity) {
        this.remotingHeartbeatThreadPoolQueueCapacity = remotingHeartbeatThreadPoolQueueCapacity;
    }

    public int getRemotingTopicRouteThreadPoolQueueCapacity() {
        return remotingTopicRouteThreadPoolQueueCapacity;
    }

    public void setRemotingTopicRouteThreadPoolQueueCapacity(int remotingTopicRouteThreadPoolQueueCapacity) {
        this.remotingTopicRouteThreadPoolQueueCapacity = remotingTopicRouteThreadPoolQueueCapacity;
    }

    public int getRemotingSendThreadPoolQueueCapacity() {
        return remotingSendThreadPoolQueueCapacity;
    }

    public void setRemotingSendThreadPoolQueueCapacity(int remotingSendThreadPoolQueueCapacity) {
        this.remotingSendThreadPoolQueueCapacity = remotingSendThreadPoolQueueCapacity;
    }

    public int getRemotingPullThreadPoolQueueCapacity() {
        return remotingPullThreadPoolQueueCapacity;
    }

    public void setRemotingPullThreadPoolQueueCapacity(int remotingPullThreadPoolQueueCapacity) {
        this.remotingPullThreadPoolQueueCapacity = remotingPullThreadPoolQueueCapacity;
    }

    public int getRemotingUpdateOffsetThreadPoolQueueCapacity() {
        return remotingUpdateOffsetThreadPoolQueueCapacity;
    }

    public void setRemotingUpdateOffsetThreadPoolQueueCapacity(int remotingUpdateOffsetThreadPoolQueueCapacity) {
        this.remotingUpdateOffsetThreadPoolQueueCapacity = remotingUpdateOffsetThreadPoolQueueCapacity;
    }

    public int getRemotingDefaultThreadPoolQueueCapacity() {
        return remotingDefaultThreadPoolQueueCapacity;
    }

    public void setRemotingDefaultThreadPoolQueueCapacity(int remotingDefaultThreadPoolQueueCapacity) {
        this.remotingDefaultThreadPoolQueueCapacity = remotingDefaultThreadPoolQueueCapacity;
    }

    public long getRemotingWaitTimeMillsInSendQueue() {
        return remotingWaitTimeMillsInSendQueue;
    }

    public void setRemotingWaitTimeMillsInSendQueue(long remotingWaitTimeMillsInSendQueue) {
        this.remotingWaitTimeMillsInSendQueue = remotingWaitTimeMillsInSendQueue;
    }

    public long getRemotingWaitTimeMillsInPullQueue() {
        return remotingWaitTimeMillsInPullQueue;
    }

    public void setRemotingWaitTimeMillsInPullQueue(long remotingWaitTimeMillsInPullQueue) {
        this.remotingWaitTimeMillsInPullQueue = remotingWaitTimeMillsInPullQueue;
    }

    public long getRemotingWaitTimeMillsInHeartbeatQueue() {
        return remotingWaitTimeMillsInHeartbeatQueue;
    }

    public void setRemotingWaitTimeMillsInHeartbeatQueue(long remotingWaitTimeMillsInHeartbeatQueue) {
        this.remotingWaitTimeMillsInHeartbeatQueue = remotingWaitTimeMillsInHeartbeatQueue;
    }

    public long getRemotingWaitTimeMillsInUpdateOffsetQueue() {
        return remotingWaitTimeMillsInUpdateOffsetQueue;
    }

    public void setRemotingWaitTimeMillsInUpdateOffsetQueue(long remotingWaitTimeMillsInUpdateOffsetQueue) {
        this.remotingWaitTimeMillsInUpdateOffsetQueue = remotingWaitTimeMillsInUpdateOffsetQueue;
    }

    public long getRemotingWaitTimeMillsInTopicRouteQueue() {
        return remotingWaitTimeMillsInTopicRouteQueue;
    }

    public void setRemotingWaitTimeMillsInTopicRouteQueue(long remotingWaitTimeMillsInTopicRouteQueue) {
        this.remotingWaitTimeMillsInTopicRouteQueue = remotingWaitTimeMillsInTopicRouteQueue;
    }

    public long getRemotingWaitTimeMillsInDefaultQueue() {
        return remotingWaitTimeMillsInDefaultQueue;
    }

    public void setRemotingWaitTimeMillsInDefaultQueue(long remotingWaitTimeMillsInDefaultQueue) {
        this.remotingWaitTimeMillsInDefaultQueue = remotingWaitTimeMillsInDefaultQueue;
    }

    public boolean isSendLatencyEnable() {
        return sendLatencyEnable;
    }

    public boolean isStartDetectorEnable() {
        return startDetectorEnable;
    }

    public void setStartDetectorEnable(boolean startDetectorEnable) {
        this.startDetectorEnable = startDetectorEnable;
    }

    public void setSendLatencyEnable(boolean sendLatencyEnable) {
        this.sendLatencyEnable = sendLatencyEnable;
    }

    public boolean getStartDetectorEnable() {
        return this.startDetectorEnable;
    }

    public boolean getSendLatencyEnable() {
        return this.sendLatencyEnable;
    }

    public int getDetectTimeout() {
        return detectTimeout;
    }

    public void setDetectTimeout(int detectTimeout) {
        this.detectTimeout = detectTimeout;
    }

    public int getDetectInterval() {
        return detectInterval;
    }

    public void setDetectInterval(int detectInterval) {
        this.detectInterval = detectInterval;
    }

    public boolean isEnableBatchAck() {
        return enableBatchAck;
    }

    public void setEnableBatchAck(boolean enableBatchAck) {
        this.enableBatchAck = enableBatchAck;
    }

    public boolean isEnableMessageBodyEmptyCheck() {
        return enableMessageBodyEmptyCheck;
    }

    public void setEnableMessageBodyEmptyCheck(boolean enableMessageBodyEmptyCheck) {
        this.enableMessageBodyEmptyCheck = enableMessageBodyEmptyCheck;
    }
}
