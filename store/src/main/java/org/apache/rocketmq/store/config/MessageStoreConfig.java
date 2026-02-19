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
package org.apache.rocketmq.store.config;

import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.StoreType;
import org.apache.rocketmq.store.queue.BatchConsumeQueue;
import org.rocksdb.CompressionType;
import org.rocksdb.util.SizeUnit;

import java.io.File;

/**
 * 消息存储配置<br>
 * 定义 Broker 存储模块运行参数
 */
public class MessageStoreConfig {

    /**
     * 多存储路径分隔符
     */
    public static final String MULTI_PATH_SPLITTER = System.getProperty("rocketmq.broker.multiPathSplitter", ",");

    //The root directory in which the log data is kept
    // 存储日志数据的根目录
    @ImportantField
    private String storePathRootDir = System.getProperty("user.home") + File.separator + "store";

    //The directory in which the commitlog is kept
    // CommitLog 存储路径
    @ImportantField
    private String storePathCommitLog = null;

    /**
     * DLedger CommitLog 存储路径
     */
    @ImportantField
    private String storePathDLedgerCommitLog = null;

    //The directory in which the epochFile is kept
    // Epoch 文件存储路径
    @ImportantField
    private String storePathEpochFile = null;

    /**
     * Broker 标识文件路径
     */
    @ImportantField
    private String storePathBrokerIdentity = null;

    /**
     * 只读 CommitLog 存储路径列表
     */
    private String readOnlyCommitLogStorePaths = null;

    // CommitLog file size,default is 1G
    // CommitLog 映射文件大小
    private int mappedFileSizeCommitLog = 1024 * 1024 * 1024;

    // CompactionLog file size, default is 100M
    // CompactionLog 映射文件大小
    private int compactionMappedFileSize = 100 * 1024 * 1024;

    // CompactionLog consumeQueue file size, default is 10M
    // Compaction ConsumeQueue 映射文件大小
    private int compactionCqMappedFileSize = 10 * 1024 * 1024;

    /**
     * 压缩任务调度间隔
     */
    private int compactionScheduleInternal = 15 * 60 * 1000;

    /**
     * 位点映射最大大小
     */
    private int maxOffsetMapSize = 100 * 1024 * 1024;

    /**
     * 压缩线程数量
     */
    private int compactionThreadNum = 6;

    /**
     * 是否启用压缩
     */
    private boolean enableCompaction = true;

    // TimerLog file size, default is 100M
    // TimerLog 映射文件大小
    private int mappedFileSizeTimerLog = 100 * 1024 * 1024;

    /**
     * 定时器精度毫秒数
     */
    private int timerPrecisionMs = 1000;

    /**
     * 定时器滚动窗口槽位数
     */
    private int timerRollWindowSlot = 3600 * 24 * 2;
    /**
     * 定时器刷盘间隔毫秒数
     */
    private int timerFlushIntervalMs = 1000;
    /**
     * 定时器拉消息线程数量
     */
    private int timerGetMessageThreadNum = 3;
    /**
     * 定时器写消息线程数量
     */
    private int timerPutMessageThreadNum = 3;

    /**
     * 定时器 Disruptor 开关
     */
    private boolean timerEnableDisruptor = false;

    /**
     * 定时器指标检查开关
     */
    private boolean timerEnableCheckMetrics = true;
    /**
     * 定时器延迟级别拦截开关
     */
    private boolean timerInterceptDelayLevel = false;
    /**
     * 定时器最大延迟秒数
     */
    private int timerMaxDelaySec = 3600 * 24 * 3;
    /**
     * 定时器时间轮开关
     */
    private boolean timerWheelEnable = true;

    /**
     * 1. Register to broker after (startTime + disappearTimeAfterStart)
     * 2. Internal msg exchange will start after (startTime + disappearTimeAfterStart)
     * A. PopReviveService
     * B. TimerDequeueGetService
     * <br>
      * disappeartimeafterstart 配置项
     */
    @ImportantField
    private int disappearTimeAfterStart = -1;

    /**
     * 定时器停止入队开关
     */
    private boolean timerStopEnqueue = false;

    /**
     * 定时器指标检查时机
     */
    private String timerCheckMetricsWhen = "05";

    /**
     * 定时器跳过未知错误开关
     */
    private boolean timerSkipUnknownError = false;
    /**
     * 定时器预热开关
     */
    private boolean timerWarmEnable = false;
    /**
     * 定时器停止出队开关
     */
    private boolean timerStopDequeue = false;
    /**
     * 定时器重试直到成功开关
     */
    private boolean timerEnableRetryUntilSuccess = false;
    /**
     * 定时器单槽位拥塞阈值
     */
    private int timerCongestNumEachSlot = Integer.MAX_VALUE;

    /**
     * 定时器小指标阈值
     */
    private int timerMetricSmallThreshold = 1000000;
    /**
     * 定时器进度日志间隔毫秒数
     */
    private int timerProgressLogIntervalMs = 10 * 1000;

    // default, defaultRocksDB
    // 存储类型默认值 defaultRocksDB
    @ImportantField
    private String storeType = StoreType.DEFAULT.getStoreType();

    // ConsumeQueue file size,default is 30W
    // ConsumeQueue 映射文件大小
    private int mappedFileSizeConsumeQueue = 300000 * ConsumeQueue.CQ_STORE_UNIT_SIZE;
    // enable consume queue ext
    // ConsumeQueue 扩展开关
    private boolean enableConsumeQueueExt = false;
    // ConsumeQueue extend file size, 48M
    // ConsumeQueue 扩展映射文件大小
    private int mappedFileSizeConsumeQueueExt = 48 * 1024 * 1024;
    /**
     * BatchConsumeQueue 映射文件大小
     */
    private int mapperFileSizeBatchConsumeQueue = 300000 * BatchConsumeQueue.CQ_STORE_UNIT_SIZE;
    // Bit count of filter bit map.
    // this will be set by pipe of calculate filter bit map.
    // 过滤位图长度
    // 该值由过滤位图计算流程设置
    private int bitMapLengthConsumeQueueExt = 64;

    // CommitLog flush interval
    // flush data to disk
    // CommitLog 刷盘间隔
    // 将数据刷入磁盘
    @ImportantField
    private int flushIntervalCommitLog = 500;

    // Only used if TransientStorePool enabled
    // flush data to FileChannel
    // 仅在启用暂存存储池时生效
    // 将数据提交到 FileChannel
    // CommitLog 提交间隔
    @ImportantField
    private int commitIntervalCommitLog = 200;

    /**
     * CommitLog 恢复扫描文件数量上限
     */
    private int maxRecoveryCommitlogFiles = 30;

    /**
     * 磁盘空间告警比例
     */
    private int diskSpaceWarningLevelRatio = 90;

    /**
     * 磁盘空间强制清理比例
     */
    private int diskSpaceCleanForciblyRatio = 85;

    /**
     * introduced since 4.0.x. Determine whether to use mutex reentrantLock when putting message.<br/>
      * 写消息时可重入锁开关
     */
    private boolean useReentrantLockWhenPutMessage = true;

    // Whether schedule flush
    // CommitLog 定时刷盘开关
    @ImportantField
    private boolean flushCommitLogTimed = true;
    // ConsumeQueue flush interval
    // ConsumeQueue 刷盘间隔
    private int flushIntervalConsumeQueue = 1000;
    // Resource reclaim interval
    // 资源清理间隔
    private int cleanResourceInterval = 10000;
    // CommitLog removal interval
    // CommitLog 文件删除间隔
    private int deleteCommitLogFilesInterval = 100;
    // ConsumeQueue removal interval
    // ConsumeQueue 文件删除间隔
    private int deleteConsumeQueueFilesInterval = 100;
    /**
     * 强制销毁映射文件间隔
     */
    private int destroyMapedFileIntervalForcibly = 1000 * 120;
    /**
     * 挂起文件重删间隔
     */
    private int redeleteHangedFileInterval = 1000 * 120;
    // When to delete,default is at 4 am
    // 文件删除执行时机
    @ImportantField
    private String deleteWhen = "04";
    /**
     * 磁盘已用空间比例上限
     */
    private int diskMaxUsedSpaceRatio = 75;
    // The number of hours to keep a log file before deleting it (in hours)
    // 文件保留时长
    @ImportantField
    private int fileReservedTime = 72;
    /**
     * 批量删除文件数量上限
     */
    @ImportantField
    private int deleteFileBatchMax = 10;
    // Flow control for ConsumeQueue
    // 写索引高水位阈值
    private int putMsgIndexHightWater = 600000;
    // The maximum size of message body,default is 4M,4M only for body length,not include others.
    // 单条消息最大大小
    private int maxMessageSize = 1024 * 1024 * 4;

    // The maximum size of message body can be  set in config;count with maxMsgNums * CQ_STORE_UNIT_SIZE(20 || 46)
    // 过滤消息最大大小
    private int maxFilterMessageSize = 16000;
    // Whether check the CRC32 of the records consumed.
    // This ensures no on-the-wire or on-disk corruption to the messages occurred.
    // This check adds some overhead,so it may be disabled in cases seeking extreme performance.
    // 是否校验已消费记录的 CRC32
    // 确保传输或落盘阶段未发生消息损坏
    // 恢复过程 CRC 校验开关
    private boolean checkCRCOnRecover = true;
    // How many pages are to be flushed when flush CommitLog
    // CommitLog 最少刷盘页数
    private int flushCommitLogLeastPages = 4;
    // How many pages are to be committed when commit data to file
    // CommitLog 最少提交页数
    private int commitCommitLogLeastPages = 4;
    // Flush page size when the disk in warming state
    // 预热映射文件时最少刷盘页数
    private int flushLeastPagesWhenWarmMapedFile = 1024 / 4 * 16;
    // How many pages are to be flushed when flush ConsumeQueue
    // ConsumeQueue 最少刷盘页数
    private int flushConsumeQueueLeastPages = 2;
    /**
     * CommitLog 彻底刷盘间隔
     */
    private int flushCommitLogThoroughInterval = 1000 * 10;
    /**
     * CommitLog 彻底提交间隔
     */
    private int commitCommitLogThoroughInterval = 200;
    /**
     * ConsumeQueue 彻底刷盘间隔
     */
    private int flushConsumeQueueThoroughInterval = 1000 * 60;
    /**
     * 内存消息最大传输字节数
     */
    @ImportantField
    private int maxTransferBytesOnMessageInMemory = 1024 * 256;
    /**
     * 内存消息最大传输条数
     */
    @ImportantField
    private int maxTransferCountOnMessageInMemory = 32;
    /**
     * 磁盘消息最大传输字节数
     */
    @ImportantField
    private int maxTransferBytesOnMessageInDisk = 1024 * 64;
    /**
     * 磁盘消息最大传输条数
     */
    @ImportantField
    private int maxTransferCountOnMessageInDisk = 8;
    /**
     * 内存消息访问比例上限
     */
    @ImportantField
    private int accessMessageInMemoryMaxRatio = 40;
    /**
     * 消息索引开关
     */
    @ImportantField
    private boolean messageIndexEnable = true;
    /**
     * 索引哈希槽数量上限
     */
    private int maxHashSlotNum = 5000000;
    /**
     * 索引条目数量上限
     */
    private int maxIndexNum = 5000000 * 4;
    /**
     * 批量消息数量上限
     */
    private int maxMsgsNumBatch = 64;
    /**
     * 消息索引安全模式开关
     */
    @ImportantField
    private boolean messageIndexSafe = false;
    /**
     * HA 监听端口
     */
    private int haListenPort = 10912;
    /**
     * HA 心跳发送间隔
     */
    private int haSendHeartbeatInterval = 1000 * 5;
    /**
     * HA 保活间隔
     */
    private int haHousekeepingInterval = 1000 * 20;
    /**
     * Maximum size of data to transfer to slave.
     * NOTE: cannot be larger than HAClient.READ_MAX_BUFFER_SIZE
     * <br>
      * HA 批量传输大小
     */
    private int haTransferBatchSize = 1024 * 32;
    /**
     * HA 主节点地址
     */
    @ImportantField
    private String haMasterAddress = null;
    /**
     * HA 不同步最大差值
     */
    private int haMaxGapNotInSync = 1024 * 1024 * 256;
    /**
     * Broker 角色
     */
    @ImportantField
    private volatile BrokerRole brokerRole = BrokerRole.ASYNC_MASTER;
    /**
     * 刷盘类型
     */
    @ImportantField
    private FlushDiskType flushDiskType = FlushDiskType.ASYNC_FLUSH;
    // Used by GroupTransferService to sync messages from master to slave
    // 同步刷盘超时时间
    private int syncFlushTimeout = 1000 * 5;
    // Used by PutMessage to wait messages be flushed to disk and synchronized in current broker member group.
    // 写消息超时时间
    private int putMessageTimeout = 1000 * 8;
    /**
     * 从节点超时时间
     */
    private int slaveTimeout = 3000;
    /**
     * 延迟消息级别定义
     */
    private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
    /**
     * 延迟位点刷盘间隔
     */
    private long flushDelayOffsetInterval = 1000 * 10;
    /**
     * 强制清理文件开关
     */
    @ImportantField
    private boolean cleanFileForciblyEnable = true;
    /**
     * 映射文件预热开关
     */
    private boolean warmMapedFileEnable = false;
    /**
     * 从节点位点校验开关
     */
    private boolean offsetCheckInSlave = false;
    /**
     * 调试锁开关
     */
    private boolean debugLockEnable = false;
    /**
     * 复制模式开关
     */
    private boolean duplicationEnable = false;
    /**
     * 磁盘落后记录开关
     */
    private boolean diskFallRecorded = true;
    /**
     * OS PageCache 繁忙超时时间
     */
    private long osPageCacheBusyTimeOutMills = 1000;
    /**
     * 默认查询数量上限
     */
    private int defaultQueryMaxNum = 32;

    /**
     * 暂存存储池开关
     */
    @ImportantField
    private boolean transientStorePoolEnable = false;
    /**
     * 暂存存储池大小
     */
    private int transientStorePoolSize = 5;
    /**
     * 存储池无缓冲快速失败开关
     */
    private boolean fastFailIfNoBufferInStorePool = false;

    // DLedger message store config
    // DLedger CommitLog 开关
    private boolean enableDLegerCommitLog = false;
    /**
     * DLedger 组标识
     */
    private String dLegerGroup;
    /**
     * DLedger 节点列表
     */
    private String dLegerPeers;
    /**
     * DLedger 本机 ID
     */
    private String dLegerSelfId;
    /**
     * 优选 Leader ID
     */
    private String preferredLeaderId;
    /**
     * 是否启用批量推送
     */
    private boolean enableBatchPush = false;

    /**
     * 是否启用调度消息统计
     */
    private boolean enableScheduleMessageStats = true;

    /**
     * 是否启用 LMQ
     */
    private boolean enableLmq = false;
    /**
     * 是否启用多路分发
     */
    private boolean enableMultiDispatch = false;
    /**
     * LMQ ConsumeQueue 数量上限
     */
    private int maxLmqConsumeQueueNum = 20000;

    /**
     * 是否启用调度异步投递
     */
    private boolean enableScheduleAsyncDeliver = false;
    /**
     * 调度异步投递待处理上限
     */
    private int scheduleAsyncDeliverMaxPendingLimit = 2000;
    /**
     * 调度异步投递阻塞前最大重试次数
     */
    private int scheduleAsyncDeliverMaxResendNum2Blocked = 3;

    /**
     * 批量删除文件数量上限
     */
    private int maxBatchDeleteFilesNum = 50;
    //Polish dispatch
    // ConsumeQueue 分发线程数量
    private int dispatchCqThreads = 10;
    /**
     * ConsumeQueue 分发缓存数量
     */
    private int dispatchCqCacheNum = 1024 * 4;
    /**
     * 是否启用异步 Reput
     */
    private boolean enableAsyncReput = true;
    //For recheck the reput
    // ConsumeQueue Reput 位点重检开关
    private boolean recheckReputOffsetFromCq = false;

    // Maximum length of topic, it will be removed in the future release
    // 主题最大长度
    @Deprecated
    private int maxTopicLength = Byte.MAX_VALUE;

    /**
     * Use MessageVersion.MESSAGE_VERSION_V2 automatically if topic length larger than Bytes.MAX_VALUE.
     * Otherwise, store use MESSAGE_VERSION_V1. Note: Client couldn't decode MESSAGE_VERSION_V2 version message.
     * Enable this config to resolve this issue. https://github.com/apache/rocketmq/issues/5568
     * <br>
      * 按主题长度自动升级消息版本开关
     */
    private boolean autoMessageVersionOnTopicLen = true;

    /**
     * It cannot be changed after the broker is started.
     * Modifications need to be restarted to take effect.
     * <br>
      * 属性 CRC 追加开关
     */
    private boolean enabledAppendPropCRC = false;
    /**
     * 属性 CRC 强制校验开关
     */
    private boolean forceVerifyPropCRC = false;
    /**
     * 拉消息遍历 ConsumeQueue 文件数量
     */
    private int travelCqFileNumWhenGetMessage = 1;
    // Sleep interval between to corrections
    // 修正逻辑最小位点休眠间隔
    private int correctLogicMinOffsetSleepInterval = 1;
    // Force correct min offset interval
    // 强制修正逻辑最小位点间隔
    private int correctLogicMinOffsetForceInterval = 5 * 60 * 1000;
    // swap
    // 映射文件置换开关
    private boolean mappedFileSwapEnable = true;
    /**
     * CommitLog 强制置换映射间隔
     */
    private long commitLogForceSwapMapInterval = 12L * 60 * 60 * 1000;
    /**
     * CommitLog 置换映射间隔
     */
    private long commitLogSwapMapInterval = 1L * 60 * 60 * 1000;
    /**
     * CommitLog 置换映射保留文件数量
     */
    private int commitLogSwapMapReserveFileNum = 100;
    /**
     * 逻辑队列强制置换映射间隔
     */
    private long logicQueueForceSwapMapInterval = 12L * 60 * 60 * 1000;
    /**
     * 逻辑队列置换映射间隔
     */
    private long logicQueueSwapMapInterval = 1L * 60 * 60 * 1000;
    /**
     * 置换映射清理间隔
     */
    private long cleanSwapedMapInterval = 5L * 60 * 1000;
    /**
     * 逻辑队列置换映射保留文件数量
     */
    private int logicQueueSwapMapReserveFileNum = 20;

    /**
     * 是否通过缓存搜索 BCQ
     */
    private boolean searchBcqByCacheEnable = true;

    /**
     * 发送线程分发开关
     */
    @ImportantField
    private boolean dispatchFromSenderThread = false;

    /**
     * 写消息时唤醒 Commit 开关
     */
    @ImportantField
    private boolean wakeCommitWhenPutMessage = true;
    /**
     * 写消息时唤醒 Flush 开关
     */
    @ImportantField
    private boolean wakeFlushWhenPutMessage = false;

    /**
     * 是否启用过期位点清理
     */
    @ImportantField
    private boolean enableCleanExpiredOffset = false;

    /**
     * 异步写消息请求上限
     */
    private int maxAsyncPutMessageRequests = 5000;

    /**
     * 批量拉消息数量上限
     */
    private int pullBatchMaxMessageCount = 160;

    /**
     * 副本总数
     */
    @ImportantField
    private int totalReplicas = 1;

    /**
     * Each message must be written successfully to at least in-sync replicas.
     * The master broker is considered one of the in-sync replicas, and it's included in the count of total.
     * If a master broker is ASYNC_MASTER, inSyncReplicas will be ignored.
     * If enableControllerMode is true and ackAckInSyncStateSet is true, inSyncReplicas will be ignored.
     * <br>
      * INsyncreplicas 配置项
     */
    @ImportantField
    private int inSyncReplicas = 1;

    /**
     * Will be worked in auto multiple replicas mode, to provide minimum in-sync replicas.
     * It is still valid in controller mode.
     * <br>
      * MININsyncreplicas 配置项
     */
    @ImportantField
    private int minInSyncReplicas = 1;

    /**
     * Each message must be written successfully to all replicas in SyncStateSet.
     * <br>
      * SyncStateSet 全量确认开关
     */
    @ImportantField
    private boolean allAckInSyncStateSet = false;

    /**
     * Dynamically adjust in-sync replicas to provide higher availability, the real time in-sync replicas
     * will smaller than inSyncReplicas config.
     * <br>
      * 自动同步副本调整开关
     */
    @ImportantField
    private boolean enableAutoInSyncReplicas = false;

    /**
     * Enable or not ha flow control
     * <br>
      * HA 流控开关
     */
    @ImportantField
    private boolean haFlowControlEnable = false;

    /**
     * The max speed for one slave when transfer data in ha
     * <br>
      * HA 每秒最大传输字节数
     */
    private long maxHaTransferByteInSecond = 100 * 1024 * 1024;

    /**
     * The max gap time that slave doesn't catch up to master.
     * <br>
      * 从节点未追平主节点最大时长
     */
    private long haMaxTimeSlaveNotCatchup = 1000 * 15;

    /**
     * Sync flush offset from master when broker startup, used in upgrading from old version broker.
     * <br>
      * 启动时同步主节点刷盘位点开关
     */
    private boolean syncMasterFlushOffsetWhenStartup = false;

    /**
     * Max checksum range.
     * <br>
      * 最大校验范围
     */
    private long maxChecksumRange = 1024 * 1024 * 1024;

    /**
     * 每个磁盘分区副本数
     */
    private int replicasPerDiskPartition = 1;

    /**
     * 逻辑磁盘空间强制清理阈值
     */
    private double logicalDiskSpaceCleanForciblyThreshold = 0.8;

    /**
     * 从节点最大重发长度
     */
    private long maxSlaveResendLength = 256 * 1024 * 1024;

    /**
     * Whether sync from lastFile when a new broker replicas(no data) join the master.
     * <br>
      * 从最后文件开始同步开关
     */
    private boolean syncFromLastFile = false;

    /**
     * 异步 Learner 开关
     */
    private boolean asyncLearner = false;

    /**
     * Number of records to scan before starting to estimate.
     * <br>
      * ConsumeQueue 最大扫描记录数
     */
    private int maxConsumeQueueScan = 20_000;

    /**
     * Number of matched records before starting to estimate.
     * <br>
      * 采样命中阈值
     */
    private int sampleCountThreshold = 5000;

    /**
     * 冷数据流控开关
     */
    private boolean coldDataFlowControlEnable = false;
    /**
     * 冷数据扫描开关
     */
    private boolean coldDataScanEnable = false;
    /**
     * 数据预读开关
     */
    private boolean dataReadAheadEnable = true;
    /**
     * 定时器冷数据检查间隔
     */
    private int timerColdDataCheckIntervalMs = 60 * 1000;
    /**
     * 采样步长
     */
    private int sampleSteps = 32;
    /**
     * 内存热点消息访问比例
     */
    private int accessMessageInMemoryHotRatio = 26;
    /**
     * Build ConsumeQueue concurrently with multi-thread
     * <br>
      * 并发构建 ConsumeQueue 开关
     */
    private boolean enableBuildConsumeQueueConcurrently = false;

    /**
     * 批量分发请求线程池大小
     */
    private int batchDispatchRequestThreadPoolNums = 16;

    // rocksdb mode
    // 清理 RocksDB 脏 ConsumeQueue 间隔
    private long cleanRocksDBDirtyCQIntervalMin = 60;
    /**
     * RocksDB ConsumeQueue 统计间隔秒数
     */
    private long statRocksDBCQIntervalSec = 10;
    /**
     * MemTable 刷盘间隔毫秒数
     */
    private long memTableFlushIntervalMs = 60 * 60 * 1000L;
    /**
     * RocksDB 配置实时持久化开关
     */
    private boolean realTimePersistRocksDBConfig = true;
    /**
     * RocksDB 日志开关
     */
    private boolean enableRocksDBLog = false;

    /**
     * 主题队列锁数量
     */
    private int topicQueueLockNum = 32;

    /**
     * If readUnCommitted is true, the dispatch of the consume queue will exceed the confirmOffset, which may cause the client to read uncommitted messages.
     * For example, reput offset exceeding the flush offset during synchronous disk flushing.
     * <br>
      * 读取未提交消息开关
     */
    private boolean readUnCommitted = false;

    /**
     * FileChannel 写 ConsumeQueue 数据开关
     */
    private boolean putConsumeQueueDataByFileChannel = true;

    /**
     * RocksDB ConsumeQueue 双写开关
     */
    private boolean rocksdbCQDoubleWriteEnable = false;

    /**
     * If ConsumeQueueStore is RocksDB based, this option is to configure bottom-most tier compression type.
     * The following values are valid:
     * <br>
     * 当 ConsumeQueueStore 使用 RocksDB 时, 该选项用于配置最底层压缩类型
     * <ul>
     *     <li>snappy</li>
     *     <li>z</li>
     *     <li>bzip2</li>
     *     <li>lz4</li>
     *     <li>lz4hc</li>
     *     <li>xpress</li>
     *     <li>zstd</li>
     * </ul>
     *
     * LZ4 is the recommended one.
     */
    private String bottomMostCompressionTypeForConsumeQueueStore = CompressionType.ZSTD_COMPRESSION.getLibraryName();

    /**
     * RocksDB 压缩类型
     */
    private String rocksdbCompressionType = CompressionType.LZ4_COMPRESSION.getLibraryName();

    /**
     * Flush RocksDB WAL frequency, aka, flush WAL every N write ops.
     * <br>
      * RocksDB WAL 刷盘频率
     */
    private int rocksdbFlushWalFrequency = 1024;

    /**
     * RocksDB WAL 文件滚动阈值
     */
    private long rocksdbWalFileRollingThreshold = SizeUnit.GB;

    public String getRocksdbCompressionType() {
        return rocksdbCompressionType;
    }

    public void setRocksdbCompressionType(String compressionType) {
        this.rocksdbCompressionType = compressionType;
    }

    /**
     * Spin number in the retreat strategy of spin lock
     * Default is 1000
      * 自旋锁冲突退避次数
     */
    private int spinLockCollisionRetreatOptimalDegree = 1000;

    /**
     * Use AdaptiveBackOffLock
      * ABS 锁开关
     **/
    private boolean useABSLock = false;

    public boolean isRocksdbCQDoubleWriteEnable() {
        return rocksdbCQDoubleWriteEnable;
    }

    public void setRocksdbCQDoubleWriteEnable(boolean rocksdbWriteEnable) {
        this.rocksdbCQDoubleWriteEnable = rocksdbWriteEnable;
    }


    public boolean isEnabledAppendPropCRC() {
        return enabledAppendPropCRC;
    }

    public void setEnabledAppendPropCRC(boolean enabledAppendPropCRC) {
        this.enabledAppendPropCRC = enabledAppendPropCRC;
    }

    public boolean isDebugLockEnable() {
        return debugLockEnable;
    }

    public void setDebugLockEnable(final boolean debugLockEnable) {
        this.debugLockEnable = debugLockEnable;
    }

    public boolean isDuplicationEnable() {
        return duplicationEnable;
    }

    public void setDuplicationEnable(final boolean duplicationEnable) {
        this.duplicationEnable = duplicationEnable;
    }

    public long getOsPageCacheBusyTimeOutMills() {
        return osPageCacheBusyTimeOutMills;
    }

    public void setOsPageCacheBusyTimeOutMills(final long osPageCacheBusyTimeOutMills) {
        this.osPageCacheBusyTimeOutMills = osPageCacheBusyTimeOutMills;
    }

    public boolean isDiskFallRecorded() {
        return diskFallRecorded;
    }

    public void setDiskFallRecorded(final boolean diskFallRecorded) {
        this.diskFallRecorded = diskFallRecorded;
    }

    public boolean isWarmMapedFileEnable() {
        return warmMapedFileEnable;
    }

    public void setWarmMapedFileEnable(boolean warmMapedFileEnable) {
        this.warmMapedFileEnable = warmMapedFileEnable;
    }

    public int getCompactionMappedFileSize() {
        return compactionMappedFileSize;
    }

    public int getCompactionCqMappedFileSize() {
        return compactionCqMappedFileSize;
    }

    public void setCompactionMappedFileSize(int compactionMappedFileSize) {
        this.compactionMappedFileSize = compactionMappedFileSize;
    }

    public void setCompactionCqMappedFileSize(int compactionCqMappedFileSize) {
        this.compactionCqMappedFileSize = compactionCqMappedFileSize;
    }

    public int getCompactionScheduleInternal() {
        return compactionScheduleInternal;
    }

    public void setCompactionScheduleInternal(int compactionScheduleInternal) {
        this.compactionScheduleInternal = compactionScheduleInternal;
    }

    public int getMaxOffsetMapSize() {
        return maxOffsetMapSize;
    }

    public void setMaxOffsetMapSize(int maxOffsetMapSize) {
        this.maxOffsetMapSize = maxOffsetMapSize;
    }

    public int getCompactionThreadNum() {
        return compactionThreadNum;
    }

    public void setCompactionThreadNum(int compactionThreadNum) {
        this.compactionThreadNum = compactionThreadNum;
    }

    public boolean isEnableCompaction() {
        return enableCompaction;
    }

    public void setEnableCompaction(boolean enableCompaction) {
        this.enableCompaction = enableCompaction;
    }

    public int getMappedFileSizeCommitLog() {
        return mappedFileSizeCommitLog;
    }

    public void setMappedFileSizeCommitLog(int mappedFileSizeCommitLog) {
        this.mappedFileSizeCommitLog = mappedFileSizeCommitLog;
    }

    public boolean isEnableRocksDBStore() {
        return StoreType.DEFAULT_ROCKSDB.getStoreType().equalsIgnoreCase(this.storeType);
    }

    public String getStoreType() {
        return storeType;
    }

    public void setStoreType(String storeType) {
        this.storeType = storeType;
    }

    public int getMappedFileSizeConsumeQueue() {
        int factor = (int) Math.ceil(this.mappedFileSizeConsumeQueue / (ConsumeQueue.CQ_STORE_UNIT_SIZE * 1.0));
        return (int) (factor * ConsumeQueue.CQ_STORE_UNIT_SIZE);
    }

    public void setMappedFileSizeConsumeQueue(int mappedFileSizeConsumeQueue) {
        this.mappedFileSizeConsumeQueue = mappedFileSizeConsumeQueue;
    }

    public boolean isEnableConsumeQueueExt() {
        return enableConsumeQueueExt;
    }

    public void setEnableConsumeQueueExt(boolean enableConsumeQueueExt) {
        this.enableConsumeQueueExt = enableConsumeQueueExt;
    }

    public int getMappedFileSizeConsumeQueueExt() {
        return mappedFileSizeConsumeQueueExt;
    }

    public void setMappedFileSizeConsumeQueueExt(int mappedFileSizeConsumeQueueExt) {
        this.mappedFileSizeConsumeQueueExt = mappedFileSizeConsumeQueueExt;
    }

    public int getBitMapLengthConsumeQueueExt() {
        return bitMapLengthConsumeQueueExt;
    }

    public void setBitMapLengthConsumeQueueExt(int bitMapLengthConsumeQueueExt) {
        this.bitMapLengthConsumeQueueExt = bitMapLengthConsumeQueueExt;
    }

    public int getFlushIntervalCommitLog() {
        return flushIntervalCommitLog;
    }

    public void setFlushIntervalCommitLog(int flushIntervalCommitLog) {
        this.flushIntervalCommitLog = flushIntervalCommitLog;
    }

    public int getFlushIntervalConsumeQueue() {
        return flushIntervalConsumeQueue;
    }

    public void setFlushIntervalConsumeQueue(int flushIntervalConsumeQueue) {
        this.flushIntervalConsumeQueue = flushIntervalConsumeQueue;
    }

    public int getPutMsgIndexHightWater() {
        return putMsgIndexHightWater;
    }

    public void setPutMsgIndexHightWater(int putMsgIndexHightWater) {
        this.putMsgIndexHightWater = putMsgIndexHightWater;
    }

    public int getCleanResourceInterval() {
        return cleanResourceInterval;
    }

    public void setCleanResourceInterval(int cleanResourceInterval) {
        this.cleanResourceInterval = cleanResourceInterval;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public int getMaxFilterMessageSize() {
        return maxFilterMessageSize;
    }

    public void setMaxFilterMessageSize(int maxFilterMessageSize) {
        this.maxFilterMessageSize = maxFilterMessageSize;
    }

    @Deprecated
    public int getMaxTopicLength() {
        return maxTopicLength;
    }

    @Deprecated
    public void setMaxTopicLength(int maxTopicLength) {
        this.maxTopicLength = maxTopicLength;
    }

    public boolean isAutoMessageVersionOnTopicLen() {
        return autoMessageVersionOnTopicLen;
    }

    public void setAutoMessageVersionOnTopicLen(boolean autoMessageVersionOnTopicLen) {
        this.autoMessageVersionOnTopicLen = autoMessageVersionOnTopicLen;
    }

    public int getTravelCqFileNumWhenGetMessage() {
        return travelCqFileNumWhenGetMessage;
    }

    public void setTravelCqFileNumWhenGetMessage(int travelCqFileNumWhenGetMessage) {
        this.travelCqFileNumWhenGetMessage = travelCqFileNumWhenGetMessage;
    }

    public int getCorrectLogicMinOffsetSleepInterval() {
        return correctLogicMinOffsetSleepInterval;
    }

    public void setCorrectLogicMinOffsetSleepInterval(int correctLogicMinOffsetSleepInterval) {
        this.correctLogicMinOffsetSleepInterval = correctLogicMinOffsetSleepInterval;
    }

    public int getCorrectLogicMinOffsetForceInterval() {
        return correctLogicMinOffsetForceInterval;
    }

    public void setCorrectLogicMinOffsetForceInterval(int correctLogicMinOffsetForceInterval) {
        this.correctLogicMinOffsetForceInterval = correctLogicMinOffsetForceInterval;
    }

    public boolean isCheckCRCOnRecover() {
        return checkCRCOnRecover;
    }

    public boolean getCheckCRCOnRecover() {
        return checkCRCOnRecover;
    }

    public void setCheckCRCOnRecover(boolean checkCRCOnRecover) {
        this.checkCRCOnRecover = checkCRCOnRecover;
    }

    public boolean isForceVerifyPropCRC() {
        return forceVerifyPropCRC;
    }

    public void setForceVerifyPropCRC(boolean forceVerifyPropCRC) {
        this.forceVerifyPropCRC = forceVerifyPropCRC;
    }

    public String getStorePathCommitLog() {
        if (storePathCommitLog == null) {
            return storePathRootDir + File.separator + "commitlog";
        }
        return storePathCommitLog;
    }

    public void setStorePathCommitLog(String storePathCommitLog) {
        this.storePathCommitLog = storePathCommitLog;
    }

    public String getStorePathDLedgerCommitLog() {
        return storePathDLedgerCommitLog;
    }

    public void setStorePathDLedgerCommitLog(String storePathDLedgerCommitLog) {
        this.storePathDLedgerCommitLog = storePathDLedgerCommitLog;
    }

    public String getStorePathEpochFile() {
        if (storePathEpochFile == null) {
            return storePathRootDir + File.separator + "epochFileCheckpoint";
        }
        return storePathEpochFile;
    }

    public void setStorePathEpochFile(String storePathEpochFile) {
        this.storePathEpochFile = storePathEpochFile;
    }

    public String getStorePathBrokerIdentity() {
        if (storePathBrokerIdentity == null) {
            return storePathRootDir + File.separator + "brokerIdentity";
        }
        return storePathBrokerIdentity;
    }

    public void setStorePathBrokerIdentity(String storePathBrokerIdentity) {
        this.storePathBrokerIdentity = storePathBrokerIdentity;
    }

    public String getDeleteWhen() {
        return deleteWhen;
    }

    public void setDeleteWhen(String deleteWhen) {
        this.deleteWhen = deleteWhen;
    }

    public int getDiskMaxUsedSpaceRatio() {
        if (this.diskMaxUsedSpaceRatio < 10)
            return 10;

        if (this.diskMaxUsedSpaceRatio > 95)
            return 95;

        return diskMaxUsedSpaceRatio;
    }

    public void setDiskMaxUsedSpaceRatio(int diskMaxUsedSpaceRatio) {
        this.diskMaxUsedSpaceRatio = diskMaxUsedSpaceRatio;
    }

    public int getDeleteCommitLogFilesInterval() {
        return deleteCommitLogFilesInterval;
    }

    public void setDeleteCommitLogFilesInterval(int deleteCommitLogFilesInterval) {
        this.deleteCommitLogFilesInterval = deleteCommitLogFilesInterval;
    }

    public int getDeleteConsumeQueueFilesInterval() {
        return deleteConsumeQueueFilesInterval;
    }

    public void setDeleteConsumeQueueFilesInterval(int deleteConsumeQueueFilesInterval) {
        this.deleteConsumeQueueFilesInterval = deleteConsumeQueueFilesInterval;
    }

    public int getMaxTransferBytesOnMessageInMemory() {
        return maxTransferBytesOnMessageInMemory;
    }

    public void setMaxTransferBytesOnMessageInMemory(int maxTransferBytesOnMessageInMemory) {
        this.maxTransferBytesOnMessageInMemory = maxTransferBytesOnMessageInMemory;
    }

    public int getMaxTransferCountOnMessageInMemory() {
        return maxTransferCountOnMessageInMemory;
    }

    public void setMaxTransferCountOnMessageInMemory(int maxTransferCountOnMessageInMemory) {
        this.maxTransferCountOnMessageInMemory = maxTransferCountOnMessageInMemory;
    }

    public int getMaxTransferBytesOnMessageInDisk() {
        return maxTransferBytesOnMessageInDisk;
    }

    public void setMaxTransferBytesOnMessageInDisk(int maxTransferBytesOnMessageInDisk) {
        this.maxTransferBytesOnMessageInDisk = maxTransferBytesOnMessageInDisk;
    }

    public int getMaxTransferCountOnMessageInDisk() {
        return maxTransferCountOnMessageInDisk;
    }

    public void setMaxTransferCountOnMessageInDisk(int maxTransferCountOnMessageInDisk) {
        this.maxTransferCountOnMessageInDisk = maxTransferCountOnMessageInDisk;
    }

    public int getFlushCommitLogLeastPages() {
        return flushCommitLogLeastPages;
    }

    public void setFlushCommitLogLeastPages(int flushCommitLogLeastPages) {
        this.flushCommitLogLeastPages = flushCommitLogLeastPages;
    }

    public int getFlushConsumeQueueLeastPages() {
        return flushConsumeQueueLeastPages;
    }

    public void setFlushConsumeQueueLeastPages(int flushConsumeQueueLeastPages) {
        this.flushConsumeQueueLeastPages = flushConsumeQueueLeastPages;
    }

    public int getFlushCommitLogThoroughInterval() {
        return flushCommitLogThoroughInterval;
    }

    public void setFlushCommitLogThoroughInterval(int flushCommitLogThoroughInterval) {
        this.flushCommitLogThoroughInterval = flushCommitLogThoroughInterval;
    }

    public int getFlushConsumeQueueThoroughInterval() {
        return flushConsumeQueueThoroughInterval;
    }

    public void setFlushConsumeQueueThoroughInterval(int flushConsumeQueueThoroughInterval) {
        this.flushConsumeQueueThoroughInterval = flushConsumeQueueThoroughInterval;
    }

    public int getDestroyMapedFileIntervalForcibly() {
        return destroyMapedFileIntervalForcibly;
    }

    public void setDestroyMapedFileIntervalForcibly(int destroyMapedFileIntervalForcibly) {
        this.destroyMapedFileIntervalForcibly = destroyMapedFileIntervalForcibly;
    }

    public int getFileReservedTime() {
        return fileReservedTime;
    }

    public void setFileReservedTime(int fileReservedTime) {
        this.fileReservedTime = fileReservedTime;
    }

    public int getRedeleteHangedFileInterval() {
        return redeleteHangedFileInterval;
    }

    public void setRedeleteHangedFileInterval(int redeleteHangedFileInterval) {
        this.redeleteHangedFileInterval = redeleteHangedFileInterval;
    }

    public int getAccessMessageInMemoryMaxRatio() {
        return accessMessageInMemoryMaxRatio;
    }

    public void setAccessMessageInMemoryMaxRatio(int accessMessageInMemoryMaxRatio) {
        this.accessMessageInMemoryMaxRatio = accessMessageInMemoryMaxRatio;
    }

    public boolean isMessageIndexEnable() {
        return messageIndexEnable;
    }

    public void setMessageIndexEnable(boolean messageIndexEnable) {
        this.messageIndexEnable = messageIndexEnable;
    }

    public int getMaxHashSlotNum() {
        return maxHashSlotNum;
    }

    public void setMaxHashSlotNum(int maxHashSlotNum) {
        this.maxHashSlotNum = maxHashSlotNum;
    }

    public int getMaxIndexNum() {
        return maxIndexNum;
    }

    public void setMaxIndexNum(int maxIndexNum) {
        this.maxIndexNum = maxIndexNum;
    }

    public int getMaxMsgsNumBatch() {
        return maxMsgsNumBatch;
    }

    public void setMaxMsgsNumBatch(int maxMsgsNumBatch) {
        this.maxMsgsNumBatch = maxMsgsNumBatch;
    }

    public int getHaListenPort() {
        return haListenPort;
    }

    public void setHaListenPort(int haListenPort) {
        if (haListenPort < 0) {
            this.haListenPort = 0;
            return;
        }
        this.haListenPort = haListenPort;
    }

    public int getHaSendHeartbeatInterval() {
        return haSendHeartbeatInterval;
    }

    public void setHaSendHeartbeatInterval(int haSendHeartbeatInterval) {
        this.haSendHeartbeatInterval = haSendHeartbeatInterval;
    }

    public int getHaHousekeepingInterval() {
        return haHousekeepingInterval;
    }

    public void setHaHousekeepingInterval(int haHousekeepingInterval) {
        this.haHousekeepingInterval = haHousekeepingInterval;
    }

    public BrokerRole getBrokerRole() {
        return brokerRole;
    }

    public void setBrokerRole(BrokerRole brokerRole) {
        this.brokerRole = brokerRole;
    }

    public void setBrokerRole(String brokerRole) {
        this.brokerRole = BrokerRole.valueOf(brokerRole);
    }

    public int getHaTransferBatchSize() {
        return haTransferBatchSize;
    }

    public void setHaTransferBatchSize(int haTransferBatchSize) {
        this.haTransferBatchSize = haTransferBatchSize;
    }

    public int getHaMaxGapNotInSync() {
        return haMaxGapNotInSync;
    }

    public void setHaMaxGapNotInSync(int haMaxGapNotInSync) {
        this.haMaxGapNotInSync = haMaxGapNotInSync;
    }

    public FlushDiskType getFlushDiskType() {
        return flushDiskType;
    }

    public void setFlushDiskType(FlushDiskType flushDiskType) {
        this.flushDiskType = flushDiskType;
    }

    public void setFlushDiskType(String type) {
        this.flushDiskType = FlushDiskType.valueOf(type);
    }

    public int getSyncFlushTimeout() {
        return syncFlushTimeout;
    }

    public void setSyncFlushTimeout(int syncFlushTimeout) {
        this.syncFlushTimeout = syncFlushTimeout;
    }

    public int getPutMessageTimeout() {
        return putMessageTimeout;
    }

    public void setPutMessageTimeout(int putMessageTimeout) {
        this.putMessageTimeout = putMessageTimeout;
    }

    public int getSlaveTimeout() {
        return slaveTimeout;
    }

    public void setSlaveTimeout(int slaveTimeout) {
        this.slaveTimeout = slaveTimeout;
    }

    public String getHaMasterAddress() {
        return haMasterAddress;
    }

    public void setHaMasterAddress(String haMasterAddress) {
        this.haMasterAddress = haMasterAddress;
    }

    public String getMessageDelayLevel() {
        return messageDelayLevel;
    }

    public void setMessageDelayLevel(String messageDelayLevel) {
        this.messageDelayLevel = messageDelayLevel;
    }

    public long getFlushDelayOffsetInterval() {
        return flushDelayOffsetInterval;
    }

    public void setFlushDelayOffsetInterval(long flushDelayOffsetInterval) {
        this.flushDelayOffsetInterval = flushDelayOffsetInterval;
    }

    public boolean isCleanFileForciblyEnable() {
        return cleanFileForciblyEnable;
    }

    public void setCleanFileForciblyEnable(boolean cleanFileForciblyEnable) {
        this.cleanFileForciblyEnable = cleanFileForciblyEnable;
    }

    public boolean isMessageIndexSafe() {
        return messageIndexSafe;
    }

    public void setMessageIndexSafe(boolean messageIndexSafe) {
        this.messageIndexSafe = messageIndexSafe;
    }

    public boolean isFlushCommitLogTimed() {
        return flushCommitLogTimed;
    }

    public void setFlushCommitLogTimed(boolean flushCommitLogTimed) {
        this.flushCommitLogTimed = flushCommitLogTimed;
    }

    public String getStorePathRootDir() {
        return storePathRootDir;
    }

    public void setStorePathRootDir(String storePathRootDir) {
        this.storePathRootDir = storePathRootDir;
    }

    public int getFlushLeastPagesWhenWarmMapedFile() {
        return flushLeastPagesWhenWarmMapedFile;
    }

    public void setFlushLeastPagesWhenWarmMapedFile(int flushLeastPagesWhenWarmMapedFile) {
        this.flushLeastPagesWhenWarmMapedFile = flushLeastPagesWhenWarmMapedFile;
    }

    public boolean isOffsetCheckInSlave() {
        return offsetCheckInSlave;
    }

    public void setOffsetCheckInSlave(boolean offsetCheckInSlave) {
        this.offsetCheckInSlave = offsetCheckInSlave;
    }

    public int getDefaultQueryMaxNum() {
        return defaultQueryMaxNum;
    }

    public void setDefaultQueryMaxNum(int defaultQueryMaxNum) {
        this.defaultQueryMaxNum = defaultQueryMaxNum;
    }

    public boolean isTransientStorePoolEnable() {
        return transientStorePoolEnable;
    }

    public void setTransientStorePoolEnable(final boolean transientStorePoolEnable) {
        this.transientStorePoolEnable = transientStorePoolEnable;
    }

    public int getTransientStorePoolSize() {
        return transientStorePoolSize;
    }

    public void setTransientStorePoolSize(final int transientStorePoolSize) {
        this.transientStorePoolSize = transientStorePoolSize;
    }

    public int getCommitIntervalCommitLog() {
        return commitIntervalCommitLog;
    }

    public void setCommitIntervalCommitLog(final int commitIntervalCommitLog) {
        this.commitIntervalCommitLog = commitIntervalCommitLog;
    }

    public boolean isFastFailIfNoBufferInStorePool() {
        return fastFailIfNoBufferInStorePool;
    }

    public void setFastFailIfNoBufferInStorePool(final boolean fastFailIfNoBufferInStorePool) {
        this.fastFailIfNoBufferInStorePool = fastFailIfNoBufferInStorePool;
    }

    public boolean isUseReentrantLockWhenPutMessage() {
        return useReentrantLockWhenPutMessage;
    }

    public void setUseReentrantLockWhenPutMessage(final boolean useReentrantLockWhenPutMessage) {
        this.useReentrantLockWhenPutMessage = useReentrantLockWhenPutMessage;
    }

    public int getCommitCommitLogLeastPages() {
        return commitCommitLogLeastPages;
    }

    public void setCommitCommitLogLeastPages(final int commitCommitLogLeastPages) {
        this.commitCommitLogLeastPages = commitCommitLogLeastPages;
    }

    public int getCommitCommitLogThoroughInterval() {
        return commitCommitLogThoroughInterval;
    }

    public void setCommitCommitLogThoroughInterval(final int commitCommitLogThoroughInterval) {
        this.commitCommitLogThoroughInterval = commitCommitLogThoroughInterval;
    }

    public boolean isWakeCommitWhenPutMessage() {
        return wakeCommitWhenPutMessage;
    }

    public void setWakeCommitWhenPutMessage(boolean wakeCommitWhenPutMessage) {
        this.wakeCommitWhenPutMessage = wakeCommitWhenPutMessage;
    }

    public boolean isWakeFlushWhenPutMessage() {
        return wakeFlushWhenPutMessage;
    }

    public void setWakeFlushWhenPutMessage(boolean wakeFlushWhenPutMessage) {
        this.wakeFlushWhenPutMessage = wakeFlushWhenPutMessage;
    }

    public int getMapperFileSizeBatchConsumeQueue() {
        return mapperFileSizeBatchConsumeQueue;
    }

    public void setMapperFileSizeBatchConsumeQueue(int mapperFileSizeBatchConsumeQueue) {
        this.mapperFileSizeBatchConsumeQueue = mapperFileSizeBatchConsumeQueue;
    }

    public boolean isEnableCleanExpiredOffset() {
        return enableCleanExpiredOffset;
    }

    public void setEnableCleanExpiredOffset(boolean enableCleanExpiredOffset) {
        this.enableCleanExpiredOffset = enableCleanExpiredOffset;
    }

    public String getReadOnlyCommitLogStorePaths() {
        return readOnlyCommitLogStorePaths;
    }

    public void setReadOnlyCommitLogStorePaths(String readOnlyCommitLogStorePaths) {
        this.readOnlyCommitLogStorePaths = readOnlyCommitLogStorePaths;
    }

    /**
     * 获取 DLedger 组标识
     *
     * @return DLedger 组标识
     */
    public String getdLegerGroup() {
        return dLegerGroup;
    }

    /**
     * 设置 DLedger 组标识
     *
     * @param dLegerGroup DLedger 组标识
     */
    public void setdLegerGroup(String dLegerGroup) {
        this.dLegerGroup = dLegerGroup;
    }

    /**
     * 获取 DLedger 节点列表
     *
     * @return DLedger 节点列表
     */
    public String getdLegerPeers() {
        return dLegerPeers;
    }

    /**
     * 设置 DLedger 节点列表
     *
     * @param dLegerPeers DLedger 节点列表
     */
    public void setdLegerPeers(String dLegerPeers) {
        this.dLegerPeers = dLegerPeers;
    }

    /**
     * 获取 DLedger 本机 ID
     *
     * @return DLedger 本机 ID
     */
    public String getdLegerSelfId() {
        return dLegerSelfId;
    }

    /**
     * 设置 DLedger 本机 ID
     *
     * @param dLegerSelfId DLedger 本机 ID
     */
    public void setdLegerSelfId(String dLegerSelfId) {
        this.dLegerSelfId = dLegerSelfId;
    }

    public boolean isEnableDLegerCommitLog() {
        return enableDLegerCommitLog;
    }

    public void setEnableDLegerCommitLog(boolean enableDLegerCommitLog) {
        this.enableDLegerCommitLog = enableDLegerCommitLog;
    }

    public String getPreferredLeaderId() {
        return preferredLeaderId;
    }

    public void setPreferredLeaderId(String preferredLeaderId) {
        this.preferredLeaderId = preferredLeaderId;
    }

    public boolean isEnableBatchPush() {
        return enableBatchPush;
    }

    public void setEnableBatchPush(boolean enableBatchPush) {
        this.enableBatchPush = enableBatchPush;
    }

    public boolean isEnableScheduleMessageStats() {
        return enableScheduleMessageStats;
    }

    public void setEnableScheduleMessageStats(boolean enableScheduleMessageStats) {
        this.enableScheduleMessageStats = enableScheduleMessageStats;
    }

    public int getMaxAsyncPutMessageRequests() {
        return maxAsyncPutMessageRequests;
    }

    public void setMaxAsyncPutMessageRequests(int maxAsyncPutMessageRequests) {
        this.maxAsyncPutMessageRequests = maxAsyncPutMessageRequests;
    }

    public int getMaxRecoveryCommitlogFiles() {
        return maxRecoveryCommitlogFiles;
    }

    public void setMaxRecoveryCommitlogFiles(final int maxRecoveryCommitlogFiles) {
        this.maxRecoveryCommitlogFiles = maxRecoveryCommitlogFiles;
    }

    public boolean isDispatchFromSenderThread() {
        return dispatchFromSenderThread;
    }

    public void setDispatchFromSenderThread(boolean dispatchFromSenderThread) {
        this.dispatchFromSenderThread = dispatchFromSenderThread;
    }

    public int getDispatchCqThreads() {
        return dispatchCqThreads;
    }

    public void setDispatchCqThreads(final int dispatchCqThreads) {
        this.dispatchCqThreads = dispatchCqThreads;
    }

    public int getDispatchCqCacheNum() {
        return dispatchCqCacheNum;
    }

    public void setDispatchCqCacheNum(final int dispatchCqCacheNum) {
        this.dispatchCqCacheNum = dispatchCqCacheNum;
    }

    public boolean isEnableAsyncReput() {
        return enableAsyncReput;
    }

    public void setEnableAsyncReput(final boolean enableAsyncReput) {
        this.enableAsyncReput = enableAsyncReput;
    }

    public boolean isRecheckReputOffsetFromCq() {
        return recheckReputOffsetFromCq;
    }

    public void setRecheckReputOffsetFromCq(final boolean recheckReputOffsetFromCq) {
        this.recheckReputOffsetFromCq = recheckReputOffsetFromCq;
    }

    public long getCommitLogForceSwapMapInterval() {
        return commitLogForceSwapMapInterval;
    }

    public void setCommitLogForceSwapMapInterval(long commitLogForceSwapMapInterval) {
        this.commitLogForceSwapMapInterval = commitLogForceSwapMapInterval;
    }

    public int getCommitLogSwapMapReserveFileNum() {
        return commitLogSwapMapReserveFileNum;
    }

    public void setCommitLogSwapMapReserveFileNum(int commitLogSwapMapReserveFileNum) {
        this.commitLogSwapMapReserveFileNum = commitLogSwapMapReserveFileNum;
    }

    public long getLogicQueueForceSwapMapInterval() {
        return logicQueueForceSwapMapInterval;
    }

    public void setLogicQueueForceSwapMapInterval(long logicQueueForceSwapMapInterval) {
        this.logicQueueForceSwapMapInterval = logicQueueForceSwapMapInterval;
    }

    public int getLogicQueueSwapMapReserveFileNum() {
        return logicQueueSwapMapReserveFileNum;
    }

    public void setLogicQueueSwapMapReserveFileNum(int logicQueueSwapMapReserveFileNum) {
        this.logicQueueSwapMapReserveFileNum = logicQueueSwapMapReserveFileNum;
    }

    public long getCleanSwapedMapInterval() {
        return cleanSwapedMapInterval;
    }

    public void setCleanSwapedMapInterval(long cleanSwapedMapInterval) {
        this.cleanSwapedMapInterval = cleanSwapedMapInterval;
    }

    public long getCommitLogSwapMapInterval() {
        return commitLogSwapMapInterval;
    }

    public void setCommitLogSwapMapInterval(long commitLogSwapMapInterval) {
        this.commitLogSwapMapInterval = commitLogSwapMapInterval;
    }

    public long getLogicQueueSwapMapInterval() {
        return logicQueueSwapMapInterval;
    }

    public void setLogicQueueSwapMapInterval(long logicQueueSwapMapInterval) {
        this.logicQueueSwapMapInterval = logicQueueSwapMapInterval;
    }

    public int getMaxBatchDeleteFilesNum() {
        return maxBatchDeleteFilesNum;
    }

    public void setMaxBatchDeleteFilesNum(int maxBatchDeleteFilesNum) {
        this.maxBatchDeleteFilesNum = maxBatchDeleteFilesNum;
    }

    public boolean isSearchBcqByCacheEnable() {
        return searchBcqByCacheEnable;
    }

    public void setSearchBcqByCacheEnable(boolean searchBcqByCacheEnable) {
        this.searchBcqByCacheEnable = searchBcqByCacheEnable;
    }

    public int getDiskSpaceWarningLevelRatio() {
        return diskSpaceWarningLevelRatio;
    }

    public void setDiskSpaceWarningLevelRatio(int diskSpaceWarningLevelRatio) {
        this.diskSpaceWarningLevelRatio = diskSpaceWarningLevelRatio;
    }

    public int getDiskSpaceCleanForciblyRatio() {
        return diskSpaceCleanForciblyRatio;
    }

    public void setDiskSpaceCleanForciblyRatio(int diskSpaceCleanForciblyRatio) {
        this.diskSpaceCleanForciblyRatio = diskSpaceCleanForciblyRatio;
    }

    public boolean isMappedFileSwapEnable() {
        return mappedFileSwapEnable;
    }

    public void setMappedFileSwapEnable(boolean mappedFileSwapEnable) {
        this.mappedFileSwapEnable = mappedFileSwapEnable;
    }

    public int getPullBatchMaxMessageCount() {
        return pullBatchMaxMessageCount;
    }

    public void setPullBatchMaxMessageCount(int pullBatchMaxMessageCount) {
        this.pullBatchMaxMessageCount = pullBatchMaxMessageCount;
    }

    public int getDeleteFileBatchMax() {
        return deleteFileBatchMax;
    }

    public void setDeleteFileBatchMax(int deleteFileBatchMax) {
        this.deleteFileBatchMax = deleteFileBatchMax;
    }

    public int getTotalReplicas() {
        return totalReplicas;
    }

    public void setTotalReplicas(int totalReplicas) {
        this.totalReplicas = totalReplicas;
    }

    public int getInSyncReplicas() {
        return inSyncReplicas;
    }

    public void setInSyncReplicas(int inSyncReplicas) {
        this.inSyncReplicas = inSyncReplicas;
    }

    public int getMinInSyncReplicas() {
        return minInSyncReplicas;
    }

    public void setMinInSyncReplicas(int minInSyncReplicas) {
        this.minInSyncReplicas = minInSyncReplicas;
    }

    public boolean isAllAckInSyncStateSet() {
        return allAckInSyncStateSet;
    }

    public void setAllAckInSyncStateSet(boolean allAckInSyncStateSet) {
        this.allAckInSyncStateSet = allAckInSyncStateSet;
    }

    public boolean isEnableAutoInSyncReplicas() {
        return enableAutoInSyncReplicas;
    }

    public void setEnableAutoInSyncReplicas(boolean enableAutoInSyncReplicas) {
        this.enableAutoInSyncReplicas = enableAutoInSyncReplicas;
    }

    public boolean isHaFlowControlEnable() {
        return haFlowControlEnable;
    }

    public void setHaFlowControlEnable(boolean haFlowControlEnable) {
        this.haFlowControlEnable = haFlowControlEnable;
    }

    public long getMaxHaTransferByteInSecond() {
        return maxHaTransferByteInSecond;
    }

    public void setMaxHaTransferByteInSecond(long maxHaTransferByteInSecond) {
        this.maxHaTransferByteInSecond = maxHaTransferByteInSecond;
    }

    public long getHaMaxTimeSlaveNotCatchup() {
        return haMaxTimeSlaveNotCatchup;
    }

    public void setHaMaxTimeSlaveNotCatchup(long haMaxTimeSlaveNotCatchup) {
        this.haMaxTimeSlaveNotCatchup = haMaxTimeSlaveNotCatchup;
    }

    public boolean isSyncMasterFlushOffsetWhenStartup() {
        return syncMasterFlushOffsetWhenStartup;
    }

    public void setSyncMasterFlushOffsetWhenStartup(boolean syncMasterFlushOffsetWhenStartup) {
        this.syncMasterFlushOffsetWhenStartup = syncMasterFlushOffsetWhenStartup;
    }

    public long getMaxChecksumRange() {
        return maxChecksumRange;
    }

    public void setMaxChecksumRange(long maxChecksumRange) {
        this.maxChecksumRange = maxChecksumRange;
    }

    public int getReplicasPerDiskPartition() {
        return replicasPerDiskPartition;
    }

    public void setReplicasPerDiskPartition(int replicasPerDiskPartition) {
        this.replicasPerDiskPartition = replicasPerDiskPartition;
    }

    public double getLogicalDiskSpaceCleanForciblyThreshold() {
        return logicalDiskSpaceCleanForciblyThreshold;
    }

    public void setLogicalDiskSpaceCleanForciblyThreshold(double logicalDiskSpaceCleanForciblyThreshold) {
        this.logicalDiskSpaceCleanForciblyThreshold = logicalDiskSpaceCleanForciblyThreshold;
    }

    public int getDisappearTimeAfterStart() {
        return disappearTimeAfterStart;
    }

    public void setDisappearTimeAfterStart(int disappearTimeAfterStart) {
        this.disappearTimeAfterStart = disappearTimeAfterStart;
    }

    public long getMaxSlaveResendLength() {
        return maxSlaveResendLength;
    }

    public void setMaxSlaveResendLength(long maxSlaveResendLength) {
        this.maxSlaveResendLength = maxSlaveResendLength;
    }

    public boolean isSyncFromLastFile() {
        return syncFromLastFile;
    }

    public void setSyncFromLastFile(boolean syncFromLastFile) {
        this.syncFromLastFile = syncFromLastFile;
    }

    public boolean isEnableLmq() {
        return enableLmq;
    }

    public void setEnableLmq(boolean enableLmq) {
        this.enableLmq = enableLmq;
    }

    public boolean isEnableMultiDispatch() {
        return enableMultiDispatch;
    }

    public void setEnableMultiDispatch(boolean enableMultiDispatch) {
        this.enableMultiDispatch = enableMultiDispatch;
    }

    public int getMaxLmqConsumeQueueNum() {
        return maxLmqConsumeQueueNum;
    }

    public void setMaxLmqConsumeQueueNum(int maxLmqConsumeQueueNum) {
        this.maxLmqConsumeQueueNum = maxLmqConsumeQueueNum;
    }

    public boolean isEnableScheduleAsyncDeliver() {
        return enableScheduleAsyncDeliver;
    }

    public void setEnableScheduleAsyncDeliver(boolean enableScheduleAsyncDeliver) {
        this.enableScheduleAsyncDeliver = enableScheduleAsyncDeliver;
    }

    public int getScheduleAsyncDeliverMaxPendingLimit() {
        return scheduleAsyncDeliverMaxPendingLimit;
    }

    public void setScheduleAsyncDeliverMaxPendingLimit(int scheduleAsyncDeliverMaxPendingLimit) {
        this.scheduleAsyncDeliverMaxPendingLimit = scheduleAsyncDeliverMaxPendingLimit;
    }

    public int getScheduleAsyncDeliverMaxResendNum2Blocked() {
        return scheduleAsyncDeliverMaxResendNum2Blocked;
    }

    public void setScheduleAsyncDeliverMaxResendNum2Blocked(int scheduleAsyncDeliverMaxResendNum2Blocked) {
        this.scheduleAsyncDeliverMaxResendNum2Blocked = scheduleAsyncDeliverMaxResendNum2Blocked;
    }

    public boolean isAsyncLearner() {
        return asyncLearner;
    }

    public void setAsyncLearner(boolean asyncLearner) {
        this.asyncLearner = asyncLearner;
    }

    public int getMappedFileSizeTimerLog() {
        return mappedFileSizeTimerLog;
    }

    public void setMappedFileSizeTimerLog(final int mappedFileSizeTimerLog) {
        this.mappedFileSizeTimerLog = mappedFileSizeTimerLog;
    }

    public int getTimerPrecisionMs() {
        return timerPrecisionMs;
    }

    public void setTimerPrecisionMs(int timerPrecisionMs) {
        int[] candidates = {100, 200, 500, 1000};
        for (int i = 1; i < candidates.length; i++) {
            if (timerPrecisionMs < candidates[i]) {
                this.timerPrecisionMs = candidates[i - 1];
                return;
            }
        }
        this.timerPrecisionMs = candidates[candidates.length - 1];
    }

    public int getTimerRollWindowSlot() {
        return timerRollWindowSlot;
    }

    public int getTimerGetMessageThreadNum() {
        return timerGetMessageThreadNum;
    }

    public void setTimerGetMessageThreadNum(int timerGetMessageThreadNum) {
        this.timerGetMessageThreadNum = timerGetMessageThreadNum;
    }

    public int getTimerPutMessageThreadNum() {
        return timerPutMessageThreadNum;
    }

    public void setTimerPutMessageThreadNum(int timerPutMessageThreadNum) {
        this.timerPutMessageThreadNum = timerPutMessageThreadNum;
    }

    public boolean isTimerEnableDisruptor() {
        return timerEnableDisruptor;
    }

    public boolean isTimerEnableCheckMetrics() {
        return timerEnableCheckMetrics;
    }

    public void setTimerEnableCheckMetrics(boolean timerEnableCheckMetrics) {
        this.timerEnableCheckMetrics = timerEnableCheckMetrics;
    }

    public boolean isTimerStopEnqueue() {
        return timerStopEnqueue;
    }

    public void setTimerStopEnqueue(boolean timerStopEnqueue) {
        this.timerStopEnqueue = timerStopEnqueue;
    }

    public String getTimerCheckMetricsWhen() {
        return timerCheckMetricsWhen;
    }

    public boolean isTimerSkipUnknownError() {
        return timerSkipUnknownError;
    }

    public void setTimerSkipUnknownError(boolean timerSkipUnknownError) {
        this.timerSkipUnknownError = timerSkipUnknownError;
    }

    public boolean isTimerEnableRetryUntilSuccess() {
        return timerEnableRetryUntilSuccess;
    }

    public void setTimerEnableRetryUntilSuccess(boolean timerEnableRetryUntilSuccess) {
        this.timerEnableRetryUntilSuccess = timerEnableRetryUntilSuccess;
    }

    public boolean isTimerWarmEnable() {
        return timerWarmEnable;
    }

    public boolean isTimerWheelEnable() {
        return timerWheelEnable;
    }

    public void setTimerWheelEnable(boolean timerWheelEnable) {
        this.timerWheelEnable = timerWheelEnable;
    }

    public boolean isTimerStopDequeue() {
        return timerStopDequeue;
    }

    public int getTimerMetricSmallThreshold() {
        return timerMetricSmallThreshold;
    }

    public void setTimerMetricSmallThreshold(int timerMetricSmallThreshold) {
        this.timerMetricSmallThreshold = timerMetricSmallThreshold;
    }

    public int getTimerCongestNumEachSlot() {
        return timerCongestNumEachSlot;
    }

    public void setTimerCongestNumEachSlot(int timerCongestNumEachSlot) {
        // In order to get this value from messageStoreConfig properties file created before v4.4.1.
        // 兼容 4.4.1 之前版本配置文件中的该参数
        this.timerCongestNumEachSlot = timerCongestNumEachSlot;
    }

    public int getTimerFlushIntervalMs() {
        return timerFlushIntervalMs;
    }

    public void setTimerFlushIntervalMs(final int timerFlushIntervalMs) {
        this.timerFlushIntervalMs = timerFlushIntervalMs;
    }

    public void setTimerRollWindowSlot(final int timerRollWindowSlot) {
        this.timerRollWindowSlot = timerRollWindowSlot;
    }

    public int getTimerProgressLogIntervalMs() {
        return timerProgressLogIntervalMs;
    }

    public void setTimerProgressLogIntervalMs(final int timerProgressLogIntervalMs) {
        this.timerProgressLogIntervalMs = timerProgressLogIntervalMs;
    }

    public boolean isTimerInterceptDelayLevel() {
        return timerInterceptDelayLevel;
    }

    public void setTimerInterceptDelayLevel(boolean timerInterceptDelayLevel) {
        this.timerInterceptDelayLevel = timerInterceptDelayLevel;
    }

    public int getTimerMaxDelaySec() {
        return timerMaxDelaySec;
    }

    public void setTimerMaxDelaySec(final int timerMaxDelaySec) {
        this.timerMaxDelaySec = timerMaxDelaySec;
    }

    public int getMaxConsumeQueueScan() {
        return maxConsumeQueueScan;
    }

    public void setMaxConsumeQueueScan(int maxConsumeQueueScan) {
        this.maxConsumeQueueScan = maxConsumeQueueScan;
    }

    public int getSampleCountThreshold() {
        return sampleCountThreshold;
    }

    public void setSampleCountThreshold(int sampleCountThreshold) {
        this.sampleCountThreshold = sampleCountThreshold;
    }

    public boolean isColdDataFlowControlEnable() {
        return coldDataFlowControlEnable;
    }

    public void setColdDataFlowControlEnable(boolean coldDataFlowControlEnable) {
        this.coldDataFlowControlEnable = coldDataFlowControlEnable;
    }

    public boolean isColdDataScanEnable() {
        return coldDataScanEnable;
    }

    public void setColdDataScanEnable(boolean coldDataScanEnable) {
        this.coldDataScanEnable = coldDataScanEnable;
    }

    public int getTimerColdDataCheckIntervalMs() {
        return timerColdDataCheckIntervalMs;
    }

    public void setTimerColdDataCheckIntervalMs(int timerColdDataCheckIntervalMs) {
        this.timerColdDataCheckIntervalMs = timerColdDataCheckIntervalMs;
    }

    public int getSampleSteps() {
        return sampleSteps;
    }

    public void setSampleSteps(int sampleSteps) {
        this.sampleSteps = sampleSteps;
    }

    public int getAccessMessageInMemoryHotRatio() {
        return accessMessageInMemoryHotRatio;
    }

    public void setAccessMessageInMemoryHotRatio(int accessMessageInMemoryHotRatio) {
        this.accessMessageInMemoryHotRatio = accessMessageInMemoryHotRatio;
    }

    public boolean isDataReadAheadEnable() {
        return dataReadAheadEnable;
    }

    public void setDataReadAheadEnable(boolean dataReadAheadEnable) {
        this.dataReadAheadEnable = dataReadAheadEnable;
    }

    public boolean isEnableBuildConsumeQueueConcurrently() {
        return enableBuildConsumeQueueConcurrently;
    }

    public void setEnableBuildConsumeQueueConcurrently(boolean enableBuildConsumeQueueConcurrently) {
        this.enableBuildConsumeQueueConcurrently = enableBuildConsumeQueueConcurrently;
    }

    public int getBatchDispatchRequestThreadPoolNums() {
        return batchDispatchRequestThreadPoolNums;
    }

    public void setBatchDispatchRequestThreadPoolNums(int batchDispatchRequestThreadPoolNums) {
        this.batchDispatchRequestThreadPoolNums = batchDispatchRequestThreadPoolNums;
    }

    public boolean isRealTimePersistRocksDBConfig() {
        return realTimePersistRocksDBConfig;
    }

    public void setRealTimePersistRocksDBConfig(boolean realTimePersistRocksDBConfig) {
        this.realTimePersistRocksDBConfig = realTimePersistRocksDBConfig;
    }

    public long getStatRocksDBCQIntervalSec() {
        return statRocksDBCQIntervalSec;
    }

    public void setStatRocksDBCQIntervalSec(long statRocksDBCQIntervalSec) {
        this.statRocksDBCQIntervalSec = statRocksDBCQIntervalSec;
    }

    public long getCleanRocksDBDirtyCQIntervalMin() {
        return cleanRocksDBDirtyCQIntervalMin;
    }

    public void setCleanRocksDBDirtyCQIntervalMin(long cleanRocksDBDirtyCQIntervalMin) {
        this.cleanRocksDBDirtyCQIntervalMin = cleanRocksDBDirtyCQIntervalMin;
    }

    public long getMemTableFlushIntervalMs() {
        return memTableFlushIntervalMs;
    }

    public void setMemTableFlushIntervalMs(long memTableFlushIntervalMs) {
        this.memTableFlushIntervalMs = memTableFlushIntervalMs;
    }

    public boolean isEnableRocksDBLog() {
        return enableRocksDBLog;
    }

    public void setEnableRocksDBLog(boolean enableRocksDBLog) {
        this.enableRocksDBLog = enableRocksDBLog;
    }

    public int getTopicQueueLockNum() {
        return topicQueueLockNum;
    }

    public void setTopicQueueLockNum(int topicQueueLockNum) {
        this.topicQueueLockNum = topicQueueLockNum;
    }

    public boolean isReadUnCommitted() {
        return readUnCommitted;
    }

    public void setReadUnCommitted(boolean readUnCommitted) {
        this.readUnCommitted = readUnCommitted;
    }

    public boolean isPutConsumeQueueDataByFileChannel() {
        return putConsumeQueueDataByFileChannel;
    }

    public void setPutConsumeQueueDataByFileChannel(boolean putConsumeQueueDataByFileChannel) {
        this.putConsumeQueueDataByFileChannel = putConsumeQueueDataByFileChannel;
    }

    public String getBottomMostCompressionTypeForConsumeQueueStore() {
        return bottomMostCompressionTypeForConsumeQueueStore;
    }

    public void setBottomMostCompressionTypeForConsumeQueueStore(String bottomMostCompressionTypeForConsumeQueueStore) {
        this.bottomMostCompressionTypeForConsumeQueueStore = bottomMostCompressionTypeForConsumeQueueStore;
    }

    public int getRocksdbFlushWalFrequency() {
        return rocksdbFlushWalFrequency;
    }

    public void setRocksdbFlushWalFrequency(int rocksdbFlushWalFrequency) {
        this.rocksdbFlushWalFrequency = rocksdbFlushWalFrequency;
    }

    public long getRocksdbWalFileRollingThreshold() {
        return rocksdbWalFileRollingThreshold;
    }

    public void setRocksdbWalFileRollingThreshold(long rocksdbWalFileRollingThreshold) {
        this.rocksdbWalFileRollingThreshold = rocksdbWalFileRollingThreshold;
    }

    public int getSpinLockCollisionRetreatOptimalDegree() {
        return spinLockCollisionRetreatOptimalDegree;
    }

    public void setSpinLockCollisionRetreatOptimalDegree(int spinLockCollisionRetreatOptimalDegree) {
        this.spinLockCollisionRetreatOptimalDegree = spinLockCollisionRetreatOptimalDegree;
    }

    public void setUseABSLock(boolean useABSLock) {
        this.useABSLock = useABSLock;
    }

    public boolean getUseABSLock() {
        return useABSLock;
    }
}
