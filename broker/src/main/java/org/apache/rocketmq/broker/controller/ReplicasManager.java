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

package org.apache.rocketmq.broker.controller;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.EpochEntry;
import org.apache.rocketmq.remoting.protocol.body.SyncStateSet;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetMetaDataResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.ApplyBrokerIdResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.GetNextBrokerIdResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.RegisterBrokerToControllerResponseHeader;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;
import org.apache.rocketmq.store.ha.autoswitch.BrokerMetadata;
import org.apache.rocketmq.store.ha.autoswitch.TempBrokerMetadata;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.rocketmq.remoting.protocol.ResponseCode.CONTROLLER_BROKER_METADATA_NOT_EXIST;

/**
 * The manager of broker replicas, including: 0.regularly syncing controller metadata, change controller leader address,
 * both master and slave will start this timed task. 1.regularly syncing metadata from controllers, and changing broker
 * roles and master if needed, both master and slave will start this timed task. 2.regularly expanding and Shrinking
 * syncStateSet, only master will start this timed task.
 * <br>
 * broker 副本管理器, 负责同步 controller 元数据, 驱动主从角色切换和维护 SyncStateSet
 */
public class ReplicasManager {
    /**
     * 副本管理日志记录器, 用于输出注册, 选主和同步过程事件
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    /**
     * 启动基础服务失败后的重试间隔, 单位秒
     */
    private static final int RETRY_INTERVAL_SECOND = 5;

    /**
     * 定时任务线程池, 承载元数据同步和周期检查任务
     */
    private final ScheduledExecutorService scheduledService;
    /**
     * 通用异步执行线程池, 处理注册和角色切换后的补偿动作
     */
    private final ExecutorService executorService;
    /**
     * controller 地址探活线程池, 并发检查地址可达性
     */
    private final ExecutorService scanExecutor;
    /**
     * broker 控制器引用, 提供配置, 存储和注册能力
     */
    private final BrokerController brokerController;
    /**
     * AutoSwitch HA 服务, 负责主从复制状态管理
     */
    private final AutoSwitchHAService haService;
    /**
     * broker 配置快照, 用于读取集群和 controller 配置项
     */
    private final BrokerConfig brokerConfig;
    /**
     * 当前 broker 对外地址, 用于向 controller 上报身份
     */
    private final String brokerAddress;
    /**
     * broker 外部通信 API, 封装与 controller 的 RPC 交互
     */
    private final BrokerOuterAPI brokerOuterAPI;
    /**
     * controller 地址列表, 来自配置或 DNS 解析结果
     */
    private List<String> controllerAddresses;
    /**
     * 当前可用 controller 地址集合, value 仅作为占位标记
     */
    private final ConcurrentMap<String, Boolean> availableControllerAddresses;

    /**
     * 当前感知到的 controller leader 地址, 空字符串表示未知
     */
    private volatile String controllerLeaderAddress = "";
    /**
     * 副本管理器生命周期状态
     */
    private volatile State state = State.INITIAL;

    /**
     * broker 向 controller 注册流程状态
     */
    private volatile RegisterState registerState = RegisterState.INITIAL;

    /**
     * SyncStateSet 定时检查任务句柄, 仅 master 角色使用
     */
    private ScheduledFuture<?> checkSyncStateSetTaskFuture;
    /**
     * slave 同步任务句柄, 角色切换时用于取消旧任务
     */
    private ScheduledFuture<?> slaveSyncFuture;

    /**
     * controller 分配给当前 broker 的唯一 brokerId
     */
    private Long brokerControllerId;

    /**
     * 当前 broker 集内 master 的 brokerId
     */
    private Long masterBrokerId;

    /**
     * 持久化 broker 元数据文件抽象
     */
    private BrokerMetadata brokerMetadata;

    /**
     * 注册中间态元数据文件抽象, 用于防止并发注册冲突
     */
    private TempBrokerMetadata tempBrokerMetadata;

    /**
     * 当前生效的同步副本集合
     */
    private Set<Long> syncStateSet;
    /**
     * 当前 SyncStateSet 对应的版本号
     */
    private int syncStateSetEpoch = 0;
    /**
     * 当前 master 地址, 非 master 角色用于 slave 同步
     */
    private String masterAddress = "";
    /**
     * 当前 master 任期号
     */
    private int masterEpoch = 0;
    /**
     * 最近一次执行 slave 全量同步的时间戳
     */
    private long lastSyncTimeMs = System.currentTimeMillis();
    /**
     * 启动阶段随机退避工具, 用于降低并发注册冲突概率
     */
    private Random random = new Random();

    /**
     * 构建副本管理器, 初始化线程池, 元数据对象和基础依赖
     *
     * @param brokerController broker 运行时控制器
     */
    public ReplicasManager(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.brokerOuterAPI = brokerController.getBrokerOuterAPI();
        this.scheduledService = ThreadUtils.newScheduledThreadPool(3, new ThreadFactoryImpl("ReplicasManager_ScheduledService_", brokerController.getBrokerIdentity()));
        this.executorService = ThreadUtils.newThreadPoolExecutor(3, new ThreadFactoryImpl("ReplicasManager_ExecutorService_", brokerController.getBrokerIdentity()));
        this.scanExecutor = ThreadUtils.newThreadPoolExecutor(4, 10, 60, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(32), new ThreadFactoryImpl("ReplicasManager_scan_thread_", brokerController.getBrokerIdentity()));
        this.haService = (AutoSwitchHAService) brokerController.getMessageStore().getHaService();
        this.brokerConfig = brokerController.getBrokerConfig();
        this.availableControllerAddresses = new ConcurrentHashMap<>();
        this.syncStateSet = new HashSet<>();
        this.brokerAddress = brokerController.getBrokerAddr();
        this.brokerMetadata = new BrokerMetadata(this.brokerController.getMessageStoreConfig().getStorePathBrokerIdentity());
        this.tempBrokerMetadata = new TempBrokerMetadata(this.brokerController.getMessageStoreConfig().getStorePathBrokerIdentity() + "-temp");
    }

    /**
     * 副本管理器生命周期状态定义
     */
    enum State {
        INITIAL,
        FIRST_TIME_SYNC_CONTROLLER_METADATA_DONE,
        REGISTER_TO_CONTROLLER_DONE,
        RUNNING,
        SHUTDOWN,
    }

    /**
     * broker 注册流程状态定义
     */
    enum RegisterState {
        INITIAL,
        CREATE_TEMP_METADATA_FILE_DONE,
        CREATE_METADATA_FILE_DONE,
        REGISTERED
    }

    /**
     * 启动副本管理器, 初始化地址探活和注册选主流程
     */
    public void start() {
        this.state = State.INITIAL;
        updateControllerAddr();
        scanAvailableControllerAddresses();
        this.scheduledService.scheduleAtFixedRate(this::updateControllerAddr, 2 * 60 * 1000, 2 * 60 * 1000, TimeUnit.MILLISECONDS);
        this.scheduledService.scheduleAtFixedRate(this::scanAvailableControllerAddresses, 3 * 1000, 3 * 1000, TimeUnit.MILLISECONDS);
        if (!startBasicService()) {
            LOGGER.error("Failed to start replicasManager");
            this.executorService.submit(() -> {
                int retryTimes = 0;
                do {
                    try {
                        TimeUnit.SECONDS.sleep(RETRY_INTERVAL_SECOND);
                    } catch (InterruptedException ignored) {

                    }
                    retryTimes++;
                    LOGGER.warn("Failed to start replicasManager, retry times:{}, current state:{}, try it again", retryTimes, this.state);
                }
                while (!startBasicService());

                LOGGER.info("Start replicasManager success, retry times:{}", retryTimes);
            });
        }
    }

    /**
     * 启动核心服务流程, 包括同步 controller 信息, 注册 broker 和选主
     *
     * @return 启动是否成功
     */
    private boolean startBasicService() {
        if (this.state == State.SHUTDOWN)
            return false;
        if (this.state == State.INITIAL) {
            if (schedulingSyncControllerMetadata()) {
                this.state = State.FIRST_TIME_SYNC_CONTROLLER_METADATA_DONE;
                LOGGER.info("First time sync controller metadata success, change state to: {}", this.state);
            } else {
                return false;
            }
        }

        if (this.state == State.FIRST_TIME_SYNC_CONTROLLER_METADATA_DONE) {
            for (int retryTimes = 0; retryTimes < 5; retryTimes++) {
                if (register()) {
                    this.state = State.REGISTER_TO_CONTROLLER_DONE;
                    LOGGER.info("First time register broker success, change state to: {}", this.state);
                    break;
                }

                // Try to avoid registration concurrency conflicts in random sleep
                // 通过随机退避降低并发注册冲突概率
                try {
                    Thread.sleep(random.nextInt(1000));
                } catch (Exception ignore) {

                }
            }
            // register 5 times but still unsuccessful
            // 连续重试 5 次仍失败时返回启动失败
            if (this.state != State.REGISTER_TO_CONTROLLER_DONE) {
                LOGGER.error("Register to broker failed 5 times");
                return false;
            }
        }

        if (this.state == State.REGISTER_TO_CONTROLLER_DONE) {
            // The scheduled task for heartbeat sending is not starting now, so we should manually send heartbeat request
            // 心跳定时任务尚未启动, 先主动发送一次心跳避免窗口期失联
            this.sendHeartbeatToController();
            if (this.masterBrokerId != null || brokerElect()) {
                LOGGER.info("Master in this broker set is elected, masterBrokerId: {}, masterBrokerAddr: {}", this.masterBrokerId, this.masterAddress);
                this.state = State.RUNNING;
                setFenced(false);
                LOGGER.info("All register process has been done, change state to: {}", this.state);
            } else {
                return false;
            }
        }

        schedulingSyncBrokerMetadata();

        // Register syncStateSet changed listener.
        // 注册 SyncStateSet 变化监听器, 触发后向 controller 上报变更
        this.haService.registerSyncStateSetChangedListener(this::doReportSyncStateSetChanged);
        return true;
    }

    /**
     * 关闭副本管理器, 停止线程池并重置注册状态
     */
    public void shutdown() {
        this.state = State.SHUTDOWN;
        this.registerState = RegisterState.INITIAL;
        this.executorService.shutdownNow();
        this.scheduledService.shutdownNow();
        this.scanExecutor.shutdownNow();
    }

    /**
     * 根据 controller 下发的新主信息切换 broker 角色
     *
     * @param newMasterBrokerId 新 master 的 brokerId
     * @param newMasterAddress 新 master 地址
     * @param newMasterEpoch 新 master 任期
     * @param syncStateSetEpoch 新同步集合版本
     * @param syncStateSet 新同步集合
     * @throws Exception 角色切换过程中抛出的异常
     */
    public synchronized void changeBrokerRole(final Long newMasterBrokerId, final String newMasterAddress,
        final Integer newMasterEpoch,
        final Integer syncStateSetEpoch, final Set<Long> syncStateSet) throws Exception {
        if (newMasterBrokerId != null && newMasterEpoch > this.masterEpoch) {
            if (newMasterBrokerId.equals(this.brokerControllerId)) {
                changeToMaster(newMasterEpoch, syncStateSetEpoch, syncStateSet);
            } else {
                changeToSlave(newMasterAddress, newMasterEpoch, newMasterBrokerId);
            }
        }
    }

    /**
     * 将当前节点切换为 master 角色, 同步配置并触发注册
     *
     * @param newMasterEpoch 新 master 任期
     * @param syncStateSetEpoch SyncStateSet 任期
     * @param syncStateSet 目标同步副本集合
     * @throws Exception 角色切换过程异常
     */
    public void changeToMaster(final int newMasterEpoch, final int syncStateSetEpoch, final Set<Long> syncStateSet) throws Exception {
        synchronized (this) {
            if (newMasterEpoch > this.masterEpoch) {
                LOGGER.info("Begin to change to master, brokerName:{}, replicas:{}, new Epoch:{}", this.brokerConfig.getBrokerName(), this.brokerAddress, newMasterEpoch);
                this.masterEpoch = newMasterEpoch;
                if (this.masterBrokerId != null && this.masterBrokerId.equals(this.brokerControllerId) && this.brokerController.getBrokerConfig().getBrokerId() == MixAll.MASTER_ID) {
                    // Change SyncStateSet
                    // 主节点未变化时仅刷新同步副本集合
                    final HashSet<Long> newSyncStateSet = new HashSet<>(syncStateSet);
                    changeSyncStateSet(newSyncStateSet, syncStateSetEpoch);
                    // if master doesn't change
                    // 主节点未变化时保持 master 角色并刷新数据版本
                    this.haService.changeToMasterWhenLastRoleIsMaster(newMasterEpoch);
                    this.brokerController.getTopicConfigManager().getDataVersion().nextVersion(newMasterEpoch);
                    this.executorService.submit(this::checkSyncStateSetAndDoReport);
                    registerBrokerWhenRoleChange();
                    return;
                }

                // Change SyncStateSet
                // 主节点切换后先应用 controller 下发的同步副本集合
                final HashSet<Long> newSyncStateSet = new HashSet<>(syncStateSet);
                changeSyncStateSet(newSyncStateSet, syncStateSetEpoch);

                // Handle the slave synchronise
                // 切主前关闭 slave 同步任务, 避免主从逻辑并存
                handleSlaveSynchronize(BrokerRole.SYNC_MASTER);

                // Notify ha service, change to master
                // 通知 HA 服务进入 master 复制模式
                this.haService.changeToMaster(newMasterEpoch);

                this.brokerController.getBrokerConfig().setBrokerId(MixAll.MASTER_ID);
                this.brokerController.getMessageStoreConfig().setBrokerRole(BrokerRole.SYNC_MASTER);
                this.brokerController.changeSpecialServiceStatus(true);

                // Change record
                // 更新内存态主节点记录
                this.masterAddress = this.brokerAddress;
                this.masterBrokerId = this.brokerControllerId;

                schedulingCheckSyncStateSet();

                this.brokerController.getTopicConfigManager().getDataVersion().nextVersion(newMasterEpoch);
                this.executorService.submit(this::checkSyncStateSetAndDoReport);
                registerBrokerWhenRoleChange();
            }
        }
    }

    /**
     * 将当前节点切换为 slave 角色, 对齐 master 地址和复制关系
     *
     * @param newMasterAddress 新 master 地址
     * @param newMasterEpoch 新 master 任期
     * @param newMasterBrokerId 新 master brokerId
     */
    public void changeToSlave(final String newMasterAddress, final int newMasterEpoch, Long newMasterBrokerId) {
        synchronized (this) {
            if (newMasterEpoch > this.masterEpoch) {
                LOGGER.info("Begin to change to slave, brokerName={}, brokerId={}, newMasterBrokerId={}, newMasterAddress={}, newMasterEpoch={}",
                    this.brokerConfig.getBrokerName(), this.brokerControllerId, newMasterBrokerId, newMasterAddress, newMasterEpoch);

                this.masterEpoch = newMasterEpoch;
                if (newMasterBrokerId.equals(this.masterBrokerId)) {
                    // if master doesn't change
                    // 主节点未变化时仅刷新从节点复制状态
                    this.haService.changeToSlaveWhenMasterNotChange(newMasterAddress, newMasterEpoch);
                    this.brokerController.getTopicConfigManager().getDataVersion().nextVersion(newMasterEpoch);
                    registerBrokerWhenRoleChange();
                    return;
                }

                // Stop checking syncStateSet because only master is able to check
                // 仅 master 需要检查 SyncStateSet, 切从时应停止任务
                stopCheckSyncStateSet();

                // Change config(compatibility problem)
                // 更新存储角色和特殊服务状态, 兼容 brokerId 语义
                this.brokerController.getMessageStoreConfig().setBrokerRole(BrokerRole.SLAVE);
                this.brokerController.changeSpecialServiceStatus(false);
                // The brokerId in brokerConfig just means its role(master[0] or slave[>=1])
                // brokerConfig 的 brokerId 仅用于区分 master 或 slave 角色
                this.brokerConfig.setBrokerId(brokerControllerId);

                // Change record
                // 刷新当前感知的 master 标识和地址
                this.masterAddress = newMasterAddress;
                this.masterBrokerId = newMasterBrokerId;

                // Handle the slave synchronise
                // 启动从节点同步任务, 周期拉取主节点数据
                handleSlaveSynchronize(BrokerRole.SLAVE);

                // Notify ha service, change to slave
                // 通知 HA 服务切换到 slave 模式
                this.haService.changeToSlave(newMasterAddress, newMasterEpoch, brokerControllerId);

                this.brokerController.getTopicConfigManager().getDataVersion().nextVersion(newMasterEpoch);
                registerBrokerWhenRoleChange();
            }
        }
    }

    /**
     * 角色切换后重新向 NameServer 注册 broker 信息
     */
    public void registerBrokerWhenRoleChange() {

        this.executorService.submit(() -> {
            // Register broker to name-srv
            // 向 NameServer 注册最新角色和地址信息
            try {
                this.brokerController.registerBrokerAll(true, false, this.brokerController.getBrokerConfig().isForceRegister());
            } catch (final Throwable e) {
                LOGGER.error("Error happen when register broker to name-srv, Failed to change broker to {}", this.brokerController.getMessageStoreConfig().getBrokerRole(), e);
                return;
            }
            LOGGER.info("Change broker [id:{}][address:{}] to {}, newMasterBrokerId:{}, newMasterAddress:{}, newMasterEpoch:{}, syncStateSetEpoch:{}",
                this.brokerControllerId, this.brokerAddress, this.brokerController.getMessageStoreConfig().getBrokerRole(), this.masterBrokerId, this.masterAddress, this.masterEpoch, this.syncStateSetEpoch);
        });

    }

    /**
     * 应用新的 SyncStateSet, 并同步到 HA 服务
     *
     * @param newSyncStateSet 新同步副本集合
     * @param newSyncStateSetEpoch 新集合版本号
     */
    private void changeSyncStateSet(final Set<Long> newSyncStateSet, final int newSyncStateSetEpoch) {
        synchronized (this) {
            if (newSyncStateSetEpoch > this.syncStateSetEpoch) {
                LOGGER.info("SyncStateSet changed from {} to {}", this.syncStateSet, newSyncStateSet);
                this.syncStateSetEpoch = newSyncStateSetEpoch;
                this.syncStateSet = new HashSet<>(newSyncStateSet);
                this.haService.setSyncStateSet(newSyncStateSet);
            }
        }
    }

    /**
     * 根据目标角色管理 slave 同步任务, 保证主从切换过程复制状态正确
     *
     * @param role 目标 broker 角色
     */
    private void handleSlaveSynchronize(final BrokerRole role) {
        if (role == BrokerRole.SLAVE) {
            if (this.slaveSyncFuture != null) {
                this.slaveSyncFuture.cancel(false);
            }
            this.brokerController.getSlaveSynchronize().setMasterAddr(this.masterAddress);
            slaveSyncFuture = this.brokerController.getScheduledExecutorService().scheduleAtFixedRate(() -> {
                try {
                    if (System.currentTimeMillis() - lastSyncTimeMs > 10 * 1000) {
                        brokerController.getSlaveSynchronize().syncAll();
                        lastSyncTimeMs = System.currentTimeMillis();
                    }
                    //timer checkpoint, latency-sensitive, so sync it more frequently
                    // 定时器检查点对延迟敏感, 需要更高频率同步
                    brokerController.getSlaveSynchronize().syncTimerCheckPoint();
                } catch (final Throwable e) {
                    LOGGER.error("ScheduledTask SlaveSynchronize syncAll error.", e);
                }
            }, 1000 * 3, 1000 * 3, TimeUnit.MILLISECONDS);

        } else {
            if (this.slaveSyncFuture != null) {
                this.slaveSyncFuture.cancel(false);
            }
            this.brokerController.getSlaveSynchronize().setMasterAddr(null);
        }
    }

    /**
     * 向 controller 发起选主请求, 并按返回结果执行主从切换
     *
     * @return 选主流程是否成功
     */
    private boolean brokerElect() {
        // Broker try to elect itself as a master in broker set.
        // 当前 broker 尝试在副本组内触发选主流程
        try {
            Pair<ElectMasterResponseHeader, Set<Long>> tryElectResponsePair = this.brokerOuterAPI.brokerElect(this.controllerLeaderAddress, this.brokerConfig.getBrokerClusterName(),
                this.brokerConfig.getBrokerName(), this.brokerControllerId);
            ElectMasterResponseHeader tryElectResponse = tryElectResponsePair.getObject1();
            Set<Long> syncStateSet = tryElectResponsePair.getObject2();
            final String masterAddress = tryElectResponse.getMasterAddress();
            final Long masterBrokerId = tryElectResponse.getMasterBrokerId();
            if (StringUtils.isEmpty(masterAddress) || masterBrokerId == null) {
                LOGGER.warn("Now no master in broker set");
                return false;
            }

            if (masterBrokerId.equals(this.brokerControllerId)) {
                changeToMaster(tryElectResponse.getMasterEpoch(), tryElectResponse.getSyncStateSetEpoch(), syncStateSet);
            } else {
                changeToSlave(masterAddress, tryElectResponse.getMasterEpoch(), tryElectResponse.getMasterBrokerId());
            }
            return true;
        } catch (Exception e) {
            LOGGER.error("Failed to try elect", e);
            return false;
        }
    }

    /**
     * 向所有可用 controller 发送心跳, 上报 broker 当前复制状态
     */
    public void sendHeartbeatToController() {
        final List<String> controllerAddresses = this.getAvailableControllerAddresses();
        for (String controllerAddress : controllerAddresses) {
            if (StringUtils.isNotEmpty(controllerAddress)) {
                this.brokerOuterAPI.sendHeartbeatToController(
                    controllerAddress,
                    this.brokerConfig.getBrokerClusterName(),
                    this.brokerAddress,
                    this.brokerConfig.getBrokerName(),
                    this.brokerControllerId,
                    this.brokerConfig.getSendHeartbeatTimeoutMillis(),
                    this.brokerConfig.isInBrokerContainer(), this.getLastEpoch(),
                    this.brokerController.getMessageStore().getMaxPhyOffset(),
                    this.brokerController.getMessageStore().getConfirmOffset(),
                    this.brokerConfig.getControllerHeartBeatTimeoutMills(),
                    this.brokerConfig.getBrokerElectionPriority()
                );
            }
        }
    }

    /**
     * Register broker to controller, and persist the metadata to file
     * <br>
     * 向 controller 注册 broker 并持久化本地元数据
     *
     * @return whether registering process succeeded
     */
    private boolean register() {
        try {
            // 1. confirm now registering state
            // 1. 确认当前注册流程所处状态
            confirmNowRegisteringState();
            LOGGER.info("Confirm now register state: {}", this.registerState);
            // 2. check metadata/tempMetadata if valid
            // 2. 校验 metadata 和 tempMetadata 是否与当前配置一致
            if (!checkMetadataValid()) {
                LOGGER.error("Check and find that metadata/tempMetadata invalid, you can modify the broker config to make them valid");
                return false;
            }
            // 2. get next assigning brokerId, and create temp metadata file
            // 2. 获取待分配 brokerId 并写入临时元数据文件
            if (this.registerState == RegisterState.INITIAL) {
                Long nextBrokerId = getNextBrokerId();
                if (nextBrokerId == null || !createTempMetadataFile(nextBrokerId)) {
                    LOGGER.error("Failed to create temp metadata file, nextBrokerId: {}", nextBrokerId);
                    return false;
                }
                this.registerState = RegisterState.CREATE_TEMP_METADATA_FILE_DONE;
                LOGGER.info("Register state change to {}, temp metadata: {}", this.registerState, this.tempBrokerMetadata);
            }
            // 3. apply brokerId to controller, and create metadata file
            // 3. 向 controller 申请 brokerId 并落盘正式元数据
            if (this.registerState == RegisterState.CREATE_TEMP_METADATA_FILE_DONE) {
                if (!applyBrokerId()) {
                    // apply broker id failed, means that this brokerId has been used
                    // brokerId 申请失败通常表示该 id 已被占用
                    // delete temp metadata file
                    // 删除临时元数据文件, 避免脏状态残留
                    this.tempBrokerMetadata.clear();
                    // back to the first step
                    // 回退到初始状态, 下次重新分配 brokerId
                    this.registerState = RegisterState.INITIAL;
                    LOGGER.info("Register state change to: {}", this.registerState);
                    return false;
                }
                if (!createMetadataFileAndDeleteTemp()) {
                    LOGGER.error("Failed to create metadata file and delete temp metadata file, temp metadata: {}", this.tempBrokerMetadata);
                    return false;
                }
                this.registerState = RegisterState.CREATE_METADATA_FILE_DONE;
                LOGGER.info("Register state change to: {}, metadata: {}", this.registerState, this.brokerMetadata);
            }
            // 4. register
            // 4. 携带正式元数据完成 broker 注册
            if (this.registerState == RegisterState.CREATE_METADATA_FILE_DONE) {
                if (!registerBrokerToController()) {
                    LOGGER.error("Failed to register broker to controller");
                    return false;
                }
                this.registerState = RegisterState.REGISTERED;
                LOGGER.info("Register state change to: {}, masterBrokerId: {}, masterBrokerAddr: {}", this.registerState, this.masterBrokerId, this.masterAddress);
            }
            return true;
        } catch (final Exception e) {
            LOGGER.error("Failed to register broker to controller", e);
            return false;
        }
    }

    /**
     * Send GetNextBrokerRequest to controller for getting next assigning brokerId in this broker-set
     * <br>
     * 向 controller 申请当前 broker 集内可用的下一个 brokerId
     *
     * @return next brokerId in this broker-set
     */
    private Long getNextBrokerId() {
        try {
            GetNextBrokerIdResponseHeader nextBrokerIdResp = this.brokerOuterAPI.getNextBrokerId(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.getBrokerName(), this.controllerLeaderAddress);
            return nextBrokerIdResp.getNextBrokerId();
        } catch (Exception e) {
            LOGGER.error("fail to get next broker id from controller", e);
            return null;
        }
    }

    /**
     * Create temp metadata file in local file system, records the brokerId and registerCheckCode
     * <br>
     * 在本地创建临时元数据文件, 记录待申请 brokerId 和校验码
     *
     * @param brokerId the brokerId that is expected to be assigned
     * @return whether the temp meta file is created successfully
     */

    private boolean createTempMetadataFile(Long brokerId) {
        // generate register check code, format like that: $ipAddress;$timestamp
        // 生成注册校验码, 格式为 ipAddress;timestamp
        String registerCheckCode = this.brokerAddress + ";" + System.currentTimeMillis();
        try {
            this.tempBrokerMetadata.updateAndPersist(brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(), brokerId, registerCheckCode);
            return true;
        } catch (Exception e) {
            LOGGER.error("update and persist temp broker metadata file failed", e);
            this.tempBrokerMetadata.clear();
            return false;
        }
    }

    /**
     * Send applyBrokerId request to controller
     * <br>
     * 向 controller 申请使用临时元数据中的 brokerId
     *
     * @return whether controller has assigned this brokerId for this broker
     */
    private boolean applyBrokerId() {
        try {
            ApplyBrokerIdResponseHeader response = this.brokerOuterAPI.applyBrokerId(brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(),
                tempBrokerMetadata.getBrokerId(), tempBrokerMetadata.getRegisterCheckCode(), this.controllerLeaderAddress);
            return true;

        } catch (Exception e) {
            LOGGER.error("fail to apply broker id: {}", tempBrokerMetadata.getBrokerId(), e);
            return false;
        }
    }

    /**
     * Create metadata file and delete temp metadata file
     * <br>
     * 生成正式元数据文件并删除临时元数据文件
     *
     * @return whether process success
     */
    private boolean createMetadataFileAndDeleteTemp() {
        // create metadata file and delete temp metadata file
        // 落盘正式元数据后清理临时文件并刷新本地 brokerId
        try {
            this.brokerMetadata.updateAndPersist(brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(), tempBrokerMetadata.getBrokerId());
            this.tempBrokerMetadata.clear();
            this.brokerControllerId = this.brokerMetadata.getBrokerId();
            this.haService.setLocalBrokerId(this.brokerControllerId);
            return true;
        } catch (Exception e) {
            LOGGER.error("fail to create metadata file", e);
            this.brokerMetadata.clear();
            return false;
        }
    }

    /**
     * Send registerBrokerToController request to inform controller that now broker has been registered successfully and
     * controller should update broker ipAddress if changed
     * <br>
     * 通知 controller 当前 broker 注册成功并在地址变更时刷新记录
     *
     * @return whether request success
     */
    private boolean registerBrokerToController() {
        try {
            Pair<RegisterBrokerToControllerResponseHeader, Set<Long>> responsePair = this.brokerOuterAPI.registerBrokerToController(brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(), brokerControllerId, brokerAddress, controllerLeaderAddress);
            if (responsePair == null)
                return false;
            RegisterBrokerToControllerResponseHeader response = responsePair.getObject1();
            Set<Long> syncStateSet = responsePair.getObject2();
            final Long masterBrokerId = response.getMasterBrokerId();
            final String masterAddress = response.getMasterAddress();
            if (masterBrokerId == null) {
                return true;
            }
            if (this.brokerControllerId.equals(masterBrokerId)) {
                changeToMaster(response.getMasterEpoch(), response.getSyncStateSetEpoch(), syncStateSet);
            } else {
                changeToSlave(masterAddress, response.getMasterEpoch(), masterBrokerId);
            }
            return true;
        } catch (Exception e) {
            LOGGER.error("fail to send registerBrokerToController request to controller", e);
            return false;
        }
    }

    /**
     * Confirm the registering state now
     * <br>
     * 根据本地 metadata 和 tempMetadata 文件恢复当前注册状态
     */
    private void confirmNowRegisteringState() {
        // 1. check if metadata exist
        // 1. 检查正式元数据是否存在
        try {
            this.brokerMetadata.readFromFile();
        } catch (Exception e) {
            LOGGER.error("Read metadata file failed", e);
        }
        if (this.brokerMetadata.isLoaded()) {
            this.registerState = RegisterState.CREATE_METADATA_FILE_DONE;
            this.brokerControllerId = brokerMetadata.getBrokerId();
            this.haService.setLocalBrokerId(this.brokerControllerId);
            return;
        }
        // 2. check if temp metadata exist
        // 2. 检查临时元数据是否存在
        try {
            this.tempBrokerMetadata.readFromFile();
        } catch (Exception e) {
            LOGGER.error("Read temp metadata file failed", e);
        }
        if (this.tempBrokerMetadata.isLoaded()) {
            this.registerState = RegisterState.CREATE_TEMP_METADATA_FILE_DONE;
        }
    }

    /**
     * 校验本地元数据文件内容是否与当前 broker 配置一致
     *
     * @return 元数据是否有效
     */
    private boolean checkMetadataValid() {
        if (this.registerState == RegisterState.CREATE_TEMP_METADATA_FILE_DONE) {
            if (this.tempBrokerMetadata.getClusterName() == null || !this.tempBrokerMetadata.getClusterName().equals(this.brokerConfig.getBrokerClusterName())) {
                LOGGER.error("The clusterName: {} in broker temp metadata is different from the clusterName: {} in broker config",
                    this.tempBrokerMetadata.getClusterName(), this.brokerConfig.getBrokerClusterName());
                return false;
            }
            if (this.tempBrokerMetadata.getBrokerName() == null || !this.tempBrokerMetadata.getBrokerName().equals(this.brokerConfig.getBrokerName())) {
                LOGGER.error("The brokerName: {} in broker temp metadata is different from the brokerName: {} in broker config",
                    this.tempBrokerMetadata.getBrokerName(), this.brokerConfig.getBrokerName());
                return false;
            }
        }
        if (this.registerState == RegisterState.CREATE_METADATA_FILE_DONE) {
            if (this.brokerMetadata.getClusterName() == null || !this.brokerMetadata.getClusterName().equals(this.brokerConfig.getBrokerClusterName())) {
                LOGGER.error("The clusterName: {} in broker metadata is different from the clusterName: {} in broker config",
                    this.brokerMetadata.getClusterName(), this.brokerConfig.getBrokerClusterName());
                return false;
            }
            if (this.brokerMetadata.getBrokerName() == null || !this.brokerMetadata.getBrokerName().equals(this.brokerConfig.getBrokerName())) {
                LOGGER.error("The brokerName: {} in broker metadata is different from the brokerName: {} in broker config",
                    this.brokerMetadata.getBrokerName(), this.brokerConfig.getBrokerName());
                return false;
            }
        }
        return true;
    }

    /**
     * Scheduling sync broker metadata form controller.
     * <br>
     * 定时从 controller 同步副本元数据并按需触发角色切换
     */
    private void schedulingSyncBrokerMetadata() {
        this.scheduledService.scheduleAtFixedRate(() -> {
            try {
                final Pair<GetReplicaInfoResponseHeader, SyncStateSet> result = this.brokerOuterAPI.getReplicaInfo(this.controllerLeaderAddress, this.brokerConfig.getBrokerName());
                final GetReplicaInfoResponseHeader info = result.getObject1();
                final SyncStateSet syncStateSet = result.getObject2();
                final String newMasterAddress = info.getMasterAddress();
                final int newMasterEpoch = info.getMasterEpoch();
                final Long masterBrokerId = info.getMasterBrokerId();
                synchronized (this) {
                    // Check if master changed
                    // 检查 master 任期是否发生变化
                    if (newMasterEpoch > this.masterEpoch) {
                        if (StringUtils.isNoneEmpty(newMasterAddress) && masterBrokerId != null) {
                            if (masterBrokerId.equals(this.brokerControllerId)) {
                                // If this broker is now the master
                                // 当前节点被选为 master, 进入主节点切换流程
                                changeToMaster(newMasterEpoch, syncStateSet.getSyncStateSetEpoch(), syncStateSet.getSyncStateSet());
                            } else {
                                // If this broker is now the slave, and master has been changed
                                // 当前节点成为 slave 且 master 已变化, 进入从节点切换流程
                                changeToSlave(newMasterAddress, newMasterEpoch, masterBrokerId);
                            }
                        } else {
                            // In this case, the master in controller is null, try elect in controller, this will trigger the electMasterEvent in controller.
                            // controller 暂无 master 时主动触发一次选主
                            brokerElect();
                        }
                    } else if (newMasterEpoch == this.masterEpoch) {
                        // Check if SyncStateSet changed
                        // master 未变化时仅检查同步副本集合是否变化
                        if (isMasterState()) {
                            changeSyncStateSet(syncStateSet.getSyncStateSet(), syncStateSet.getSyncStateSetEpoch());
                        }
                    }
                }
            } catch (final MQBrokerException exception) {
                LOGGER.warn("Error happen when get broker {}'s metadata", this.brokerConfig.getBrokerName(), exception);
                if (exception.getResponseCode() == CONTROLLER_BROKER_METADATA_NOT_EXIST) {
                    try {
                        registerBrokerToController();
                        TimeUnit.SECONDS.sleep(2);
                    } catch (InterruptedException ignore) {

                    }
                }
            } catch (final Exception e) {
                LOGGER.warn("Error happen when get broker {}'s metadata", this.brokerConfig.getBrokerName(), e);
            }
        }, 3 * 1000, this.brokerConfig.getSyncBrokerMetadataPeriod(), TimeUnit.MILLISECONDS);
    }

    /**
     * Scheduling sync controller metadata.
     * <br>
     * 定时同步 controller 元数据, 获取 leader 地址
     */
    private boolean schedulingSyncControllerMetadata() {
        // Get controller metadata first.
        // 启动阶段先同步一次 controller 元数据
        int tryTimes = 0;
        while (tryTimes < 3) {
            boolean flag = updateControllerMetadata();
            if (flag) {
                this.scheduledService.scheduleAtFixedRate(this::updateControllerMetadata, 1000 * 3, this.brokerConfig.getSyncControllerMetadataPeriod(), TimeUnit.MILLISECONDS);
                return true;
            }
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException ignore) {

            }
            tryTimes++;
        }
        LOGGER.error("Failed to init controller metadata, maybe the controllers in {} is not available", this.controllerAddresses);
        return false;
    }

    /**
     * Update controller leader address by rpc.
     * <br>
     * 通过 RPC 更新 controller leader 地址
     */
    private boolean updateControllerMetadata() {
        for (String address : this.availableControllerAddresses.keySet()) {
            try {
                final GetMetaDataResponseHeader responseHeader = this.brokerOuterAPI.getControllerMetaData(address);
                if (responseHeader != null && StringUtils.isNoneEmpty(responseHeader.getControllerLeaderAddress())) {
                    this.controllerLeaderAddress = responseHeader.getControllerLeaderAddress();
                    LOGGER.info("Update controller leader address to {}", this.controllerLeaderAddress);
                    return true;
                }
            } catch (final Exception e) {
                LOGGER.error("Failed to update controller metadata", e);
            }
        }
        return false;
    }

    /**
     * Scheduling check syncStateSet.
     * <br>
     * 定时检查并上报 SyncStateSet 变化
     */
    private void schedulingCheckSyncStateSet() {
        if (this.checkSyncStateSetTaskFuture != null) {
            this.checkSyncStateSetTaskFuture.cancel(false);
        }
        this.checkSyncStateSetTaskFuture = this.scheduledService.scheduleAtFixedRate(this::checkSyncStateSetAndDoReport, 3 * 1000,
            this.brokerConfig.getCheckSyncStateSetPeriod(), TimeUnit.MILLISECONDS);
    }

    /**
     * 检查本地 SyncStateSet 是否变化, 变化时上报 controller
     */
    private void checkSyncStateSetAndDoReport() {
        try {
            final Set<Long> newSyncStateSet = this.haService.maybeShrinkSyncStateSet();
            newSyncStateSet.add(this.brokerControllerId);
            synchronized (this) {
                if (this.syncStateSet != null) {
                    // Check if syncStateSet changed
                    // 若集合元素未变化则跳过上报
                    if (this.syncStateSet.size() == newSyncStateSet.size() && this.syncStateSet.containsAll(newSyncStateSet)) {
                        return;
                    }
                }
            }
            doReportSyncStateSetChanged(newSyncStateSet);
        } catch (Exception e) {
            LOGGER.error("Check syncStateSet error", e);
        }
    }

    /**
     * 上报 SyncStateSet 变化并应用 controller 返回的新版本
     *
     * @param newSyncStateSet 期望上报的新同步副本集合
     */
    private void doReportSyncStateSetChanged(Set<Long> newSyncStateSet) {
        try {
            final SyncStateSet result = this.brokerOuterAPI.alterSyncStateSet(this.controllerLeaderAddress, this.brokerConfig.getBrokerName(), this.brokerControllerId, this.masterEpoch, newSyncStateSet, this.syncStateSetEpoch);
            if (result != null) {
                changeSyncStateSet(result.getSyncStateSet(), result.getSyncStateSetEpoch());
            }
        } catch (final Exception e) {
            LOGGER.error("Error happen when change SyncStateSet, broker:{}, masterAddress:{}, masterEpoch:{}, oldSyncStateSet:{}, newSyncStateSet:{}, syncStateSetEpoch:{}",
                this.brokerConfig.getBrokerName(), this.masterAddress, this.masterEpoch, this.syncStateSet, newSyncStateSet, this.syncStateSetEpoch, e);
        }
    }

    /**
     * 停止 SyncStateSet 定时检查任务
     */
    private void stopCheckSyncStateSet() {
        if (this.checkSyncStateSetTaskFuture != null) {
            this.checkSyncStateSetTaskFuture.cancel(false);
        }
    }

    /**
     * 扫描并维护可用 controller 地址集合
     */
    private void scanAvailableControllerAddresses() {
        if (controllerAddresses == null) {
            LOGGER.warn("scanAvailableControllerAddresses addresses of controller is null!");
            return;
        }

        for (String address : availableControllerAddresses.keySet()) {
            if (!controllerAddresses.contains(address)) {
                LOGGER.warn("scanAvailableControllerAddresses remove invalid address {}", address);
                availableControllerAddresses.remove(address);
            }
        }

        for (String address : controllerAddresses) {
            scanExecutor.submit(() -> {
                if (brokerOuterAPI.checkAddressReachable(address)) {
                    availableControllerAddresses.putIfAbsent(address, true);
                } else {
                    Boolean value = availableControllerAddresses.remove(address);
                    if (value != null) {
                        LOGGER.warn("scanAvailableControllerAddresses remove unconnected address {}", address);
                    }
                }
            });
        }
    }

    /**
     * 更新 controller 地址列表, 支持 DNS 解析或静态地址配置
     */
    private void updateControllerAddr() {
        if (brokerConfig.isFetchControllerAddrByDnsLookup()) {
            List<String> adders = brokerOuterAPI.dnsLookupAddressByDomain(this.brokerConfig.getControllerAddr());
            if (CollectionUtils.isNotEmpty(adders)) {
                this.controllerAddresses = adders;
            }
        } else {
            final String controllerPaths = this.brokerConfig.getControllerAddr();
            final String[] controllers = controllerPaths.split(";");
            assert controllers.length > 0;
            this.controllerAddresses = Arrays.asList(controllers);
        }
    }

    public int getLastEpoch() {
        return this.haService.getLastEpoch();
    }

    public BrokerRole getBrokerRole() {
        return this.brokerController.getMessageStoreConfig().getBrokerRole();
    }

    public boolean isMasterState() {
        return getBrokerRole() == BrokerRole.SYNC_MASTER;
    }

    public SyncStateSet getSyncStateSet() {
        return new SyncStateSet(this.syncStateSet, this.syncStateSetEpoch);
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    public String getMasterAddress() {
        return masterAddress;
    }

    public int getMasterEpoch() {
        return masterEpoch;
    }

    public List<String> getControllerAddresses() {
        return controllerAddresses;
    }

    public List<EpochEntry> getEpochEntries() {
        return this.haService.getEpochEntries();
    }

    public List<String> getAvailableControllerAddresses() {
        return new ArrayList<>(availableControllerAddresses.keySet());
    }

    public Long getBrokerControllerId() {
        return brokerControllerId;
    }

    public RegisterState getRegisterState() {
        return registerState;
    }

    public State getState() {
        return state;
    }

    public BrokerMetadata getBrokerMetadata() {
        return brokerMetadata;
    }

    public TempBrokerMetadata getTempBrokerMetadata() {
        return tempBrokerMetadata;
    }

    public void setFenced(boolean fenced) {
        this.brokerController.setIsolated(fenced);
        this.brokerController.getMessageStore().getRunningFlags().makeFenced(fenced);
    }
}
