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
package org.apache.rocketmq.broker.dledger;

import io.openmessaging.storage.dledger.DLedgerLeaderElector;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class DLedgerRoleChangeHandler implements DLedgerLeaderElector.RoleChangeHandler {

    /**
     * 角色切换处理日志记录器, 用于输出主从切换过程状态
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    /**
     * 角色切换串行执行线程池, 保证同一时刻仅处理一个切换任务
     */
    private ExecutorService executorService;
    /**
     * Broker 控制器引用, 用于更新服务状态和执行注册动作
     */
    private BrokerController brokerController;
    /**
     * 默认消息存储实现, 提供角色切换时的数据状态判断能力
     */
    private DefaultMessageStore messageStore;
    /**
     * DLedger 提交日志对象, 提供节点标识和复制进度信息
     */
    private DLedgerCommitLog dLedgerCommitLog;
    /**
     * DLedger 服务实例, 用于判断成员角色和提交状态
     */
    private DLedgerServer dLegerServer;
    /**
     * slave 定时同步任务句柄, 角色变化时用于取消旧任务
     */
    private Future<?> slaveSyncFuture;
    /**
     * 最近一次执行全量同步的时间戳, 用于控制同步频率
     */
    private long lastSyncTimeMs = System.currentTimeMillis();

    /**
     * 构建 DLedger 角色切换处理器, 初始化依赖和串行执行线程池
     *
     * @param brokerController broker 控制器
     * @param messageStore 消息存储实例
     */
    public DLedgerRoleChangeHandler(BrokerController brokerController, DefaultMessageStore messageStore) {
        this.brokerController = brokerController;
        this.messageStore = messageStore;
        this.dLedgerCommitLog = (DLedgerCommitLog) messageStore.getCommitLog();
        this.dLegerServer = dLedgerCommitLog.getdLedgerServer();
        this.executorService = ThreadUtils.newSingleThreadExecutor(
            new ThreadFactoryImpl("DLegerRoleChangeHandler_", brokerController.getBrokerIdentity()));
    }

    /**
     * 接收 DLedger 角色变更事件, 异步串行执行对应的主从切换逻辑
     *
     * @param term 当前任期
     * @param role 目标角色
     */
    @Override
    public void handle(long term, MemberState.Role role) {
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                long start = System.currentTimeMillis();
                try {
                    boolean succ = true;
                    LOGGER.info("Begin handling broker role change term={} role={} currStoreRole={}", term, role, messageStore.getMessageStoreConfig().getBrokerRole());
                    switch (role) {
                        case CANDIDATE:
                            if (messageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE) {
                                changeToSlave(dLedgerCommitLog.getId());
                            }
                            break;
                        case FOLLOWER:
                            changeToSlave(dLedgerCommitLog.getId());
                            break;
                        case LEADER:
                            while (true) {
                                if (!dLegerServer.getMemberState().isLeader()) {
                                    succ = false;
                                    break;
                                }
                                if (dLegerServer.getDLedgerStore().getLedgerEndIndex() == -1) {
                                    break;
                                }
                                if (dLegerServer.getDLedgerStore().getLedgerEndIndex() == dLegerServer.getDLedgerStore().getCommittedIndex()
                                    && messageStore.dispatchBehindBytes() == 0) {
                                    break;
                                }
                                Thread.sleep(100);
                            }
                            if (succ) {
                                messageStore.recoverTopicQueueTable();
                                changeToMaster(BrokerRole.SYNC_MASTER);
                            }
                            break;
                        default:
                            break;
                    }
                    LOGGER.info("Finish handling broker role change succ={} term={} role={} currStoreRole={} cost={}", succ, term, role, messageStore.getMessageStoreConfig().getBrokerRole(), DLedgerUtils.elapsed(start));
                } catch (Throwable t) {
                    LOGGER.info("[MONITOR]Failed handling broker role change term={} role={} currStoreRole={} cost={}", term, role, messageStore.getMessageStoreConfig().getBrokerRole(), DLedgerUtils.elapsed(start), t);
                }
            }
        };
        executorService.submit(runnable);
    }

    /**
     * 根据目标角色管理 slave 同步任务, 确保主从状态下同步行为一致
     *
     * @param role broker 当前目标角色
     */
    private void handleSlaveSynchronize(BrokerRole role) {
        if (role == BrokerRole.SLAVE) {
            if (null != slaveSyncFuture) {
                slaveSyncFuture.cancel(false);
            }
            this.brokerController.getSlaveSynchronize().setMasterAddr(null);
            slaveSyncFuture = this.brokerController.getScheduledExecutorService().scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (System.currentTimeMillis() - lastSyncTimeMs > 10 * 1000) {
                            brokerController.getSlaveSynchronize().syncAll();
                            lastSyncTimeMs = System.currentTimeMillis();
                        }
                        //timer checkpoint, latency-sensitive, so sync it more frequently
                        // 定时器检查点对延迟敏感, 需要更高频率同步
                        brokerController.getSlaveSynchronize().syncTimerCheckPoint();
                    } catch (Throwable e) {
                        LOGGER.error("ScheduledTask SlaveSynchronize syncAll error.", e);
                    }
                }
            }, 1000 * 3, 1000 * 3, TimeUnit.MILLISECONDS);
        } else {
            //handle the slave synchronise
            // 退出 slave 角色时停止同步任务并清理 master 地址
            if (null != slaveSyncFuture) {
                slaveSyncFuture.cancel(false);
            }
            this.brokerController.getSlaveSynchronize().setMasterAddr(null);
        }
    }

    /**
     * 将当前 broker 切换为 slave 角色, 并向 NameServer 重新注册
     *
     * @param brokerId 新角色下使用的 brokerId
     */
    public void changeToSlave(int brokerId) {
        LOGGER.info("Begin to change to slave brokerName={} brokerId={}", this.brokerController.getBrokerConfig().getBrokerName(), brokerId);

        //change the role
        // 切换 broker 标识和存储角色, 使当前节点进入从节点流程
        this.brokerController.getBrokerConfig().setBrokerId(brokerId == 0 ? 1 : brokerId); //TO DO check
        this.brokerController.getMessageStoreConfig().setBrokerRole(BrokerRole.SLAVE);

        this.brokerController.changeSpecialServiceStatus(false);

        //handle the slave synchronise
        // 启动从节点同步任务, 持续拉取主节点数据
        handleSlaveSynchronize(BrokerRole.SLAVE);

        try {
            this.brokerController.registerBrokerAll(true, true, this.brokerController.getBrokerConfig().isForceRegister());
        } catch (Throwable ignored) {

        }
        LOGGER.info("Finish to change to slave brokerName={} brokerId={}", this.brokerController.getBrokerConfig().getBrokerName(), brokerId);
    }

    /**
     * 将当前 broker 切换为 master 角色, 并恢复主节点服务能力
     *
     * @param role 目标主节点角色, 通常为 SYNC_MASTER
     */
    public void changeToMaster(BrokerRole role) {
        if (role == BrokerRole.SLAVE) {
            return;
        }
        LOGGER.info("Begin to change to master brokerName={}", this.brokerController.getBrokerConfig().getBrokerName());

        //handle the slave synchronise
        // 切换到主节点前先停止从节点同步任务
        handleSlaveSynchronize(role);

        this.brokerController.changeSpecialServiceStatus(true);

        //if the operations above are totally successful, we change to master
        // 上述步骤完成后再提交角色变更, 避免主从服务状态不一致
        this.brokerController.getBrokerConfig().setBrokerId(0); //TO DO check
        this.brokerController.getMessageStoreConfig().setBrokerRole(role);

        try {
            this.brokerController.registerBrokerAll(true, true, this.brokerController.getBrokerConfig().isForceRegister());
        } catch (Throwable ignored) {

        }
        LOGGER.info("Finish to change to master brokerName={}", this.brokerController.getBrokerConfig().getBrokerName());
    }

    /**
     * 处理器启动入口, 当前实现无需额外初始化动作
     */
    @Override
    public void startup() {

    }

    /**
     * 关闭角色切换处理器, 停止串行执行线程池
     */
    @Override
    public void shutdown() {
        executorService.shutdown();
    }
}
