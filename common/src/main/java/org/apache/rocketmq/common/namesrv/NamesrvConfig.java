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

/**
 * $Id: NamesrvConfig.java 1839 2013-05-16 02:12:02Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.namesrv;

import org.apache.rocketmq.common.MixAll;

import java.io.File;

public class NamesrvConfig {

    /**
     * RocketMQ 安装根目录<br>
     * 优先读取系统属性 ROCKETMQ_HOME_PROPERTY, 未设置时读取环境变量 ROCKETMQ_HOME_ENV
     */
    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));
    /**
     * NameServer 的 KV 配置持久化路径<br>
     * 默认位于当前用户目录下的 namesrv 子目录
     */
    private String kvConfigPath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator + "kvConfig.json";
    /**
     * NameServer 配置存储文件路径<br>
     * 默认位于当前用户目录下的 namesrv 子目录
     */
    private String configStorePath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator + "namesrv.properties";
    /**
     * 产品环境名称<br>
     * 用于区分不同部署环境下的 NameServer 行为
     */
    private String productEnvName = "center";
    /**
     * 集群测试开关<br>
     * 启用后可让 NameServer 进入测试场景相关逻辑
     */
    private boolean clusterTest = false;
    /**
     * 顺序消息开关<br>
     * 启用后允许 NameServer 提供顺序消息相关的配置能力
     */
    private boolean orderMessageEnable = false;
    /**
     * 是否向 Broker 返回顺序主题配置<br>
     * 启用后 Broker 在路由交互中可获取顺序主题相关信息
     */
    private boolean returnOrderTopicConfigToBroker = true;

    /**
     * Indicates the nums of thread to handle client requests, like GET_ROUTEINTO_BY_TOPIC.<br>
     * 处理客户端请求的线程池线程数<br>
     * 示例请求类型: GET_ROUTEINTO_BY_TOPIC
     */
    private int clientRequestThreadPoolNums = 8;
    /**
     * Indicates the nums of thread to handle broker or operation requests, like REGISTER_BROKER.<br>
     * 处理 Broker 请求与运维请求的线程池线程数<br>
     * 示例请求类型: REGISTER_BROKER
     */
    private int defaultThreadPoolNums = 16;
    /**
     * Indicates the capacity of queue to hold client requests.<br>
     * 客户端请求队列容量<br>
     * 用于限制待处理客户端请求的排队数量
     */
    private int clientRequestThreadPoolQueueCapacity = 50000;
    /**
     * Indicates the capacity of queue to hold broker or operation requests.<br>
     * Broker 请求与运维请求队列容量<br>
     * 用于限制待处理 Broker 请求的排队数量
     */
    private int defaultThreadPoolQueueCapacity = 10000;
    /**
     * Interval of periodic scanning for non-active broker;<br>
     * 非活跃 Broker 周期扫描间隔, 单位为毫秒<br>
     * NameServer 会按该周期清理长时间未上报心跳的 Broker
     */
    private long scanNotActiveBrokerInterval = 5 * 1000;

    /**
     * Broker 下线任务队列容量<br>
     * 用于限制反注册事件的堆积数量, 防止突发下线导致内存压力过高
     */
    private int unRegisterBrokerQueueCapacity = 3000;

    /**
     * Support acting master or not.<br>
     * 是否支持代理主节点模式
     * <p>
     * The slave can be an acting master when master node is down to support following operations:<br>
     * 1. support lock/unlock message queue operation.<br>
     * 2. support searchOffset, query maxOffset/minOffset operation.<br>
     * 3. support query earliest msg store time.<br>
     * 当主节点不可用时, 从节点可临时承担主节点职责, 支持以下能力:<br>
     * - 支持消息队列锁定与解锁操作<br>
     * - 支持 searchOffset 与最大最小偏移量查询<br>
     * - 支持最早消息存储时间查询
     */
    private boolean supportActingMaster = false;

    /**
     * 全量主题列表开关<br>
     * 启用后允许返回全部主题名称集合
     */
    private volatile boolean enableAllTopicList = true;

    /**
     * 主题列表开关<br>
     * 启用后允许执行主题列表查询能力
     */
    private volatile boolean enableTopicList = true;

    /**
     * 最小 BrokerId 变更通知开关<br>
     * 启用后 NameServer 在主从角色关键变更时可触发通知流程
     */
    private volatile boolean notifyMinBrokerIdChanged = false;

    /**
     * Is startup the controller in this name-srv<br>
     * 是否在当前 NameServer 进程中启动 Controller
     */
    private boolean enableControllerInNamesrv = false;

    /**
     * 服务可用前等待开关<br>
     * 启用后 NameServer 启动阶段会等待关键服务就绪
     */
    private volatile boolean needWaitForService = false;

    /**
     * 服务等待时长, 单位为秒<br>
     * 仅在 needWaitForService 启用时生效
     */
    private int waitSecondsForService = 45;

    /**
     * If enable this flag, the topics that don't exist in broker registration payload will be deleted from name server.<br>
     * 是否根据 Broker 注册数据删除缺失主题<br>
     * 启用后, 若某主题不在 Broker 注册载荷中, NameServer 会删除该主题路由
     * <p>
     * WARNING:
     * 1. Enable this flag and "enableSingleTopicRegister" of broker config meanwhile to avoid losing topic route info unexpectedly.<br>
     * 2. This flag does not support static topic currently.<br>
     * 注意: 相关约束如下<br>
     * - 需同时启用 Broker 侧 enableSingleTopicRegister, 避免意外丢失主题路由<br>
     * - 当前不支持静态主题场景
     */
    private boolean deleteTopicWithBrokerRegistration = false;
    /**
     * Config in this black list will be not allowed to update by command.<br>
     * Try to update this config black list by restart process.<br>
     * Try to update configures in black list by restart process.<br>
     * 配置黑名单<br>
     * 黑名单中的配置项不允许通过命令动态修改<br>
     * 若需调整黑名单内容或黑名单项对应配置, 需通过重启进程生效
     */
    private String configBlackList = "configBlackList;configStorePath;kvConfigPath";

    public String getConfigBlackList() {
        return configBlackList;
    }

    public void setConfigBlackList(String configBlackList) {
        this.configBlackList = configBlackList;
    }

    public boolean isOrderMessageEnable() {
        return orderMessageEnable;
    }

    public void setOrderMessageEnable(boolean orderMessageEnable) {
        this.orderMessageEnable = orderMessageEnable;
    }

    public String getRocketmqHome() {
        return rocketmqHome;
    }

    public void setRocketmqHome(String rocketmqHome) {
        this.rocketmqHome = rocketmqHome;
    }

    public String getKvConfigPath() {
        return kvConfigPath;
    }

    public void setKvConfigPath(String kvConfigPath) {
        this.kvConfigPath = kvConfigPath;
    }

    public String getProductEnvName() {
        return productEnvName;
    }

    public void setProductEnvName(String productEnvName) {
        this.productEnvName = productEnvName;
    }

    public boolean isClusterTest() {
        return clusterTest;
    }

    public void setClusterTest(boolean clusterTest) {
        this.clusterTest = clusterTest;
    }

    public String getConfigStorePath() {
        return configStorePath;
    }

    public void setConfigStorePath(final String configStorePath) {
        this.configStorePath = configStorePath;
    }

    public boolean isReturnOrderTopicConfigToBroker() {
        return returnOrderTopicConfigToBroker;
    }

    public void setReturnOrderTopicConfigToBroker(boolean returnOrderTopicConfigToBroker) {
        this.returnOrderTopicConfigToBroker = returnOrderTopicConfigToBroker;
    }

    public int getClientRequestThreadPoolNums() {
        return clientRequestThreadPoolNums;
    }

    public void setClientRequestThreadPoolNums(final int clientRequestThreadPoolNums) {
        this.clientRequestThreadPoolNums = clientRequestThreadPoolNums;
    }

    public int getDefaultThreadPoolNums() {
        return defaultThreadPoolNums;
    }

    public void setDefaultThreadPoolNums(final int defaultThreadPoolNums) {
        this.defaultThreadPoolNums = defaultThreadPoolNums;
    }

    public int getClientRequestThreadPoolQueueCapacity() {
        return clientRequestThreadPoolQueueCapacity;
    }

    public void setClientRequestThreadPoolQueueCapacity(final int clientRequestThreadPoolQueueCapacity) {
        this.clientRequestThreadPoolQueueCapacity = clientRequestThreadPoolQueueCapacity;
    }

    public int getDefaultThreadPoolQueueCapacity() {
        return defaultThreadPoolQueueCapacity;
    }

    public void setDefaultThreadPoolQueueCapacity(final int defaultThreadPoolQueueCapacity) {
        this.defaultThreadPoolQueueCapacity = defaultThreadPoolQueueCapacity;
    }

    public long getScanNotActiveBrokerInterval() {
        return scanNotActiveBrokerInterval;
    }

    public void setScanNotActiveBrokerInterval(long scanNotActiveBrokerInterval) {
        this.scanNotActiveBrokerInterval = scanNotActiveBrokerInterval;
    }

    public int getUnRegisterBrokerQueueCapacity() {
        return unRegisterBrokerQueueCapacity;
    }

    public void setUnRegisterBrokerQueueCapacity(final int unRegisterBrokerQueueCapacity) {
        this.unRegisterBrokerQueueCapacity = unRegisterBrokerQueueCapacity;
    }

    public boolean isSupportActingMaster() {
        return supportActingMaster;
    }

    public void setSupportActingMaster(final boolean supportActingMaster) {
        this.supportActingMaster = supportActingMaster;
    }

    public boolean isEnableAllTopicList() {
        return enableAllTopicList;
    }

    public void setEnableAllTopicList(boolean enableAllTopicList) {
        this.enableAllTopicList = enableAllTopicList;
    }

    public boolean isEnableTopicList() {
        return enableTopicList;
    }

    public void setEnableTopicList(boolean enableTopicList) {
        this.enableTopicList = enableTopicList;
    }

    public boolean isNotifyMinBrokerIdChanged() {
        return notifyMinBrokerIdChanged;
    }

    public void setNotifyMinBrokerIdChanged(boolean notifyMinBrokerIdChanged) {
        this.notifyMinBrokerIdChanged = notifyMinBrokerIdChanged;
    }

    public boolean isEnableControllerInNamesrv() {
        return enableControllerInNamesrv;
    }

    public void setEnableControllerInNamesrv(boolean enableControllerInNamesrv) {
        this.enableControllerInNamesrv = enableControllerInNamesrv;
    }

    public boolean isNeedWaitForService() {
        return needWaitForService;
    }

    public void setNeedWaitForService(boolean needWaitForService) {
        this.needWaitForService = needWaitForService;
    }

    public int getWaitSecondsForService() {
        return waitSecondsForService;
    }

    public void setWaitSecondsForService(int waitSecondsForService) {
        this.waitSecondsForService = waitSecondsForService;
    }

    public boolean isDeleteTopicWithBrokerRegistration() {
        return deleteTopicWithBrokerRegistration;
    }

    public void setDeleteTopicWithBrokerRegistration(boolean deleteTopicWithBrokerRegistration) {
        this.deleteTopicWithBrokerRegistration = deleteTopicWithBrokerRegistration;
    }
}
