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

import org.apache.rocketmq.common.metrics.MetricsExporterType;

import java.io.File;
import java.util.Arrays;

/**
 * Controller 配置模型<br>
 * 统一承载选主, 线程池, 存储路径, 指标导出等核心参数
 */
public class ControllerConfig {
    /**
     * RocketMQ 安装根目录<br>
     * 优先读取系统属性 ROCKETMQ_HOME_PROPERTY, 未设置时回退到环境变量 ROCKETMQ_HOME_ENV
     */
    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));
    /**
     * Controller 配置文件路径<br>
     * 默认路径为 user.home/controller/controller.properties
     */
    private String configStorePath = System.getProperty("user.home") + File.separator + "controller" + File.separator + "controller.properties";
    /**
     * DLedger 控制器类型常量<br>
     * 当 controllerType 取该值时启用 DLedger 选主链路
     */
    public static final String DLEDGER_CONTROLLER = "DLedger";
    /**
     * jRaft 控制器类型常量<br>
     * 当 controllerType 取该值时启用 jRaft 选主链路
     */
    public static final String JRAFT_CONTROLLER = "jRaft";

    /**
     * jRaft 子配置对象<br>
     * 仅在 controllerType 为 JRAFT_CONTROLLER 时生效
     */
    private JraftConfig jraftConfig = new JraftConfig();

    /**
     * 控制器实现类型<br>
     * 默认值为 DLEDGER_CONTROLLER
     */
    private String controllerType = DLEDGER_CONTROLLER;
    /**
     * Interval of periodic scanning for non-active broker;<br>
     * Unit: millisecond<br>
     * 非活跃 Broker 周期扫描间隔, 单位为毫秒<br>
     * Controller 按该周期检测 Broker 心跳并清理失活节点
     */
    private long scanNotActiveBrokerInterval = 5 * 1000;

    /**
     * Indicates the nums of thread to handle broker or operation requests, like REGISTER_BROKER.<br>
     * 处理 Broker 请求与运维请求的线程池大小<br>
     * 典型请求包括 REGISTER_BROKER 等控制面操作
     */
    private int controllerThreadPoolNums = 16;

    /**
     * Indicates the capacity of queue to hold client requests.<br>
     * 控制器请求队列容量<br>
     * 用于限制待处理请求数量, 避免突发流量导致资源挤压
     */
    private int controllerRequestThreadPoolQueueCapacity = 50000;

    /**
     * DLedger 组名<br>
     * 用于标识当前 Controller 所属复制组
     */
    private String controllerDLegerGroup;
    /**
     * DLedger 节点列表<br>
     * 格式通常为 id-address 片段并以分号分隔
     */
    private String controllerDLegerPeers;
    /**
     * 当前 Controller 在 DLedger 节点列表中的节点 id<br>
     * 该值用于定位本节点对应地址并参与选举
     */
    private String controllerDLegerSelfId;
    /**
     * 元数据映射文件大小, 单位为字节<br>
     * 默认值为 1GB
     */
    private int mappedFileSize = 1024 * 1024 * 1024;
    /**
     * Controller 存储目录<br>
     * 为空时在 getControllerStorePath 中按 controllerType 自动生成默认路径
     */
    private String controllerStorePath = "";

    /**
     * Max retry count for electing master when failed because of network or system error.<br>
     * 选主失败时的最大重试次数<br>
     * 失败原因通常包括网络异常或系统级错误
     */
    private int electMasterMaxRetryCount = 3;


    /**
     * Whether the controller can elect a master which is not in the syncStateSet.<br>
     * 是否允许选举 syncStateSet 之外的副本为主节点<br>
     * 启用后可提升可用性, 但可能降低数据一致性保障
     */
    private boolean enableElectUncleanMaster = false;

    /**
     * Whether process read event<br>
     * 是否处理读事件<br>
     * 关闭后控制器仅读取写路径相关事件
     */
    private boolean isProcessReadEvent = false;

    /**
     * Whether notify broker when its role changed<br>
     * Broker 角色变化时是否主动通知 Broker<br>
     * 启用后可加快角色变更感知速度
     */
    private volatile boolean notifyBrokerRoleChanged = true;
    /**
     * Interval of periodic scanning for non-active master in each broker-set;<br>
     * Unit: millisecond<br>
     * 每个 broker-set 中非活跃主节点扫描间隔, 单位为毫秒<br>
     * 超过阈值未续约的主节点会被识别为失活
     */
    private long scanInactiveMasterInterval = 5 * 1000;

    /**
     * 指标导出器类型<br>
     * 默认关闭导出, 可按需切换为 grpc 或 prometheus
     */
    private MetricsExporterType metricsExporterType = MetricsExporterType.DISABLE;

    /**
     * grpc 指标导出目标地址<br>
     * 常见形式为 host: port
     */
    private String metricsGrpcExporterTarget = "";
    /**
     * grpc 指标导出请求头<br>
     * 支持透传鉴权或租户等扩展信息
     */
    private String metricsGrpcExporterHeader = "";
    /**
     * grpc 指标导出超时时间, 单位为毫秒<br>
     * 请求超时后将中断本轮导出
     */
    private long metricGrpcExporterTimeOutInMills = 3 * 1000;
    /**
     * grpc 指标导出间隔, 单位为毫秒<br>
     * 控制指标推送频率
     */
    private long metricGrpcExporterIntervalInMills = 60 * 1000;
    /**
     * 日志指标导出间隔, 单位为毫秒<br>
     * 控制日志形式指标输出频率
     */
    private long metricLoggingExporterIntervalInMills = 10 * 1000;

    /**
     * Prometheus 指标服务端口<br>
     * 启用 Prometheus 导出时用于暴露抓取端口
     */
    private int metricsPromExporterPort = 5557;
    /**
     * Prometheus 指标服务绑定地址<br>
     * 为空时由运行时采用默认绑定策略
     */
    private String metricsPromExporterHost = "";

    // Label pairs in CSV. Each label follows pattern of Key:Value. eg: instance_id:xxx,uid:xxx
    /**
     * 指标标签配置, 使用 CSV 形式<br>
     * 每个标签格式为 Key: Value, 示例 instance_id: xxx 或 uid: xxx
     */
    private String metricsLabel = "";

    /**
     * 指标值是否按增量模式导出<br>
     * 开启后指标输出更偏向周期增量统计
     */
    private boolean metricsInDelta = false;

    /**
     * Config in this black list will be not allowed to update by command.<br>
     * Try to update this config black list by restart process.<br>
     * Try to update configures in black list by restart process.<br>
     * 配置黑名单中的项不允许通过命令动态更新<br>
     * 调整黑名单或黑名单项对应值时需重启进程生效
     */
    private String configBlackList = "configBlackList;configStorePath";

    public String getConfigBlackList() {
        return configBlackList;
    }

    public void setConfigBlackList(String configBlackList) {
        this.configBlackList = configBlackList;
    }

    public String getRocketmqHome() {
        return rocketmqHome;
    }

    public void setRocketmqHome(String rocketmqHome) {
        this.rocketmqHome = rocketmqHome;
    }

    public String getConfigStorePath() {
        return configStorePath;
    }

    public void setConfigStorePath(String configStorePath) {
        this.configStorePath = configStorePath;
    }

    public long getScanNotActiveBrokerInterval() {
        return scanNotActiveBrokerInterval;
    }

    public void setScanNotActiveBrokerInterval(long scanNotActiveBrokerInterval) {
        this.scanNotActiveBrokerInterval = scanNotActiveBrokerInterval;
    }

    public int getControllerThreadPoolNums() {
        return controllerThreadPoolNums;
    }

    public void setControllerThreadPoolNums(int controllerThreadPoolNums) {
        this.controllerThreadPoolNums = controllerThreadPoolNums;
    }

    public int getControllerRequestThreadPoolQueueCapacity() {
        return controllerRequestThreadPoolQueueCapacity;
    }

    public void setControllerRequestThreadPoolQueueCapacity(int controllerRequestThreadPoolQueueCapacity) {
        this.controllerRequestThreadPoolQueueCapacity = controllerRequestThreadPoolQueueCapacity;
    }

    public String getControllerDLegerGroup() {
        return controllerDLegerGroup;
    }

    public void setControllerDLegerGroup(String controllerDLegerGroup) {
        this.controllerDLegerGroup = controllerDLegerGroup;
    }

    public String getControllerDLegerPeers() {
        return controllerDLegerPeers;
    }

    public void setControllerDLegerPeers(String controllerDLegerPeers) {
        this.controllerDLegerPeers = controllerDLegerPeers;
    }

    public String getControllerDLegerSelfId() {
        return controllerDLegerSelfId;
    }

    public void setControllerDLegerSelfId(String controllerDLegerSelfId) {
        this.controllerDLegerSelfId = controllerDLegerSelfId;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public void setMappedFileSize(int mappedFileSize) {
        this.mappedFileSize = mappedFileSize;
    }

    public String getControllerStorePath() {
        if (controllerStorePath.isEmpty()) {
            controllerStorePath = System.getProperty("user.home") + File.separator + controllerType + "Controller";
        }
        return controllerStorePath;
    }

    public void setControllerStorePath(String controllerStorePath) {
        this.controllerStorePath = controllerStorePath;
    }

    public boolean isEnableElectUncleanMaster() {
        return enableElectUncleanMaster;
    }

    public void setEnableElectUncleanMaster(boolean enableElectUncleanMaster) {
        this.enableElectUncleanMaster = enableElectUncleanMaster;
    }

    public boolean isProcessReadEvent() {
        return isProcessReadEvent;
    }

    public void setProcessReadEvent(boolean processReadEvent) {
        isProcessReadEvent = processReadEvent;
    }

    public boolean isNotifyBrokerRoleChanged() {
        return notifyBrokerRoleChanged;
    }

    public void setNotifyBrokerRoleChanged(boolean notifyBrokerRoleChanged) {
        this.notifyBrokerRoleChanged = notifyBrokerRoleChanged;
    }

    public long getScanInactiveMasterInterval() {
        return scanInactiveMasterInterval;
    }

    public void setScanInactiveMasterInterval(long scanInactiveMasterInterval) {
        this.scanInactiveMasterInterval = scanInactiveMasterInterval;
    }

    public String getDLedgerAddress() {
        return Arrays.stream(this.controllerDLegerPeers.split(";"))
            .filter(x -> this.controllerDLegerSelfId.equals(x.split("-")[0]))
            .map(x -> x.split("-")[1]).findFirst().get();
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

    public String getControllerType() {
        return controllerType;
    }

    public void setControllerType(String controllerType) {
        this.controllerType = controllerType;
    }

    public JraftConfig getJraftConfig() {
        return jraftConfig;
    }

    public void setJraftConfig(JraftConfig jraftConfig) {
        this.jraftConfig = jraftConfig;
    }

    public int getElectMasterMaxRetryCount() {
        return this.electMasterMaxRetryCount;
    }

    public void setElectMasterMaxRetryCount(int electMasterMaxRetryCount) {
        this.electMasterMaxRetryCount = electMasterMaxRetryCount;
    }
}
