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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class BrokerIdentity {
    /**
     * Broker 默认集群名称
     */
    private static final String DEFAULT_CLUSTER_NAME = "DefaultCluster";

    /**
     * 公共日志记录器
     */
    protected static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    /**
     * 本机主机名缓存
     */
    private static String localHostName;

    static {
        try {
            localHostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOGGER.error("Failed to obtain the host name", e);
        }
    }

    // load it after the localHostName is initialized
    // 需要在 localHostName 完成初始化后再创建容器标识
    /**
     * Broker 容器身份单例
     */
    public static final BrokerIdentity BROKER_CONTAINER_IDENTITY = new BrokerIdentity(true);

    /**
     * Broker 名称
     */
    @ImportantField
    private String brokerName = defaultBrokerName();
    /**
     * Broker 所属集群名称
     */
    @ImportantField
    private String brokerClusterName = DEFAULT_CLUSTER_NAME;
    /**
     * Broker 节点 ID
     */
    @ImportantField
    private volatile long brokerId = MixAll.MASTER_ID;

    /**
     * 是否为 BrokerContainer 身份
     */
    private boolean isBrokerContainer = false;

    // Do not set it manually, it depends on the startup mode
    // Broker start by BrokerStartup is false, start or add by BrokerContainer is true
    // 该标识由启动模式决定, 不应手工设置
    /**
     * 当前 Broker 是否运行在 BrokerContainer 内
     */
    private boolean isInBrokerContainer = false;

    /**
     * 创建默认 Broker 身份实例
     */
    public BrokerIdentity() {
    }

    /**
     * 创建 Broker 身份实例并指定是否为容器身份
     *
     * @param isBrokerContainer 是否为容器身份
     */
    public BrokerIdentity(boolean isBrokerContainer) {
        this.isBrokerContainer = isBrokerContainer;
    }

    /**
     * 创建 Broker 身份实例并指定集群名, Broker 名称与 Broker ID
     *
     * @param brokerClusterName Broker 集群名称
     * @param brokerName Broker 名称
     * @param brokerId Broker ID
     */
    public BrokerIdentity(String brokerClusterName, String brokerName, long brokerId) {
        this.brokerName = brokerName;
        this.brokerClusterName = brokerClusterName;
        this.brokerId = brokerId;
    }

    /**
     * 创建 Broker 身份实例并指定容器部署标识
     *
     * @param brokerClusterName Broker 集群名称
     * @param brokerName Broker 名称
     * @param brokerId Broker ID
     * @param isInBrokerContainer 是否运行在 BrokerContainer 内
     */
    public BrokerIdentity(String brokerClusterName, String brokerName, long brokerId, boolean isInBrokerContainer) {
        this.brokerName = brokerName;
        this.brokerClusterName = brokerClusterName;
        this.brokerId = brokerId;
        this.isInBrokerContainer = isInBrokerContainer;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(final String brokerName) {
        this.brokerName = brokerName;
    }

    public String getBrokerClusterName() {
        return brokerClusterName;
    }

    public void setBrokerClusterName(final String brokerClusterName) {
        this.brokerClusterName = brokerClusterName;
    }

    public long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(final long brokerId) {
        this.brokerId = brokerId;
    }

    public boolean isInBrokerContainer() {
        return isInBrokerContainer;
    }

    public void setInBrokerContainer(boolean inBrokerContainer) {
        isInBrokerContainer = inBrokerContainer;
    }

    /**
     * 计算默认 Broker 名称
     *
     * @return 本机主机名不可用时返回 DEFAULT_BROKER
     */
    private String defaultBrokerName() {
        return StringUtils.isEmpty(localHostName) ? "DEFAULT_BROKER" : localHostName;
    }

    public String getCanonicalName() {
        return isBrokerContainer ? "BrokerContainer" : String.format("%s_%s_%d", brokerClusterName, brokerName,
            brokerId);
    }

    public String getIdentifier() {
        return "#" + getCanonicalName() + "#";
    }

    /**
     * 按 Broker 唯一属性比较两个身份对象
     *
     * @param o 待比较对象
     * @return 属性一致时返回 true
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final BrokerIdentity identity = (BrokerIdentity) o;

        return new EqualsBuilder()
            .append(brokerId, identity.brokerId)
            .append(brokerName, identity.brokerName)
            .append(brokerClusterName, identity.brokerClusterName)
            .isEquals();
    }

    /**
     * 基于 Broker 名称, 集群名称与 Broker ID 生成哈希值
     *
     * @return 当前对象哈希值
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
            .append(brokerName)
            .append(brokerClusterName)
            .append(brokerId)
            .toHashCode();
    }
}
