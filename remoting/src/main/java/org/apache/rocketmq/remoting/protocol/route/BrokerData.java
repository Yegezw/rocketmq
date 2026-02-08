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

package org.apache.rocketmq.remoting.protocol.route;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * The class describes that a typical broker cluster's (in replication) details: the cluster (in sharding) name
 * that it belongs to, and all the single instance information for this cluster.
 * <br>
 * 该类描述 Broker 复制组在路由中的信息: 包括所属集群和各实例地址
 */
public class BrokerData implements Comparable<BrokerData> {
    /**
     * 所属集群名称
     */
    private String cluster;
    /**
     * Broker 名称
     */
    private String brokerName;

    /**
     * The container that store the all single instances for the current broker replication cluster.
     * The key is the brokerId, and the value is the address of the single broker instance.
     * <br>
     * 当前 Broker 复制组下的实例地址映射<br>
     * key 为 brokerId, value 为实例地址
     */
    private HashMap<Long, String> brokerAddrs;
    /**
     * 可用区名称
     */
    private String zoneName;
    /**
     * 地址随机选择器
     */
    private final Random random = new Random();

    /**
     * Enable acting master or not, used for old version HA adaption,
     * <br>
     * 是否启用代理主节点能力, 用于旧版 HA 兼容
     */
    private boolean enableActingMaster = false;

    /**
     * 创建空 Broker 路由数据
     */
    public BrokerData() {

    }

    /**
     * 使用已有对象创建副本
     *
     * @param brokerData 源 Broker 数据
     */
    public BrokerData(BrokerData brokerData) {
        this.cluster = brokerData.cluster;
        this.brokerName = brokerData.brokerName;
        if (brokerData.brokerAddrs != null) {
            this.brokerAddrs = new HashMap<>(brokerData.brokerAddrs);
        }
        this.zoneName = brokerData.zoneName;
        this.enableActingMaster = brokerData.enableActingMaster;
    }

    /**
     * 构建 Broker 路由数据
     *
     * @param cluster 集群名称
     * @param brokerName Broker 名称
     * @param brokerAddrs Broker 地址映射
     */
    public BrokerData(String cluster, String brokerName, HashMap<Long, String> brokerAddrs) {
        this.cluster = cluster;
        this.brokerName = brokerName;
        this.brokerAddrs = brokerAddrs;
    }

    /**
     * 构建 Broker 路由数据
     *
     * @param cluster 集群名称
     * @param brokerName Broker 名称
     * @param brokerAddrs Broker 地址映射
     * @param enableActingMaster 是否启用代理主节点
     */
    public BrokerData(String cluster, String brokerName, HashMap<Long, String> brokerAddrs,
        boolean enableActingMaster) {
        this.cluster = cluster;
        this.brokerName = brokerName;
        this.brokerAddrs = brokerAddrs;
        this.enableActingMaster = enableActingMaster;
    }

    /**
     * 构建 Broker 路由数据
     *
     * @param cluster 集群名称
     * @param brokerName Broker 名称
     * @param brokerAddrs Broker 地址映射
     * @param enableActingMaster 是否启用代理主节点
     * @param zoneName 可用区名称
     */
    public BrokerData(String cluster, String brokerName, HashMap<Long, String> brokerAddrs, boolean enableActingMaster,
        String zoneName) {
        this.cluster = cluster;
        this.brokerName = brokerName;
        this.brokerAddrs = brokerAddrs;
        this.enableActingMaster = enableActingMaster;
        this.zoneName = zoneName;
    }

    /**
     * Selects a (preferably master) broker address from the registered list. If the master's address cannot be found, a
     * slave broker address is selected in a random manner.
     * <br>
     * 从已注册地址中选择 Broker 地址, 优先返回 Master 地址
     * 当 Master 不存在时从从节点地址中随机选择
     *
     * @return Broker address.
     */
    public String selectBrokerAddr() {
        String masterAddress = this.brokerAddrs.get(MixAll.MASTER_ID);

        if (masterAddress == null) {
            List<String> addrs = new ArrayList<>(brokerAddrs.values());
            return addrs.get(random.nextInt(addrs.size()));
        }

        return masterAddress;
    }

    public HashMap<Long, String> getBrokerAddrs() {
        return brokerAddrs;
    }

    public void setBrokerAddrs(HashMap<Long, String> brokerAddrs) {
        this.brokerAddrs = brokerAddrs;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public boolean isEnableActingMaster() {
        return enableActingMaster;
    }

    public void setEnableActingMaster(boolean enableActingMaster) {
        this.enableActingMaster = enableActingMaster;
    }

    public String getZoneName() {
        return zoneName;
    }

    public void setZoneName(String zoneName) {
        this.zoneName = zoneName;
    }

    /**
     * 计算对象哈希值
     *
     * @return 当前对象哈希值
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerAddrs == null) ? 0 : brokerAddrs.hashCode());
        result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
        return result;
    }

    /**
     * 判断两个 Broker 路由对象是否相等
     *
     * @param obj 待比较对象
     * @return 相等时返回 true
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        BrokerData other = (BrokerData) obj;
        if (brokerAddrs == null) {
            if (other.brokerAddrs != null) {
                return false;
            }
        } else if (!brokerAddrs.equals(other.brokerAddrs)) {
            return false;
        }
        return StringUtils.equals(brokerName, other.brokerName);
    }

    /**
     * 生成对象可读字符串
     *
     * @return 当前对象字符串
     */
    @Override
    public String toString() {
        return "BrokerData [brokerName=" + brokerName + ", brokerAddrs=" + brokerAddrs + ", enableActingMaster=" + enableActingMaster + "]";
    }

    /**
     * 按 Broker 名称进行字典序比较
     *
     * @param o 另一个 Broker 路由对象
     * @return 比较结果
     */
    @Override
    public int compareTo(BrokerData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
}
