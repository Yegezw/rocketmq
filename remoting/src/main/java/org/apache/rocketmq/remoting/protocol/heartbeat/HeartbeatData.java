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
 * $Id: HeartbeatData.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 * 客户端心跳载荷
 */
package org.apache.rocketmq.remoting.protocol.heartbeat;

import java.util.HashSet;
import java.util.Set;
import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class HeartbeatData extends RemotingSerializable {
    /**
     * 客户端唯一标识
     */
    private String clientID;
    /**
     * 生产者数据集合
     */
    private Set<ProducerData> producerDataSet = new HashSet<>();
    /**
     * 消费者数据集合
     */
    private Set<ConsumerData> consumerDataSet = new HashSet<>();
    /**
     * 心跳指纹值
     */
    private int heartbeatFingerprint = 0;
    /**
     * 是否不携带订阅信息
     */
    private boolean isWithoutSub = false;

    public String getClientID() {
        return clientID;
    }

    public void setClientID(String clientID) {
        this.clientID = clientID;
    }

    public Set<ProducerData> getProducerDataSet() {
        return producerDataSet;
    }

    public void setProducerDataSet(Set<ProducerData> producerDataSet) {
        this.producerDataSet = producerDataSet;
    }

    public Set<ConsumerData> getConsumerDataSet() {
        return consumerDataSet;
    }

    public void setConsumerDataSet(Set<ConsumerData> consumerDataSet) {
        this.consumerDataSet = consumerDataSet;
    }

    public int getHeartbeatFingerprint() {
        return heartbeatFingerprint;
    }

    public void setHeartbeatFingerprint(int heartbeatFingerprint) {
        this.heartbeatFingerprint = heartbeatFingerprint;
    }

    public boolean isWithoutSub() {
        return isWithoutSub;
    }

    public void setWithoutSub(boolean withoutSub) {
        isWithoutSub = withoutSub;
    }

    /**
     * 生成对象可读字符串
     *
     * @return 当前对象字符串
     */
    @Override
    public String toString() {
        return "HeartbeatData [clientID=" + clientID + ", producerDataSet=" + producerDataSet
            + ", consumerDataSet=" + consumerDataSet + "]";
    }

    /**
     * 计算心跳内容指纹
     * 该方法会忽略订阅版本和客户端标识等动态字段
     *
     * @return 指纹值
     */
    public int computeHeartbeatFingerprint() {
        // 使用深拷贝避免直接修改当前对象
        HeartbeatData heartbeatDataCopy = JSON.parseObject(JSON.toJSONString(this), HeartbeatData.class);
        // 将订阅版本归零以消除时间戳差异
        for (ConsumerData consumerData : heartbeatDataCopy.getConsumerDataSet()) {
            for (SubscriptionData subscriptionData : consumerData.getSubscriptionDataSet()) {
                subscriptionData.setSubVersion(0L);
            }
        }
        // 将不参与比较的动态字段重置
        heartbeatDataCopy.setWithoutSub(false);
        heartbeatDataCopy.setHeartbeatFingerprint(0);
        heartbeatDataCopy.setClientID("");
        // 序列化后计算哈希作为指纹
        return JSON.toJSONString(heartbeatDataCopy).hashCode();
    }
}
