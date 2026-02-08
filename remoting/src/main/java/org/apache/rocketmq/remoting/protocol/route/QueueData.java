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

/*
  $Id: QueueData.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
  历史版本标识
 */
package org.apache.rocketmq.remoting.protocol.route;

/**
 * 队列路由数据
 */
public class QueueData implements Comparable<QueueData> {
    /**
     * Broker 名称
     */
    private String brokerName;
    /**
     * 可读队列数量
     */
    private int readQueueNums;
    /**
     * 可写队列数量
     */
    private int writeQueueNums;
    /**
     * 队列权限位
     */
    private int perm;
    /**
     * Topic 系统标记
     */
    private int topicSysFlag;

    /**
     * 创建空队列路由数据
     */
    public QueueData() {

    }

    /**
     * 使用已有对象构造副本
     *
     * @param queueData 源队列路由数据
     */
    // Deep copy QueueData
    // 深拷贝 QueueData 对象
    public QueueData(QueueData queueData) {
        this.brokerName = queueData.brokerName;
        this.readQueueNums = queueData.readQueueNums;
        this.writeQueueNums = queueData.writeQueueNums;
        this.perm = queueData.perm;
        this.topicSysFlag = queueData.topicSysFlag;
    }

    public int getReadQueueNums() {
        return readQueueNums;
    }

    public void setReadQueueNums(int readQueueNums) {
        this.readQueueNums = readQueueNums;
    }

    public int getWriteQueueNums() {
        return writeQueueNums;
    }

    public void setWriteQueueNums(int writeQueueNums) {
        this.writeQueueNums = writeQueueNums;
    }

    public int getPerm() {
        return perm;
    }

    public void setPerm(int perm) {
        this.perm = perm;
    }

    public int getTopicSysFlag() {
        return topicSysFlag;
    }

    public void setTopicSysFlag(int topicSysFlag) {
        this.topicSysFlag = topicSysFlag;
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
        result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
        result = prime * result + perm;
        result = prime * result + readQueueNums;
        result = prime * result + writeQueueNums;
        result = prime * result + topicSysFlag;
        return result;
    }

    /**
     * 判断两个队列路由对象是否相等
     *
     * @param obj 待比较对象
     * @return 相等时返回 true
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        QueueData other = (QueueData) obj;
        if (brokerName == null) {
            if (other.brokerName != null)
                return false;
        } else if (!brokerName.equals(other.brokerName))
            return false;
        if (perm != other.perm)
            return false;
        if (readQueueNums != other.readQueueNums)
            return false;
        if (writeQueueNums != other.writeQueueNums)
            return false;
        return topicSysFlag == other.topicSysFlag;
    }

    /**
     * 生成对象可读字符串
     *
     * @return 当前对象字符串
     */
    @Override
    public String toString() {
        return "QueueData [brokerName=" + brokerName + ", readQueueNums=" + readQueueNums
            + ", writeQueueNums=" + writeQueueNums + ", perm=" + perm + ", topicSysFlag=" + topicSysFlag
            + "]";
    }

    /**
     * 按 Broker 名称进行字典序比较
     *
     * @param o 另一个队列路由对象
     * @return 比较结果
     */
    @Override
    public int compareTo(QueueData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
}
