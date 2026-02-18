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

package org.apache.rocketmq.proxy.common;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.netty.channel.Channel;

/**
 * 回执句柄分组键<br>
 * 由连接通道与消费组共同组成
 */
public class ReceiptHandleGroupKey {
    /**
     * 客户端连接通道
     */
    protected final Channel channel;
    /**
     * 消费组名称
     */
    protected final String group;

    /**
     * 构造分组键
     *
     * @param channel 客户端连接通道
     * @param group 消费组名称
     */
    public ReceiptHandleGroupKey(Channel channel, String group) {
        this.channel = channel;
        this.group = group;
    }

    protected String getChannelId() {
        return channel.id().asLongText();
    }

    public String getGroup() {
        return group;
    }

    public Channel getChannel() {
        return channel;
    }

    /**
     * 比较两个分组键是否相等
     *
     * @param o 对比对象
     * @return true 表示相等
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReceiptHandleGroupKey key = (ReceiptHandleGroupKey) o;
        return Objects.equal(getChannelId(), key.getChannelId()) && Objects.equal(group, key.group);
    }

    /**
     * 计算分组键哈希值
     *
     * @return 哈希值
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(getChannelId(), group);
    }

    /**
     * 输出分组键可读字符串
     *
     * @return 字符串描述
     */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("channelId", getChannelId())
            .add("group", group)
            .toString();
    }
}
