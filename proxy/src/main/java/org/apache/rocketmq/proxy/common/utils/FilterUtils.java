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
package org.apache.rocketmq.proxy.common.utils;

import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

import java.util.Set;

/**
 * 过滤工具类<br>
 * 提供订阅过滤匹配相关能力
 */
public class FilterUtils {
    /**
     * Whether the message's tag matches consumerGroup's SubscriptionData
     * <br>
     * 判断消息 Tag 是否匹配消费组订阅数据
     *
     * @param tagsSet tagSet in {@link SubscriptionData}, tagSet empty means SubscriptionData.SUB_ALL(*).<br>订阅标签集合为空表示订阅全部
     * @param tags    message's tags, null means not tag attached to the message.<br>消息标签为空表示无标签
     */
    public static boolean isTagMatched(Set<String> tagsSet, String tags) {
        if (tagsSet.isEmpty()) {
            return true;
        }

        if (tags == null) {
            return false;
        }

        return tagsSet.contains(tags);
    }
}
