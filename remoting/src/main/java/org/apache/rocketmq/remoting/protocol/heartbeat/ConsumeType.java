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
 * $Id: ConsumeType.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 * 消费类型枚举
 */
package org.apache.rocketmq.remoting.protocol.heartbeat;

public enum ConsumeType {

    /**
     * 主动拉取消费
     */
    CONSUME_ACTIVELY("PULL"),

    /**
     * 被动推送消费
     */
    CONSUME_PASSIVELY("PUSH"),

    /**
     * POP 消费模式
     */
    CONSUME_POP("POP");

    /**
     * 类型展示值
     */
    private String typeCN;

    /**
     * 创建消费类型枚举值
     *
     * @param typeCN 类型展示值
     */
    ConsumeType(String typeCN) {
        this.typeCN = typeCN;
    }

    public String getTypeCN() {
        return typeCN;
    }
}
