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
package org.apache.rocketmq.common.resource;

import com.alibaba.fastjson2.annotation.JSONField;
import org.apache.commons.lang3.StringUtils;

/**
 * 资源类型枚举
 */
public enum ResourceType {

    /**
     * 未知资源
     */
    UNKNOWN((byte) 0, "Unknown"),

    /**
     * 任意资源
     */
    ANY((byte) 1, "Any"),

    /**
     * 集群资源
     */
    CLUSTER((byte) 2, "Cluster"),

    /**
     * 命名空间资源
     */
    NAMESPACE((byte) 3, "Namespace"),

    /**
     * Topic 资源
     */
    TOPIC((byte) 4, "Topic"),

    /**
     * 消费组资源
     */
    GROUP((byte) 5, "Group");

    /**
     * 资源类型编码
     */
    @JSONField(value = true)
    private final byte code;
    /**
     * 资源类型名称
     */
    private final String name;

    /**
     * 构建资源类型枚举值
     *
     * @param code 类型编码
     * @param name 类型名称
     */
    ResourceType(byte code, String name) {
        this.code = code;
        this.name = name;
    }

    /**
     * 按名称查找资源类型
     *
     * @param name 类型名称
     * @return 匹配的资源类型, 未匹配时返回 null
     */
    public static ResourceType getByName(String name) {
        for (ResourceType resourceType : ResourceType.values()) {
            if (StringUtils.equalsIgnoreCase(resourceType.getName(), name)) {
                return resourceType;
            }
        }
        return null;
    }

    public byte getCode() {
        return code;
    }

    public String getName() {
        return name;
    }
}
