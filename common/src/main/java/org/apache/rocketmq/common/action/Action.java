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
package org.apache.rocketmq.common.action;

import com.alibaba.fastjson2.annotation.JSONField;
import org.apache.commons.lang3.StringUtils;

/**
 * 鉴权动作枚举
 */
public enum Action {

    /**
     * 未知动作
     */
    UNKNOWN((byte) 0, "Unknown"),

    /**
     * 全量动作
     */
    ALL((byte) 1, "All"),

    /**
     * 任意动作
     */
    ANY((byte) 2, "Any"),

    /**
     * 发布动作
     */
    PUB((byte) 3, "Pub"),

    /**
     * 订阅动作
     */
    SUB((byte) 4, "Sub"),

    /**
     * 创建动作
     */
    CREATE((byte) 5, "Create"),

    /**
     * 更新动作
     */
    UPDATE((byte) 6, "Update"),

    /**
     * 删除动作
     */
    DELETE((byte) 7, "Delete"),

    /**
     * 查询动作
     */
    GET((byte) 8, "Get"),

    /**
     * 列表动作
     */
    LIST((byte) 9, "List");

    /**
     * 动作编码
     */
    @JSONField(value = true)
    private final byte code;
    /**
     * 动作名称
     */
    private final String name;

    /**
     * 构建动作枚举值
     *
     * @param code 动作编码
     * @param name 动作名称
     */
    Action(byte code, String name) {
        this.code = code;
        this.name = name;
    }

    /**
     * 按名称查找动作
     *
     * @param name 动作名称
     * @return 匹配的动作, 未匹配时返回 null
     */
    public static Action getByName(String name) {
        for (Action action : Action.values()) {
            if (StringUtils.equalsIgnoreCase(action.getName(), name)) {
                return action;
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
