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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.annotation.JSONField;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.PermName;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.rocketmq.common.TopicAttributes.TOPIC_MESSAGE_TYPE_ATTRIBUTE;

/**
 * Topic 配置模型, 用于描述 Topic 的基础元数据
 */
public class TopicConfig {
    /**
     * Topic 配置编码分隔符
     */
    private static final String SEPARATOR = " ";
    /**
     * 默认读队列数量
     */
    public static int defaultReadQueueNums = 16;
    /**
     * 默认写队列数量
     */
    public static int defaultWriteQueueNums = 16;
    /**
     * Fastjson 反序列化类型引用, 用于解析 attributes 字段
     */
    private static final TypeReference<Map<String, String>> ATTRIBUTES_TYPE_REFERENCE = new TypeReference<Map<String, String>>() {
    };
    /**
     * Topic 名称
     */
    private String topicName;
    /**
     * Topic 读队列数量
     */
    private int readQueueNums = defaultReadQueueNums;
    /**
     * Topic 写队列数量
     */
    private int writeQueueNums = defaultWriteQueueNums;
    /**
     * Topic 权限位, 使用 PermName 中的读写掩码
     */
    private int perm = PermName.PERM_READ | PermName.PERM_WRITE;
    /**
     * Topic 过滤类型
     */
    private TopicFilterType topicFilterType = TopicFilterType.SINGLE_TAG;
    /**
     * Topic 系统标记位
     */
    private int topicSysFlag = 0;
    /**
     * 是否为顺序 Topic
     */
    private boolean order = false;
    // Field attributes should not have ' ' char in key or value, otherwise will lead to decode failure
    /**
     * Topic 的自定义属性, attributes 字段的键和值不能包含空格, 否则会导致 decode 失败
     */
    private Map<String, String> attributes = new HashMap<>();

    /**
     * 创建空的 Topic 配置
     */
    public TopicConfig() {
    }

    /**
     * 使用 Topic 名称创建配置
     *
     * @param topicName Topic 名称
     */
    public TopicConfig(String topicName) {
        this.topicName = topicName;
    }

    /**
     * 使用名称和读写队列创建配置
     *
     * @param topicName      Topic 名称
     * @param readQueueNums  读队列数量
     * @param writeQueueNums 写队列数量
     */
    public TopicConfig(String topicName, int readQueueNums, int writeQueueNums) {
        this.topicName = topicName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
    }

    /**
     * 使用名称、队列和权限创建配置
     *
     * @param topicName      Topic 名称
     * @param readQueueNums  读队列数量
     * @param writeQueueNums 写队列数量
     * @param perm           权限位
     */
    public TopicConfig(String topicName, int readQueueNums, int writeQueueNums, int perm) {
        this.topicName = topicName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
        this.perm = perm;
    }

    /**
     * 使用名称、队列、权限和系统标记创建配置
     *
     * @param topicName      Topic 名称
     * @param readQueueNums  读队列数量
     * @param writeQueueNums 写队列数量
     * @param perm           权限位
     * @param topicSysFlag   系统标记位
     */
    public TopicConfig(String topicName, int readQueueNums, int writeQueueNums, int perm, int topicSysFlag) {
        this.topicName = topicName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
        this.perm = perm;
        this.topicSysFlag = topicSysFlag;
    }

    /**
     * 使用已有配置创建副本, attributes 字段为浅拷贝
     *
     * @param other 原始 Topic 配置
     */
    public TopicConfig(TopicConfig other) {
        this.topicName = other.topicName;
        this.readQueueNums = other.readQueueNums;
        this.writeQueueNums = other.writeQueueNums;
        this.perm = other.perm;
        this.topicFilterType = other.topicFilterType;
        this.topicSysFlag = other.topicSysFlag;
        this.order = other.order;
        this.attributes = other.attributes;
    }

    /**
     * 按固定字段顺序将 Topic 配置编码为字符串
     *
     * @return 编码后的字符串
     */
    public String encode() {
        StringBuilder sb = new StringBuilder();

        //[0]
        // 字段索引 0: Topic 名称
        sb.append(this.topicName);
        sb.append(SEPARATOR);

        //[1]
        // 字段索引 1: 读队列数量
        sb.append(this.readQueueNums);
        sb.append(SEPARATOR);

        //[2]
        // 字段索引 2: 写队列数量
        sb.append(this.writeQueueNums);
        sb.append(SEPARATOR);

        //[3]
        // 字段索引 3: 权限位
        sb.append(this.perm);
        sb.append(SEPARATOR);

        //[4]
        // 字段索引 4: 过滤类型
        sb.append(this.topicFilterType);
        sb.append(SEPARATOR);

        //[5]
        // 字段索引 5: 扩展属性 JSON
        if (attributes != null) {
            sb.append(JSON.toJSONString(attributes));
        }

        return sb.toString();
    }

    /**
     * 从编码字符串解析 Topic 配置
     *
     * @param in 编码字符串
     * @return 解析是否成功
     */
    public boolean decode(final String in) {
        // 按分隔符拆分输入字符串
        String[] strs = in.split(SEPARATOR);
        if (strs.length >= 5) {
            // 解析字段索引 0: Topic 名称
            this.topicName = strs[0];

            // 解析字段索引 1: 读队列数量
            this.readQueueNums = Integer.parseInt(strs[1]);

            // 解析字段索引 2: 写队列数量
            this.writeQueueNums = Integer.parseInt(strs[2]);

            // 解析字段索引 3: 权限位
            this.perm = Integer.parseInt(strs[3]);

            // 解析字段索引 4: 过滤类型
            this.topicFilterType = TopicFilterType.valueOf(strs[4]);

            if (strs.length >= 6) {
                try {
                    // 解析字段索引 5: 扩展属性 JSON
                    this.attributes = JSON.parseObject(strs[5], ATTRIBUTES_TYPE_REFERENCE.getType());
                } catch (Exception e) {
                    // ignore exception when parse failed, cause map's key/value can have ' ' char
                    // 解析失败时忽略异常, 因为 map 的键和值可能包含空格
                }
            }

            return true;
        }

        return false;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
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

    public TopicFilterType getTopicFilterType() {
        return topicFilterType;
    }

    public void setTopicFilterType(TopicFilterType topicFilterType) {
        this.topicFilterType = topicFilterType;
    }

    public int getTopicSysFlag() {
        return topicSysFlag;
    }

    public void setTopicSysFlag(int topicSysFlag) {
        this.topicSysFlag = topicSysFlag;
    }

    /**
     * 返回是否为顺序 Topic
     *
     * @return true 表示顺序 Topic
     */
    public boolean isOrder() {
        return order;
    }

    public void setOrder(boolean isOrder) {
        this.order = isOrder;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    @JSONField(serialize = false, deserialize = false)
    public TopicMessageType getTopicMessageType() {
        if (attributes == null) {
            return TopicMessageType.NORMAL;
        }
        String content = attributes.get(TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName());
        if (content == null) {
            return TopicMessageType.NORMAL;
        }
        return TopicMessageType.valueOf(content);
    }

    @JSONField(serialize = false, deserialize = false)
    public void setTopicMessageType(TopicMessageType topicMessageType) {
        attributes.put(TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName(), topicMessageType.getValue());
    }

    /**
     * 比较两个 Topic 配置是否完全一致
     *
     * @param o 待比较对象
     * @return true 表示内容一致
     */
    @Override
    public boolean equals(Object o) {
        // 同一对象引用直接判定为相等
        if (this == o) {
            return true;
        }

        // 为空或类型不同时直接判定为不相等
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TopicConfig that = (TopicConfig) o;

        // 逐项比较核心字段
        if (readQueueNums != that.readQueueNums) {
            return false;
        }
        if (writeQueueNums != that.writeQueueNums) {
            return false;
        }
        if (perm != that.perm) {
            return false;
        }
        if (topicSysFlag != that.topicSysFlag) {
            return false;
        }
        if (order != that.order) {
            return false;
        }
        if (!Objects.equals(topicName, that.topicName)) {
            return false;
        }
        if (topicFilterType != that.topicFilterType) {
            return false;
        }
        return Objects.equals(attributes, that.attributes);
    }

    /**
     * 计算 Topic 配置的哈希值
     *
     * @return 当前对象哈希值
     */
    @Override
    public int hashCode() {
        int result = topicName != null ? topicName.hashCode() : 0;
        result = 31 * result + readQueueNums;
        result = 31 * result + writeQueueNums;
        result = 31 * result + perm;
        result = 31 * result + (topicFilterType != null ? topicFilterType.hashCode() : 0);
        result = 31 * result + topicSysFlag;
        result = 31 * result + (order ? 1 : 0);
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        return result;
    }

    /**
     * 返回便于日志输出的 Topic 配置文本
     *
     * @return Topic 配置描述字符串
     */
    @Override
    public String toString() {
        return "TopicConfig [topicName=" + topicName + ", readQueueNums=" + readQueueNums
            + ", writeQueueNums=" + writeQueueNums + ", perm=" + PermName.perm2String(perm)
            + ", topicFilterType=" + topicFilterType + ", topicSysFlag=" + topicSysFlag + ", order=" + order
            + ", attributes=" + attributes + "]";
    }
}
