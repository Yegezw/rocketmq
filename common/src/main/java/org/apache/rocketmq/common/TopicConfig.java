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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.PermName;

import static org.apache.rocketmq.common.TopicAttributes.TOPIC_MESSAGE_TYPE_ATTRIBUTE;

/**
 * Topic 配置类
 */
public class TopicConfig {
    /**
     * 编码 OR 解码 Topic 配置时使用的分隔符
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
     * 用于从 JSON 解析属性 Map 的类型引用
     */
    private static final TypeReference<Map<String, String>> ATTRIBUTES_TYPE_REFERENCE = new TypeReference<Map<String, String>>() {
    };

    /**
     * Topic 名称
     */
    private String topicName;

    /**
     * 读队列数量, 用于消费消息
     */
    private int readQueueNums = defaultReadQueueNums;

    /**
     * 写队列数量, 用于生产消息
     */
    private int writeQueueNums = defaultWriteQueueNums;

    /**
     * Topic 权限, 读写权限的组合
     */
    private int perm = PermName.PERM_READ | PermName.PERM_WRITE;

    /**
     * Topic 过滤类型, 默认为 SINGLE_TAG
     */
    private TopicFilterType topicFilterType = TopicFilterType.SINGLE_TAG;

    /**
     * Topic 系统标志, 用于内部系统 Topic
     */
    private int topicSysFlag = 0;

    /**
     * 是否为顺序 Topic
     */
    private boolean order = false;

    /**
     * Topic 的自定义属性, 以键值对形式存储<br>
     * 注意: 键和值中不能包含空格字符, 否则会导致解码失败
     */
    private Map<String, String> attributes = new HashMap<>();

    public TopicConfig() {
    }

    public TopicConfig(String topicName) {
        this.topicName = topicName;
    }

    public TopicConfig(String topicName, int readQueueNums, int writeQueueNums) {
        this.topicName = topicName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
    }

    public TopicConfig(String topicName, int readQueueNums, int writeQueueNums, int perm) {
        this.topicName = topicName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
        this.perm = perm;
    }

    public TopicConfig(String topicName, int readQueueNums, int writeQueueNums, int perm, int topicSysFlag) {
        this.topicName = topicName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
        this.perm = perm;
        this.topicSysFlag = topicSysFlag;
    }

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
     * 将 Topic 配置编码为字符串形式<br>
     * 编码格式为: topicName readQueueNums writeQueueNums perm topicFilterType attributes(JSON)
     *
     * @return 编码后的 Topic 配置字符串
     */
    public String encode() {
        StringBuilder sb = new StringBuilder();
        // [0] Topic 名称
        sb.append(this.topicName);
        sb.append(SEPARATOR);
        // [1] 读队列数量
        sb.append(this.readQueueNums);
        sb.append(SEPARATOR);
        // [2] 写队列数量
        sb.append(this.writeQueueNums);
        sb.append(SEPARATOR);
        // [3] 权限
        sb.append(this.perm);
        sb.append(SEPARATOR);
        // [4] 过滤类型
        sb.append(this.topicFilterType);
        sb.append(SEPARATOR);
        // [5] 属性 (JSON 格式)
        if (attributes != null) {
            sb.append(JSON.toJSONString(attributes));
        }

        return sb.toString();
    }

    /**
     * 从字符串解码 Topic 配置
     *
     * @param in 待解码的字符串
     * @return 如果解码成功返回 true, 否则返回 false
     */
    public boolean decode(final String in) {
        String[] strs = in.split(SEPARATOR);
        if (strs.length >= 5) {
            // 解析 Topic 名称
            this.topicName = strs[0];

            // 解析读队列数量
            this.readQueueNums = Integer.parseInt(strs[1]);

            // 解析写队列数量
            this.writeQueueNums = Integer.parseInt(strs[2]);

            // 解析权限
            this.perm = Integer.parseInt(strs[3]);

            // 解析过滤类型
            this.topicFilterType = TopicFilterType.valueOf(strs[4]);

            // 如果有第 6 个字段, 解析属性
            if (strs.length >= 6) {
                try {
                    this.attributes = JSON.parseObject(strs[5], ATTRIBUTES_TYPE_REFERENCE.getType());
                } catch (Exception e) {
                    // 忽略解析失败的异常, 因为 map 的键值对可能包含空格字符
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

    /**
     * 获取 Topic 消息类型<br>
     * 该方法不参与 JSON 序列化和反序列化
     *
     * @return Topic 消息类型, 如果未设置则返回 NORMAL
     */
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

    /**
     * 设置 Topic 消息类型<br>
     * 该方法不参与 JSON 序列化和反序列化
     *
     * @param topicMessageType Topic 消息类型
     */
    @JSONField(serialize = false, deserialize = false)
    public void setTopicMessageType(TopicMessageType topicMessageType) {
        attributes.put(TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName(), topicMessageType.getValue());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TopicConfig that = (TopicConfig) o;

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

    @Override
    public String toString() {
        return "TopicConfig [topicName=" + topicName + ", readQueueNums=" + readQueueNums
            + ", writeQueueNums=" + writeQueueNums + ", perm=" + PermName.perm2String(perm)
            + ", topicFilterType=" + topicFilterType + ", topicSysFlag=" + topicSysFlag + ", order=" + order
            + ", attributes=" + attributes + "]";
    }
}
