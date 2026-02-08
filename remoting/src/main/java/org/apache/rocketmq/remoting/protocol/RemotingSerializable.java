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
package org.apache.rocketmq.remoting.protocol;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Remoting 协议对象序列化基类<br>
 * 基于 Fastjson 提供 JSON 与字节数组转换能力
 */
public abstract class RemotingSerializable {
    /**
     * UTF-8 字符集常量
     */
    private final static Charset CHARSET_UTF8 = StandardCharsets.UTF_8;

    /**
     * 将对象编码为 UTF-8 字节数组
     *
     * @param obj 待编码对象
     * @return 编码结果, 当对象为空时返回 null
     */
    public static byte[] encode(final Object obj) {
        if (obj == null) {
            return null;
        }
        final String json = toJson(obj, false);
        return json.getBytes(CHARSET_UTF8);
    }

    /**
     * 将对象序列化为 JSON 字符串
     *
     * @param obj 待序列化对象
     * @param prettyFormat 是否格式化输出
     * @return JSON 字符串
     */
    public static String toJson(final Object obj, boolean prettyFormat) {
        return JSON.toJSONString(obj, prettyFormat);
    }

    /**
     * 将字节数组反序列化为目标对象
     *
     * @param <T> 目标类型
     * @param data JSON 字节数组
     * @param classOfT 目标类型
     * @return 反序列化对象, 当输入为空时返回 null
     */
    public static <T> T decode(final byte[] data, Class<T> classOfT) {
        if (data == null) {
            return null;
        }
        return fromJson(data, classOfT);
    }

    /**
     * 将字节数组反序列化为目标类型列表
     *
     * @param <T> 列表元素类型
     * @param data JSON 数组字节数据
     * @param classOfT 列表元素类型
     * @return 反序列化列表, 当输入为空时返回 null
     */
    public static <T> List<T> decodeList(final byte[] data, Class<T> classOfT) {
        if (data == null) {
            return null;
        }
        String json = new String(data, CHARSET_UTF8);
        return JSON.parseArray(json, classOfT);
    }

    /**
     * 将 JSON 字符串反序列化为目标对象
     *
     * @param <T> 目标类型
     * @param json JSON 字符串
     * @param classOfT 目标类型
     * @return 反序列化对象
     */
    public static <T> T fromJson(String json, Class<T> classOfT) {
        return JSON.parseObject(json, classOfT);
    }

    /**
     * 将 JSON 字节数组反序列化为目标对象
     *
     * @param <T> 目标类型
     * @param data JSON 字节数组
     * @param classOfT 目标类型
     * @return 反序列化对象
     */
    private static <T> T fromJson(byte[] data, Class<T> classOfT) {
        return JSON.parseObject(data, classOfT);
    }

    /**
     * 将当前对象编码为 UTF-8 字节数组
     *
     * @return 编码结果, 当 JSON 为空时返回 null
     */
    public byte[] encode() {
        final String json = this.toJson();
        if (json != null) {
            return json.getBytes(CHARSET_UTF8);
        }
        return null;
    }

    /**
     * Allow call-site to apply specific features according to their requirements.
     * <br>
     * 允许调用方按需传入序列化特性
     *
     * @param features Features to apply 序列化特性列表
     * @return serialized data. 序列化后的字节数组
     */
    public byte[] encode(SerializerFeature...features) {
        final String json = JSON.toJSONString(this, features);
        return json.getBytes(CHARSET_UTF8);
    }

    /**
     * 将当前对象序列化为紧凑 JSON 字符串
     *
     * @return JSON 字符串
     */
    public String toJson() {
        return toJson(false);
    }

    /**
     * 将当前对象序列化为 JSON 字符串
     *
     * @param prettyFormat 是否格式化输出
     * @return JSON 字符串
     */
    public String toJson(final boolean prettyFormat) {
        return toJson(this, prettyFormat);
    }
}
