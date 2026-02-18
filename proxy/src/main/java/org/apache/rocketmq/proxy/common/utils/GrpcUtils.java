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

import io.grpc.Attributes;
import io.grpc.Metadata;
import io.grpc.ServerCall;

/**
 * gRPC 通用工具类<br>
 * 提供 Header 与 Attribute 访问能力
 */
public class GrpcUtils {

    /**
     * 工具类构造函数
     */
    private GrpcUtils() {
    }

    /**
     * 在 Header 不存在指定键时写入值
     *
     * @param headers gRPC Header
     * @param key Header 键
     * @param value Header 值
     * @param <T> Header 值类型
     */
    public static <T> void putHeaderIfNotExist(Metadata headers, Metadata.Key<T> key, T value) {
        if (headers == null) {
            return;
        }
        if (!headers.containsKey(key) && value != null) {
            headers.put(key, value);
        }
    }

    public static <R, W, T> T getAttribute(ServerCall<R, W> call, Attributes.Key<T> key) {
        Attributes attributes = call.getAttributes();
        if (attributes == null) {
            return null;
        }
        return attributes.get(key);
    }
}
