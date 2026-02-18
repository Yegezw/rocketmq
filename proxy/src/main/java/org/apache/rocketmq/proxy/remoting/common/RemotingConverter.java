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

package org.apache.rocketmq.proxy.remoting.common;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * Remoting 消息转换器, 负责将 MessageExt 编码为字节数组
 */
public class RemotingConverter {
    /**
     * Proxy 模块日志记录器
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    /**
     * 单例创建锁
     */
    protected static final Object INSTANCE_CREATE_LOCK = new Object();
    /**
     * 单例实例
     */
    protected static volatile RemotingConverter instance;

    public static RemotingConverter getInstance() {
        if (instance == null) {
            synchronized (INSTANCE_CREATE_LOCK) {
                if (instance == null) {
                    instance = new RemotingConverter();
                }
            }
        }
        return instance;
    }

    /**
     * 将消息对象编码为字节数组
     *
     * @param msg 消息对象
     * @return 编码后的字节数组
     * @throws Exception 编码异常
     */
    public byte[] convertMsgToBytes(final MessageExt msg) throws Exception {
        // change to 0 for recalculate storeSize
        // 置为 0 以便重新计算 storeSize
        msg.setStoreSize(0);
        if (msg.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("Topic length is too long, topic: {}", msg.getTopic());
        }
        return MessageDecoder.encode(msg, false);
    }
}
