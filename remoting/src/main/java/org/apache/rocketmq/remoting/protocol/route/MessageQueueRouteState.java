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
package org.apache.rocketmq.remoting.protocol.route;

/**
 * 消息队列路由状态
 */
public enum MessageQueueRouteState {
    // do not change below order, since ordinal() is used
    // 不可调整以下顺序, 因为依赖 ordinal() 值
    /**
     * 路由已过期
     */
    Expired,
    /**
     * 仅允许读取
     */
    ReadOnly,
    /**
     * 读写正常
     */
    Normal,
    /**
     * 仅允许写入
     */
    WriteOnly,
    ;
}
