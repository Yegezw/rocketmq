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
package org.apache.rocketmq.broker.mqtrace;

/**
 * 消费消息钩子接口, 用于在消息消费前后扩展审计与统计逻辑
 */
public interface ConsumeMessageHook {
    /**
     * 返回钩子名称, 用于日志与诊断输出
     *
     * @return 钩子名称
     */
    String hookName();

    /**
     * 消费前回调, 可读取消费上下文并预置扩展信息
     *
     * @param context 消费上下文
     */
    void consumeMessageBefore(final ConsumeMessageContext context);

    /**
     * 消费后回调, 可根据结果补充统计与追踪数据
     *
     * @param context 消费上下文
     */
    void consumeMessageAfter(final ConsumeMessageContext context);
}
