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

package org.apache.rocketmq.common.thread;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * 线程池状态监控器接口
 */
public interface ThreadPoolStatusMonitor {

    /**
     * 返回监控项描述名称
     *
     * @return 监控项名称
     */
    String describe();

    /**
     * 计算当前监控值
     *
     * @param executor 线程池执行器
     * @return 监控值
     */
    double value(ThreadPoolExecutor executor);

    /**
     * 根据监控值判断是否需要打印线程栈
     *
     * @param executor 线程池执行器
     * @param value 当前监控值
     * @return 是否打印线程栈
     */
    boolean needPrintJstack(ThreadPoolExecutor executor, double value);
}
