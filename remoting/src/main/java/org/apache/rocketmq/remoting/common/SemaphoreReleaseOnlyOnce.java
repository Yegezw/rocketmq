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
package org.apache.rocketmq.remoting.common;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Semaphore 单次释放包装器, 保证 release 仅生效一次
 */
public class SemaphoreReleaseOnlyOnce {
    /**
     * 释放状态标记
     */
    private final AtomicBoolean released = new AtomicBoolean(false);
    /**
     * 需要受控释放的 Semaphore
     */
    private final Semaphore semaphore;

    /**
     * 创建 SemaphoreReleaseOnlyOnce 实例
     *
     * @param semaphore 目标 Semaphore
     */
    public SemaphoreReleaseOnlyOnce(Semaphore semaphore) {
        this.semaphore = semaphore;
    }

    /**
     * 执行信号量释放, 同一实例仅允许首次调用生效
     */
    public void release() {
        if (this.semaphore != null) {
            // 使用 CAS 保证 release 逻辑只执行一次
            if (this.released.compareAndSet(false, true)) {
                this.semaphore.release();
            }
        }
    }

    public Semaphore getSemaphore() {
        return semaphore;
    }
}
