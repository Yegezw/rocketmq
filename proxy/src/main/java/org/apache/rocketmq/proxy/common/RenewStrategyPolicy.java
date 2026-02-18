/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.common;

import com.google.common.base.MoreObjects;
import org.apache.rocketmq.remoting.protocol.subscription.RetryPolicy;

import java.util.concurrent.TimeUnit;

/**
 * 续期重试策略<br>
 * 按续期次数返回下一次延迟时间
 */
public class RenewStrategyPolicy implements RetryPolicy {
    // 1m 3m 5m 6m 10m 30m 1h
    // 默认续期时间序列 单位毫秒
    /**
     * 续期间隔数组
     */
    private long[] next = new long[]{
            TimeUnit.MINUTES.toMillis(1),
            TimeUnit.MINUTES.toMillis(3),
            TimeUnit.MINUTES.toMillis(5),
            TimeUnit.MINUTES.toMillis(10),
            TimeUnit.MINUTES.toMillis(30),
            TimeUnit.HOURS.toMillis(1)
    };

    /**
     * 构造默认续期策略
     */
    public RenewStrategyPolicy() {
    }

    /**
     * 按指定间隔构造续期策略
     *
     * @param next 续期间隔数组
     */
    public RenewStrategyPolicy(long[] next) {
        this.next = next;
    }

    public long[] getNext() {
        return next;
    }

    public void setNext(long[] next) {
        this.next = next;
    }

    /**
     * 输出策略可读字符串
     *
     * @return 字符串描述
     */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("next", next)
                .toString();
    }

    /**
     * 根据续期次数计算下一次延迟
     *
     * @param renewTimes 已续期次数
     * @return 下一次延迟时间 毫秒
     */
    @Override
    public long nextDelayDuration(int renewTimes) {
        if (renewTimes < 0) {
            renewTimes = 0;
        }
        int index = renewTimes;
        if (index >= next.length) {
            index = next.length - 1;
        }
        return next[index];
    }
}
