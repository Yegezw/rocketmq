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
package org.apache.rocketmq.broker.offset;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.MixAll;

public class BroadcastOffsetStore {

    /**
     * 按队列维度保存广播消费位点, 仅在 broker 内存中维护
     */
    private final ConcurrentMap<Integer, AtomicLong> offsetTable = new ConcurrentHashMap<>();

    /**
     * 更新指定队列的广播消费位点, 可按需限制为仅递增更新
     *
     * @param queueId 消费队列标识
     * @param offset 待写入的消费位点
     * @param increaseOnly 为 true 时仅允许位点前移
     */
    public void updateOffset(int queueId, long offset, boolean increaseOnly) {
        AtomicLong offsetOld = this.offsetTable.get(queueId);
        if (null == offsetOld) {
            offsetOld = this.offsetTable.putIfAbsent(queueId, new AtomicLong(offset));
        }

        if (null != offsetOld) {
            if (increaseOnly) {
                MixAll.compareAndIncreaseOnly(offsetOld, offset);
            } else {
                offsetOld.set(offset);
            }
        }
    }

    /**
     * 读取指定队列的广播消费位点, 不存在时返回 -1
     *
     * @param queueId 消费队列标识
     * @return 当前记录的消费位点, 未命中返回 -1
     */
    public long readOffset(int queueId) {
        AtomicLong offset = this.offsetTable.get(queueId);
        if (offset != null) {
            return offset.get();
        }
        return -1L;
    }

    /**
     * 返回当前存在位点记录的队列集合
     *
     * @return 已记录位点的队列 ID 集合
     */
    public Set<Integer> queueList() {
        return offsetTable.keySet();
    }
}
