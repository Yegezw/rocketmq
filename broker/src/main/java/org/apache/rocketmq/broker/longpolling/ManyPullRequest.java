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
package org.apache.rocketmq.broker.longpolling;

import java.util.ArrayList;
import java.util.List;

/**
 * 保存同一挂起键对应的拉取请求集合, 通过同步方法保证并发可见性
 */
public class ManyPullRequest {
    /**
     * 挂起拉取请求的实际存储容器, 请求被唤醒后会整体克隆并清空
     */
    private final ArrayList<PullRequest> pullRequestList = new ArrayList<>();

    /**
     * 追加单个挂起拉取请求
     *
     * @param pullRequest 需要加入挂起队列的请求
     */
    public synchronized void addPullRequest(final PullRequest pullRequest) {
        this.pullRequestList.add(pullRequest);
    }

    /**
     * 批量追加挂起拉取请求, 常用于将未满足条件的请求重新入队
     *
     * @param many 需要重新挂起的请求列表
     */
    public synchronized void addPullRequest(final List<PullRequest> many) {
        this.pullRequestList.addAll(many);
    }

    /**
     * 克隆当前请求列表并清空原列表, 便于调用方在锁外执行后续处理
     *
     * @return 当前快照列表, 若无请求则返回 null
     */
    public synchronized List<PullRequest> cloneListAndClear() {
        if (!this.pullRequestList.isEmpty()) {
            List<PullRequest> result = (ArrayList<PullRequest>) this.pullRequestList.clone();
            this.pullRequestList.clear();
            return result;
        }

        return null;
    }

    public ArrayList<PullRequest> getPullRequestList() {
        return pullRequestList;
    }

    public synchronized boolean isEmpty() {
        return this.pullRequestList.isEmpty();
    }
}
