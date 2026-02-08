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

package org.apache.rocketmq.remoting.netty;

import io.netty.channel.Channel;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 请求任务, 封装待执行逻辑与请求上下文
 */
public class RequestTask implements Runnable {
    /**
     * 实际执行的任务逻辑
     */
    private final Runnable runnable;
    /**
     * 任务创建时间戳, 单位为毫秒
     */
    private final long createTimestamp = System.currentTimeMillis();
    /**
     * 请求所属 Channel
     */
    private final Channel channel;
    /**
     * 原始请求命令
     */
    private final RemotingCommand request;
    /**
     * 任务停止标记, true 表示禁止继续执行
     */
    private volatile boolean stopRun = false;

    /**
     * 构造请求任务
     *
     * @param runnable 任务逻辑
     * @param channel  请求所属 Channel
     * @param request  原始请求命令
     */
    public RequestTask(final Runnable runnable, final Channel channel, final RemotingCommand request) {
        this.runnable = runnable;
        this.channel = channel;
        this.request = request;
    }

    /**
     * 计算任务哈希值
     *
     * @return 任务哈希值
     */
    @Override
    public int hashCode() {
        int result = runnable != null ? runnable.hashCode() : 0;
        result = 31 * result + (int) (getCreateTimestamp() ^ (getCreateTimestamp() >>> 32));
        result = 31 * result + (channel != null ? channel.hashCode() : 0);
        result = 31 * result + (request != null ? request.hashCode() : 0);
        result = 31 * result + (isStopRun() ? 1 : 0);
        return result;
    }

    /**
     * 判断两个请求任务是否相等
     *
     * @param o 待比较对象
     * @return 相等时返回 true, 否则返回 false
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (!(o instanceof RequestTask))
            return false;

        final RequestTask that = (RequestTask) o;

        if (getCreateTimestamp() != that.getCreateTimestamp())
            return false;
        if (isStopRun() != that.isStopRun())
            return false;
        if (channel != null ? !channel.equals(that.channel) : that.channel != null)
            return false;
        return request != null ? request.getOpaque() == that.request.getOpaque() : that.request == null;

    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public boolean isStopRun() {
        return stopRun;
    }

    public void setStopRun(final boolean stopRun) {
        this.stopRun = stopRun;
    }

    /**
     * 执行任务逻辑
     */
    @Override
    public void run() {
        if (!this.stopRun)
            this.runnable.run();
    }

    /**
     * 构造并回写响应命令
     *
     * @param code 响应码
     * @param remark 响应说明
     */
    public void returnResponse(int code, String remark) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(code, remark);
        response.setOpaque(request.getOpaque());
        this.channel.writeAndFlush(response);
    }
}
