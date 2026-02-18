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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 线程池监控对象包装器
 */
public class ThreadPoolWrapper {
    /**
     * 线程池名称
     */
    private String name;
    /**
     * 线程池执行器实例
     */
    private ThreadPoolExecutor threadPoolExecutor;
    /**
     * 线程池状态监控器列表
     */
    private List<ThreadPoolStatusMonitor> statusPrinters;

    /**
     * 构建线程池包装对象
     *
     * @param name 线程池名称
     * @param threadPoolExecutor 线程池执行器
     * @param statusPrinters 状态监控器列表
     */
    ThreadPoolWrapper(final String name, final ThreadPoolExecutor threadPoolExecutor,
        final List<ThreadPoolStatusMonitor> statusPrinters) {
        this.name = name;
        this.threadPoolExecutor = threadPoolExecutor;
        this.statusPrinters = statusPrinters;
    }

    /**
     * 线程池包装器构建器
     */
    public static class ThreadPoolWrapperBuilder {
        /**
         * 线程池名称
         */
        private String name;
        /**
         * 线程池执行器
         */
        private ThreadPoolExecutor threadPoolExecutor;
        /**
         * 状态监控器列表
         */
        private List<ThreadPoolStatusMonitor> statusPrinters;

        /**
         * 创建构建器实例
         */
        ThreadPoolWrapperBuilder() {
        }

        /**
         * 设置线程池名称
         *
         * @param name 线程池名称
         * @return 构建器实例
         */
        public ThreadPoolWrapper.ThreadPoolWrapperBuilder name(final String name) {
            this.name = name;
            return this;
        }

        /**
         * 设置线程池执行器
         *
         * @param threadPoolExecutor 线程池执行器
         * @return 构建器实例
         */
        public ThreadPoolWrapper.ThreadPoolWrapperBuilder threadPoolExecutor(
            final ThreadPoolExecutor threadPoolExecutor) {
            this.threadPoolExecutor = threadPoolExecutor;
            return this;
        }

        /**
         * 设置状态监控器列表
         *
         * @param statusPrinters 状态监控器列表
         * @return 构建器实例
         */
        public ThreadPoolWrapper.ThreadPoolWrapperBuilder statusPrinters(
            final List<ThreadPoolStatusMonitor> statusPrinters) {
            this.statusPrinters = statusPrinters;
            return this;
        }

        /**
         * 构建线程池包装对象
         *
         * @return 线程池包装对象
         */
        public ThreadPoolWrapper build() {
            return new ThreadPoolWrapper(this.name, this.threadPoolExecutor, this.statusPrinters);
        }

        /**
         * 输出构建器关键信息
         *
         * @return 构建器字符串
         */
        @java.lang.Override
        public java.lang.String toString() {
            return "ThreadPoolWrapper.ThreadPoolWrapperBuilder(name=" + this.name + ", threadPoolExecutor=" + this.threadPoolExecutor + ", statusPrinters=" + this.statusPrinters + ")";
        }
    }

    /**
     * 创建线程池包装器构建器
     *
     * @return 构建器实例
     */
    public static ThreadPoolWrapper.ThreadPoolWrapperBuilder builder() {
        return new ThreadPoolWrapper.ThreadPoolWrapperBuilder();
    }

    public String getName() {
        return this.name;
    }

    public ThreadPoolExecutor getThreadPoolExecutor() {
        return this.threadPoolExecutor;
    }

    public List<ThreadPoolStatusMonitor> getStatusPrinters() {
        return this.statusPrinters;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public void setThreadPoolExecutor(final ThreadPoolExecutor threadPoolExecutor) {
        this.threadPoolExecutor = threadPoolExecutor;
    }

    public void setStatusPrinters(final List<ThreadPoolStatusMonitor> statusPrinters) {
        this.statusPrinters = statusPrinters;
    }

    /**
     * 比较两个包装对象是否等价
     *
     * @param o 待比较对象
     * @return 是否等价
     */
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ThreadPoolWrapper wrapper = (ThreadPoolWrapper) o;
        return Objects.equal(name, wrapper.name) && Objects.equal(threadPoolExecutor, wrapper.threadPoolExecutor) && Objects.equal(statusPrinters, wrapper.statusPrinters);
    }

    /**
     * 计算包装对象哈希值
     *
     * @return 哈希值
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(name, threadPoolExecutor, statusPrinters);
    }

    /**
     * 输出包装对象关键信息
     *
     * @return 包装对象字符串
     */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("name", name)
            .add("threadPoolExecutor", threadPoolExecutor)
            .add("statusPrinters", statusPrinters)
            .toString();
    }
}
