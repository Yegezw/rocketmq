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
package org.apache.rocketmq.proxy.common;

import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;

import javax.annotation.Nonnull;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 抽象缓存加载器<br>
 * 支持通过线程池异步刷新缓存值
 */
public abstract class AbstractCacheLoader<K, V> extends CacheLoader<K, V> {
    /**
     * 缓存刷新线程池
     */
    private final ThreadPoolExecutor cacheRefreshExecutor;

    /**
     * 构造抽象缓存加载器
     *
     * @param cacheRefreshExecutor 缓存刷新线程池
     */
    public AbstractCacheLoader(ThreadPoolExecutor cacheRefreshExecutor) {
        this.cacheRefreshExecutor = cacheRefreshExecutor;
    }

    /**
     * 异步刷新缓存值
     *
     * @param key 缓存键
     * @param oldValue 旧值
     * @return 异步刷新结果
     * @throws Exception 刷新过程异常
     */
    @Override
    public ListenableFuture<V> reload(@Nonnull K key, @Nonnull V oldValue) throws Exception {
        ListenableFutureTask<V> task = ListenableFutureTask.create(() -> {
            try {
                return getDirectly(key);
            } catch (Exception e) {
                onErr(key, e);
                return oldValue;
            }
        });
        cacheRefreshExecutor.execute(task);
        return task;
    }

    /**
     * 同步加载缓存值
     *
     * @param key 缓存键
     * @return 加载结果
     * @throws Exception 加载过程异常
     */
    @Override
    public V load(@Nonnull K key) throws Exception {
        return getDirectly(key);
    }

    /**
     * 直接从数据源获取值
     *
     * @param key 缓存键
     * @return 数据源返回值
     * @throws Exception 获取过程异常
     */
    protected abstract V getDirectly(K key) throws Exception;

    /**
     * 刷新失败回调
     *
     * @param key 缓存键
     * @param e 异常对象
     */
    protected abstract void onErr(K key, Exception e);
}
