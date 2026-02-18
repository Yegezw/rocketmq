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
package org.apache.rocketmq.proxy.auth;

import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authentication.provider.AuthenticationMetadataProvider;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Proxy 鉴权元数据提供者<br>
 * 负责对接 MetadataService 提供认证相关元数据访问
 */
public class ProxyAuthenticationMetadataProvider implements AuthenticationMetadataProvider {

    /**
     * 认证配置对象
     */
    protected AuthConfig authConfig;
    /**
     * 元数据服务
     */
    protected MetadataService metadataService;

    /**
     * 初始化鉴权元数据提供者
     *
     * @param authConfig 认证配置
     * @param metadataService 元数据服务提供器
     */
    @Override
    public void initialize(AuthConfig authConfig, Supplier<?> metadataService) {
        this.authConfig = authConfig;
        if (metadataService != null) {
            this.metadataService = (MetadataService) metadataService.get();
        }
    }

    /**
     * 关闭提供者并释放资源
     */
    @Override
    public void shutdown() {

    }

    /**
     * 创建用户
     *
     * @param user 用户对象
     * @return 异步结果
     */
    @Override
    public CompletableFuture<Void> createUser(User user) {
        return null;
    }

    /**
     * 删除用户
     *
     * @param username 用户名
     * @return 异步结果
     */
    @Override
    public CompletableFuture<Void> deleteUser(String username) {
        return null;
    }

    /**
     * 更新用户
     *
     * @param user 用户对象
     * @return 异步结果
     */
    @Override
    public CompletableFuture<Void> updateUser(User user) {
        return null;
    }

    @Override
    public CompletableFuture<User> getUser(String username) {
        return this.metadataService.getUser(null, username);
    }

    /**
     * 按过滤条件列出用户
     *
     * @param filter 过滤条件
     * @return 用户列表异步结果
     */
    @Override
    public CompletableFuture<List<User>> listUser(String filter) {
        return null;
    }
}
