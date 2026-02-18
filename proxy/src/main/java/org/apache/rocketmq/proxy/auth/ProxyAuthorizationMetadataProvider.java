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

import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authorization.model.Acl;
import org.apache.rocketmq.auth.authorization.provider.AuthorizationMetadataProvider;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Proxy 鉴权授权元数据提供者<br>
 * 负责对接 MetadataService 提供 ACL 元数据访问
 */
public class ProxyAuthorizationMetadataProvider implements AuthorizationMetadataProvider {

    /**
     * 认证配置对象
     */
    protected AuthConfig authConfig;

    /**
     * 元数据服务
     */
    protected MetadataService metadataService;

    /**
     * 初始化授权元数据提供者
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
     * 创建 ACL
     *
     * @param acl ACL 对象
     * @return 异步结果
     */
    @Override
    public CompletableFuture<Void> createAcl(Acl acl) {
        return null;
    }

    /**
     * 删除 ACL
     *
     * @param subject 授权主体
     * @return 异步结果
     */
    @Override
    public CompletableFuture<Void> deleteAcl(Subject subject) {
        return null;
    }

    /**
     * 更新 ACL
     *
     * @param acl ACL 对象
     * @return 异步结果
     */
    @Override
    public CompletableFuture<Void> updateAcl(Acl acl) {
        return null;
    }

    @Override
    public CompletableFuture<Acl> getAcl(Subject subject) {
        return this.metadataService.getAcl(null, subject);
    }

    /**
     * 按过滤条件列出 ACL
     *
     * @param subjectFilter 主体过滤条件
     * @param resourceFilter 资源过滤条件
     * @return ACL 列表异步结果
     */
    @Override
    public CompletableFuture<List<Acl>> listAcl(String subjectFilter, String resourceFilter) {
        return null;
    }
}
