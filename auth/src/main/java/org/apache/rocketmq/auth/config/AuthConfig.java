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
package org.apache.rocketmq.auth.config;

/**
 * 认证授权配置<br>
 * 定义认证与鉴权插件及缓存相关参数
 */
public class AuthConfig implements Cloneable {

    /**
     * 当前节点配置名
     */
    private String configName;

    /**
     * 当前节点所属集群名
     */
    private String clusterName;

    /**
     * 认证授权配置文件路径
     */
    private String authConfigPath;

    /**
     * 是否启用认证能力
     */
    private boolean authenticationEnabled = false;

    /**
     * 认证提供者实现类
     */
    private String authenticationProvider;

    /**
     * 认证元数据提供者实现类
     */
    private String authenticationMetadataProvider;

    /**
     * 认证策略实现类
     */
    private String authenticationStrategy;

    /**
     * 认证白名单
     */
    private String authenticationWhitelist;

    /**
     * 初始化认证用户配置
     */
    private String initAuthenticationUser;

    /**
     * 内部客户端认证凭证
     */
    private String innerClientAuthenticationCredentials;

    /**
     * 是否启用鉴权能力
     */
    private boolean authorizationEnabled = false;

    /**
     * 鉴权提供者实现类
     */
    private String authorizationProvider;

    /**
     * 鉴权元数据提供者实现类
     */
    private String authorizationMetadataProvider;

    /**
     * 鉴权策略实现类
     */
    private String authorizationStrategy;

    /**
     * 鉴权白名单
     */
    private String authorizationWhitelist;

    /**
     * 是否启用 v1 鉴权数据迁移
     */
    private boolean migrateAuthFromV1Enabled = false;

    /**
     * 用户缓存最大条目数
     */
    private int userCacheMaxNum = 1000;

    /**
     * 用户缓存过期时间, 单位秒
     */
    private int userCacheExpiredSecond = 600;

    /**
     * 用户缓存刷新间隔, 单位秒
     */
    private int userCacheRefreshSecond = 60;

    /**
     * ACL 缓存最大条目数
     */
    private int aclCacheMaxNum = 1000;

    /**
     * ACL 缓存过期时间, 单位秒
     */
    private int aclCacheExpiredSecond = 600;

    /**
     * ACL 缓存刷新间隔, 单位秒
     */
    private int aclCacheRefreshSecond = 60;

    /**
     * 有状态认证缓存最大条目数
     */
    private int statefulAuthenticationCacheMaxNum = 10000;

    /**
     * 有状态认证缓存过期时间, 单位秒
     */
    private int statefulAuthenticationCacheExpiredSecond = 60;

    /**
     * 有状态鉴权缓存最大条目数
     */
    private int statefulAuthorizationCacheMaxNum = 10000;

    /**
     * 有状态鉴权缓存过期时间, 单位秒
     */
    private int statefulAuthorizationCacheExpiredSecond = 60;

    /**
     * 克隆认证授权配置对象
     *
     * @return 认证授权配置副本
     */
    @Override
    public AuthConfig clone() {
        try {
            return (AuthConfig) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    public String getConfigName() {
        return configName;
    }

    public void setConfigName(String configName) {
        this.configName = configName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getAuthConfigPath() {
        return authConfigPath;
    }

    public void setAuthConfigPath(String authConfigPath) {
        this.authConfigPath = authConfigPath;
    }

    public boolean isAuthenticationEnabled() {
        return authenticationEnabled;
    }

    public void setAuthenticationEnabled(boolean authenticationEnabled) {
        this.authenticationEnabled = authenticationEnabled;
    }

    public String getAuthenticationProvider() {
        return authenticationProvider;
    }

    public void setAuthenticationProvider(String authenticationProvider) {
        this.authenticationProvider = authenticationProvider;
    }

    public String getAuthenticationMetadataProvider() {
        return authenticationMetadataProvider;
    }

    public void setAuthenticationMetadataProvider(String authenticationMetadataProvider) {
        this.authenticationMetadataProvider = authenticationMetadataProvider;
    }

    public String getAuthenticationStrategy() {
        return authenticationStrategy;
    }

    public void setAuthenticationStrategy(String authenticationStrategy) {
        this.authenticationStrategy = authenticationStrategy;
    }

    public String getAuthenticationWhitelist() {
        return authenticationWhitelist;
    }

    public void setAuthenticationWhitelist(String authenticationWhitelist) {
        this.authenticationWhitelist = authenticationWhitelist;
    }

    public String getInitAuthenticationUser() {
        return initAuthenticationUser;
    }

    public void setInitAuthenticationUser(String initAuthenticationUser) {
        this.initAuthenticationUser = initAuthenticationUser;
    }

    public String getInnerClientAuthenticationCredentials() {
        return innerClientAuthenticationCredentials;
    }

    public void setInnerClientAuthenticationCredentials(String innerClientAuthenticationCredentials) {
        this.innerClientAuthenticationCredentials = innerClientAuthenticationCredentials;
    }

    public boolean isAuthorizationEnabled() {
        return authorizationEnabled;
    }

    public void setAuthorizationEnabled(boolean authorizationEnabled) {
        this.authorizationEnabled = authorizationEnabled;
    }

    public String getAuthorizationProvider() {
        return authorizationProvider;
    }

    public void setAuthorizationProvider(String authorizationProvider) {
        this.authorizationProvider = authorizationProvider;
    }

    public String getAuthorizationMetadataProvider() {
        return authorizationMetadataProvider;
    }

    public void setAuthorizationMetadataProvider(String authorizationMetadataProvider) {
        this.authorizationMetadataProvider = authorizationMetadataProvider;
    }

    public String getAuthorizationStrategy() {
        return authorizationStrategy;
    }

    public void setAuthorizationStrategy(String authorizationStrategy) {
        this.authorizationStrategy = authorizationStrategy;
    }

    public String getAuthorizationWhitelist() {
        return authorizationWhitelist;
    }

    public void setAuthorizationWhitelist(String authorizationWhitelist) {
        this.authorizationWhitelist = authorizationWhitelist;
    }

    public boolean isMigrateAuthFromV1Enabled() {
        return migrateAuthFromV1Enabled;
    }

    public void setMigrateAuthFromV1Enabled(boolean migrateAuthFromV1Enabled) {
        this.migrateAuthFromV1Enabled = migrateAuthFromV1Enabled;
    }

    public int getUserCacheMaxNum() {
        return userCacheMaxNum;
    }

    public void setUserCacheMaxNum(int userCacheMaxNum) {
        this.userCacheMaxNum = userCacheMaxNum;
    }

    public int getUserCacheExpiredSecond() {
        return userCacheExpiredSecond;
    }

    public void setUserCacheExpiredSecond(int userCacheExpiredSecond) {
        this.userCacheExpiredSecond = userCacheExpiredSecond;
    }

    public int getUserCacheRefreshSecond() {
        return userCacheRefreshSecond;
    }

    public void setUserCacheRefreshSecond(int userCacheRefreshSecond) {
        this.userCacheRefreshSecond = userCacheRefreshSecond;
    }

    public int getAclCacheMaxNum() {
        return aclCacheMaxNum;
    }

    public void setAclCacheMaxNum(int aclCacheMaxNum) {
        this.aclCacheMaxNum = aclCacheMaxNum;
    }

    public int getAclCacheExpiredSecond() {
        return aclCacheExpiredSecond;
    }

    public void setAclCacheExpiredSecond(int aclCacheExpiredSecond) {
        this.aclCacheExpiredSecond = aclCacheExpiredSecond;
    }

    public int getAclCacheRefreshSecond() {
        return aclCacheRefreshSecond;
    }

    public void setAclCacheRefreshSecond(int aclCacheRefreshSecond) {
        this.aclCacheRefreshSecond = aclCacheRefreshSecond;
    }

    public int getStatefulAuthenticationCacheMaxNum() {
        return statefulAuthenticationCacheMaxNum;
    }

    public void setStatefulAuthenticationCacheMaxNum(int statefulAuthenticationCacheMaxNum) {
        this.statefulAuthenticationCacheMaxNum = statefulAuthenticationCacheMaxNum;
    }

    public int getStatefulAuthenticationCacheExpiredSecond() {
        return statefulAuthenticationCacheExpiredSecond;
    }

    public void setStatefulAuthenticationCacheExpiredSecond(int statefulAuthenticationCacheExpiredSecond) {
        this.statefulAuthenticationCacheExpiredSecond = statefulAuthenticationCacheExpiredSecond;
    }

    public int getStatefulAuthorizationCacheMaxNum() {
        return statefulAuthorizationCacheMaxNum;
    }

    public void setStatefulAuthorizationCacheMaxNum(int statefulAuthorizationCacheMaxNum) {
        this.statefulAuthorizationCacheMaxNum = statefulAuthorizationCacheMaxNum;
    }

    public int getStatefulAuthorizationCacheExpiredSecond() {
        return statefulAuthorizationCacheExpiredSecond;
    }

    public void setStatefulAuthorizationCacheExpiredSecond(int statefulAuthorizationCacheExpiredSecond) {
        this.statefulAuthorizationCacheExpiredSecond = statefulAuthorizationCacheExpiredSecond;
    }
}
