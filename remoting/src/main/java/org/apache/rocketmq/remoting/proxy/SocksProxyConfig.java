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
package org.apache.rocketmq.remoting.proxy;

/**
 * SOCKS 代理配置对象
 */
public class SocksProxyConfig {
    /**
     * 代理地址
     */
    private String addr;
    /**
     * 认证用户名
     */
    private String username;
    /**
     * 认证密码
     */
    private String password;

    /**
     * 构造空代理配置
     */
    public SocksProxyConfig() {
    }

    /**
     * 按地址构造代理配置
     *
     * @param addr 代理地址
     */
    public SocksProxyConfig(String addr) {
        this.addr = addr;
    }

    /**
     * 按地址和认证信息构造代理配置
     *
     * @param addr 代理地址
     * @param username 认证用户名
     * @param password 认证密码
     */
    public SocksProxyConfig(String addr, String username, String password) {
        this.addr = addr;
        this.username = username;
        this.password = password;
    }

    public String getAddr() {
        return addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * 输出代理配置字符串
     *
     * @return 可读字符串
     */
    @Override
    public String toString() {
        return String.format("SocksProxy address: %s, username: %s, password: %s", addr, username, password);
    }
}
