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

import com.google.common.net.HostAndPort;
import org.apache.rocketmq.common.utils.IPAddressUtils;

import java.util.Objects;

/**
 * 代理地址模型<br>
 * 包含地址类型与主机端口信息
 */
public class Address {

    /**
     * 地址类型
     */
    public enum AddressScheme {
        IPv4,
        IPv6,
        DOMAIN_NAME,
        UNRECOGNIZED
    }

    /**
     * 地址类型
     */
    private AddressScheme addressScheme;
    /**
     * 主机与端口
     */
    private HostAndPort hostAndPort;

    /**
     * 根据主机端口构造地址对象
     *
     * @param hostAndPort 主机与端口
     */
    public Address(HostAndPort hostAndPort) {
        this.addressScheme = buildScheme(hostAndPort);
        this.hostAndPort = hostAndPort;
    }

    /**
     * 根据地址类型与主机端口构造地址对象
     *
     * @param addressScheme 地址类型
     * @param hostAndPort 主机与端口
     */
    public Address(AddressScheme addressScheme, HostAndPort hostAndPort) {
        this.addressScheme = addressScheme;
        this.hostAndPort = hostAndPort;
    }

    public AddressScheme getAddressScheme() {
        return addressScheme;
    }

    public void setAddressScheme(AddressScheme addressScheme) {
        this.addressScheme = addressScheme;
    }

    public HostAndPort getHostAndPort() {
        return hostAndPort;
    }

    public void setHostAndPort(HostAndPort hostAndPort) {
        this.hostAndPort = hostAndPort;
    }

    /**
     * 根据主机地址推导地址类型
     *
     * @param hostAndPort 主机与端口
     * @return 地址类型
     */
    private AddressScheme buildScheme(HostAndPort hostAndPort) {
        if (hostAndPort == null) {
            return AddressScheme.UNRECOGNIZED;
        }
        String address = hostAndPort.getHost();
        if (IPAddressUtils.isValidIPv4(address)) {
            return AddressScheme.IPv4;
        }
        if (IPAddressUtils.isValidIPv6(address)) {
            return AddressScheme.IPv6;
        }
        return AddressScheme.DOMAIN_NAME;
    }

    /**
     * 比较两个地址对象是否相等
     *
     * @param o 对比对象
     * @return true 表示相等
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Address address = (Address) o;
        return addressScheme == address.addressScheme && Objects.equals(hostAndPort, address.hostAndPort);
    }

    /**
     * 计算地址对象哈希值
     *
     * @return 哈希值
     */
    @Override
    public int hashCode() {
        return Objects.hash(addressScheme, hostAndPort);
    }
}
