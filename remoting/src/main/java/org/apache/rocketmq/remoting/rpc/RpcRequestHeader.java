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
package org.apache.rocketmq.remoting.rpc;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import org.apache.rocketmq.remoting.CommandCustomHeader;

/**
 * RPC 请求头抽象基类
 */
public abstract class RpcRequestHeader implements CommandCustomHeader {
    //the namespace name
    /**
     * 命名空间名称
     */
    protected String ns;
    //if the data has been namespaced
    /**
     * 数据是否已带命名空间标记
     */
    protected Boolean nsd;
    //the abstract remote addr name, usually the physical broker name
    /**
     * 抽象远端地址标识, 通常为物理 Broker 名称
     */
    protected String bname;
    //oneway
    /**
     * 是否单向请求
     */
    protected Boolean oway;

    @Deprecated
    public String getBname() {
        return bname;
    }

    @Deprecated
    public void setBname(String brokerName) {
        this.bname = brokerName;
    }

    public String getBrokerName() {
        return bname;
    }

    public void setBrokerName(String brokerName) {
        this.bname = brokerName;
    }

    public String getNamespace() {
        return ns;
    }

    public void setNamespace(String namespace) {
        this.ns = namespace;
    }

    public Boolean getNamespaced() {
        return nsd;
    }

    public void setNamespaced(Boolean namespaced) {
        this.nsd = namespaced;
    }

    public Boolean getOneway() {
        return oway;
    }

    public void setOneway(Boolean oneway) {
        this.oway = oneway;
    }

    /**
     * 判断请求头对象是否相等
     *
     * @param o 待比较对象
     * @return 字段一致时返回 true
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RpcRequestHeader header = (RpcRequestHeader) o;
        return Objects.equals(ns, header.ns) && Objects.equals(nsd, header.nsd) && Objects.equals(bname, header.bname) && Objects.equals(oway, header.oway);
    }

    /**
     * 计算请求头哈希值
     *
     * @return 当前对象哈希值
     */
    @Override
    public int hashCode() {
        return Objects.hash(ns, nsd, bname, oway);
    }

    /**
     * 生成请求头可读字符串
     *
     * @return 当前对象字符串
     */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("namespace", ns)
            .add("namespaced", nsd)
            .add("brokerName", bname)
            .add("oneway", oway)
            .toString();
    }
}
