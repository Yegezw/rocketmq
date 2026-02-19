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
package org.apache.rocketmq.common.namesrv;

import com.google.common.base.Strings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Map;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.utils.HttpTinyClient;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class DefaultTopAddressing implements TopAddressing {
    /**
     * 通用日志记录器, 用于输出地址发现与回退链路日志
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    /**
     * NameServer 地址缓存, 可由外部调用方显式设置
     */
    private String nsAddr;
    /**
     * 地址服务入口, 默认通过 HTTP 从该地址拉取 NameServer 信息
     */
    private String wsAddr;
    /**
     * 单元化标识, 非空时会拼接到地址服务 URL 中
     */
    private String unitName;
    /**
     * 额外查询参数, 用于扩展地址服务请求条件
     */
    private Map<String, String> para;
    /**
     * 可插拔地址发现实现列表, 查询时优先于默认 HTTP 逻辑
     */
    private List<TopAddressing> topAddressingList;

    /**
     * 构造默认地址发现实现
     *
     * @param wsAddr 地址服务入口
     */
    public DefaultTopAddressing(final String wsAddr) {
        this(wsAddr, null);
    }

    /**
     * 构造默认地址发现实现
     *
     * @param wsAddr 地址服务入口
     * @param unitName 单元化标识
     */
    public DefaultTopAddressing(final String wsAddr, final String unitName) {
        this.wsAddr = wsAddr;
        this.unitName = unitName;
        this.topAddressingList = loadCustomTopAddressing();
    }

    /**
     * 构造默认地址发现实现
     *
     * @param unitName 单元化标识
     * @param para 附加查询参数
     * @param wsAddr 地址服务入口
     */
    public DefaultTopAddressing(final String unitName, final Map<String, String> para, final String wsAddr) {
        this.wsAddr = wsAddr;
        this.unitName = unitName;
        this.para = para;
        this.topAddressingList = loadCustomTopAddressing();
    }

    /**
     * 清理响应字符串中的换行符
     *
     * @param str 原始响应字符串
     * @return 去除尾部换行后的字符串
     */
    private static String clearNewLine(final String str) {
        String newString = str.trim();
        int index = newString.indexOf("\r");
        if (index != -1) {
            return newString.substring(0, index);
        }

        index = newString.indexOf("\n");
        if (index != -1) {
            return newString.substring(0, index);
        }

        return newString;
    }

    /**
     * 加载自定义地址发现实现
     * 当前仅保留 ServiceLoader 返回的第一个实现, 作为默认逻辑前的扩展入口
     *
     * @return 自定义地址发现实现列表
     */
    private List<TopAddressing> loadCustomTopAddressing() {
        ServiceLoader<TopAddressing> serviceLoader = ServiceLoader.load(TopAddressing.class);
        Iterator<TopAddressing> iterator = serviceLoader.iterator();
        List<TopAddressing> topAddressingList = new ArrayList<>();
        if (iterator.hasNext()) {
            topAddressingList.add(iterator.next());
        }
        return topAddressingList;
    }

    /**
     * 获取 NameServer 地址
     * 先尝试自定义 TopAddressing 实现, 未命中时回退到内置 HTTP 地址发现
     *
     * @return NameServer 地址, 失败时返回 null
     */
    @Override
    public final String fetchNSAddr() {
        if (!topAddressingList.isEmpty()) {
            for (TopAddressing topAddressing : topAddressingList) {
                String nsAddress = topAddressing.fetchNSAddr();
                if (!Strings.isNullOrEmpty(nsAddress)) {
                    return nsAddress;
                }
            }
        }
        // Return result of default implementation
        // 返回默认实现的结果
        return fetchNSAddr(true, 3000);
    }

    /**
     * 注册 NameServer 地址变更回调
     * 回调会透传给所有已加载的自定义 TopAddressing 实现
     *
     * @param changeCallBack 地址变更回调
     */
    @Override
    public void registerChangeCallBack(NameServerUpdateCallback changeCallBack) {
        if (!topAddressingList.isEmpty()) {
            for (TopAddressing topAddressing : topAddressingList) {
                topAddressing.registerChangeCallBack(changeCallBack);
            }
        }
    }

    /**
     * 通过地址服务拉取 NameServer 地址
     *
     * @param verbose 是否打印详细异常与回退日志
     * @param timeoutMills HTTP 请求超时时间, 单位毫秒
     * @return NameServer 地址, 失败时返回 null
     */
    public final String fetchNSAddr(boolean verbose, long timeoutMills) {
        StringBuilder url = new StringBuilder(this.wsAddr);
        try {
            // 组装查询参数, 包括 unitName 与附加 para
            if (null != para && para.size() > 0) {
                if (!UtilAll.isBlank(this.unitName)) {
                    url.append("-").append(this.unitName).append("?nofix=1&");
                }
                else {
                    url.append("?");
                }
                for (Map.Entry<String, String> entry : this.para.entrySet()) {
                    url.append(entry.getKey()).append("=").append(entry.getValue()).append("&");
                }
                url = new StringBuilder(url.substring(0, url.length() - 1));
            }
            else {
                if (!UtilAll.isBlank(this.unitName)) {
                    url.append("-").append(this.unitName).append("?nofix=1");
                }
            }

            // 发起 HTTP 请求获取地址服务响应
            HttpTinyClient.HttpResult result = HttpTinyClient.httpGet(url.toString(), null, null, "UTF-8", timeoutMills);
            if (200 == result.code) {
                String responseStr = result.content;
                if (responseStr != null) {
                    return clearNewLine(responseStr);
                } else {
                    LOGGER.error("fetch nameserver address is null");
                }
            } else {
                LOGGER.error("fetch nameserver address failed. statusCode=" + result.code);
            }
        } catch (IOException e) {
            // verbose 为 false 时由调用方自行控制日志节奏
            if (verbose) {
                LOGGER.error("fetch name server address exception", e);
            }
        }

        // 拉取失败时输出域名未绑定提示, 便于快速定位 hosts 配置问题
        if (verbose) {
            String errorMsg =
                "connect to " + url + " failed, maybe the domain name " + MixAll.getWSAddr() + " not bind in /etc/hosts";
            errorMsg += FAQUrl.suggestTodo(FAQUrl.NAME_SERVER_ADDR_NOT_EXIST_URL);

            LOGGER.warn(errorMsg);
        }
        return null;
    }

    public String getNsAddr() {
        return nsAddr;
    }

    public void setNsAddr(String nsAddr) {
        this.nsAddr = nsAddr;
    }
}
