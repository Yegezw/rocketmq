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

package org.apache.rocketmq.common.utils;

import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.List;

/**
 * 轻量 HTTP 客户端工具, 用于执行简化的 GET 与 POST 请求
 */
public class HttpTinyClient {

    /**
     * 发送 HTTP GET 请求
     *
     * @param url 请求地址
     * @param headers 请求头键值对列表, 采用 key value 交替方式组织
     * @param paramValues 查询参数键值对列表, 采用 key value 交替方式组织
     * @param encoding 编码格式
     * @param readTimeoutMs 连接与读取超时时间, 单位毫秒
     * @return HTTP 响应结果
     * @throws IOException 网络或 IO 异常
     */
    static public HttpResult httpGet(String url, List<String> headers, List<String> paramValues,
        String encoding, long readTimeoutMs) throws IOException {
        String encodedContent = encodingParams(paramValues, encoding);
        url += (null == encodedContent) ? "" : ("?" + encodedContent);

        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout((int) readTimeoutMs);
            conn.setReadTimeout((int) readTimeoutMs);
            setHeaders(conn, headers, encoding);

            conn.connect();
            int respCode = conn.getResponseCode();
            String resp = null;

            if (HttpURLConnection.HTTP_OK == respCode) {
                resp = IOTinyUtils.toString(conn.getInputStream(), encoding);
            } else {
                resp = IOTinyUtils.toString(conn.getErrorStream(), encoding);
            }
            return new HttpResult(respCode, resp);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    /**
     * 对参数列表执行 URL 编码<br>
     * 调用方需要保证参数按 key value 成对传入
     *
     * @param paramValues 参数键值对列表
     * @param encoding 编码格式
     * @return 编码后的查询字符串, 无参数时返回 null
     * @throws UnsupportedEncodingException 指定编码不受支持时抛出
     */
    static private String encodingParams(List<String> paramValues, String encoding)
        throws UnsupportedEncodingException {
        StringBuilder sb = new StringBuilder();
        if (null == paramValues) {
            return null;
        }

        for (Iterator<String> iter = paramValues.iterator(); iter.hasNext(); ) {
            sb.append(iter.next()).append("=");
            sb.append(URLEncoder.encode(iter.next(), encoding));
            if (iter.hasNext()) {
                sb.append("&");
            }
        }
        return sb.toString();
    }

    /**
     * 设置 HTTP 请求头<br>
     * 会附加客户端版本, 内容类型与请求时间戳
     *
     * @param conn HTTP 连接对象
     * @param headers 额外请求头键值对列表
     * @param encoding 编码格式
     */
    static private void setHeaders(HttpURLConnection conn, List<String> headers, String encoding) {
        if (null != headers) {
            for (Iterator<String> iter = headers.iterator(); iter.hasNext(); ) {
                conn.addRequestProperty(iter.next(), iter.next());
            }
        }
        conn.addRequestProperty("Client-Version", MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION));
        conn.addRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=" + encoding);

        String ts = String.valueOf(System.currentTimeMillis());
        conn.addRequestProperty("Metaq-Client-RequestTS", ts);
    }

    /**
     * @return the http response of given http post request<br>给定 HTTP POST 请求的响应结果
     */
    static public HttpResult httpPost(String url, List<String> headers, List<String> paramValues,
        String encoding, long readTimeoutMs) throws IOException {
        String encodedContent = encodingParams(paramValues, encoding);

        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("POST");
            conn.setConnectTimeout(3000);
            conn.setReadTimeout((int) readTimeoutMs);
            conn.setDoOutput(true);
            conn.setDoInput(true);
            setHeaders(conn, headers, encoding);

            conn.getOutputStream().write(encodedContent.getBytes(MixAll.DEFAULT_CHARSET));

            int respCode = conn.getResponseCode();
            String resp = null;

            if (HttpURLConnection.HTTP_OK == respCode) {
                resp = IOTinyUtils.toString(conn.getInputStream(), encoding);
            } else {
                resp = IOTinyUtils.toString(conn.getErrorStream(), encoding);
            }
            return new HttpResult(respCode, resp);
        } finally {
            if (null != conn) {
                conn.disconnect();
            }
        }
    }

    /**
     * HTTP 响应结果封装
     */
    static public class HttpResult {
        /**
         * HTTP 状态码
         */
        final public int code;
        /**
         * 响应体内容
         */
        final public String content;

        /**
         * 构造 HTTP 响应结果
         *
         * @param code HTTP 状态码
         * @param content 响应体内容
         */
        public HttpResult(int code, String content) {
            this.code = code;
            this.content = content;
        }
    }
}
