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

package org.apache.rocketmq.proxy.grpc.v2.common;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * gRPC 响应写出器<br>
 * 负责安全写流并处理客户端取消场景
 */
public class ResponseWriter {
    /**
     * Proxy 日志记录器
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    /**
     * 单例创建锁
     */
    protected static final Object INSTANCE_CREATE_LOCK = new Object();
    /**
     * 写出器单例
     */
    protected static volatile ResponseWriter instance;

    public static ResponseWriter getInstance() {
        if (instance == null) {
            synchronized (INSTANCE_CREATE_LOCK) {
                if (instance == null) {
                    instance = new ResponseWriter();
                }
            }
        }
        return instance;
    }

    /**
     * 写出单条响应并主动结束流
     *
     * @param observer 流观察者
     * @param response 响应对象
     * @param <T> 响应类型
     */
    public <T> void write(StreamObserver<T> observer, final T response) {
        if (writeResponse(observer, response)) {
            observer.onCompleted();
        }
    }

    /**
     * 写出单条响应并返回写出结果
     *
     * @param observer 流观察者
     * @param response 响应对象
     * @param <T> 响应类型
     * @return true 表示写出成功
     */
    public <T> boolean writeResponse(StreamObserver<T> observer, final T response) {
        if (null == response) {
            return false;
        }
        log.debug("start to write response. response: {}", response);
        if (isCancelled(observer)) {
            log.warn("client has cancelled the request. response to write: {}", response);
            return false;
        }
        try {
            observer.onNext(response);
        } catch (StatusRuntimeException statusRuntimeException) {
            if (Status.CANCELLED.equals(statusRuntimeException.getStatus())) {
                log.warn("client has cancelled the request. response to write: {}", response);
                return false;
            }
            throw statusRuntimeException;
        }
        return true;
    }

    public <T> boolean isCancelled(StreamObserver<T> observer) {
        if (observer instanceof ServerCallStreamObserver) {
            final ServerCallStreamObserver<T> serverCallStreamObserver = (ServerCallStreamObserver<T>) observer;
            return serverCallStreamObserver.isCancelled();
        }
        return false;
    }
}
