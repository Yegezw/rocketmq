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

package org.apache.rocketmq.proxy.grpc.interceptor;

import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * gRPC 全局异常拦截器, 统一捕获 listener 生命周期中的异常并关闭调用
 */
public class GlobalExceptionInterceptor implements ServerInterceptor {
    /**
     * Proxy 模块日志记录器
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    /**
     * 包装原始 listener, 保证回调异常被统一转换为 gRPC status
     *
     * @param call 当前 RPC 调用
     * @param headers 当前调用头信息
     * @param next 下一个调用处理器
     * @return 具备异常兜底能力的监听器
     */
    @Override
    public <R, W> ServerCall.Listener<R> interceptCall(
        ServerCall<R, W> call,
        Metadata headers,
        ServerCallHandler<R, W> next
    ) {
        final ServerCall<R, W> serverCall = new ClosableServerCall<>(call);
        ServerCall.Listener<R> delegate = next.startCall(serverCall, headers);
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<R>(delegate) {
            /**
             * 转发消息事件, 捕获业务处理异常
             *
             * @param message 客户端上行消息
             */
            @Override
            public void onMessage(R message) {
                try {
                    super.onMessage(message);
                } catch (Throwable e) {
                    closeWithException(e);
                }
            }

            /**
             * 转发半关闭事件, 捕获业务处理异常
             */
            @Override
            public void onHalfClose() {
                try {
                    super.onHalfClose();
                } catch (Throwable e) {
                    closeWithException(e);
                }
            }

            /**
             * 转发取消事件, 捕获清理流程异常
             */
            @Override
            public void onCancel() {
                try {
                    super.onCancel();
                } catch (Throwable e) {
                    closeWithException(e);
                }
            }

            /**
             * 转发完成事件, 捕获收尾流程异常
             */
            @Override
            public void onComplete() {
                try {
                    super.onComplete();
                } catch (Throwable e) {
                    closeWithException(e);
                }
            }

            /**
             * 转发可写事件, 捕获背压回调异常
             */
            @Override
            public void onReady() {
                try {
                    super.onReady();
                } catch (Throwable e) {
                    closeWithException(e);
                }
            }

            /**
             * 将异常转换为 status 并主动关闭调用
             *
             * @param t 运行期异常
             */
            private void closeWithException(Throwable t) {
                Metadata trailers = new Metadata();
                Status status = Status.INTERNAL.withDescription(t.getMessage());
                boolean printLog = true;

                if (t instanceof StatusRuntimeException) {
                    trailers = ((StatusRuntimeException) t).getTrailers();
                    status = ((StatusRuntimeException) t).getStatus();
                    // no error stack for permission denied.
                    // 对权限拒绝异常不打印堆栈, 避免重复噪声日志
                    if (status.getCode().value() == Status.PERMISSION_DENIED.getCode().value()) {
                        printLog = false;
                    }
                }

                if (printLog) {
                    log.error("grpc server has exception. errorMsg:{}, e:", t.getMessage(), t);
                }

                serverCall.close(status, trailers);
            }
        };
    }

    /**
     * 可关闭包装调用对象, 保证 close 仅执行一次
     */
    private static class ClosableServerCall<R, W> extends
        ForwardingServerCall.SimpleForwardingServerCall<R, W> {
        /**
         * 标记 close 是否已调用
         */
        private boolean closeCalled = false;

        /**
         * 构造可关闭包装调用对象
         *
         * @param delegate 原始调用对象
         */
        ClosableServerCall(ServerCall<R, W> delegate) {
            super(delegate);
        }

        /**
         * 幂等关闭调用, 避免重复写回状态
         *
         * @param status 返回状态
         * @param trailers 返回尾信息
         */
        @Override
        public synchronized void close(final Status status, final Metadata trailers) {
            if (!closeCalled) {
                closeCalled = true;
                ClosableServerCall.super.close(status, trailers);
            }
        }
    }
}
