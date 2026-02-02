package com.example.interceptor;

import io.grpc.*;

import java.util.Date;

/**
 * 客户端日志拦截器<br>
 * 记录客户端请求的详细信息
 */
public class ClientLoggingInterceptor implements ClientInterceptor {

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {

        String methodName = method.getFullMethodName();
        long startTime = System.currentTimeMillis();

        System.out.println("\n[ClientLoggingInterceptor] ========== 发送请求 ==========");
        System.out.println("[ClientLoggingInterceptor] 方法: " + methodName);
        System.out.println("[ClientLoggingInterceptor] 时间: " + new Date(startTime));

        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                next.newCall(method, callOptions)) {

            @Override
            public void sendMessage(ReqT message) {
                System.out.println("[ClientLoggingInterceptor] 发送消息: " + message.getClass().getSimpleName());
                super.sendMessage(message);
            }

            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
                    @Override
                    public void onMessage(RespT message) {
                        System.out.println("[ClientLoggingInterceptor] 收到响应: " + message.getClass().getSimpleName());
                        super.onMessage(message);
                    }

                    @Override
                    public void onClose(Status status, Metadata trailers) {
                        long endTime = System.currentTimeMillis();
                        long duration = endTime - startTime;

                        System.out.println("[ClientLoggingInterceptor] ========== 请求完成 ==========");
                        System.out.println("[ClientLoggingInterceptor] 状态: " + status.getCode());
                        System.out.println("[ClientLoggingInterceptor] 耗时: " + duration + " ms");

                        if (!status.isOk()) {
                            System.err.println("[ClientLoggingInterceptor] 错误: " + status.getDescription());
                        }

                        super.onClose(status, trailers);
                    }
                }, headers);
            }
        };
    }
}
