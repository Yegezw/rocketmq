package com.example.interceptor;

import io.grpc.*;

import java.util.Date;

/**
 * 日志拦截器 (服务端)
 * <p>
 * 这个拦截器记录请求的详细信息<br>
 * 1. 请求开始时间<br>
 * 2. 请求方法<br>
 * 3. 请求耗时<br>
 * 4. 请求结果
 */
public class LoggingInterceptor implements ServerInterceptor {

    /**
     * 拦截调用
     *
     * @param call    服务端调用对象
     * @param headers 元数据
     * @param next    下一个调用处理器
     * @param <ReqT>  请求类型
     * @param <RespT> 响应类型
     * @return 请求监听器
     */
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        String methodName = call.getMethodDescriptor().getFullMethodName();
        long startTime = System.currentTimeMillis();

        System.out.println("[LoggingInterceptor] ========== 请求开始 ==========");
        System.out.println("[LoggingInterceptor] 方法: " + methodName);
        System.out.println("[LoggingInterceptor] 时间: " + new Date(startTime));

        // 包装 ServerCall, 以便在响应时记录日志
        ServerCall<ReqT, RespT> wrappedCall = new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
            @Override
            public void close(Status status, Metadata trailers) {
                long endTime = System.currentTimeMillis();
                long duration = endTime - startTime;

                System.out.println("[LoggingInterceptor] ========== 请求结束 ==========");
                System.out.println("[LoggingInterceptor] 方法: " + methodName);
                System.out.println("[LoggingInterceptor] 状态: " + status.getCode());
                System.out.println("[LoggingInterceptor] 耗时: " + duration + " ms");

                if (!status.isOk()) {
                    System.err.println("[LoggingInterceptor] 错误: " + status.getDescription());
                }

                super.close(status, trailers);
            }
        };

        // 包装 Listener, 以便记录接收到的消息
        ServerCall.Listener<ReqT> listener = next.startCall(wrappedCall, headers);

        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(listener) {
            @Override
            public void onMessage(ReqT message) {
                System.out.println("[LoggingInterceptor] 收到消息: " + message.getClass().getSimpleName());
                super.onMessage(message);
            }

            @Override
            public void onHalfClose() {
                System.out.println("[LoggingInterceptor] 客户端完成发送");
                super.onHalfClose();
            }

            @Override
            public void onCancel() {
                System.out.println("[LoggingInterceptor] 请求被取消");
                super.onCancel();
            }

            @Override
            public void onComplete() {
                System.out.println("[LoggingInterceptor] 请求完成");
                super.onComplete();
            }
        };
    }
}
