package com.example.interceptor;

import io.grpc.*;

/**
 * 客户端认证拦截器<br>
 * 这个拦截器在客户端发送请求时，自动添加认证信息到 Metadata
 */
public class ClientAuthInterceptor implements ClientInterceptor {

    private final String username;
    private final String password;

    public ClientAuthInterceptor(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {

        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                next.newCall(method, callOptions)) {

            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                // 添加认证信息到 Metadata
                headers.put(
                        Metadata.Key.of("username", Metadata.ASCII_STRING_MARSHALLER),
                        username
                );
                headers.put(
                        Metadata.Key.of("password", Metadata.ASCII_STRING_MARSHALLER),
                        password
                );

                System.out.println("[ClientAuthInterceptor] 添加认证信息: " + username);

                super.start(responseListener, headers);
            }
        };
    }
}
