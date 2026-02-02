package com.example.interceptor;

import io.grpc.*;

/**
 * 认证拦截器 (服务端)
 * <p>
 * 这个拦截器模拟了 RocketMQ Proxy 的认证机制<br>
 * 1. 从 Metadata 中提取认证信息<br>
 * 2. 验证用户名和密码<br>
 * 3. 将用户信息存入 Context
 */
public class AuthenticationInterceptor implements ServerInterceptor {

    // 模拟用户数据库
    private static final String VALID_USERNAME = "admin";
    private static final String VALID_PASSWORD = "password123";

    // Context Key, 用于在拦截器之间传递用户信息
    public static final Context.Key<String> USER_KEY = Context.key("user");

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

        System.out.println("[AuthenticationInterceptor] 开始认证");

        // 1. 从 Metadata 中提取认证信息
        String username = headers.get(Metadata.Key.of("username", Metadata.ASCII_STRING_MARSHALLER));
        String password = headers.get(Metadata.Key.of("password", Metadata.ASCII_STRING_MARSHALLER));

        System.out.println("[AuthenticationInterceptor] 用户名: " + username);

        // 2. 验证认证信息
        if (username == null || password == null) {
            System.err.println("[AuthenticationInterceptor] 认证失败: 缺少认证信息");
            call.close(
                Status.UNAUTHENTICATED.withDescription("缺少认证信息"),
                new Metadata()
            );
            return new ServerCall.Listener<ReqT>() {};
        }

        if (!VALID_USERNAME.equals(username) || !VALID_PASSWORD.equals(password)) {
            System.err.println("[AuthenticationInterceptor] 认证失败: 用户名或密码错误");
            call.close(
                Status.UNAUTHENTICATED.withDescription("用户名或密码错误"),
                new Metadata()
            );
            return new ServerCall.Listener<ReqT>() {};
        }

        System.out.println("[AuthenticationInterceptor] 认证成功");

        // 3. 将用户信息存入 Context, 供后续拦截器和服务使用
        Context context = Context.current().withValue(USER_KEY, username);

        // 4. 继续处理请求
        return Contexts.interceptCall(context, call, headers, next);
    }
}
