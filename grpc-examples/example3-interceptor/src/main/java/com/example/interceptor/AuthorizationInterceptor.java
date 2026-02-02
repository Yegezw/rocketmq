package com.example.interceptor;

import io.grpc.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 授权拦截器 (服务端)
 * <p>
 * 这个拦截器模拟了 RocketMQ Proxy 的授权机制<br>
 * 1. 从 Context 中获取用户信息<br>
 * 2. 检查用户是否有权限访问指定的 Topic<br>
 * 3. 决定是否允许请求继续
 */
public class AuthorizationInterceptor implements ServerInterceptor {

    // 模拟权限配置: 用户 -> 允许访问的 Topic 列表
    private static final Map<String, Set<String>> USER_PERMISSIONS = new HashMap<>();

    static {
        Set<String> adminTopics = new HashSet<>();
        adminTopics.add("TopicA");
        adminTopics.add("TopicB");
        adminTopics.add("TopicC");
        USER_PERMISSIONS.put("admin", adminTopics);

        Set<String> guestTopics = new HashSet<>();
        guestTopics.add("TopicA");
        USER_PERMISSIONS.put("guest", guestTopics);
    }

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

        System.out.println("[AuthorizationInterceptor] 开始授权检查");

        // 1. 从 Context 中获取用户信息 (由 AuthenticationInterceptor 设置)
        String username = AuthenticationInterceptor.USER_KEY.get();

        if (username == null) {
            System.err.println("[AuthorizationInterceptor] 授权失败: 未找到用户信息");
            call.close(
                Status.PERMISSION_DENIED.withDescription("未找到用户信息"),
                new Metadata()
            );
            return new ServerCall.Listener<ReqT>() {};
        }

        System.out.println("[AuthorizationInterceptor] 用户: " + username);

        // 2. 获取方法名
        String methodName = call.getMethodDescriptor().getFullMethodName();
        System.out.println("[AuthorizationInterceptor] 方法: " + methodName);

        // 3. 检查权限 (这里简化处理, 实际应该解析请求获取 Topic)
        // 在实际的 RocketMQ Proxy 中, 会解析请求消息获取 Topic, 然后检查权限
        Set<String> allowedTopics = USER_PERMISSIONS.get(username);
        if (allowedTopics == null || allowedTopics.isEmpty()) {
            System.err.println("[AuthorizationInterceptor] 授权失败: 用户无任何权限");
            call.close(
                Status.PERMISSION_DENIED.withDescription("用户无任何权限"),
                new Metadata()
            );
            return new ServerCall.Listener<ReqT>() {};
        }

        System.out.println("[AuthorizationInterceptor] 授权成功, 允许访问的 Topic: " + allowedTopics);

        // 4. 继续处理请求
        return next.startCall(call, headers);
    }
}
