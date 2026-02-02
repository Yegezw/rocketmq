package com.example;

import com.example.interceptor.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.TimeUnit;

/**
 * 安全消息客户端 (带拦截器)
 * <p>
 * 这个示例展示了<br>
 * 1. 如何在客户端添加拦截器<br>
 * 2. 客户端拦截器如何自动添加认证信息<br>
 * 3. 如何处理认证和授权错误
 */
public class SecureMessagingClient {

    private final ManagedChannel channel;
    private final SecureMessagingServiceGrpc.SecureMessagingServiceBlockingStub blockingStub;

    public SecureMessagingClient(String host, int port, String username, String password) {
        // 创建通道并添加拦截器
        this.channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .intercept(
                        new ClientAuthInterceptor(username, password), // 认证拦截器          | -> response
                        new ClientLoggingInterceptor()                 // 日志拦截器    request    |
                )
                .build();

        this.blockingStub = SecureMessagingServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * 发送消息
     */
    public void sendMessage(String topic, String body) {
        System.out.println("\n========== 发送消息 ==========");
        System.out.println("Topic: " + topic);
        System.out.println("Body: " + body);

        SendMessageRequest request = SendMessageRequest.newBuilder()
                .setTopic(topic)
                .setBody(body)
                .build();

        try {
            SendMessageResponse response = blockingStub.sendMessage(request);
            System.out.println("\n响应:");
            System.out.println("  消息 ID: " + response.getMessageId());
            System.out.println("  状态: " + response.getStatus());
        } catch (StatusRuntimeException e) {
            System.err.println("\n发送失败:");
            System.err.println("  状态码: " + e.getStatus().getCode());
            System.err.println("  错误信息: " + e.getStatus().getDescription());
        }
    }

    /**
     * 查询消息
     */
    public void queryMessage(String messageId) {
        System.out.println("\n========== 查询消息 ==========");
        System.out.println("消息 ID: " + messageId);

        QueryMessageRequest request = QueryMessageRequest.newBuilder()
                .setMessageId(messageId)
                .build();

        try {
            QueryMessageResponse response = blockingStub.queryMessage(request);
            System.out.println("\n响应:");
            System.out.println("  消息 ID: " + response.getMessageId());
            System.out.println("  Topic: " + response.getTopic());
            System.out.println("  Body: " + response.getBody());
            System.out.println("  时间戳: " + new java.util.Date(response.getTimestamp()));
        } catch (StatusRuntimeException e) {
            System.err.println("\n查询失败:");
            System.err.println("  状态码: " + e.getStatus().getCode());
            System.err.println("  错误信息: " + e.getStatus().getDescription());
        }
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("========================================");
        System.out.println("测试 1: 使用正确的认证信息");
        System.out.println("========================================");
        SecureMessagingClient client1 = new SecureMessagingClient("localhost", 50053, "admin", "password123");
        try {
            // 发送消息到 TopicA (admin 有权限)
            client1.sendMessage("TopicA", "这是一条测试消息");
            Thread.sleep(1000);
            // 查询消息
            client1.queryMessage("MSG_1");
        } finally {
            client1.shutdown();
        }

        Thread.sleep(2000);

        System.out.println("\n\n========================================");
        System.out.println("测试 2: 使用错误的认证信息");
        System.out.println("========================================");
        SecureMessagingClient client2 = new SecureMessagingClient("localhost", 50053, "admin", "wrongpassword");
        try {
            // 尝试发送消息 (应该失败)
            client2.sendMessage("TopicA", "这条消息不应该被发送");
        } finally {
            client2.shutdown();
        }

        Thread.sleep(2000);

        System.out.println("\n\n========================================");
        System.out.println("测试 3: 缺少认证信息");
        System.out.println("========================================");
        SecureMessagingClient client3 = new SecureMessagingClient("localhost", 50053, "", "");
        try {
            // 尝试发送消息 (应该失败)
            client3.sendMessage("TopicA", "这条消息不应该被发送");
        } finally {
            client3.shutdown();
        }
    }
}
