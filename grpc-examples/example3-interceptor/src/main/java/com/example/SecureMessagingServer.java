package com.example;

import com.example.interceptor.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 安全消息服务端 (带拦截器)
 * <p>
 * 这个示例展示了<br>
 * 1. 如何添加多个拦截器<br>
 * 2. 拦截器的执行顺序<br>
 * 3. 拦截器之间如何传递信息 (通过 Context)<br>
 * 4. 如何在服务中使用拦截器设置的信息
 */
public class SecureMessagingServer {

    private Server server;

    // 模拟消息存储
    private final Map<String, StoredMessage> messageStore = new ConcurrentHashMap<>();
    private final AtomicLong messageIdGenerator = new AtomicLong(0);

    public void start() throws IOException {
        // 创建服务实现
        SecureMessagingServiceImpl service = new SecureMessagingServiceImpl();

        // 添加拦截器
        // 注意: 拦截器的执行顺序是从后往前的
        // 即: LoggingInterceptor -> AuthenticationInterceptor -> AuthorizationInterceptor -> Service
        int port = 50053;
        server = ServerBuilder.forPort(port)
                .addService(ServerInterceptors.intercept(
                        service,
                        new AuthorizationInterceptor(),  //       | -> response
                        new AuthenticationInterceptor(), //       |    |
                        new LoggingInterceptor()         // request    |
                ))
                .build()
                .start();

        System.out.println("安全消息服务器已启动，监听端口: " + port);
        System.out.println("拦截器链: LoggingInterceptor -> AuthenticationInterceptor -> AuthorizationInterceptor -> Service");
        System.out.println("\n测试账号:");
        System.out.println("  用户名: admin");
        System.out.println("  密码: password123");
        System.out.println("  权限: TopicA, TopicB, TopicC\n");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("*** 正在关闭服务器 ***");
            SecureMessagingServer.this.stop();
            System.err.println("*** 服务器已关闭 ***");
        }));
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * 服务实现类
     */
    class SecureMessagingServiceImpl extends SecureMessagingServiceGrpc.SecureMessagingServiceImplBase {

        @Override
        public void sendMessage(SendMessageRequest request,
                                StreamObserver<SendMessageResponse> responseObserver) {

            // 从 Context 中获取用户信息 (由 AuthenticationInterceptor 设置)
            String username = AuthenticationInterceptor.USER_KEY.get();

            System.out.println("\n[Service] 处理发送消息请求");
            System.out.println("[Service] 用户: " + username);
            System.out.println("[Service] Topic: " + request.getTopic());
            System.out.println("[Service] Body: " + request.getBody());

            // 生成消息 ID
            String messageId = "MSG_" + messageIdGenerator.incrementAndGet();

            // 存储消息
            StoredMessage message = new StoredMessage(
                    messageId,
                    request.getTopic(),
                    request.getBody(),
                    System.currentTimeMillis(),
                    username
            );
            messageStore.put(messageId, message);

            System.out.println("[Service] 消息已存储: " + messageId);

            // 构建响应
            SendMessageResponse response = SendMessageResponse.newBuilder()
                    .setMessageId(messageId)
                    .setStatus("SUCCESS")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void queryMessage(QueryMessageRequest request,
                                 StreamObserver<QueryMessageResponse> responseObserver) {

            // 从 Context 中获取用户信息 (由 AuthenticationInterceptor 设置)
            String username = AuthenticationInterceptor.USER_KEY.get();

            System.out.println("\n[Service] 处理查询消息请求");
            System.out.println("[Service] 用户: " + username);
            System.out.println("[Service] 消息 ID: " + request.getMessageId());

            // 查询消息
            StoredMessage message = messageStore.get(request.getMessageId());

            if (message == null) {
                System.err.println("[Service] 消息不存在");
                responseObserver.onError(
                        Status.NOT_FOUND
                                .withDescription("消息不存在")
                                .asRuntimeException()
                );
                return;
            }

            System.out.println("[Service] 找到消息: " + message.messageId);

            // 构建响应
            QueryMessageResponse response = QueryMessageResponse.newBuilder()
                    .setMessageId(message.messageId)
                    .setTopic(message.topic)
                    .setBody(message.body)
                    .setTimestamp(message.timestamp)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    /**
     * 存储的消息
     */
    static class StoredMessage {
        final String messageId; // 消息 ID
        final String topic;     // 主题
        final String body;      // 消息体
        final long timestamp;   // 时间戳
        final String sender;    // 发送者

        StoredMessage(String messageId, String topic, String body, long timestamp, String sender) {
            this.messageId = messageId;
            this.topic = topic;
            this.body = body;
            this.timestamp = timestamp;
            this.sender = sender;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final SecureMessagingServer server = new SecureMessagingServer();
        server.start();
        server.blockUntilShutdown();
    }
}
