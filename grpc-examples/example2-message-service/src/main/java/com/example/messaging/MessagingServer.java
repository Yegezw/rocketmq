package com.example.messaging;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 消息服务端 (模拟 RocketMQ Proxy)
 * <p>
 * 这个示例展示了<br>
 * 1. 如何实现类似 RocketMQ 的消息服务<br>
 * 2. 如何使用线程池处理请求 (类似 Proxy 的设计)<br>
 * 3. 如何实现服务端流式响应
 */
public class MessagingServer {

    private Server server;

    // 模拟消息存储 (topic -> 消息列表)
    private final Map<String, Queue<StoredMessage>> messageStore = new ConcurrentHashMap<>();

    // 消息 ID 生成器
    private final AtomicLong messageIdGenerator = new AtomicLong(0);

    // 线程池 (模拟 Proxy 的线程池设计)
    private final ExecutorService producerThreadPool; // 10
    private final ExecutorService consumerThreadPool; // 10
    private final ExecutorService routeThreadPool;    // 5

    public MessagingServer() {
        // 创建线程池 (类似 Proxy 的设计)
        this.producerThreadPool = Executors.newFixedThreadPool(10,
                new ThreadFactory() {
                    private final AtomicLong counter = new AtomicLong(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "ProducerThread-" + counter.incrementAndGet());
                    }
                });

        this.consumerThreadPool = Executors.newFixedThreadPool(10,
                new ThreadFactory() {
                    private final AtomicLong counter = new AtomicLong(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "ConsumerThread-" + counter.incrementAndGet());
                    }
                });

        this.routeThreadPool = Executors.newFixedThreadPool(5,
                new ThreadFactory() {
                    private final AtomicLong counter = new AtomicLong(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "RouteThread-" + counter.incrementAndGet());
                    }
                });
    }

    public void start() throws IOException {
        int port = 50052;
        server = ServerBuilder.forPort(port)
                .addService(new MessagingServiceImpl())
                .build()
                .start();

        System.out.println("消息服务器已启动, 监听端口: " + port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("*** 正在关闭服务器 ***");
            MessagingServer.this.stop();
            System.err.println("*** 服务器已关闭 ***");
        }));
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
        producerThreadPool.shutdown();
        consumerThreadPool.shutdown();
        routeThreadPool.shutdown();
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * 消息服务实现类
     */
    class MessagingServiceImpl extends MessagingServiceGrpc.MessagingServiceImplBase {

        /**
         * 发送消息 (一元 RPC)
         * <p>
         * 这个方法展示了<br>
         * 1. 如何使用线程池异步处理请求<br>
         * 2. 如何验证请求参数<br>
         * 3. 如何构建响应
         */
        @Override
        public void sendMessage(SendMessageRequest request,
                                StreamObserver<SendMessageResponse> responseObserver) {
            System.out.println("[SendMessage] 收到请求: topic=" + request.getTopic());

            // 使用生产者线程池处理 (类似 Proxy 的设计)
            producerThreadPool.submit(() -> {
                try {
                    // 1. 验证请求
                    if (request.getTopic().isEmpty()) {
                        SendMessageResponse response = SendMessageResponse.newBuilder()
                                .setStatus(Status.newBuilder()
                                        .setCode(Code.BAD_REQUEST)
                                        .setMessage("topic 不能为空")
                                        .build())
                                .build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                        return;
                    }

                    // 2. 存储消息
                    String messageId = "MSG_" + messageIdGenerator.incrementAndGet();
                    StoredMessage storedMessage = new StoredMessage(
                            messageId,
                            request.getTopic(),
                            request.getTag(),
                            request.getBody(),
                            System.currentTimeMillis(),
                            request.getPropertiesMap()
                    );

                    messageStore.computeIfAbsent(request.getTopic(),
                            k -> new ConcurrentLinkedQueue<>()).offer(storedMessage);

                    System.out.println("[SendMessage] 消息已存储: messageId=" + messageId);

                    // 3. 构建响应
                    SendMessageResponse response = SendMessageResponse.newBuilder()
                            .setStatus(Status.newBuilder()
                                    .setCode(Code.OK)
                                    .setMessage("发送成功")
                                    .build())
                            .setMessageId(messageId)
                            .setQueueOffset(messageIdGenerator.get())
                            .build();

                    // 4. 发送响应
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                } catch (Exception e) {
                    System.err.println("[SendMessage] 处理失败: " + e.getMessage());
                    responseObserver.onError(e);
                }
            });
        }

        /**
         * 接收消息 (服务端流 RPC)
         * <p>
         * 这个方法展示了<br>
         * 1. 如何实现服务端流<br>
         * 2. 如何从存储中读取消息<br>
         * 3. 如何发送多个响应
         */
        @Override
        public void receiveMessage(ReceiveMessageRequest request,
                                   StreamObserver<ReceiveMessageResponse> responseObserver) {

            System.out.println("[ReceiveMessage] 收到请求: topic=" + request.getTopic() +
                    ", consumerGroup=" + request.getConsumerGroup() + 
                    ", maxMessages=" + request.getMaxMessages());

            // 使用消费者线程池处理
            consumerThreadPool.submit(() -> {
                try {
                    // 1. 验证请求
                    if (request.getTopic().isEmpty()) {
                        ReceiveMessageResponse response = ReceiveMessageResponse.newBuilder()
                                .setStatus(Status.newBuilder()
                                        .setCode(Code.BAD_REQUEST)
                                        .setMessage("topic 不能为空")
                                        .build())
                                .build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                        return;
                    }

                    // 2. 获取消息队列
                    Queue<StoredMessage> queue = messageStore.get(request.getTopic());
                    if (queue == null || queue.isEmpty()) {
                        System.out.println("[ReceiveMessage] 没有消息可消费");
                        responseObserver.onCompleted();
                        return;
                    }

                    // 3. 发送消息 (流式响应)
                    int maxMessages = request.getMaxMessages() > 0 ? request.getMaxMessages() : 10;
                    int sentCount = 0;

                    while (sentCount < maxMessages && !queue.isEmpty()) {
                        StoredMessage storedMessage = queue.poll();
                        if (storedMessage != null) {
                            Message message = Message.newBuilder()
                                    .setMessageId(storedMessage.messageId)
                                    .setTopic(storedMessage.topic)
                                    .setTag(storedMessage.tag)
                                    .setBody(storedMessage.body)
                                    .setBornTimestamp(storedMessage.bornTimestamp)
                                    .putAllProperties(storedMessage.properties)
                                    .build();

                            ReceiveMessageResponse response = ReceiveMessageResponse.newBuilder()
                                    .setStatus(Status.newBuilder()
                                            .setCode(Code.OK)
                                            .setMessage("接收成功")
                                            .build())
                                    .setMessage(message)
                                    .build();

                            // 发送响应
                            responseObserver.onNext(response);
                            sentCount++;

                            System.out.println("[ReceiveMessage] 发送消息: messageId=" + storedMessage.messageId);

                            // 模拟处理延迟
                            Thread.sleep(500);
                        }
                    }

                    // 4. 完成流
                    responseObserver.onCompleted();
                    System.out.println("[ReceiveMessage] 完成, 共发送 " + sentCount + " 条消息");
                } catch (Exception e) {
                    System.err.println("[ReceiveMessage] 处理失败: " + e.getMessage());
                    responseObserver.onError(e);
                }
            });
        }

        /**
         * 查询路由 (一元 RPC)
         */
        @Override
        public void queryRoute(QueryRouteRequest request,
                               StreamObserver<QueryRouteResponse> responseObserver) {
            System.out.println("[QueryRoute] 收到请求: topic=" + request.getTopic());

            // 使用路由线程池处理
            routeThreadPool.submit(() -> {
                try {
                    // 模拟路由信息
                    Broker broker1 = Broker.newBuilder()
                            .setName("broker-a")
                            .setAddress("192.168.1.100:10911")
                            .addQueues(0)
                            .addQueues(1)
                            .addQueues(2)
                            .addQueues(3)
                            .build();

                    Broker broker2 = Broker.newBuilder()
                            .setName("broker-b")
                            .setAddress("192.168.1.101:10911")
                            .addQueues(0)
                            .addQueues(1)
                            .addQueues(2)
                            .addQueues(3)
                            .build();

                    QueryRouteResponse response = QueryRouteResponse.newBuilder()
                            .setStatus(Status.newBuilder()
                                    .setCode(Code.OK)
                                    .setMessage("查询成功")
                                    .build())
                            .addBrokers(broker1)
                            .addBrokers(broker2)
                            .build();

                    responseObserver.onNext(response);
                    responseObserver.onCompleted();

                    System.out.println("[QueryRoute] 返回 2 个 Broker");
                } catch (Exception e) {
                    System.err.println("[QueryRoute] 处理失败: " + e.getMessage());
                    responseObserver.onError(e);
                }
            });
        }
    }

    /**
     * 存储的消息
     */
    static class StoredMessage {
        final String messageId;               // 消息 ID
        final String topic;                   // 主题
        final String tag;                     // 标签
        final ByteString body;                // 消息体
        final long bornTimestamp;             // 产生时间
        final Map<String, String> properties; // 自定义属性

        StoredMessage(String messageId, String topic, String tag, ByteString body,
                      long bornTimestamp, Map<String, String> properties) {
            this.messageId = messageId;
            this.topic = topic;
            this.tag = tag;
            this.body = body;
            this.bornTimestamp = bornTimestamp;
            this.properties = properties;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final MessagingServer server = new MessagingServer();
        server.start();
        server.blockUntilShutdown();
    }
}
