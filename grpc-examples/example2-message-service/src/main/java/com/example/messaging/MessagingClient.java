package com.example.messaging;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 消息客户端 (模拟 RocketMQ Producer 和 Consumer)
 */
public class MessagingClient {

    private final ManagedChannel channel;
    private final MessagingServiceGrpc.MessagingServiceBlockingStub blockingStub;

    public MessagingClient(String host, int port) {
        this.channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();

        this.blockingStub = MessagingServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * 发送消息
     */
    public void sendMessage(String topic, String tag, String body) {
        System.out.println("\n========== 发送消息 ==========");
        System.out.println("Topic: " + topic);
        System.out.println("Tag: " + tag);
        System.out.println("Body: " + body);

        // 构建请求
        Map<String, String> properties = new HashMap<>();
        properties.put("key1", "value1");
        properties.put("key2", "value2");

        SendMessageRequest request = SendMessageRequest.newBuilder()
                .setTopic(topic)
                .setTag(tag)
                .setBody(ByteString.copyFromUtf8(body))
                .putAllProperties(properties)
                .build();

        // 调用远程方法
        try {
            SendMessageResponse response = blockingStub.sendMessage(request);

            System.out.println("响应状态: " + response.getStatus().getCode());
            System.out.println("响应消息: " + response.getStatus().getMessage());
            if (response.getStatus().getCode() == Code.OK) {
                System.out.println("消息 ID: " + response.getMessageId());
                System.out.println("队列偏移量: " + response.getQueueOffset());
            }
        } catch (Exception e) {
            System.err.println("发送失败: " + e.getMessage());
        }
    }

    /**
     * 接收消息 (服务端流)
     */
    public void receiveMessage(String topic, String consumerGroup, int maxMessages) {
        System.out.println("\n========== 接收消息 ==========");
        System.out.println("Topic: " + topic);
        System.out.println("Consumer Group: " + consumerGroup);
        System.out.println("Max Messages: " + maxMessages);

        // 构建请求
        ReceiveMessageRequest request = ReceiveMessageRequest.newBuilder()
                .setTopic(topic)
                .setConsumerGroup(consumerGroup)
                .setMaxMessages(maxMessages)
                .build();

        // 调用远程方法，接收流式响应
        try {
            Iterator<ReceiveMessageResponse> responses = blockingStub.receiveMessage(request);

            int count = 0;
            while (responses.hasNext()) {
                ReceiveMessageResponse response = responses.next();

                if (response.getStatus().getCode() == Code.OK) {
                    Message message = response.getMessage();
                    count++;

                    System.out.println("\n--- 消息 " + count + " ---");
                    System.out.println("消息 ID: " + message.getMessageId());
                    System.out.println("Topic: " + message.getTopic());
                    System.out.println("Tag: " + message.getTag());
                    System.out.println("Body: " + message.getBody().toStringUtf8());
                    System.out.println("时间戳: " + message.getBornTimestamp());
                    System.out.println("属性: " + message.getPropertiesMap());
                }
            }

            System.out.println("\n共接收 " + count + " 条消息");

        } catch (Exception e) {
            System.err.println("接收失败: " + e.getMessage());
        }
    }

    /**
     * 查询路由
     */
    public void queryRoute(String topic) {
        System.out.println("\n========== 查询路由 ==========");
        System.out.println("Topic: " + topic);

        // 构建请求
        QueryRouteRequest request = QueryRouteRequest.newBuilder()
                .setTopic(topic)
                .build();

        // 调用远程方法
        try {
            QueryRouteResponse response = blockingStub.queryRoute(request);

            System.out.println("响应状态: " + response.getStatus().getCode());
            System.out.println("响应消息: " + response.getStatus().getMessage());

            if (response.getStatus().getCode() == Code.OK) {
                System.out.println("\nBroker 列表:");
                for (Broker broker : response.getBrokersList()) {
                    System.out.println("  - 名称: " + broker.getName());
                    System.out.println("    地址: " + broker.getAddress());
                    System.out.println("    队列: " + broker.getQueuesList());
                }
            }
        } catch (Exception e) {
            System.err.println("查询失败: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws InterruptedException {
        MessagingClient client = new MessagingClient("localhost", 50052);

        try {
            // 1. 查询路由
            client.queryRoute("TestTopic");

            Thread.sleep(1000);

            // 2. 发送多条消息
            for (int i = 1; i <= 5; i++) {
                client.sendMessage(
                        "TestTopic",
                        "TagA",
                        "这是第 " + i + " 条测试消息"
                );
                Thread.sleep(500);
            }

            Thread.sleep(2000);

            // 3. 接收消息
            client.receiveMessage("TestTopic", "ConsumerGroup1", 10);
        } finally {
            client.shutdown();
        }
    }
}
