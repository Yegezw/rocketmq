package com.example.helloworld;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * gRPC 客户端示例
 * <p>
 * 这个类展示了如何实现一个简单的 gRPC 客户端
 */
public class HelloWorldClient {

    private final ManagedChannel channel;
    private final GreeterGrpc.GreeterBlockingStub blockingStub;

    /**
     * 构造函数: 创建 gRPC 通道和存根
     *
     * @param host 服务器地址
     * @param port 服务器端口
     */
    public HelloWorldClient(String host, int port) {
        // 1. 创建通道 (Channel)
        // 通道是客户端与服务器之间的连接
        this.channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext() // 使用明文传输 (生产环境应使用 TLS)
                .build();

        // 2. 创建阻塞存根 (Blocking Stub)
        // 存根是客户端调用远程方法的代理对象
        this.blockingStub = GreeterGrpc.newBlockingStub(channel);
    }

    /**
     * 关闭通道
     */
    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * 调用 SayHello 方法 (一元 RPC)
     *
     * @param name 要问候的名字
     */
    public void greet(String name) {
        System.out.println("正在调用 SayHello, 参数: " + name);

        // 构建请求
        HelloRequest request = HelloRequest.newBuilder()
                .setName(name)
                .build();

        // 调用远程方法
        HelloReply response;
        try {
            response = blockingStub.sayHello(request);
            System.out.println("收到响应: " + response.getMessage());
        } catch (StatusRuntimeException e) {
            System.err.println("RPC 调用失败: " + e.getStatus());
        }
    }

    /**
     * 调用 SayHelloStream 方法 (服务端流 RPC)
     * <p>
     * 这个方法展示了如何接收服务端流<br>
     * 1. 发送一个请求<br>
     * 2. 接收多个响应
     *
     * @param name 要问候的名字
     */
    public void greetStream(String name) {
        System.out.println("正在调用 SayHelloStream, 参数: " + name);

        // 构建请求
        HelloRequest request = HelloRequest.newBuilder()
                .setName(name)
                .build();

        // 调用远程方法, 返回一个迭代器
        Iterator<HelloReply> responses;
        try {
            responses = blockingStub.sayHelloStream(request);

            // 遍历响应流
            while (responses.hasNext()) {
                HelloReply response = responses.next();
                System.out.println("收到流式响应: " + response.getMessage());
            }

            System.out.println("流式响应接收完成");
        } catch (StatusRuntimeException e) {
            System.err.println("RPC 调用失败: " + e.getStatus());
        }
    }

    /**
     * 主函数
     */
    public static void main(String[] args) throws InterruptedException {
        // 创建客户端
        HelloWorldClient client = new HelloWorldClient("localhost", 50051);

        try {
            // 测试一元 RPC
            System.out.println("========== 测试一元 RPC ==========");
            client.greet("张三");
            client.greet("李四");

            System.out.println("\n等待 2 秒...\n");
            Thread.sleep(2000);

            // 测试服务端流 RPC
            System.out.println("========== 测试服务端流 RPC ==========");
            client.greetStream("王五");
        } finally {
            // 关闭客户端
            client.shutdown();
        }
    }
}
