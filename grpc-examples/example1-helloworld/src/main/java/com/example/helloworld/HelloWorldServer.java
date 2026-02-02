package com.example.helloworld;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * gRPC 服务端示例
 * <p>
 * 这个类展示了如何实现一个简单的 gRPC 服务端
 */
public class HelloWorldServer {

    private Server server;

    /**
     * 启动 gRPC 服务器
     */
    public void start() throws IOException {
        // 创建服务器, 绑定端口和服务实现
        int port = 50051;
        server = ServerBuilder.forPort(port)
                .addService(new GreeterImpl()) // 添加服务实现
                .build()
                .start();

        System.out.println("服务器已启动, 监听端口: " + port);

        // 添加 JVM 关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("*** JVM 正在关闭, 停止 gRPC 服务器 ***");
            try {
                HelloWorldServer.this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
            System.err.println("*** 服务器已关闭 ***");
        }));
    }

    /**
     * 停止服务器
     */
    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * 阻塞等待, 直到服务器关闭
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Greeter 服务的实现类
     * <p>
     * 继承自 protobuf 生成的 GreeterGrpc.GreeterImplBase
     */
    static class GreeterImpl extends GreeterGrpc.GreeterImplBase {

        /**
         * 实现 SayHello 方法 (一元 RPC)
         *
         * @param request          请求对象
         * @param responseObserver 响应观察者, 用于发送响应
         */
        @Override
        public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
            System.out.println("收到请求: name = " + request.getName());

            // 构建响应消息
            HelloReply reply = HelloReply.newBuilder()
                    .setMessage("你好, " + request.getName() + "!")
                    .build();

            // 发送响应
            responseObserver.onNext(reply);

            // 完成响应
            responseObserver.onCompleted();
        }

        /**
         * 实现 SayHelloStream 方法 (服务端流 RPC)
         * <p>
         * 这个方法展示了如何使用服务端流<br>
         * 1. 接收一个请求<br>
         * 2. 返回多个响应
         *
         * @param request          请求对象
         * @param responseObserver 响应观察者
         */
        @Override
        public void sayHelloStream(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
            System.out.println("收到流式请求: name = " + request.getName());

            // 发送多个响应
            for (int i = 1; i <= 5; i++) {
                HelloReply reply = HelloReply.newBuilder()
                        .setMessage("你好, " + request.getName() + "! 这是第 " + i + " 条消息")
                        .build();

                // 发送响应
                responseObserver.onNext(reply);

                // 模拟处理延迟
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
            }

            // 完成响应流
            responseObserver.onCompleted();
            System.out.println("流式响应已完成");
        }
    }

    /**
     * 主函数
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        final HelloWorldServer server = new HelloWorldServer();
        server.start();
        server.blockUntilShutdown();
    }
}
