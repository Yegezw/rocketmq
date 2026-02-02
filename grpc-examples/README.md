# RocketMQ Proxy gRPC 学习示例

从零开始学习 gRPC，理解 RocketMQ Proxy 模块的实现原理。

## 快速开始

```bash
cd example1-helloworld
mvn clean compile

# 终端 1：启动服务端
mvn exec:java -Dexec.mainClass="com.example.helloworld.HelloWorldServer"

# 终端 2：运行客户端
mvn exec:java -Dexec.mainClass="com.example.helloworld.HelloWorldClient"
```

## 学习路径

```
Example 1: Hello World (2-3小时)
  ↓ gRPC 基础、一元RPC、服务端流

Example 2: 消息服务 (3-4小时)
  ↓ 线程池设计、模拟 RocketMQ 消息服务

Example 3: 拦截器 (2-3小时)
  ↓ 认证授权、理解 Proxy 的 Pipeline

阅读 Proxy 源码 (1-2周)
```

## 三个示例

### Example 1: Hello World
- **学习内容：** gRPC 基础、Protobuf 语法、StreamObserver
- **对应 Proxy：** gRPC 服务的基本结构

### Example 2: 消息服务
- **学习内容：** 线程池设计、消息发送接收
- **对应 Proxy：** `GrpcMessagingApplication`、`SendMessageActivity`、`ReceiveMessageActivity`

### Example 3: 拦截器
- **学习内容：** 拦截器机制、认证授权、Context 传递
- **对应 Proxy：** `RequestPipeline`、`AuthenticationPipeline`、`AuthorizationPipeline`

## 参考文档

- **QUICK_REFERENCE.md** - gRPC 语法和 API 速查

## 阅读 Proxy 源码

完成示例后，按顺序阅读：

1. `GrpcMessagingApplication.java` - 服务入口、线程池
2. `RequestPipeline.java` - Pipeline 机制
3. `SendMessageActivity.java` - 发送消息
4. `ReceiveMessageActivity.java` - 接收消息
5. `MessagingProcessor.java` - 业务逻辑

## 常见问题

**编译失败？** `mvn clean compile`

**端口被占用？** 修改代码中的端口号

**连接失败？** 确保服务端已启动

## 外部资源

- [gRPC 官方文档](https://grpc.io/docs/)
- [RocketMQ 官方文档](https://rocketmq.apache.org/docs/)
