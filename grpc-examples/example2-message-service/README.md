# Example 2: 消息服务

模拟 RocketMQ Proxy 的核心功能，学习线程池设计。

## 运行

```bash
mvn clean compile

# 终端 1
mvn exec:java -Dexec.mainClass="com.example.messaging.MessagingServer"

# 终端 2
mvn exec:java -Dexec.mainClass="com.example.messaging.MessagingClient"
```

## 学习内容

### 1. 线程池设计
```java
// 模拟 Proxy 的多线程池设计
private final ExecutorService producerThreadPool;   // 处理发送消息
private final ExecutorService consumerThreadPool;   // 处理接收消息
private final ExecutorService routeThreadPool;      // 处理路由查询
```

**为什么需要多个线程池？**
- 隔离不同类型的请求
- 可以针对不同业务设置不同的线程数
- 便于监控和调优

### 2. 发送消息（一元 RPC）
```java
@Override
public void sendMessage(SendMessageRequest request, StreamObserver<SendMessageResponse> responseObserver) {
    producerThreadPool.submit(() -> {
        try {
            // 1. 验证请求
            // 2. 存储消息
            // 3. 构建响应
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    });
}
```

### 3. 接收消息（服务端流）
```java
@Override
public void receiveMessage(ReceiveMessageRequest request, StreamObserver<ReceiveMessageResponse> responseObserver) {
    consumerThreadPool.submit(() -> {
        // 循环发送多条消息
        while (hasMoreMessages) {
            responseObserver.onNext(response); // 发送一条消息
        }
        responseObserver.onCompleted();        // 完成流
    });
}
```

## 关键设计

**异步处理** - 避免阻塞 gRPC EventLoop
```java
// ❌ 错误：阻塞 EventLoop
public void sendMessage(...) {
    Response response = doHeavyWork(); // 耗时操作
    responseObserver.onNext(response);
}

// ✅ 正确：使用线程池
public void sendMessage(...) {
    threadPool.submit(() -> {
        Response response = doHeavyWork();
        responseObserver.onNext(response);
    });
}
```

## 练习

1. 添加消息确认（ACK）功能
2. 实现消息过滤（按 Tag）
3. 添加消息重试机制
4. 实现双向流心跳

## 对应 Proxy

| 本示例                  | Proxy 源码                                  |
|----------------------|-------------------------------------------|
| `MessagingServer`    | `GrpcMessagingApplication`                |
| `producerThreadPool` | `producerThreadPoolExecutor`              |
| `sendMessage()`      | `SendMessageActivity.sendMessage()`       |
| `receiveMessage()`   | `ReceiveMessageActivity.receiveMessage()` |
