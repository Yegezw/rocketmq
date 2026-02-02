# Example 1: Hello World

最简单的 gRPC 示例，学习基础概念。

## 运行

```bash
mvn clean compile

# 终端 1
mvn exec:java -Dexec.mainClass="com.example.helloworld.HelloWorldServer"

# 终端 2
mvn exec:java -Dexec.mainClass="com.example.helloworld.HelloWorldClient"
```

## 学习内容

### 1. Protobuf 定义
```protobuf
service Greeter {
  rpc SayHello (HelloRequest) returns (HelloReply) {}               // 一元 RPC
  rpc SayHelloStream (HelloRequest) returns (stream HelloReply) {}  // 服务端流
}
```

**关键点：**
- `rpc` 关键字定义一个 RPC 方法
- `stream` 关键字表示流式传输
- 每个方法都有请求类型和响应类型

### 2. 服务端实现
```java
class GreeterImpl extends GreeterGrpc.GreeterImplBase {
    @Override
    public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
        HelloReply reply = HelloReply.newBuilder().setMessage("你好, " + request.getName()).build();
        responseObserver.onNext(reply);      // 发送响应
        responseObserver.onCompleted();      // 完成
    }
}
```

### 3. 客户端调用
```java
HelloRequest request = HelloRequest.newBuilder().setName("张三").build();
HelloReply response = blockingStub.sayHello(request);
```

## 关键概念

## 关键概念

### StreamObserver

`StreamObserver` 是 gRPC 中用于异步处理响应的接口：

```java
public interface StreamObserver<V> {
    void onNext(V value);      // 发送一个值
    void onError(Throwable t); // 发送错误
    void onCompleted();        // 完成流
}
```

**使用场景：**
- 服务端：用于发送响应给客户端
- 客户端：用于接收服务端的流式响应

### Stub（存根）

gRPC 提供三种类型的存根：

1. **BlockingStub（阻塞存根）**
    - 同步调用
    - 适合简单的请求-响应场景
    - 示例：`GreeterGrpc.newBlockingStub(channel)`

2. **AsyncStub（异步存根）**
    - 异步调用
    - 使用回调处理响应
    - 示例：`GreeterGrpc.newStub(channel)`

3. **FutureStub（Future 存根）**
    - 返回 `ListenableFuture`
    - 适合一元 RPC
    - 示例：`GreeterGrpc.newFutureStub(channel)`

## 练习

1. 添加新的 RPC 方法
2. 实现客户端流 RPC
3. 实现双向流 RPC
4. 添加错误处理

## 对应 Proxy

- 服务定义 → `MessagingServiceGrpc`
- 服务实现 → `GrpcMessagingApplication`
- StreamObserver → 所有 Activity 类
