# gRPC 快速参考指南

这是一个快速参考指南，包含了学习 RocketMQ Proxy 时常用的 gRPC 概念和代码片段。

## 目录

1. [Protobuf 语法](#protobuf-语法)
2. [gRPC 服务类型](#grpc-服务类型)
3. [Java 代码模板](#java-代码模板)
4. [常用 API](#常用-api)
5. [最佳实践](#最佳实践)
6. [常见错误](#常见错误)

---

## Protobuf 语法

### 基本类型

| Protobuf 类型 | Java 类型      | 说明         |
|-------------|--------------|------------|
| `double`    | `double`     | 双精度浮点数     |
| `float`     | `float`      | 单精度浮点数     |
| `int32`     | `int`        | 32 位整数     |
| `int64`     | `long`       | 64 位整数     |
| `bool`      | `boolean`    | 布尔值        |
| `string`    | `String`     | 字符串（UTF-8） |
| `bytes`     | `ByteString` | 二进制数据      |

### 消息定义

```protobuf
syntax = "proto3";

package example;

option java_package = "com.example";
option java_multiple_files = true;

// 简单消息
message Person {
  string name = 1;
  int32 age = 2;
  string email = 3;
}

// 嵌套消息
message Company {
  string name = 1;
  repeated Person employees = 2;  // 列表
}

// 枚举
enum Status {
  UNKNOWN = 0;
  ACTIVE = 1;
  INACTIVE = 2;
}

// Map
message Config {
  map<string, string> settings = 1;
}
```

### 服务定义

```protobuf
service MyService {
  // 一元 RPC
  rpc UnaryCall (Request) returns (Response) {}

  // 服务端流
  rpc ServerStream (Request) returns (stream Response) {}

  // 客户端流
  rpc ClientStream (stream Request) returns (Response) {}

  // 双向流
  rpc BidirectionalStream (stream Request) returns (stream Response) {}
}
```

---

## gRPC 服务类型

### 1. 一元 RPC (Unary RPC)

**特点：** 客户端发送一个请求，服务端返回一个响应

**服务端实现：**
```java
@Override
public void unaryCall(Request request, StreamObserver<Response> responseObserver) {
    // 处理请求
    Response response = processRequest(request);

    // 发送响应
    responseObserver.onNext(response);
    responseObserver.onCompleted();
}
```

**客户端调用：**
```java
// 阻塞调用
Response response = blockingStub.unaryCall(request);

// 异步调用
asyncStub.unaryCall(request, new StreamObserver<Response>() {
    @Override
    public void onNext(Response response) {
        // 处理响应
    }

    @Override
    public void onError(Throwable t) {
        // 处理错误
    }

    @Override
    public void onCompleted() {
        // 完成
    }
});
```

### 2. 服务端流 (Server Streaming)

**特点：** 客户端发送一个请求，服务端返回多个响应

**服务端实现：**
```java
@Override
public void serverStream(Request request, StreamObserver<Response> responseObserver) {
    // 发送多个响应
    for (int i = 0; i < 10; i++) {
        Response response = createResponse(i);
        responseObserver.onNext(response);
    }

    // 完成流
    responseObserver.onCompleted();
}
```

**客户端调用：**
```java
// 阻塞调用
Iterator<Response> responses = blockingStub.serverStream(request);
while (responses.hasNext()) {
    Response response = responses.next();
    // 处理响应
}

// 异步调用
asyncStub.serverStream(request, new StreamObserver<Response>() {
    @Override
    public void onNext(Response response) {
        // 处理每个响应
    }

    @Override
    public void onError(Throwable t) {
        // 处理错误
    }

    @Override
    public void onCompleted() {
        // 流结束
    }
});
```

### 3. 客户端流 (Client Streaming)

**特点：** 客户端发送多个请求，服务端返回一个响应

**服务端实现：**
```java
@Override
public StreamObserver<Request> clientStream(StreamObserver<Response> responseObserver) {
    return new StreamObserver<Request>() {
        private List<Request> requests = new ArrayList<>();

        @Override
        public void onNext(Request request) {
            // 接收客户端发送的请求
            requests.add(request);
        }

        @Override
        public void onError(Throwable t) {
            // 处理错误
        }

        @Override
        public void onCompleted() {
            // 客户端发送完毕，返回响应
            Response response = processRequests(requests);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    };
}
```

**客户端调用：**
```java
StreamObserver<Response> responseObserver = new StreamObserver<Response>() {
    @Override
    public void onNext(Response response) {
        // 处理响应
    }

    @Override
    public void onError(Throwable t) {
        // 处理错误
    }

    @Override
    public void onCompleted() {
        // 完成
    }
};

StreamObserver<Request> requestObserver = asyncStub.clientStream(responseObserver);

// 发送多个请求
for (int i = 0; i < 10; i++) {
    requestObserver.onNext(createRequest(i));
}

// 完成发送
requestObserver.onCompleted();
```

### 4. 双向流 (Bidirectional Streaming)

**特点：** 客户端和服务端都可以发送多个消息

**服务端实现：**
```java
@Override
public StreamObserver<Request> bidirectionalStream(StreamObserver<Response> responseObserver) {
    return new StreamObserver<Request>() {
        @Override
        public void onNext(Request request) {
            // 接收请求并立即响应
            Response response = processRequest(request);
            responseObserver.onNext(response);
        }

        @Override
        public void onError(Throwable t) {
            // 处理错误
        }

        @Override
        public void onCompleted() {
            // 客户端完成发送
            responseObserver.onCompleted();
        }
    };
}
```

**客户端调用：**
```java
StreamObserver<Response> responseObserver = new StreamObserver<Response>() {
    @Override
    public void onNext(Response response) {
        // 处理响应
    }

    @Override
    public void onError(Throwable t) {
        // 处理错误
    }

    @Override
    public void onCompleted() {
        // 服务端完成发送
    }
};

StreamObserver<Request> requestObserver = asyncStub.bidirectionalStream(responseObserver);

// 发送多个请求
for (int i = 0; i < 10; i++) {
    requestObserver.onNext(createRequest(i));
}

// 完成发送
requestObserver.onCompleted();
```

---

## Java 代码模板

### 服务端模板

```java
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class MyServer {
    private Server server;

    public void start() throws IOException {
        server = ServerBuilder.forPort(50051)
                .addService(new MyServiceImpl())
                .build()
                .start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            MyServer.this.stop();
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

    static class MyServiceImpl extends MyServiceGrpc.MyServiceImplBase {
        @Override
        public void myMethod(Request request, StreamObserver<Response> responseObserver) {
            // 实现逻辑
            Response response = Response.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    public static void main(String[] args) throws Exception {
        MyServer server = new MyServer();
        server.start();
        server.blockUntilShutdown();
    }
}
```

### 客户端模板

```java
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class MyClient {
    private final ManagedChannel channel;
    private final MyServiceGrpc.MyServiceBlockingStub blockingStub;

    public MyClient(String host, int port) {
        this.channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();

        this.blockingStub = MyServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void callMethod() {
        Request request = Request.newBuilder().build();
        Response response = blockingStub.myMethod(request);
        // 处理响应
    }

    public static void main(String[] args) throws Exception {
        MyClient client = new MyClient("localhost", 50051);
        try {
            client.callMethod();
        } finally {
            client.shutdown();
        }
    }
}
```

---

## 常用 API

### 构建消息

```java
// 简单消息
Person person = Person.newBuilder()
        .setName("张三")
        .setAge(30)
        .setEmail("zhangsan@example.com")
        .build();

// 嵌套消息
Company company = Company.newBuilder()
        .setName("示例公司")
        .addEmployees(person)
        .build();

// Map
Config config = Config.newBuilder()
        .putSettings("key1", "value1")
        .putSettings("key2", "value2")
        .build();

// Bytes
ByteString bytes = ByteString.copyFromUtf8("Hello");
Message message = Message.newBuilder()
        .setBody(bytes)
        .build();
```

### 读取消息

```java
String name = person.getName();
int age = person.getAge();
List<Person> employees = company.getEmployeesList();
Map<String, String> settings = config.getSettingsMap();
String body = message.getBody().toStringUtf8();
```

### 通道配置

```java
ManagedChannel channel = ManagedChannelBuilder
        .forAddress("localhost", 50051)
        .usePlaintext()                          // 不使用 TLS
        .maxInboundMessageSize(10 * 1024 * 1024) // 最大消息大小 10MB
        .keepAliveTime(30, TimeUnit.SECONDS)     // 保活时间
        .build();
```

### 服务器配置

```java
Server server = ServerBuilder.forPort(50051)
        .addService(new MyServiceImpl())
        .maxInboundMessageSize(10 * 1024 * 1024)    // 最大消息大小
        .executor(Executors.newFixedThreadPool(10)) // 自定义线程池
        .build();
```

---

## 最佳实践

### 1. 使用线程池

**不要在 gRPC 的 EventLoop 中执行耗时操作：**

```java
// ❌ 错误：阻塞 EventLoop
@Override
public void myMethod(Request request, StreamObserver<Response> responseObserver) {
    // 耗时操作
    Response response = doHeavyWork(request);
    responseObserver.onNext(response);
    responseObserver.onCompleted();
}

// ✅ 正确：使用线程池
private final ExecutorService executor = Executors.newFixedThreadPool(10);

@Override
public void myMethod(Request request, StreamObserver<Response> responseObserver) {
    executor.submit(() -> {
        try {
            Response response = doHeavyWork(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    });
}
```

### 2. 错误处理

```java
@Override
public void myMethod(Request request, StreamObserver<Response> responseObserver) {
    try {
        // 验证请求
        if (request.getName().isEmpty()) {
            responseObserver.onError(
                Status.INVALID_ARGUMENT
                    .withDescription("name 不能为空")
                    .asRuntimeException()
            );
            return;
        }

        // 处理请求
        Response response = processRequest(request);
        responseObserver.onNext(response);
        responseObserver.onCompleted();

    } catch (Exception e) {
        responseObserver.onError(
            Status.INTERNAL
                .withDescription("内部错误: " + e.getMessage())
                .withCause(e)
                .asRuntimeException()
        );
    }
}
```

### 3. 资源清理

```java
// 服务端
@Override
public void shutdown() {
    if (server != null) {
        server.shutdown();
    }
    if (executor != null) {
        executor.shutdown();
        try {
            executor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }
}

// 客户端
public void close() {
    if (channel != null) {
        channel.shutdown();
        try {
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            channel.shutdownNow();
        }
    }
}
```

### 4. 超时设置

```java
// 客户端设置超时
Response response = blockingStub
        .withDeadlineAfter(5, TimeUnit.SECONDS)
        .myMethod(request);
```

---

## 常见错误

### 1. UNAVAILABLE: io exception

**原因：** 服务端未启动或网络不通

**解决：**
- 确保服务端已启动
- 检查防火墙设置
- 检查端口是否正确

### 2. CANCELLED: call already cancelled

**原因：** 客户端取消了请求

**解决：**
- 检查客户端是否设置了超时
- 检查是否手动取消了请求

### 3. RESOURCE_EXHAUSTED: grpc: received message larger than max

**原因：** 消息大小超过限制

**解决：**
```java
// 增加最大消息大小
ManagedChannel channel = ManagedChannelBuilder
        .forAddress("localhost", 50051)
        .maxInboundMessageSize(100 * 1024 * 1024)  // 100MB
        .build();
```

### 4. DEADLINE_EXCEEDED: deadline exceeded

**原因：** 请求超时

**解决：**
- 增加超时时间
- 优化服务端处理速度

### 5. StreamObserver 线程安全问题

**错误示例：**
```java
// ❌ 多线程调用 StreamObserver
executor.submit(() -> responseObserver.onNext(response1));
executor.submit(() -> responseObserver.onNext(response2));
```

**正确示例：**
```java
// ✅ 使用同步
synchronized (responseObserver) {
    responseObserver.onNext(response);
}
```

---

## RocketMQ Proxy 中的使用

### 线程池设计

```java
// GrpcMessagingApplication.java
protected ThreadPoolExecutor producerThreadPoolExecutor;
protected ThreadPoolExecutor consumerThreadPoolExecutor;
protected ThreadPoolExecutor routeThreadPoolExecutor;
```

### 请求处理

```java
// 使用线程池异步处理
addExecutor(
    producerThreadPoolExecutor,
    context,
    request,
    () -> {
        // 业务逻辑
    },
    responseObserver,
    statusResponseCreator
);
```

### 错误处理

```java
// 统一的错误处理
protected Status convertExceptionToStatus(Throwable t) {
    return ResponseBuilder.getInstance().buildStatus(t);
}
```

---

## gRPC 拦截器 (Interceptor)

### 什么是拦截器

拦截器是 gRPC 的一个强大特性，允许你在请求到达服务实现之前或响应返回客户端之前，插入自定义逻辑。它类似于 Web 框架中的中间件或过滤器。

**拦截器的作用：**
- **认证与授权**: 验证客户端身份和访问权限
- **日志记录**: 记录请求/响应信息、耗时等
- **性能监控**: 统计调用次数、响应时间
- **错误处理**: 统一处理异常和错误转换
- **限流熔断**: 实现流量控制和熔断降级
- **Metadata 处理**: 自动添加或修改请求头信息

### 拦截器类型

#### 1. 服务端拦截器 (ServerInterceptor)

**接口定义：**
```java
public interface ServerInterceptor {
    <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> call,
        Metadata headers,
        ServerCallHandler<ReqT, RespT> next
    );
}
```

**简单示例：**
```java
public class MyServerInterceptor implements ServerInterceptor {
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        // 前置处理：在请求到达服务之前执行
        System.out.println("收到请求: " + call.getMethodDescriptor().getFullMethodName());

        // 继续处理请求
        return next.startCall(call, headers);
    }
}
```

**添加拦截器到服务端：**
```java
Server server = ServerBuilder.forPort(50051)
        .addService(ServerInterceptors.intercept(
                new MyServiceImpl(),
                new MyServerInterceptor()
        ))
        .build()
        .start();
```

#### 2. 客户端拦截器 (ClientInterceptor)

**接口定义：**
```java
public interface ClientInterceptor {
    <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method,
        CallOptions callOptions,
        Channel next
    );
}
```

**简单示例：**
```java
public class MyClientInterceptor implements ClientInterceptor {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions,
            Channel next) {

        System.out.println("发送请求: " + method.getFullMethodName());

        return next.newCall(method, callOptions);
    }
}
```

**添加拦截器到客户端：**
```java
ManagedChannel channel = ManagedChannelBuilder
        .forAddress("localhost", 50051)
        .usePlaintext()
        .intercept(new MyClientInterceptor())
        .build();
```

### 完整示例：认证拦截器

#### 服务端认证拦截器

```java
public class AuthenticationInterceptor implements ServerInterceptor {

    // 定义 Context Key, 用于在拦截器之间传递信息
    public static final Context.Key<String> USER_KEY = Context.key("user");

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        // 1. 从 Metadata 中提取认证信息
        String username = headers.get(
            Metadata.Key.of("username", Metadata.ASCII_STRING_MARSHALLER)
        );
        String password = headers.get(
            Metadata.Key.of("password", Metadata.ASCII_STRING_MARSHALLER)
        );

        // 2. 验证认证信息
        if (username == null || password == null) {
            call.close(
                Status.UNAUTHENTICATED.withDescription("缺少认证信息"),
                new Metadata()
            );
            return new ServerCall.Listener<ReqT>() {};
        }

        if (!isValidUser(username, password)) {
            call.close(
                Status.UNAUTHENTICATED.withDescription("用户名或密码错误"),
                new Metadata()
            );
            return new ServerCall.Listener<ReqT>() {};
        }

        // 3. 将用户信息存入 Context, 供后续拦截器和服务使用
        Context context = Context.current().withValue(USER_KEY, username);

        // 4. 继续处理请求
        return Contexts.interceptCall(context, call, headers, next);
    }

    private boolean isValidUser(String username, String password) {
        // 验证逻辑
        return "admin".equals(username) && "password123".equals(password);
    }
}
```

**在服务中使用 Context 信息：**
```java
@Override
public void myMethod(Request request, StreamObserver<Response> responseObserver) {
    // 获取拦截器设置的用户信息
    String username = AuthenticationInterceptor.USER_KEY.get();
    System.out.println("当前用户: " + username);

    // 业务逻辑
    responseObserver.onNext(response);
    responseObserver.onCompleted();
}
```

#### 客户端认证拦截器

```java
public class ClientAuthInterceptor implements ClientInterceptor {

    private final String username;
    private final String password;

    public ClientAuthInterceptor(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions,
            Channel next) {

        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                next.newCall(method, callOptions)) {

            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                // 自动添加认证信息到 Metadata
                headers.put(
                    Metadata.Key.of("username", Metadata.ASCII_STRING_MARSHALLER),
                    username
                );
                headers.put(
                    Metadata.Key.of("password", Metadata.ASCII_STRING_MARSHALLER),
                    password
                );

                super.start(responseListener, headers);
            }
        };
    }
}
```

### 完整示例：日志拦截器

#### 服务端日志拦截器

```java
public class LoggingInterceptor implements ServerInterceptor {

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        String methodName = call.getMethodDescriptor().getFullMethodName();
        long startTime = System.currentTimeMillis();

        System.out.println("[LoggingInterceptor] 请求开始: " + methodName);

        // 包装 ServerCall, 以便在响应时记录日志
        ServerCall<ReqT, RespT> wrappedCall =
            new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {

            @Override
            public void close(Status status, Metadata trailers) {
                long duration = System.currentTimeMillis() - startTime;
                System.out.println("[LoggingInterceptor] 请求结束: " + methodName);
                System.out.println("[LoggingInterceptor] 状态: " + status.getCode());
                System.out.println("[LoggingInterceptor] 耗时: " + duration + " ms");

                super.close(status, trailers);
            }
        };

        // 包装 Listener, 以便记录接收到的消息
        ServerCall.Listener<ReqT> listener = next.startCall(wrappedCall, headers);

        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(listener) {
            @Override
            public void onMessage(ReqT message) {
                System.out.println("[LoggingInterceptor] 收到消息: " +
                    message.getClass().getSimpleName());
                super.onMessage(message);
            }

            @Override
            public void onComplete() {
                System.out.println("[LoggingInterceptor] 请求完成");
                super.onComplete();
            }
        };
    }
}
```

#### 客户端日志拦截器

```java
public class ClientLoggingInterceptor implements ClientInterceptor {

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions,
            Channel next) {

        System.out.println("[ClientLogging] 调用方法: " + method.getFullMethodName());

        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                next.newCall(method, callOptions)) {

            @Override
            public void sendMessage(ReqT message) {
                System.out.println("[ClientLogging] 发送消息: " +
                    message.getClass().getSimpleName());
                super.sendMessage(message);
            }
        };
    }
}
```

### 拦截器链与执行顺序

**服务端拦截器执行顺序：**

当添加多个拦截器时，**执行顺序是从后往前的**（类似栈结构）：

```java
Server server = ServerBuilder.forPort(50051)
        .addService(ServerInterceptors.intercept(
                service,
                new LoggingInterceptor(),        // 第三个执行
                new AuthenticationInterceptor(), // 第二个执行
                new AuthorizationInterceptor()   // 第一个执行
        ))
        .build();
```

**执行流程：**
```
请求 → AuthorizationInterceptor → AuthenticationInterceptor → LoggingInterceptor → Service
     ←                         ←                          ←                    ← 响应
```

**客户端拦截器执行顺序：**

客户端拦截器按照添加的顺序执行：

```java
ManagedChannel channel = ManagedChannelBuilder
        .forAddress("localhost", 50051)
        .intercept(
                new ClientLoggingInterceptor(),  // 第一个执行
                new ClientAuthInterceptor()      // 第二个执行
        )
        .build();
```

### 拦截器之间传递信息

使用 **Context** 在拦截器之间传递信息：

```java
// 定义 Context Key
public static final Context.Key<String> USER_KEY = Context.key("user");
public static final Context.Key<String> TRACE_ID_KEY = Context.key("traceId");

// 在拦截器 A 中设置值
Context context = Context.current()
        .withValue(USER_KEY, "admin")
        .withValue(TRACE_ID_KEY, "trace-123");
return Contexts.interceptCall(context, call, headers, next);

// 在拦截器 B 或服务中获取值
String username = USER_KEY.get();
String traceId = TRACE_ID_KEY.get();
```

### 使用场景对比

| 使用场景     | 拦截器类型   | 典型实现                 |
|----------|---------|----------------------|
| 统一认证     | 服务端     | 验证 token、用户名密码       |
| 自动添加认证信息 | 客户端     | 在 Metadata 中添加 token |
| 日志记录     | 服务端/客户端 | 记录请求、响应、耗时           |
| 权限验证     | 服务端     | 检查用户对资源的访问权限         |
| 性能监控     | 服务端/客户端 | 统计 QPS、耗时、错误率        |
| 链路追踪     | 服务端/客户端 | 传递 traceId、spanId    |
| 限流熔断     | 服务端     | 限制请求速率、实现熔断          |

### 最佳实践

#### 1. 拦截器职责单一

每个拦截器只做一件事：

```java
// ✅ 好的做法
new AuthenticationInterceptor()  // 只负责认证
new AuthorizationInterceptor()   // 只负责授权
new LoggingInterceptor()         // 只负责日志

// ❌ 不好的做法
new AllInOneInterceptor()        // 做了认证、授权、日志、监控...
```

#### 2. 注意拦截器顺序

将通用的、不依赖业务的拦截器放在前面：

```java
// ✅ 推荐顺序
ServerInterceptors.intercept(
    service,
    new LoggingInterceptor(),        // 日志（最外层）
    new MetricsInterceptor(),        // 监控
    new AuthenticationInterceptor(), // 认证
    new AuthorizationInterceptor()   // 授权（最内层）
)
```

#### 3. 正确处理异常

```java
@Override
public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(...) {
    try {
        // 拦截器逻辑
        return next.startCall(call, headers);
    } catch (Exception e) {
        call.close(
            Status.INTERNAL
                .withDescription("拦截器错误: " + e.getMessage())
                .withCause(e)
                .asRuntimeException(),
            new Metadata()
        );
        return new ServerCall.Listener<ReqT>() {};
    }
}
```

#### 4. 避免阻塞操作

不要在拦截器中执行耗时操作，如果必须执行，使用异步：

```java
// ❌ 错误：阻塞 I/O
String user = database.queryUser(userId); // 同步查询数据库

// ✅ 正确：使用缓存或异步
String user = cache.get(userId);          // 从缓存获取
```

### RocketMQ Proxy 中的拦截器应用

RocketMQ Proxy 使用拦截器实现：

1. **认证与授权**: `AuthenticationInterceptor`
   - 验证客户端 AccessKey 和 SecretKey
   - 检查 Topic 和 ConsumerGroup 的访问权限

2. **链路追踪**: `TracingInterceptor`
   - 提取或生成 TraceId
   - 将 TraceId 注入到 Context 中

3. **监控统计**: `MonitorInterceptor`
   - 统计请求 QPS、耗时
   - 记录错误率

4. **限流熔断**: `RateLimitInterceptor`
   - 基于 Token Bucket 算法限流
   - 实现熔断降级

**示例代码位置:**
- 拦截器实现: `grpc-examples/example3-interceptor/src/main/java/com/example/interceptor/`
- 服务端使用: `grpc-examples/example3-interceptor/src/main/java/com/example/SecureMessagingServer.java:40`
- 客户端使用: `grpc-examples/example3-interceptor/src/main/java/com/example/SecureMessagingClient.java:28`

---

## 参考资料

- [gRPC 官方文档](https://grpc.io/docs/)
- [Protocol Buffers 文档](https://protobuf.dev/)
- [gRPC Java 教程](https://grpc.io/docs/languages/java/)
- [gRPC Interceptor 文档](https://grpc.io/docs/guides/interceptors/)
- [RocketMQ Proxy 源码](../proxy/src/main/java/org/apache/rocketmq/proxy/grpc/)
- **Example3**: gRPC Interceptor 完整示例 (`grpc-examples/example3-interceptor/`)

---

**提示：** 将这个文件保存为书签，在学习和开发过程中随时查阅。
