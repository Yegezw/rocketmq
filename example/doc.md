Example 模块推荐阅读顺序
- 第 1-2 阶段 打基础，理解核心 API 和发送模式
- 第 3 阶段 理解消费模式差异（Push vs Pull vs Pop）
- 第 4-5 阶段 按实际需求选择性阅读
- 第 6-7 阶段 生产环境实践时参考

📘 第一阶段：基础入门（必读）
1. quickstart/ - 快速开始
   - Producer.java - 同步发送消息，带详细注释，展示三种发送方式对比
   - Consumer.java - Push 模式消费，注册 MessageListener 并发消费

📗 第二阶段：生产者模式（核心）
2. simple/ - 生产者发送模式
   - Producer.java - 同步发送（基础版，无详细注释）
   - AsyncProducer.java - 异步发送 + SendCallback 回调，使用 CountDownLatch 控制
   - OnewayProducer.java - 单向发送（不关心结果，最快但不可靠）
3. ordermessage/ - 顺序消息
   - Producer.java - 使用 MessageQueueSelector 实现顺序发送（同一 orderId 路由到同一队列）
   - Consumer.java - MessageListenerOrderly 实现顺序消费
4. transaction/ - 事务消息
   - TransactionProducer.java - TransactionMQProducer + ExecutorService 线程池
   - TransactionListenerImpl.java - 实现本地事务执行和回查逻辑
5. schedule/ - 延迟/定时消息
   - ScheduledMessageProducer.java - setDelayTimeLevel 实现延迟投递
   - ScheduledMessageConsumer.java - 消费延迟消息
   - TimerMessageProducer.java - 精确定时投递（新特性）
   - TimerMessageConsumer.java - 消费定时消息

📙 第三阶段：消费者模式
6. simple/ - 消费者模式
   - PushConsumer.java - Push 模式，Broker 主动推送
   - PullConsumer.java - Pull 模式，客户端主动拉取
   - LitePullConsumerSubscribe.java - 轻量级 Pull（订阅模式）
   - LitePullConsumerAssign.java - 轻量级 Pull（指定队列）
   - PopConsumer.java - Pop 模式（新特性，无需维护消费位点）
7. broadcast/ - 广播消费
   - PushConsumer.java - setMessageModel(BROADCASTING) 实现集群内所有消费者都收到

📕 第四阶段：过滤与批量
8. filter/ - 消息过滤
   - TagFilterProducer.java / TagFilterConsumer.java - Tag 过滤（基础）
   - SqlFilterProducer.java / SqlFilterConsumer.java - SQL92 语法过滤（高级）
9. batch/ - 批量消息
   - SimpleBatchProducer.java - 批量发送（总大小 < 4MB）
   - SplitBatchProducer.java - 自动分割批量消息（处理超大批次）

📔 第五阶段：高级特性
10. rpc/ - 请求-响应模式
    - RequestProducer.java - 同步 RPC 请求
    - AsyncRequestProducer.java - 异步 RPC 请求
    - ResponseConsumer.java - 响应消息处理
11. namespace/ - 多租户
    - ProducerWithNamespace.java - 设置命名空间隔离
    - PushConsumerWithNamespace.java - 消费带命名空间的消息
    - PullConsumerWithNamespace.java - Pull 模式 + 命名空间
12. lmq/ - 轻量队列（Light Message Queue）
    - LMQProducer.java - 发送到轻量队列
    - LMQPushConsumer.java - Push 消费轻量队列
    - LMQPullConsumer.java - Pull 消费轻量队列
    - LMQPushPopConsumer.java - Pop 消费轻量队列

📓 第六阶段：运维与监控
13. tracemessage/ - 消息追踪
    - TraceProducer.java / TracePushConsumer.java - 开启消息轨迹
    - OpenTracingProducer.java / OpenTracingPushConsumer.java - 集成 OpenTracing
    - OpenTracingTransactionProducer.java - 事务消息追踪
14. benchmark/ - 性能测试
    - Producer.java / Consumer.java - 性能基准测试
    - BatchProducer.java - 批量性能测试
    - TransactionProducer.java - 事务性能测试
    - timer/ - 定时消息性能测试
    - AclClient.java - ACL 权限测试

📒 第七阶段：特殊场景（选读）
15. operation/ - 运维操作示例
    - Producer.java / Consumer.java - 生产环境运维示例
16. openmessaging/ - OpenMessaging 标准
    - SimpleProducer.java / SimplePushConsumer.java / SimplePullConsumer.java - 支持 OpenMessaging 规范
17. simple/ - 其他工具类
    - AclClient.java - ACL 权限控制
    - PullScheduleService.java - 定时 Pull 服务
    - RandomAsyncCommit.java - 随机异步提交示例
    - CachedQueue.java - 队列缓存示例