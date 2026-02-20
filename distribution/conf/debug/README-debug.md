# RocketMQ 5.3.3 本地单机调试配置说明

该目录用于本地源码调试（单机模式），覆盖以下场景：

- NameServer + Broker
- Proxy Cluster 模式
- Proxy Local 模式

## 配置文件

- namesrv-debug.properties
- broker-debug.conf
- rmq-proxy-cluster-debug.json
- rmq-proxy-local-debug.json

## 依赖预热与编译

```bash
mvn -pl namesrv,broker,proxy,example -am -DskipTests -Dspotbugs.skip=true -Dcheckstyle.skip=true -Djacoco.skip=true compile
```

## 启动顺序

### 流程 A（经典单机 + Proxy Cluster）

1. RMQ-Namesrv-Debug
2. RMQ-Broker-Debug
3. RMQ-Proxy-Cluster-Debug
4. RMQ-Example-Consumer
5. RMQ-Example-Producer

### 流程 B（单机 + Proxy Local）

1. RMQ-Namesrv-Debug
2. RMQ-Proxy-Local-Debug
3. RMQ-Example-Consumer
4. RMQ-Example-Producer

注意：流程 B 中不要再单独启动 RMQ-Broker-Debug，因为 Proxy Local 模式会在同一进程内启动 Broker。
