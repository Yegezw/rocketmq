#!/bin/bash

# 自动获取本机 IP (排除 127.0.0.1)
HOST_IP=$(ifconfig | grep "inet " | grep -v 127.0.0.1 | awk '{print $2}' | head -1)

echo "检测到本机 IP: $HOST_IP"

# 停止并删除旧容器
echo "清理旧容器 ..."
docker stop rmqbroker rmqnamesrv rmqdashboard 2>/dev/null
docker rm rmqbroker rmqnamesrv rmqdashboard 2>/dev/null

# 创建网络 (如果不存在)
docker network inspect rocketmq >/dev/null 2>&1 || docker network create rocketmq

# 启动 NameServer
echo "启动 NameServer ..."
docker run -d \
  --name rmqnamesrv \
  -p 9876:9876 \
  --network rocketmq \
  apache/rocketmq:5.3.2 \
  sh mqnamesrv

sleep 5

# 启动 Broker
echo "启动 Broker (IP: $HOST_IP) ..."
docker run -d \
  --name rmqbroker \
  --network rocketmq \
  -p 10912:10912 -p 10911:10911 -p 10909:10909 \
  -p 8080:8080 -p 8081:8081 \
  -e "NAMESRV_ADDR=rmqnamesrv:9876" \
  apache/rocketmq:5.3.2 \
  sh -c "echo 'brokerIP1=$HOST_IP' > /tmp/broker.conf && sh mqbroker --enable-proxy -c /tmp/broker.conf"

sleep 12

# 创建 Topic
echo "创建 Topic: TopicTest ..."
docker exec rmqbroker sh mqadmin updatetopic -t TopicTest -c DefaultCluster

# 启动 Dashboard
echo "启动 Dashboard ..."
docker run -d \
  --name rmqdashboard \
  --network rocketmq \
  -p 8088:8080 \
  -e "JAVA_OPTS=-Drocketmq.namesrv.addr=rmqnamesrv:9876" \
  apacherocketmq/rocketmq-dashboard:latest

sleep 8

echo ""
echo "======================================"
echo "RocketMQ 部署完成！"
echo "======================================"
echo "Broker IP: $HOST_IP"
echo "NameServer: 127.0.0.1:9876"
echo "Dashboard: http://localhost:8088"
echo ""
echo "查看容器状态: docker ps --filter name=rmq"
echo "停止服务: docker stop rmqbroker rmqnamesrv rmqdashboard"
echo "======================================"
