#!/bin/bash

echo "停止 RocketMQ 服务 ..."

docker stop rmqdashboard rmqbroker rmqnamesrv 2>/dev/null
docker rm rmqdashboard rmqbroker rmqnamesrv 2>/dev/null

echo "RocketMQ 已停止"
echo ""
echo "如需删除网络，执行: docker network rm rocketmq"
