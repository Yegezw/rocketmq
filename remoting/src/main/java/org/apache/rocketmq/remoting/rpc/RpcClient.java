/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.remoting.rpc;

import org.apache.rocketmq.common.message.MessageQueue;

import java.util.concurrent.Future;

/**
 * RPC 客户端接口
 */
public interface RpcClient {

    //common invoke paradigm, the logic remote addr is defined in "bname" field of request
    //For oneway request, the sign is labeled in request, and do not need an another method named "invokeOneway"
    //For one
    // 通用调用模式 逻辑地址在请求头 bname 字段中定义
    // 单向请求通过请求头标记 无需额外 invokeOneway 方法
    /**
     * 按请求头中的 broker 名称发起调用
     *
     * @param request RPC 请求
     * @param timeoutMs 超时时间 毫秒
     * @return RPC 响应 Future
     * @throws RpcException RPC 异常
     */
    Future<RpcResponse>  invoke(RpcRequest request, long timeoutMs) throws RpcException;

    //For rocketmq, most requests are corresponded to MessageQueue
    //And for LogicQueue, the broker name is mocked, the physical addr could only be defined by MessageQueue
    // 对于 RocketMQ 多数请求会绑定具体 MessageQueue
    // 对于逻辑队列 broker 名称为 mock 值 物理地址需由 MessageQueue 推导
    /**
     * 按消息队列发起调用
     *
     * @param mq 消息队列
     * @param request RPC 请求
     * @param timeoutMs 超时时间 毫秒
     * @return RPC 响应 Future
     * @throws RpcException RPC 异常
     */
    Future<RpcResponse>  invoke(MessageQueue mq, RpcRequest request, long timeoutMs) throws RpcException;

}
