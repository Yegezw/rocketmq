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

/**
 * RPC 客户端 Hook 抽象类<br>
 * 支持在请求前后注入自定义逻辑
 */
public abstract class RpcClientHook {

    //if the return is not null, return it
    // 返回值非空时直接返回该响应
    /**
     * 请求发送前 Hook
     *
     * @param rpcRequest RPC 请求
     * @return 可短路返回的响应
     * @throws RpcException RPC 异常
     */
    public abstract RpcResponse beforeRequest(RpcRequest rpcRequest) throws RpcException;

    //if the return is not null, return it
    // 返回值非空时直接返回该响应
    /**
     * 请求响应后 Hook
     *
     * @param rpcResponse RPC 响应
     * @return 可替换返回的响应
     * @throws RpcException RPC 异常
     */
    public abstract RpcResponse afterResponse(RpcResponse rpcResponse) throws RpcException;

}
