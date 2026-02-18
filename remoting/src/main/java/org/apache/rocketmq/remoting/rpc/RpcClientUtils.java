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

import java.nio.ByteBuffer;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * RPC 客户端工具类
 */
public class RpcClientUtils {

    /**
     * 根据 RPC 请求创建 remoting 命令
     *
     * @param rpcRequest RPC 请求
     * @return remoting 命令
     */
    public static RemotingCommand createCommandForRpcRequest(RpcRequest rpcRequest) {
        RemotingCommand cmd = RemotingCommand.createRequestCommand(rpcRequest.getCode(), rpcRequest.getHeader());
        cmd.setBody(encodeBody(rpcRequest.getBody()));
        return cmd;
    }

    /**
     * 根据 RPC 响应创建 remoting 命令
     *
     * @param rpcResponse RPC 响应
     * @return remoting 命令
     */
    public static RemotingCommand createCommandForRpcResponse(RpcResponse rpcResponse) {
        RemotingCommand cmd = RemotingCommand.createResponseCommandWithHeader(rpcResponse.getCode(), rpcResponse.getHeader());
        cmd.setRemark(rpcResponse.getException() == null ? "" : rpcResponse.getException().getMessage());
        cmd.setBody(encodeBody(rpcResponse.getBody()));
        return cmd;
    }

    /**
     * 编码响应体对象为字节数组
     *
     * @param body 响应体对象
     * @return 编码结果
     */
    public static byte[] encodeBody(Object body) {
        if (body == null) {
            return null;
        }
        if (body instanceof byte[]) {
            return (byte[])body;
        } else if (body instanceof RemotingSerializable) {
            return ((RemotingSerializable) body).encode();
        } else if (body instanceof ByteBuffer) {
            ByteBuffer buffer = (ByteBuffer)body;
            buffer.mark();
            byte[] data = new byte[buffer.remaining()];
            buffer.get(data);
            buffer.reset();
            return data;
        } else {
            throw new RuntimeException("Unsupported body type " + body.getClass());
        }
    }
}
