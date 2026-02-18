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

package org.apache.rocketmq.remoting.rpchook;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestType;

/**
 * 流式请求类型 RPC Hook<br>
 * 在请求发出前标记流式请求类型
 */
public class StreamTypeRPCHook implements RPCHook {
    /**
     * 请求发送前写入流式请求类型
     *
     * @param remoteAddr 远端地址
     * @param request 请求命令
     */
    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        request.addExtField(MixAll.REQ_T, String.valueOf(RequestType.STREAM.getCode()));
    }

    /**
     * 请求响应后回调
     *
     * @param remoteAddr 远端地址
     * @param request 请求命令
     * @param response 响应命令
     */
    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request,
        RemotingCommand response) {

    }
}
