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

import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
import org.apache.rocketmq.remoting.protocol.header.*;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

/**
 * RPC 客户端默认实现<br>
 * 基于 remoting 客户端完成各类请求调用
 */
public class RpcClientImpl implements RpcClient {

    /**
     * 客户端元数据
     */
    private ClientMetadata clientMetadata;

    /**
     * remoting 客户端
     */
    private RemotingClient remotingClient;

    /**
     * 客户端 Hook 列表
     */
    private List<RpcClientHook> clientHookList = new ArrayList<>();

    /**
     * 构造 RPC 客户端实现
     *
     * @param clientMetadata 客户端元数据
     * @param remotingClient remoting 客户端
     */
    public RpcClientImpl(ClientMetadata clientMetadata, RemotingClient remotingClient) {
        this.clientMetadata = clientMetadata;
        this.remotingClient = remotingClient;
    }

    /**
     * 注册客户端 Hook
     *
     * @param hook Hook 实例
     */
    public void registerHook(RpcClientHook hook) {
        clientHookList.add(hook);
    }

    /**
     * 按消息队列发起 RPC 调用
     *
     * @param mq 消息队列
     * @param request RPC 请求
     * @param timeoutMs 超时时间 毫秒
     * @return RPC 响应 Future
     * @throws RpcException RPC 异常
     */
    @Override
    public Future<RpcResponse>  invoke(MessageQueue mq, RpcRequest request, long timeoutMs) throws RpcException {
        String bname =  clientMetadata.getBrokerNameFromMessageQueue(mq);
        request.getHeader().setBrokerName(bname);
        return invoke(request, timeoutMs);
    }


    /**
     * 创建响应 Promise
     *
     * @return 响应 Promise
     */
    public Promise<RpcResponse> createResponseFuture()  {
        return ImmediateEventExecutor.INSTANCE.newPromise();
    }

    /**
     * 发起 RPC 调用
     *
     * @param request RPC 请求
     * @param timeoutMs 超时时间 毫秒
     * @return RPC 响应 Future
     * @throws RpcException RPC 异常
     */
    @Override
    public Future<RpcResponse>  invoke(RpcRequest request, long timeoutMs) throws RpcException {
        if (clientHookList.size() > 0) {
            for (RpcClientHook rpcClientHook: clientHookList) {
                RpcResponse response = rpcClientHook.beforeRequest(request);
                if (response != null) {
                    //For 1.6, there is not easy-to-use future impl
                    // Java 1.6 无易用 Future 实现 直接返回成功 Promise
                    return createResponseFuture().setSuccess(response);
                }
            }
        }
        String addr = getBrokerAddrByNameOrException(request.getHeader().bname);
        Promise<RpcResponse> rpcResponsePromise = null;
        try {
            switch (request.getCode()) {
                case RequestCode.PULL_MESSAGE:
                    rpcResponsePromise = handlePullMessage(addr, request, timeoutMs);
                    break;
                case RequestCode.GET_MIN_OFFSET:
                    rpcResponsePromise = handleGetMinOffset(addr, request, timeoutMs);
                    break;
                case RequestCode.GET_MAX_OFFSET:
                    rpcResponsePromise = handleGetMaxOffset(addr, request, timeoutMs);
                    break;
                case RequestCode.SEARCH_OFFSET_BY_TIMESTAMP:
                    rpcResponsePromise = handleSearchOffset(addr, request, timeoutMs);
                    break;
                case RequestCode.GET_EARLIEST_MSG_STORETIME:
                    rpcResponsePromise = handleGetEarliestMsgStoretime(addr, request, timeoutMs);
                    break;
                case RequestCode.QUERY_CONSUMER_OFFSET:
                    rpcResponsePromise = handleQueryConsumerOffset(addr, request, timeoutMs);
                    break;
                case RequestCode.UPDATE_CONSUMER_OFFSET:
                    rpcResponsePromise = handleUpdateConsumerOffset(addr, request, timeoutMs);
                    break;
                case RequestCode.GET_TOPIC_STATS_INFO:
                    rpcResponsePromise = handleCommonBodyRequest(addr, request, timeoutMs, TopicStatsTable.class);
                    break;
                case RequestCode.GET_TOPIC_CONFIG:
                    rpcResponsePromise = handleCommonBodyRequest(addr, request, timeoutMs, TopicConfigAndQueueMapping.class);
                    break;
                default:
                    throw new RpcException(ResponseCode.REQUEST_CODE_NOT_SUPPORTED, "Unknown request code " + request.getCode());
            }
        } catch (RpcException rpcException) {
            throw rpcException;
        } catch (Exception e) {
            throw new RpcException(ResponseCode.RPC_UNKNOWN, "error from remoting layer", e);
        }
        return rpcResponsePromise;
    }

    private String getBrokerAddrByNameOrException(String bname) throws RpcException {
        String addr = this.clientMetadata.findMasterBrokerAddr(bname);
        if (addr == null) {
            throw new RpcException(ResponseCode.SYSTEM_ERROR, "cannot find addr for broker " + bname);
        }
        return addr;
    }


    /**
     * 处理请求失败响应
     *
     * @param addr 目标地址
     * @param requestCommand 请求命令
     * @param responseFuture 异步响应 Future
     * @param rpcResponsePromise RPC 响应 Promise
     */
    private void processFailedResponse(String addr, RemotingCommand requestCommand,  ResponseFuture responseFuture, Promise<RpcResponse> rpcResponsePromise) {
        RemotingCommand responseCommand = responseFuture.getResponseCommand();
        if (responseCommand != null) {
            //this should not happen
            // 理论上失败场景不应出现响应命令
            return;
        }
        int errorCode = ResponseCode.RPC_UNKNOWN;
        String errorMessage = null;
        if (!responseFuture.isSendRequestOK()) {
            errorCode = ResponseCode.RPC_SEND_TO_CHANNEL_FAILED;
            errorMessage = "send request failed to " + addr + ". Request: " + requestCommand;
        } else if (responseFuture.isTimeout()) {
            errorCode = ResponseCode.RPC_TIME_OUT;
            errorMessage = "wait response from " + addr + " timeout :" + responseFuture.getTimeoutMillis() + "ms" + ". Request: " + requestCommand;
        } else {
            errorMessage = "unknown reason. addr: " + addr + ", timeoutMillis: " + responseFuture.getTimeoutMillis() + ". Request: " + requestCommand;
        }
        rpcResponsePromise.setSuccess(new RpcResponse(new RpcException(errorCode, errorMessage)));
    }


    /**
     * 处理拉消息请求
     *
     * @param addr 目标地址
     * @param rpcRequest RPC 请求
     * @param timeoutMillis 超时时间 毫秒
     * @return RPC 响应 Promise
     * @throws Exception 调用过程异常
     */
    public Promise<RpcResponse> handlePullMessage(final String addr, RpcRequest rpcRequest, long timeoutMillis)  throws Exception {
        final RemotingCommand requestCommand = RpcClientUtils.createCommandForRpcRequest(rpcRequest);

        final Promise<RpcResponse> rpcResponsePromise = createResponseFuture();

        InvokeCallback callback = new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {

            }

            @Override
            public void operationSucceed(RemotingCommand response) {
                try {
                    switch (response.getCode()) {
                        case ResponseCode.SUCCESS:
                        case ResponseCode.PULL_NOT_FOUND:
                        case ResponseCode.PULL_RETRY_IMMEDIATELY:
                        case ResponseCode.PULL_OFFSET_MOVED:
                            PullMessageResponseHeader responseHeader =
                                (PullMessageResponseHeader) response.decodeCommandCustomHeader(PullMessageResponseHeader.class);
                            rpcResponsePromise.setSuccess(new RpcResponse(response.getCode(), responseHeader, response.getBody()));
                            break;
                        default:
                            RpcResponse rpcResponse = new RpcResponse(new RpcException(response.getCode(), "unexpected remote response code"));
                            rpcResponsePromise.setSuccess(rpcResponse);

                    }
                } catch (Exception e) {
                    String errorMessage = "process failed. addr: " + addr + ", timeoutMillis: " + timeoutMillis + ". Request: " + requestCommand;
                    RpcResponse rpcResponse = new RpcResponse(new RpcException(ResponseCode.RPC_UNKNOWN, errorMessage, e));
                    rpcResponsePromise.setSuccess(rpcResponse);
                }
            }

            @Override
            public void operationFail(Throwable throwable) {
                String errorMessage = "process failed. addr: " + addr + ". Request: " + requestCommand;
                RpcResponse rpcResponse = new RpcResponse(new RpcException(ResponseCode.RPC_UNKNOWN, errorMessage, throwable));
                rpcResponsePromise.setSuccess(rpcResponse);
            }
        };

        this.remotingClient.invokeAsync(addr, requestCommand, timeoutMillis, callback);
        return rpcResponsePromise;
    }

    /**
     * 处理按时间查询偏移量请求
     *
     * @param addr 目标地址
     * @param rpcRequest RPC 请求
     * @param timeoutMillis 超时时间 毫秒
     * @return RPC 响应 Promise
     * @throws Exception 调用过程异常
     */
    public Promise<RpcResponse> handleSearchOffset(String addr, RpcRequest rpcRequest, long timeoutMillis) throws Exception {
        final Promise<RpcResponse> rpcResponsePromise = createResponseFuture();

        RemotingCommand requestCommand = RpcClientUtils.createCommandForRpcRequest(rpcRequest);
        RemotingCommand responseCommand = this.remotingClient.invokeSync(addr, requestCommand, timeoutMillis);
        assert responseCommand != null;
        switch (responseCommand.getCode()) {
            case ResponseCode.SUCCESS: {
                SearchOffsetResponseHeader responseHeader =
                        (SearchOffsetResponseHeader) responseCommand.decodeCommandCustomHeader(SearchOffsetResponseHeader.class);
                rpcResponsePromise.setSuccess(new RpcResponse(responseCommand.getCode(), responseHeader, responseCommand.getBody()));
                break;
            }
            default: {
                rpcResponsePromise.setSuccess(new RpcResponse(new RpcException(responseCommand.getCode(), "unknown remote error")));
            }
        }
        return rpcResponsePromise;
    }


    /**
     * 处理查询消费位点请求
     *
     * @param addr 目标地址
     * @param rpcRequest RPC 请求
     * @param timeoutMillis 超时时间 毫秒
     * @return RPC 响应 Promise
     * @throws Exception 调用过程异常
     */
    public Promise<RpcResponse> handleQueryConsumerOffset(String addr, RpcRequest rpcRequest, long timeoutMillis) throws Exception {
        final Promise<RpcResponse> rpcResponsePromise = createResponseFuture();

        RemotingCommand requestCommand = RpcClientUtils.createCommandForRpcRequest(rpcRequest);
        RemotingCommand responseCommand = this.remotingClient.invokeSync(addr, requestCommand, timeoutMillis);
        assert responseCommand != null;
        switch (responseCommand.getCode()) {
            case ResponseCode.SUCCESS: {
                QueryConsumerOffsetResponseHeader responseHeader =
                        (QueryConsumerOffsetResponseHeader) responseCommand.decodeCommandCustomHeader(QueryConsumerOffsetResponseHeader.class);
                rpcResponsePromise.setSuccess(new RpcResponse(responseCommand.getCode(), responseHeader, responseCommand.getBody()));
                break;
            }
            case ResponseCode.QUERY_NOT_FOUND: {
                rpcResponsePromise.setSuccess(new RpcResponse(responseCommand.getCode(), null, null));
                break;
            }
            default: {
                rpcResponsePromise.setSuccess(new RpcResponse(new RpcException(responseCommand.getCode(), "unknown remote error")));
            }
        }
        return rpcResponsePromise;
    }

    /**
     * 处理更新消费位点请求
     *
     * @param addr 目标地址
     * @param rpcRequest RPC 请求
     * @param timeoutMillis 超时时间 毫秒
     * @return RPC 响应 Promise
     * @throws Exception 调用过程异常
     */
    public Promise<RpcResponse> handleUpdateConsumerOffset(String addr, RpcRequest rpcRequest, long timeoutMillis) throws Exception {
        final Promise<RpcResponse> rpcResponsePromise = createResponseFuture();

        RemotingCommand requestCommand = RpcClientUtils.createCommandForRpcRequest(rpcRequest);
        RemotingCommand responseCommand = this.remotingClient.invokeSync(addr, requestCommand, timeoutMillis);
        assert responseCommand != null;
        switch (responseCommand.getCode()) {
            case ResponseCode.SUCCESS: {
                UpdateConsumerOffsetResponseHeader responseHeader =
                    (UpdateConsumerOffsetResponseHeader) responseCommand.decodeCommandCustomHeader(UpdateConsumerOffsetResponseHeader.class);
                rpcResponsePromise.setSuccess(new RpcResponse(responseCommand.getCode(), responseHeader, responseCommand.getBody()));
                break;
            }
            default: {
                rpcResponsePromise.setSuccess(new RpcResponse(new RpcException(responseCommand.getCode(), "unknown remote error")));
            }
        }
        return rpcResponsePromise;
    }

    /**
     * 处理通用 Body 请求
     *
     * @param addr 目标地址
     * @param rpcRequest RPC 请求
     * @param timeoutMillis 超时时间 毫秒
     * @param bodyClass 响应体类型
     * @return RPC 响应 Promise
     * @throws Exception 调用过程异常
     */
    public Promise<RpcResponse> handleCommonBodyRequest(final String addr, RpcRequest rpcRequest, long timeoutMillis, Class bodyClass) throws Exception {
        final Promise<RpcResponse> rpcResponsePromise = createResponseFuture();
        RemotingCommand requestCommand = RpcClientUtils.createCommandForRpcRequest(rpcRequest);
        RemotingCommand responseCommand = this.remotingClient.invokeSync(addr, requestCommand, timeoutMillis);
        assert responseCommand != null;
        switch (responseCommand.getCode()) {
            case ResponseCode.SUCCESS: {
                rpcResponsePromise.setSuccess(new RpcResponse(ResponseCode.SUCCESS, null, RemotingSerializable.decode(responseCommand.getBody(), bodyClass)));
                break;
            }
            default: {
                rpcResponsePromise.setSuccess(new RpcResponse(new RpcException(responseCommand.getCode(), "unknown remote error")));
            }
        }
        return rpcResponsePromise;
    }

    /**
     * 处理查询最小位点请求
     *
     * @param addr 目标地址
     * @param rpcRequest RPC 请求
     * @param timeoutMillis 超时时间 毫秒
     * @return RPC 响应 Promise
     * @throws Exception 调用过程异常
     */
    public Promise<RpcResponse> handleGetMinOffset(String addr, RpcRequest rpcRequest, long timeoutMillis) throws Exception {
        final Promise<RpcResponse> rpcResponsePromise = createResponseFuture();

        RemotingCommand requestCommand = RpcClientUtils.createCommandForRpcRequest(rpcRequest);

        RemotingCommand responseCommand = this.remotingClient.invokeSync(addr, requestCommand, timeoutMillis);
        assert responseCommand != null;
        switch (responseCommand.getCode()) {
            case ResponseCode.SUCCESS: {
                GetMinOffsetResponseHeader responseHeader =
                        (GetMinOffsetResponseHeader) responseCommand.decodeCommandCustomHeader(GetMinOffsetResponseHeader.class);
                rpcResponsePromise.setSuccess(new RpcResponse(responseCommand.getCode(), responseHeader, responseCommand.getBody()));
                break;
            }
            default: {
                rpcResponsePromise.setSuccess(new RpcResponse(new RpcException(responseCommand.getCode(), "unknown remote error")));
            }
        }
        return rpcResponsePromise;
    }

    /**
     * 处理查询最大位点请求
     *
     * @param addr 目标地址
     * @param rpcRequest RPC 请求
     * @param timeoutMillis 超时时间 毫秒
     * @return RPC 响应 Promise
     * @throws Exception 调用过程异常
     */
    public Promise<RpcResponse> handleGetMaxOffset(String addr, RpcRequest rpcRequest, long timeoutMillis) throws Exception {
        final Promise<RpcResponse> rpcResponsePromise = createResponseFuture();

        RemotingCommand requestCommand = RpcClientUtils.createCommandForRpcRequest(rpcRequest);

        RemotingCommand responseCommand = this.remotingClient.invokeSync(addr, requestCommand, timeoutMillis);
        assert responseCommand != null;
        switch (responseCommand.getCode()) {
            case ResponseCode.SUCCESS: {
                GetMaxOffsetResponseHeader responseHeader =
                        (GetMaxOffsetResponseHeader) responseCommand.decodeCommandCustomHeader(GetMaxOffsetResponseHeader.class);
                rpcResponsePromise.setSuccess(new RpcResponse(responseCommand.getCode(), responseHeader, responseCommand.getBody()));
                break;
            }
            default: {
                rpcResponsePromise.setSuccess(new RpcResponse(new RpcException(responseCommand.getCode(), "unknown remote error")));
            }
        }
        return rpcResponsePromise;
    }

    /**
     * 处理查询最早存储时间请求
     *
     * @param addr 目标地址
     * @param rpcRequest RPC 请求
     * @param timeoutMillis 超时时间 毫秒
     * @return RPC 响应 Promise
     * @throws Exception 调用过程异常
     */
    public Promise<RpcResponse> handleGetEarliestMsgStoretime(String addr, RpcRequest rpcRequest, long timeoutMillis) throws Exception {
        final Promise<RpcResponse> rpcResponsePromise = createResponseFuture();

        RemotingCommand requestCommand = RpcClientUtils.createCommandForRpcRequest(rpcRequest);

        RemotingCommand responseCommand = this.remotingClient.invokeSync(addr, requestCommand, timeoutMillis);
        assert responseCommand != null;
        switch (responseCommand.getCode()) {
            case ResponseCode.SUCCESS: {
                GetEarliestMsgStoretimeResponseHeader responseHeader =
                        (GetEarliestMsgStoretimeResponseHeader) responseCommand.decodeCommandCustomHeader(GetEarliestMsgStoretimeResponseHeader.class);
                rpcResponsePromise.setSuccess(new RpcResponse(responseCommand.getCode(), responseHeader, responseCommand.getBody()));
                break;
            }
            default: {
                rpcResponsePromise.setSuccess(new RpcResponse(new RpcException(responseCommand.getCode(), "unknown remote error")));
            }
        }
        return rpcResponsePromise;
    }

}
