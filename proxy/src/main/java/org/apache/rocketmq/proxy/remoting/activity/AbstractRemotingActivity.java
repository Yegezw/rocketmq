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

package org.apache.rocketmq.proxy.remoting.activity;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.utils.ExceptionUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractRemotingActivity implements NettyRequestProcessor {
    /**
     * Remoting 活动日志记录器
     */
    protected final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    /**
     * 消息处理核心组件
     */
    protected final MessagingProcessor messagingProcessor;
    /**
     * Broker 名称扩展字段键
     */
    protected static final String BROKER_NAME_FIELD = "bname";
    /**
     * SEND_MESSAGE_V2 使用的 Broker 名称字段键
     */
    protected static final String BROKER_NAME_FIELD_FOR_SEND_MESSAGE_V2 = "n";
    /**
     * Proxy 异常码到 Remoting 响应码的映射关系
     */
    private static final Map<ProxyExceptionCode, Integer> PROXY_EXCEPTION_RESPONSE_CODE_MAP = new HashMap<ProxyExceptionCode, Integer>() {
        {
            put(ProxyExceptionCode.FORBIDDEN, ResponseCode.NO_PERMISSION);
            put(ProxyExceptionCode.MESSAGE_PROPERTY_CONFLICT_WITH_TYPE, ResponseCode.MESSAGE_ILLEGAL);
            put(ProxyExceptionCode.INTERNAL_SERVER_ERROR, ResponseCode.SYSTEM_ERROR);
            put(ProxyExceptionCode.TRANSACTION_DATA_NOT_FOUND, ResponseCode.SUCCESS);
        }
    };
    /**
     * 请求处理管道
     */
    protected final RequestPipeline requestPipeline;

    /**
     * 构造 Remoting 活动基类
     *
     * @param requestPipeline 请求处理管道
     * @param messagingProcessor 消息处理核心组件
     */
    public AbstractRemotingActivity(RequestPipeline requestPipeline, MessagingProcessor messagingProcessor) {
        this.requestPipeline = requestPipeline;
        this.messagingProcessor = messagingProcessor;
    }

    /**
     * 代理请求到 Broker 并根据调用模式写回响应
     *
     * @param ctx Netty 上下文
     * @param request 请求命令
     * @param context Proxy 上下文
     * @param timeoutMillis 超时时间
     * @return 单向请求返回 null, 异步请求由回调写回响应
     * @throws Exception 请求异常
     */
    protected RemotingCommand request(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context, long timeoutMillis) throws Exception {
        String brokerName;
        if (request.getCode() == RequestCode.SEND_MESSAGE_V2 || request.getCode() == RequestCode.SEND_BATCH_MESSAGE) {
            if (request.getExtFields().get(BROKER_NAME_FIELD_FOR_SEND_MESSAGE_V2) == null) {
                return RemotingCommand.buildErrorResponse(ResponseCode.VERSION_NOT_SUPPORTED,
                    "Request doesn't have field bname");
            }
            brokerName = request.getExtFields().get(BROKER_NAME_FIELD_FOR_SEND_MESSAGE_V2);
        } else {
            if (request.getExtFields().get(BROKER_NAME_FIELD) == null) {
                return RemotingCommand.buildErrorResponse(ResponseCode.VERSION_NOT_SUPPORTED,
                    "Request doesn't have field bname");
            }
            brokerName = request.getExtFields().get(BROKER_NAME_FIELD);
        }
        if (request.isOnewayRPC()) {
            messagingProcessor.requestOneway(context, brokerName, request, timeoutMillis);
            return null;
        }
        messagingProcessor.request(context, brokerName, request, timeoutMillis)
            .thenAccept(r -> writeResponse(ctx, context, request, r))
            .exceptionally(t -> {
                writeErrResponse(ctx, context, request, t);
                return null;
            });
        return null;
    }

    /**
     * 执行公共处理流程, 包含管线执行与异常兜底
     *
     * @param ctx Netty 上下文
     * @param request 请求命令
     * @return 返回值固定为 null, 实际响应通过网络写回
     * @throws Exception 处理异常
     */
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        ProxyContext context = createContext();
        try {
            this.requestPipeline.execute(ctx, request, context);
            RemotingCommand response = this.processRequest0(ctx, request, context);
            if (response != null) {
                writeResponse(ctx, context, request, response);
            }
            return null;
        } catch (Throwable t) {
            writeErrResponse(ctx, context, request, t);
            return null;
        }
    }

    /**
     * 当前活动不主动拒绝请求
     *
     * @return 恒为 false
     */
    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * 子类实现的具体业务处理逻辑
     *
     * @param ctx Netty 上下文
     * @param request 请求命令
     * @param context Proxy 上下文
     * @return 可选响应对象
     * @throws Exception 处理异常
     */
    protected abstract RemotingCommand processRequest0(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception;

    /**
     * 创建请求级 Proxy 上下文
     *
     * @return Proxy 上下文
     */
    protected ProxyContext createContext() {
        return ProxyContext.create();
    }

    /**
     * 将异常转换为标准 Remoting 错误响应
     *
     * @param ctx Netty 上下文
     * @param context Proxy 上下文
     * @param request 原始请求
     * @param t 原始异常
     */
    protected void writeErrResponse(ChannelHandlerContext ctx, final ProxyContext context,
        final RemotingCommand request, Throwable t) {
        t = ExceptionUtils.getRealException(t);
        if (t instanceof ProxyException) {
            ProxyException e = (ProxyException) t;
            writeResponse(ctx, context, request,
                RemotingCommand.createResponseCommand(
                    PROXY_EXCEPTION_RESPONSE_CODE_MAP.getOrDefault(e.getCode(), ResponseCode.SYSTEM_ERROR),
                    e.getMessage()),
                t);
        } else if (t instanceof MQClientException) {
            MQClientException e = (MQClientException) t;
            writeResponse(ctx, context, request, RemotingCommand.createResponseCommand(e.getResponseCode(), e.getErrorMessage()), t);
        } else if (t instanceof MQBrokerException) {
            MQBrokerException e = (MQBrokerException) t;
            writeResponse(ctx, context, request, RemotingCommand.createResponseCommand(e.getResponseCode(), e.getErrorMessage()), t);
        } else if (t instanceof AclException) {
            writeResponse(ctx, context, request, RemotingCommand.createResponseCommand(ResponseCode.NO_PERMISSION, t.getMessage()), t);
        } else {
            writeResponse(ctx, context, request,
                RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, t.getMessage()), t);
        }
    }

    /**
     * 写回正常响应
     *
     * @param ctx Netty 上下文
     * @param context Proxy 上下文
     * @param request 原始请求
     * @param response 响应命令
     */
    protected void writeResponse(ChannelHandlerContext ctx, final ProxyContext context,
        final RemotingCommand request, RemotingCommand response) {
        writeResponse(ctx, context, request, response, null);
    }

    /**
     * 写回响应并附加必要扩展字段
     *
     * @param ctx Netty 上下文
     * @param context Proxy 上下文
     * @param request 原始请求
     * @param response 响应命令
     * @param t 可选异常信息
     */
    protected void writeResponse(ChannelHandlerContext ctx, final ProxyContext context,
        final RemotingCommand request, RemotingCommand response, Throwable t) {
        if (request.isOnewayRPC()) {
            return;
        }
        if (!ctx.channel().isWritable()) {
            return;
        }

        ProxyConfig config = ConfigurationManager.getProxyConfig();

        response.setOpaque(request.getOpaque());
        response.markResponseType();
        response.addExtField(MessageConst.PROPERTY_MSG_REGION, config.getRegionId());
        response.addExtField(MessageConst.PROPERTY_TRACE_SWITCH, String.valueOf(config.isTraceOn()));
        if (t != null) {
            response.setRemark(t.getMessage());
        }

        ctx.writeAndFlush(response);
    }
}
