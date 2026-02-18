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
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.TransactionStatus;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 事务消息请求处理活动
 */
public class TransactionActivity extends AbstractRemotingActivity {

    /**
     * 构造事务活动处理器
     *
     * @param requestPipeline 请求处理管道
     * @param messagingProcessor 消息处理核心组件
     */
    public TransactionActivity(RequestPipeline requestPipeline,
        MessagingProcessor messagingProcessor) {
        super(requestPipeline, messagingProcessor);
    }

    /**
     * 处理事务结束请求并转换事务状态
     *
     * @param ctx Netty 上下文
     * @param request 请求命令
     * @param context Proxy 上下文
     * @return 成功响应
     * @throws Exception 处理异常
     */
    @Override
    protected RemotingCommand processRequest0(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        final EndTransactionRequestHeader requestHeader = (EndTransactionRequestHeader) request.decodeCommandCustomHeader(EndTransactionRequestHeader.class);

        TransactionStatus transactionStatus = TransactionStatus.UNKNOWN;
        switch (requestHeader.getCommitOrRollback()) {
            case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                transactionStatus = TransactionStatus.COMMIT;
                break;
            case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                transactionStatus = TransactionStatus.ROLLBACK;
                break;
            default:
                break;
        }

        this.messagingProcessor.endTransaction(
            context,
            requestHeader.getTopic(),
            requestHeader.getTransactionId(),
            requestHeader.getMsgId(),
            requestHeader.getProducerGroup(),
            transactionStatus,
            requestHeader.getFromTransactionCheck()
        );
        return response;
    }
}
