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
package org.apache.rocketmq.remoting.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/*
 * 长度的语义为 "再向后读 length 字节"
 * Message Length (4 byte) | Serialization Type (1 byte) + Header Length (3 byte)
 * Data Header             | Message Body
 * ROCKETMQ 序列化方式下的消息头包括
 * code   (2 byte) | language (1 byte) | version (2 byte)
 * opaque (4 byte) | flag     (4 byte) | remark  (4 + ? byte) | extFields (4 + ? byte)
 */

@ChannelHandler.Sharable
public class NettyEncoder extends MessageToByteEncoder<RemotingCommand> {
    /**
     * Remoting 日志记录器
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);

    /**
     * 将 RemotingCommand 编码为网络字节流
     *
     * @param ctx             Channel 上下文
     * @param remotingCommand 待编码命令
     * @param out             输出缓冲区
     * @throws Exception 编码过程中发生异常时抛出
     */
    @Override
    public void encode(ChannelHandlerContext ctx, RemotingCommand remotingCommand, ByteBuf out)
        throws Exception {
        try {
            // 先编码协议头
            remotingCommand.fastEncodeHeader(out);
            // 再按需写入消息体
            byte[] body = remotingCommand.getBody();
            if (body != null) {
                out.writeBytes(body);
            }
        } catch (Exception e) {
            // 编码异常时输出上下文信息并关闭连接
            log.error("encode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
            if (remotingCommand != null) {
                log.error(remotingCommand.toString());
            }
            RemotingHelper.closeChannel(ctx.channel());
        }
    }
}
