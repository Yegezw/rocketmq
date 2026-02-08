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

import com.google.common.base.Stopwatch;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
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

public class NettyDecoder extends LengthFieldBasedFrameDecoder {
    /**
     * Remoting 日志记录器
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);

    /**
     * 单帧最大长度, 默认值为 16 MB, 可通过系统属性覆盖
     */
    private static final int FRAME_MAX_LENGTH =
        Integer.parseInt(System.getProperty("com.rocketmq.remoting.frameMaxLength", "16777216"));

    /**
     * 创建 Netty 解码器, 并初始化长度字段解码规则
     */
    public NettyDecoder() {
        super(FRAME_MAX_LENGTH, 0, 4, 0, 4);
    }

    /**
     * 将网络字节流解码为 RemotingCommand
     *
     * @param ctx Channel 上下文
     * @param in  输入缓冲区
     * @return 解码后的命令对象, 若数据不足或异常则返回 null
     * @throws Exception 解码过程中发生异常时抛出
     */
    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        // frame 由父类按长度字段切分, 需在 finally 中释放
        ByteBuf frame = null;
        // 记录处理耗时, 后续注入到命令对象中
        Stopwatch timer = Stopwatch.createStarted();
        try {
            frame = (ByteBuf) super.decode(ctx, in);
            // 当前可读数据不足一帧时直接返回
            if (null == frame) {
                return null;
            }
            // 反序列化 Remoting 命令并绑定处理计时器
            RemotingCommand cmd = RemotingCommand.decode(frame);
            cmd.setProcessTimer(timer);
            return cmd;
        } catch (Exception e) {
            // 解码异常时记录日志并关闭连接, 避免脏通道继续收发
            log.error("decode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
            RemotingHelper.closeChannel(ctx.channel());
        } finally {
            // 防止 ByteBuf 泄漏
            if (null != frame) {
                frame.release();
            }
        }

        return null;
    }
}
