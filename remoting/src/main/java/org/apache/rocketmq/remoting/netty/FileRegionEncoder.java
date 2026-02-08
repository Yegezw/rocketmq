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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.netty.handler.codec.MessageToByteEncoder;

import io.netty.handler.ssl.SslHandler;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * <p>
 *     By default, file region are directly transferred to socket channel which is known as zero copy. In case we need
 *     to encrypt transmission, data being sent should go through the {@link SslHandler}. This encoder ensures this
 *     process.
 * </p>
 * <p>
 *     默认情况下, file region 会直接传输到 socket channel, 这种方式称为 zero copy<br>
 *     当需要加密传输时, 发送数据应经过 {@link SslHandler}<br>
 *     该编码器用于确保这一过程
 * </p>
 */
public class FileRegionEncoder extends MessageToByteEncoder<FileRegion> {

    /**
     * Encode a message into a {@link io.netty.buffer.ByteBuf}. This method will be called for each written message that
     * can be handled by this encoder.
     * <br>
     * 将消息编码到 {@link io.netty.buffer.ByteBuf} 中, 此方法会在每次写入且可由当前编码器处理的消息上调用
     *
     * @param ctx the {@link io.netty.channel.ChannelHandlerContext} which this {@link
     * io.netty.handler.codec.MessageToByteEncoder} belongs to<br>
     *            当前 {@link io.netty.handler.codec.MessageToByteEncoder} 所属的 {@link io.netty.channel.ChannelHandlerContext}
     * @param msg the message to encode<br>
     *            需要编码的消息
     * @param out the {@link io.netty.buffer.ByteBuf} into which the encoded message will be written<br>
     *            编码结果将写入的 {@link io.netty.buffer.ByteBuf}
     * @throws Exception is thrown if an error occurs<br>当发生错误时抛出异常
     */
    @Override
    protected void encode(ChannelHandlerContext ctx, FileRegion msg, final ByteBuf out) throws Exception {
        // 构建中间写通道, 将 FileRegion 内容写入 out, 以便后续由 SslHandler 处理
        WritableByteChannel writableByteChannel = new WritableByteChannel() {
            /**
             * 将 ByteBuffer 中的数据写入目标 ByteBuf
             *
             * @param src 待写入的源缓冲区
             * @return 本次实际写入的字节数
             */
            @Override
            public int write(ByteBuffer src) {
                int prev = out.writerIndex();
                out.writeBytes(src);
                return out.writerIndex() - prev;
            }

            /**
             * 返回当前通道是否可用
             *
             * @return 始终返回 true
             */
            @Override
            public boolean isOpen() {
                return true;
            }

            /**
             * 关闭当前通道
             *
             * @throws IOException 关闭时可能抛出的 IO 异常
             */
            @Override
            public void close() throws IOException {
            }
        };

        // 记录总传输量, 用于控制传输循环
        long toTransfer = msg.count();

        // 持续传输直到剩余字节数为 0
        while (true) {
            long transferred = msg.transferred();
            if (toTransfer - transferred <= 0) {
                break;
            }
            msg.transferTo(writableByteChannel, transferred);
        }
    }
}
