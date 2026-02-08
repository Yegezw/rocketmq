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

package org.apache.rocketmq.namesrv.processor;

import com.alibaba.fastjson.serializer.SerializerFeature;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.namesrv.NamesrvUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * 客户端请求处理器, 负责处理路由查询请求 {@link RequestCode#GET_ROUTEINFO_BY_TOPIC}
 */
public class ClientRequestProcessor implements NettyRequestProcessor {

    /**
     * NameServer 日志对象, 记录客户端请求处理日志
     */
    private static Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    /**
     * NameServer 控制器引用, 用于访问路由和配置管理组件
     */
    protected NamesrvController namesrvController;

    /**
     * 启动时间戳, 用于判断服务是否已达到可服务状态
     */
    private long startupTimeMillis;

    /**
     * 创建 ClientRequestProcessor 实例, 初始化控制器和启动时间
     *
     * @param namesrvController NameServer 控制器
     */
    public ClientRequestProcessor(final NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
        this.startupTimeMillis = System.currentTimeMillis();
    }

    /**
     * 处理客户端请求入口, 当前仅转发到按 Topic 查询路由的方法
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return 远程响应命令
     * @throws Exception 请求处理异常
     */
    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx,
        final RemotingCommand request) throws Exception {
        return this.getRouteInfoByTopic(ctx, request);
    }

    /**
     * 按 Topic 查询路由信息并返回给客户端
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return 路由查询响应
     * @throws RemotingCommandException 请求头解码异常
     */
    public RemotingCommand getRouteInfoByTopic(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        // 创建响应对象并解析请求头
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetRouteInfoRequestHeader requestHeader =
            (GetRouteInfoRequestHeader) request.decodeCommandCustomHeader(GetRouteInfoRequestHeader.class);

        // 根据启动时间和配置判断 NameServer 是否可对外服务
        boolean namesrvReady = System.currentTimeMillis() - startupTimeMillis >= TimeUnit.SECONDS.toMillis(namesrvController.getNamesrvConfig().getWaitSecondsForService());

        if (namesrvController.getNamesrvConfig().isNeedWaitForService() && !namesrvReady) {
            log.warn("name server not ready. request code {} ", request.getCode());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("name server not ready");
            return response;
        }

        // 从路由管理器查询 Topic 路由
        TopicRouteData topicRouteData = this.namesrvController.getRouteInfoManager().pickupTopicRouteData(requestHeader.getTopic());

        if (topicRouteData != null) {
            // 开启顺序消息时补充顺序主题配置
            if (this.namesrvController.getNamesrvConfig().isOrderMessageEnable()) {
                String orderTopicConf =
                    this.namesrvController.getKvConfigManager().getKVConfig(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG,
                        requestHeader.getTopic());
                topicRouteData.setOrderTopicConf(orderTopicConf);
            }

            byte[] content;
            // 根据请求版本或标记选择标准 JSON 编码方式
            Boolean standardJsonOnly = Optional.ofNullable(requestHeader.getAcceptStandardJsonOnly()).orElse(false);
            if (request.getVersion() >= MQVersion.Version.V4_9_4.ordinal() || standardJsonOnly) {
                content = topicRouteData.encode(SerializerFeature.BrowserCompatible,
                    SerializerFeature.QuoteFieldNames, SerializerFeature.SkipTransientField,
                    SerializerFeature.MapSortField);
            } else {
                content = topicRouteData.encode();
            }

            // 查询成功时写入响应体和状态码
            response.setBody(content);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        // 未查询到路由时返回主题不存在错误
        response.setCode(ResponseCode.TOPIC_NOT_EXIST);
        response.setRemark("No topic route info in name server for the topic: " + requestHeader.getTopic()
            + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
        return response;
    }

    /**
     * 是否拒绝请求, 当前始终返回 false
     *
     * @return false 表示不拒绝请求
     */
    @Override
    public boolean rejectRequest() {
        return false;
    }
}
