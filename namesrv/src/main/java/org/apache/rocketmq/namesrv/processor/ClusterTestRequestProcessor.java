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

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.namesrv.NamesrvUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

/**
 * 集群测试请求处理器, 优先返回当前 NameServer 路由, 缺失时回源产品环境查询
 */
public class ClusterTestRequestProcessor extends ClientRequestProcessor {
    /**
     * NameServer 日志对象, 记录集群测试请求处理日志
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    /**
     * 管理工具实例, 用于回源产品环境查询主题路由
     */
    private final DefaultMQAdminExt adminExt;
    /**
     * 产品环境名称, 用于标识回源查询目标环境
     */
    private final String productEnvName;

    /**
     * 创建 ClusterTestRequestProcessor 实例, 初始化管理工具并启动
     *
     * @param namesrvController NameServer 控制器
     * @param productEnvName    产品环境名称
     */
    public ClusterTestRequestProcessor(NamesrvController namesrvController, String productEnvName) {
        super(namesrvController);
        this.productEnvName = productEnvName;

        // 创建管理工具并配置实例名和单元名
        adminExt = new DefaultMQAdminExt();
        adminExt.setInstanceName("CLUSTER_TEST_NS_INS_" + productEnvName);
        adminExt.setUnitName(productEnvName);

        // 启动管理工具, 失败时记录错误日志
        try {
            adminExt.start();
        } catch (MQClientException e) {
            log.error("Failed to start processor", e);
        }
    }

    /**
     * 按 Topic 查询路由信息, 先查当前 NameServer, 再回源产品环境
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return 路由查询响应
     * @throws RemotingCommandException 请求头解码异常
     */
    @Override
    public RemotingCommand getRouteInfoByTopic(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        // 创建响应对象并解析请求头
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetRouteInfoRequestHeader requestHeader =
            (GetRouteInfoRequestHeader) request.decodeCommandCustomHeader(GetRouteInfoRequestHeader.class);

        // 先从当前 NameServer 查询主题路由
        TopicRouteData topicRouteData = this.namesrvController.getRouteInfoManager().pickupTopicRouteData(requestHeader.getTopic());
        if (topicRouteData != null) {
            // 命中路由时补充顺序主题配置
            String orderTopicConf =
                this.namesrvController.getKvConfigManager().getKVConfig(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG,
                    requestHeader.getTopic());
            topicRouteData.setOrderTopicConf(orderTopicConf);
        } else {
            // 当前环境未命中时回源产品环境查询
            try {
                topicRouteData = adminExt.examineTopicRouteInfo(requestHeader.getTopic());
            } catch (Exception e) {
                log.info("get route info by topic from product environment failed. envName={},", productEnvName);
            }
        }

        if (topicRouteData != null) {
            // 查询成功时编码路由并返回成功状态
            byte[] content = topicRouteData.encode();
            response.setBody(content);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        // 两次查询均未命中时返回主题不存在错误
        response.setCode(ResponseCode.TOPIC_NOT_EXIST);
        response.setRemark("No topic route info in name server for the topic: " + requestHeader.getTopic()
            + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
        return response;
    }
}
