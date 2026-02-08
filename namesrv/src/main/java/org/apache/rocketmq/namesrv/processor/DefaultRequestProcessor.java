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
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MQVersion.Version;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NamesrvUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.*;
import org.apache.rocketmq.remoting.protocol.body.*;
import org.apache.rocketmq.remoting.protocol.header.GetBrokerMemberGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetTopicsByClusterRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.*;
import org.apache.rocketmq.remoting.protocol.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * NameServer 默认请求处理器, 负责处理 KV、Broker、Topic 和配置相关请求
 */
public class DefaultRequestProcessor implements NettyRequestProcessor {
    /**
     * NameServer 日志对象, 记录请求处理日志
     */
    private static Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    /**
     * NameServer 控制器引用, 用于访问路由、配置和 KV 管理能力
     */
    protected final NamesrvController namesrvController;

    /**
     * 配置黑名单集合, 禁止通过远程接口动态修改的配置项
     */
    protected Set<String> configBlackList = new HashSet<>();

    /**
     * 创建 DefaultRequestProcessor 实例, 初始化控制器和黑名单
     *
     * @param namesrvController NameServer 控制器
     */
    public DefaultRequestProcessor(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
        initConfigBlackList();
    }

    /**
     * 初始化配置黑名单, 合并内置项和用户自定义项
     */
    private void initConfigBlackList() {
        configBlackList.add("configBlackList");
        configBlackList.add("configStorePath");
        configBlackList.add("kvConfigPath");
        configBlackList.add("rocketmqHome");
        String[] configArray = namesrvController.getNamesrvConfig().getConfigBlackList().split(";");
        configBlackList.addAll(Arrays.asList(configArray));
    }

    /**
     * 处理 NameServer 远程请求, 根据请求码分发到对应处理方法
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return 远程响应命令
     * @throws RemotingCommandException 请求解码异常
     */
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {

        // 打印请求来源和请求码, 便于问题排查
        if (ctx != null) {
            log.debug("receive request, {} {} {}",
                request.getCode(),
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                request);
        }

        // 按请求码路由到具体处理逻辑
        switch (request.getCode()) {
            case RequestCode.PUT_KV_CONFIG:
                return this.putKVConfig(ctx, request);
            case RequestCode.GET_KV_CONFIG:
                return this.getKVConfig(ctx, request);
            case RequestCode.DELETE_KV_CONFIG:
                return this.deleteKVConfig(ctx, request);
            case RequestCode.QUERY_DATA_VERSION:
                return this.queryBrokerTopicConfig(ctx, request);
            case RequestCode.REGISTER_BROKER:
                return this.registerBroker(ctx, request);
            case RequestCode.UNREGISTER_BROKER:
                return this.unregisterBroker(ctx, request);
            case RequestCode.BROKER_HEARTBEAT:
                return this.brokerHeartbeat(ctx, request);
            case RequestCode.GET_BROKER_MEMBER_GROUP:
                return this.getBrokerMemberGroup(ctx, request);
            case RequestCode.GET_BROKER_CLUSTER_INFO:
                return this.getBrokerClusterInfo(ctx, request);
            case RequestCode.WIPE_WRITE_PERM_OF_BROKER:
                return this.wipeWritePermOfBroker(ctx, request);
            case RequestCode.ADD_WRITE_PERM_OF_BROKER:
                return this.addWritePermOfBroker(ctx, request);
            case RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER:
                return this.getAllTopicListFromNameserver(ctx, request);
            case RequestCode.DELETE_TOPIC_IN_NAMESRV:
                return this.deleteTopicInNamesrv(ctx, request);
            case RequestCode.REGISTER_TOPIC_IN_NAMESRV:
                return this.registerTopicToNamesrv(ctx, request);
            case RequestCode.GET_KVLIST_BY_NAMESPACE:
                return this.getKVListByNamespace(ctx, request);
            case RequestCode.GET_TOPICS_BY_CLUSTER:
                return this.getTopicsByCluster(ctx, request);
            case RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_NS:
                return this.getSystemTopicListFromNs(ctx, request);
            case RequestCode.GET_UNIT_TOPIC_LIST:
                return this.getUnitTopicList(ctx, request);
            case RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST:
                return this.getHasUnitSubTopicList(ctx, request);
            case RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST:
                return this.getHasUnitSubUnUnitTopicList(ctx, request);
            case RequestCode.UPDATE_NAMESRV_CONFIG:
                return this.updateConfig(ctx, request);
            case RequestCode.GET_NAMESRV_CONFIG:
                return this.getConfig(ctx, request);
            default:
                String error = " request type " + request.getCode() + " not supported";
                return RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
        }
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

    /**
     * 写入 KV 配置
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return 处理结果
     * @throws RemotingCommandException 请求头解码异常
     */
    public RemotingCommand putKVConfig(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        // 解析请求并校验核心参数
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final PutKVConfigRequestHeader requestHeader =
            (PutKVConfigRequestHeader) request.decodeCommandCustomHeader(PutKVConfigRequestHeader.class);

        if (requestHeader.getNamespace() == null || requestHeader.getKey() == null) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("namespace or key is null");
            return response;
        }

        // 将配置写入 KV 管理器并持久化
        this.namesrvController.getKvConfigManager().putKVConfig(
            requestHeader.getNamespace(),
            requestHeader.getKey(),
            requestHeader.getValue()
        );

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 查询 KV 配置
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return 处理结果
     * @throws RemotingCommandException 请求头解码异常
     */
    public RemotingCommand getKVConfig(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        // 解析命名空间和键并读取配置值
        final RemotingCommand response = RemotingCommand.createResponseCommand(GetKVConfigResponseHeader.class);
        final GetKVConfigResponseHeader responseHeader = (GetKVConfigResponseHeader) response.readCustomHeader();
        final GetKVConfigRequestHeader requestHeader =
            (GetKVConfigRequestHeader) request.decodeCommandCustomHeader(GetKVConfigRequestHeader.class);

        String value = this.namesrvController.getKvConfigManager().getKVConfig(
            requestHeader.getNamespace(),
            requestHeader.getKey()
        );

        if (value != null) {
            responseHeader.setValue(value);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        // 未命中时返回查询失败
        response.setCode(ResponseCode.QUERY_NOT_FOUND);
        response.setRemark("No config item, Namespace: " + requestHeader.getNamespace() + " Key: " + requestHeader.getKey());
        return response;
    }

    /**
     * 删除 KV 配置
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return 处理结果
     * @throws RemotingCommandException 请求头解码异常
     */
    public RemotingCommand deleteKVConfig(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        // 解析请求后删除指定键
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final DeleteKVConfigRequestHeader requestHeader =
            (DeleteKVConfigRequestHeader) request.decodeCommandCustomHeader(DeleteKVConfigRequestHeader.class);

        this.namesrvController.getKvConfigManager().deleteKVConfig(
            requestHeader.getNamespace(),
            requestHeader.getKey()
        );

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 注册 Broker 信息和 Topic 配置
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return 处理结果
     * @throws RemotingCommandException 请求解码异常
     */
    public RemotingCommand registerBroker(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        // 解析注册请求头
        final RemotingCommand response = RemotingCommand.createResponseCommand(RegisterBrokerResponseHeader.class);
        final RegisterBrokerResponseHeader responseHeader = (RegisterBrokerResponseHeader) response.readCustomHeader();
        final RegisterBrokerRequestHeader requestHeader =
            (RegisterBrokerRequestHeader) request.decodeCommandCustomHeader(RegisterBrokerRequestHeader.class);

        // 校验请求体 CRC32, 防止网络传输损坏
        if (!checksum(ctx, request, requestHeader)) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("crc32 not match");
            return response;
        }

        TopicConfigSerializeWrapper topicConfigWrapper = null;
        List<String> filterServerList = null;

        Version brokerVersion = MQVersion.value2Version(request.getVersion());
        if (brokerVersion.ordinal() >= MQVersion.Version.V3_0_11.ordinal()) {
            // 新版本请求体包含完整 RegisterBrokerBody
            final RegisterBrokerBody registerBrokerBody = extractRegisterBrokerBodyFromRequest(request, requestHeader);
            topicConfigWrapper = registerBrokerBody.getTopicConfigSerializeWrapper();
            filterServerList = registerBrokerBody.getFilterServerList();
        } else {
            // 旧版本请求体仅包含 TopicConfig
            topicConfigWrapper = extractRegisterTopicConfigFromRequest(request);
        }

        // 调用路由管理器完成 Broker 注册
        RegisterBrokerResult result = this.namesrvController.getRouteInfoManager().registerBroker(
            requestHeader.getClusterName(),
            requestHeader.getBrokerAddr(),
            requestHeader.getBrokerName(),
            requestHeader.getBrokerId(),
            requestHeader.getHaServerAddr(),
            request.getExtFields().get(MixAll.ZONE_NAME),
            requestHeader.getHeartbeatTimeoutMillis(),
            requestHeader.getEnableActingMaster(),
            topicConfigWrapper,
            filterServerList,
            ctx.channel()
        );

        if (result == null) {
            // 单 Topic 路由注册必须在 Broker 首次注册成功之后
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("register broker failed");
            return response;
        }

        // 回填主从地址信息
        responseHeader.setHaServerAddr(result.getHaServerAddr());
        responseHeader.setMasterAddr(result.getMasterAddr());

        // 按配置返回顺序 Topic 配置, 供 Broker 本地缓存
        if (this.namesrvController.getNamesrvConfig().isReturnOrderTopicConfigToBroker()) {
            byte[] jsonValue = this.namesrvController.getKvConfigManager().getKVListByNamespace(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG);
            response.setBody(jsonValue);
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 从旧版本注册请求体中提取 Topic 配置
     *
     * @param request 远程请求命令
     * @return Topic 配置包装对象
     */
    private TopicConfigSerializeWrapper extractRegisterTopicConfigFromRequest(final RemotingCommand request) {
        TopicConfigSerializeWrapper topicConfigWrapper;
        if (request.getBody() != null) {
            // 请求体存在时直接反序列化
            topicConfigWrapper = TopicConfigSerializeWrapper.decode(request.getBody(), TopicConfigSerializeWrapper.class);
        } else {
            // 请求体缺失时构造默认版本信息
            topicConfigWrapper = new TopicConfigSerializeWrapper();
            topicConfigWrapper.getDataVersion().setCounter(new AtomicLong(0));
            topicConfigWrapper.getDataVersion().setTimestamp(0L);
            topicConfigWrapper.getDataVersion().setStateVersion(0L);
        }
        return topicConfigWrapper;
    }

    /**
     * 从新版本注册请求体中提取 RegisterBrokerBody
     *
     * @param request       远程请求命令
     * @param requestHeader 注册请求头
     * @return 注册请求体对象
     * @throws RemotingCommandException 请求体解码异常
     */
    private RegisterBrokerBody extractRegisterBrokerBodyFromRequest(RemotingCommand request,
        RegisterBrokerRequestHeader requestHeader) throws RemotingCommandException {
        RegisterBrokerBody registerBrokerBody = new RegisterBrokerBody();

        if (request.getBody() != null) {
            try {
                // 按 Broker 协议版本和压缩标记解码请求体
                Version brokerVersion = MQVersion.value2Version(request.getVersion());
                registerBrokerBody = RegisterBrokerBody.decode(request.getBody(), requestHeader.isCompressed(), brokerVersion);
            } catch (Exception e) {
                throw new RemotingCommandException("Failed to decode RegisterBrokerBody", e);
            }
        } else {
            // 请求体缺失时初始化默认版本号
            registerBrokerBody.getTopicConfigSerializeWrapper().getDataVersion().setCounter(new AtomicLong(0));
            registerBrokerBody.getTopicConfigSerializeWrapper().getDataVersion().setTimestamp(0L);
            registerBrokerBody.getTopicConfigSerializeWrapper().getDataVersion().setStateVersion(0L);
        }
        return registerBrokerBody;
    }

    /**
     * 查询 Broker 成员组信息
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return 成员组查询结果
     * @throws RemotingCommandException 请求头解码异常
     */
    private RemotingCommand getBrokerMemberGroup(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        // 解析集群名和 Broker 名称
        GetBrokerMemberGroupRequestHeader requestHeader = (GetBrokerMemberGroupRequestHeader) request.decodeCommandCustomHeader(GetBrokerMemberGroupRequestHeader.class);

        // 从路由管理器获取成员组并编码返回
        BrokerMemberGroup memberGroup = this.namesrvController.getRouteInfoManager()
            .getBrokerMemberGroup(requestHeader.getClusterName(), requestHeader.getBrokerName());

        GetBrokerMemberGroupResponseBody responseBody = new GetBrokerMemberGroupResponseBody();
        responseBody.setBrokerMemberGroup(memberGroup);

        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        response.setBody(responseBody.encode());
        return response;
    }

    /**
     * 校验注册请求体 CRC32
     *
     * @param ctx           Netty 通道上下文
     * @param request       远程请求命令
     * @param requestHeader 注册请求头
     * @return true 表示校验通过
     */
    private boolean checksum(ChannelHandlerContext ctx, RemotingCommand request,
        RegisterBrokerRequestHeader requestHeader) {
        // 仅在请求头携带 CRC32 时执行校验
        if (requestHeader.getBodyCrc32() != 0) {
            final int crc32 = UtilAll.crc32(request.getBody());
            if (crc32 != requestHeader.getBodyCrc32()) {
                log.warn(String.format("receive registerBroker request,crc32 not match,from %s",
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel())));
                return false;
            }
        }
        return true;
    }

    /**
     * 查询 Broker 的 Topic 配置版本
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return 版本查询结果
     * @throws RemotingCommandException 请求头解码异常
     */
    public RemotingCommand queryBrokerTopicConfig(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        // 解析请求头和请求体中的 DataVersion
        final RemotingCommand response = RemotingCommand.createResponseCommand(QueryDataVersionResponseHeader.class);
        final QueryDataVersionResponseHeader responseHeader = (QueryDataVersionResponseHeader) response.readCustomHeader();
        final QueryDataVersionRequestHeader requestHeader =
            (QueryDataVersionRequestHeader) request.decodeCommandCustomHeader(QueryDataVersionRequestHeader.class);
        DataVersion dataVersion = DataVersion.decode(request.getBody(), DataVersion.class);
        String clusterName = requestHeader.getClusterName();
        String brokerAddr = requestHeader.getBrokerAddr();

        // 判断配置是否变更并刷新 Broker 心跳时间戳
        Boolean changed = this.namesrvController.getRouteInfoManager().isBrokerTopicConfigChanged(clusterName, brokerAddr, dataVersion);
        this.namesrvController.getRouteInfoManager().updateBrokerInfoUpdateTimestamp(clusterName, brokerAddr);

        // 返回 NameServer 侧最新配置版本
        DataVersion nameSeverDataVersion = this.namesrvController.getRouteInfoManager().queryBrokerTopicConfig(clusterName, brokerAddr);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        if (nameSeverDataVersion != null) {
            response.setBody(nameSeverDataVersion.encode());
        }
        responseHeader.setChanged(changed);
        return response;
    }

    /**
     * 注销 Broker
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return 注销结果
     * @throws RemotingCommandException 请求头解码异常
     */
    public RemotingCommand unregisterBroker(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        // 提交异步注销任务, 提交失败时返回系统错误
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final UnRegisterBrokerRequestHeader requestHeader = (UnRegisterBrokerRequestHeader) request.decodeCommandCustomHeader(UnRegisterBrokerRequestHeader.class);

        if (!this.namesrvController.getRouteInfoManager().submitUnRegisterBrokerRequest(requestHeader)) {
            log.warn("Couldn't submit the unregister broker request to handler, broker info: {}", requestHeader);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(null);
            return response;
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 处理 Broker 心跳, 刷新 Broker 更新时间
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return 处理结果
     * @throws RemotingCommandException 请求头解码异常
     */
    public RemotingCommand brokerHeartbeat(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        // 解析心跳并更新 Broker 最近活跃时间
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final BrokerHeartbeatRequestHeader requestHeader =
            (BrokerHeartbeatRequestHeader) request.decodeCommandCustomHeader(BrokerHeartbeatRequestHeader.class);

        this.namesrvController.getRouteInfoManager().updateBrokerInfoUpdateTimestamp(requestHeader.getClusterName(), requestHeader.getBrokerAddr());

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 查询集群和 Broker 拓扑信息
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return 集群拓扑信息
     */
    private RemotingCommand getBrokerClusterInfo(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        // 读取全部集群信息并编码返回
        byte[] content = this.namesrvController.getRouteInfoManager().getAllClusterInfo().encode();
        response.setBody(content);

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 清理指定 Broker 的写权限
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return 清理结果
     * @throws RemotingCommandException 请求头解码异常
     */
    private RemotingCommand wipeWritePermOfBroker(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        // 解析请求并执行写权限清理
        final RemotingCommand response = RemotingCommand.createResponseCommand(WipeWritePermOfBrokerResponseHeader.class);
        final WipeWritePermOfBrokerResponseHeader responseHeader = (WipeWritePermOfBrokerResponseHeader) response.readCustomHeader();
        final WipeWritePermOfBrokerRequestHeader requestHeader =
            (WipeWritePermOfBrokerRequestHeader) request.decodeCommandCustomHeader(WipeWritePermOfBrokerRequestHeader.class);

        int wipeTopicCnt = this.namesrvController.getRouteInfoManager().wipeWritePermOfBrokerByLock(requestHeader.getBrokerName());

        if (ctx != null) {
            log.info("wipe write perm of broker[{}], client: {}, {}",
                requestHeader.getBrokerName(),
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                wipeTopicCnt);
        }

        responseHeader.setWipeTopicCount(wipeTopicCnt);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 为指定 Broker 恢复写权限
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return 恢复结果
     * @throws RemotingCommandException 请求头解码异常
     */
    private RemotingCommand addWritePermOfBroker(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        // 解析请求并执行写权限恢复
        final RemotingCommand response = RemotingCommand.createResponseCommand(AddWritePermOfBrokerResponseHeader.class);
        final AddWritePermOfBrokerResponseHeader responseHeader = (AddWritePermOfBrokerResponseHeader) response.readCustomHeader();
        final AddWritePermOfBrokerRequestHeader requestHeader = (AddWritePermOfBrokerRequestHeader) request.decodeCommandCustomHeader(AddWritePermOfBrokerRequestHeader.class);

        int addTopicCnt = this.namesrvController.getRouteInfoManager().addWritePermOfBrokerByLock(requestHeader.getBrokerName());

        log.info("add write perm of broker[{}], client: {}, {}",
            requestHeader.getBrokerName(),
            RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
            addTopicCnt);

        responseHeader.setAddTopicCount(addTopicCnt);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 获取 NameServer 中全部 Topic 列表
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return Topic 列表结果
     */
    private RemotingCommand getAllTopicListFromNameserver(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        // 按开关控制是否允许拉取全量 Topic 列表
        boolean enableAllTopicList = namesrvController.getNamesrvConfig().isEnableAllTopicList();
        log.warn("getAllTopicListFromNameserver {} enable {}", ctx.channel().remoteAddress(), enableAllTopicList);
        if (enableAllTopicList) {
            byte[] body = this.namesrvController.getRouteInfoManager().getAllTopicList().encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("disable");
        }

        return response;
    }

    /**
     * 向 NameServer 注册 Topic 路由数据
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return 注册结果
     * @throws RemotingCommandException 请求头解码异常
     */
    private RemotingCommand registerTopicToNamesrv(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        final RegisterTopicRequestHeader requestHeader =
            (RegisterTopicRequestHeader) request.decodeCommandCustomHeader(RegisterTopicRequestHeader.class);

        // 请求体包含队列信息时写入路由表
        TopicRouteData topicRouteData = TopicRouteData.decode(request.getBody(), TopicRouteData.class);
        if (topicRouteData != null && topicRouteData.getQueueDatas() != null && !topicRouteData.getQueueDatas().isEmpty()) {
            this.namesrvController.getRouteInfoManager().registerTopic(requestHeader.getTopic(), topicRouteData.getQueueDatas());
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 从 NameServer 删除 Topic 路由
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return 删除结果
     * @throws RemotingCommandException 请求头解码异常
     */
    private RemotingCommand deleteTopicInNamesrv(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        // 按是否指定集群名选择删除范围
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final DeleteTopicFromNamesrvRequestHeader requestHeader =
            (DeleteTopicFromNamesrvRequestHeader) request.decodeCommandCustomHeader(DeleteTopicFromNamesrvRequestHeader.class);

        if (requestHeader.getClusterName() != null
            && !requestHeader.getClusterName().isEmpty()) {
            this.namesrvController.getRouteInfoManager().deleteTopic(requestHeader.getTopic(), requestHeader.getClusterName());
        } else {
            this.namesrvController.getRouteInfoManager().deleteTopic(requestHeader.getTopic());
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 按命名空间查询 KV 列表
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return 查询结果
     * @throws RemotingCommandException 请求头解码异常
     */
    private RemotingCommand getKVListByNamespace(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        // 查询指定命名空间并返回序列化结果
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetKVListByNamespaceRequestHeader requestHeader =
            (GetKVListByNamespaceRequestHeader) request.decodeCommandCustomHeader(GetKVListByNamespaceRequestHeader.class);

        byte[] jsonValue = this.namesrvController.getKvConfigManager().getKVListByNamespace(
            requestHeader.getNamespace());
        if (null != jsonValue) {
            response.setBody(jsonValue);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        response.setCode(ResponseCode.QUERY_NOT_FOUND);
        response.setRemark("No config item, Namespace: " + requestHeader.getNamespace());
        return response;
    }

    /**
     * 按集群查询 Topic 列表
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return 查询结果
     * @throws RemotingCommandException 请求头解码异常
     */
    private RemotingCommand getTopicsByCluster(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        // 开关关闭时拒绝返回 Topic 列表
        boolean enableTopicList = namesrvController.getNamesrvConfig().isEnableTopicList();
        if (!enableTopicList) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("disable");
            return response;
        }
        final GetTopicsByClusterRequestHeader requestHeader =
            (GetTopicsByClusterRequestHeader) request.decodeCommandCustomHeader(GetTopicsByClusterRequestHeader.class);

        TopicList topicsByCluster = this.namesrvController.getRouteInfoManager().getTopicsByCluster(requestHeader.getCluster());
        byte[] body = topicsByCluster.encode();

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    /**
     * 获取系统 Topic 列表
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return 系统 Topic 列表
     */
    private RemotingCommand getSystemTopicListFromNs(ChannelHandlerContext ctx,
        RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        // 读取系统 Topic 并直接返回
        TopicList systemTopicList = this.namesrvController.getRouteInfoManager().getSystemTopicList();
        byte[] body = systemTopicList.encode();

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 获取单元化 Topic 列表
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return 单元化 Topic 列表
     */
    private RemotingCommand getUnitTopicList(ChannelHandlerContext ctx,
        RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        boolean enableTopicList = namesrvController.getNamesrvConfig().isEnableTopicList();

        // 开关开启时返回数据, 否则返回禁用状态
        if (enableTopicList) {
            TopicList unitTopicList = this.namesrvController.getRouteInfoManager().getUnitTopics();
            byte[] body = unitTopicList.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("disable");
        }

        return response;
    }

    /**
     * 获取存在单元化订阅的 Topic 列表
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return Topic 列表结果
     */
    private RemotingCommand getHasUnitSubTopicList(ChannelHandlerContext ctx,
        RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        boolean enableTopicList = namesrvController.getNamesrvConfig().isEnableTopicList();

        // 按开关决定是否返回单元化订阅 Topic
        if (enableTopicList) {
            TopicList hasUnitSubTopicList = this.namesrvController.getRouteInfoManager().getHasUnitSubTopicList();
            byte[] body = hasUnitSubTopicList.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("disable");
        }

        return response;
    }

    /**
     * 获取存在单元化订阅但 Topic 自身非单元化的列表
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return Topic 列表结果
     */
    private RemotingCommand getHasUnitSubUnUnitTopicList(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        boolean enableTopicList = namesrvController.getNamesrvConfig().isEnableTopicList();

        // 按开关决定是否返回反向筛选的 Topic 列表
        if (enableTopicList) {
            TopicList hasUnitSubUnUnitTopicList = this.namesrvController.getRouteInfoManager().getHasUnitSubUnUnitTopicList();
            byte[] body = hasUnitSubUnUnitTopicList.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("disable");
        }

        return response;
    }

    /**
     * 更新 NameServer 配置
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return 更新结果
     */
    private RemotingCommand updateConfig(ChannelHandlerContext ctx, RemotingCommand request) {
        // 记录配置更新调用来源
        if (ctx != null) {
            log.info("updateConfig called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        }

        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        // 读取请求体并转换为 Properties
        byte[] body = request.getBody();
        if (body != null) {
            String bodyStr;
            try {
                bodyStr = new String(body, MixAll.DEFAULT_CHARSET);
            } catch (UnsupportedEncodingException e) {
                log.error("updateConfig byte array to string error: ", e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }

            Properties properties = MixAll.string2Properties(bodyStr);
            if (properties == null) {
                log.error("updateConfig MixAll.string2Properties error {}", bodyStr);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("string2Properties error");
                return response;
            }

            // 命中黑名单配置时拒绝更新
            if (validateBlackListConfigExist(properties)) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark("Can not update config in black list.");
                return response;
            }

            // 执行配置更新
            this.namesrvController.getConfiguration().update(properties);
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 获取 NameServer 全量配置
     *
     * @param ctx     Netty 通道上下文
     * @param request 远程请求命令
     * @return 配置查询结果
     */
    private RemotingCommand getConfig(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        // 读取格式化配置文本并写入响应体
        String content = this.namesrvController.getConfiguration().getAllConfigsFormatString();
        if (StringUtils.isNotBlank(content)) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("getConfig error, ", e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 校验待更新配置是否包含黑名单项
     *
     * @param properties 待更新配置
     * @return true 表示包含黑名单项
     */
    private boolean validateBlackListConfigExist(Properties properties) {
        // 遍历黑名单并检查是否存在冲突键
        for (String blackConfig : configBlackList) {
            if (properties.containsKey(blackConfig)) {
                return true;
            }
        }
        return false;
    }

}
