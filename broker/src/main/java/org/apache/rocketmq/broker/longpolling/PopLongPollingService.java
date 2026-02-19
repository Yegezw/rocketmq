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

package org.apache.rocketmq.broker.longpolling;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import io.netty.channel.ChannelHandlerContext;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.CommandCallback;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.MessageFilter;

import static org.apache.rocketmq.broker.longpolling.PollingResult.NOT_POLLING;
import static org.apache.rocketmq.broker.longpolling.PollingResult.POLLING_FULL;
import static org.apache.rocketmq.broker.longpolling.PollingResult.POLLING_SUC;
import static org.apache.rocketmq.broker.longpolling.PollingResult.POLLING_TIMEOUT;

/**
 * POP 长轮询核心服务, 负责请求挂起, 过滤命中唤醒, 超时回收与资源清理
 */
public class PopLongPollingService extends ServiceThread {

    /**
     * POP 长轮询专用日志器
     */
    private static final Logger POP_LOGGER =
        LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    /**
     * broker 控制器, 提供配置, 存储与执行线程池访问能力
     */
    private final BrokerController brokerController;
    /**
     * 请求处理器, 在请求被唤醒时继续执行业务逻辑
     */
    private final NettyRequestProcessor processor;
    /**
     * topic 到消费组集合的索引表, 用于消息到达时快速定位待通知消费组
     */
    private final ConcurrentLinkedHashMap<String, ConcurrentHashMap<String, Byte>> topicCidMap;
    /**
     * 长轮询请求主存储, 键为 topic + cid + queueId
     */
    private final ConcurrentLinkedHashMap<String, ConcurrentSkipListSet<PopRequest>> pollingMap;
    /**
     * 最近一次清理无效资源的时间戳
     */
    private long lastCleanTime = 0;

    /**
     * 当前挂起请求总量统计, 用于全局限流与监控
     */
    private final AtomicLong totalPollingNum = new AtomicLong(0);
    /**
     * 请求出队策略开关, true 表示优先唤醒最新请求
     */
    private final boolean notifyLast;

    /**
     * 创建 POP 长轮询服务
     *
     * @param brokerController broker 控制器
     * @param processor 被唤醒请求的处理器
     * @param notifyLast 是否优先唤醒最新请求
     */
    public PopLongPollingService(BrokerController brokerController, NettyRequestProcessor processor, boolean notifyLast) {
        this.brokerController = brokerController;
        this.processor = processor;
        // 100000 topic default, 100000 lru topic + cid + qid, 默认容量按主题数与轮询键数分层配置
        this.topicCidMap = new ConcurrentLinkedHashMap.Builder<String, ConcurrentHashMap<String, Byte>>()
            .maximumWeightedCapacity(this.brokerController.getBrokerConfig().getPopPollingMapSize() * 2L).build();
        this.pollingMap = new ConcurrentLinkedHashMap.Builder<String, ConcurrentSkipListSet<PopRequest>>()
            .maximumWeightedCapacity(this.brokerController.getBrokerConfig().getPopPollingMapSize()).build();
        this.notifyLast = notifyLast;
    }

    @Override
    public String getServiceName() {
        if (brokerController.getBrokerConfig().isInBrokerContainer()) {
            return brokerController.getBrokerIdentity().getIdentifier() + PopLongPollingService.class.getSimpleName();
        }
        return PopLongPollingService.class.getSimpleName();
    }

    /**
     * 扫描挂起请求并处理超时唤醒, 周期性输出统计并清理无效资源
     */
    @Override
    public void run() {
        int i = 0;
        while (!this.stopped) {
            try {
                this.waitForRunning(20);
                i++;
                if (pollingMap.isEmpty()) {
                    continue;
                }
                long tmpTotalPollingNum = 0;
                for (Map.Entry<String, ConcurrentSkipListSet<PopRequest>> entry : pollingMap.entrySet()) {
                    String key = entry.getKey();
                    ConcurrentSkipListSet<PopRequest> popQ = entry.getValue();
                    if (popQ == null) {
                        continue;
                    }
                    PopRequest first;
                    do {
                        first = popQ.pollFirst();
                        if (first == null) {
                            break;
                        }
                        if (!first.isTimeout()) {
                            if (popQ.add(first)) {
                                break;
                            } else {
                                POP_LOGGER.info("polling, add fail again: {}", first);
                            }
                        }
                        if (brokerController.getBrokerConfig().isEnablePopLog()) {
                            POP_LOGGER.info("timeout , wakeUp polling : {}", first);
                        }
                        totalPollingNum.decrementAndGet();
                        wakeUp(first);
                    }
                    while (true);
                    if (i >= 100) {
                        long tmpPollingNum = popQ.size();
                        tmpTotalPollingNum = tmpTotalPollingNum + tmpPollingNum;
                        if (tmpPollingNum > 100) {
                            POP_LOGGER.info("polling queue {} , size={} ", key, tmpPollingNum);
                        }
                    }
                }

                if (i >= 100) {
                    POP_LOGGER.info("pollingMapSize={},tmpTotalSize={},atomicTotalSize={},diffSize={}",
                        pollingMap.size(), tmpTotalPollingNum, totalPollingNum.get(),
                        Math.abs(totalPollingNum.get() - tmpTotalPollingNum));
                    totalPollingNum.set(tmpTotalPollingNum);
                    i = 0;
                }

                // clean unused, 清理无效 topic 与订阅组对应的缓存项
                if (lastCleanTime == 0 || System.currentTimeMillis() - lastCleanTime > 5 * 60 * 1000) {
                    cleanUnusedResource();
                }
            } catch (Throwable e) {
                POP_LOGGER.error("checkPolling error", e);
            }
        }
        // clean all, 服务退出前主动唤醒剩余请求
        try {
            for (Map.Entry<String, ConcurrentSkipListSet<PopRequest>> entry : pollingMap.entrySet()) {
                ConcurrentSkipListSet<PopRequest> popQ = entry.getValue();
                PopRequest first;
                while ((first = popQ.pollFirst()) != null) {
                    wakeUp(first);
                }
            }
        } catch (Throwable e) {
        }
    }

    /**
     * 对外暴露简化消息到达通知接口, 默认不提供过滤上下文
     *
     * @param topic 到达消息主题
     * @param queueId 到达消息队列编号
     */
    public void notifyMessageArrivingWithRetryTopic(final String topic, final int queueId) {
        this.notifyMessageArrivingWithRetryTopic(topic, queueId, -1L, null, 0L, null, null);
    }

    /**
     * 处理重试主题到普通主题的映射, 随后触发轮询唤醒流程
     *
     * @param topic 到达消息主题, 可能是重试主题
     * @param queueId 队列编号
     * @param offset 消费队列偏移量
     * @param tagsCode 标签码
     * @param msgStoreTime 存储时间戳
     * @param filterBitMap 过滤位图
     * @param properties 消息属性
     */
    public void notifyMessageArrivingWithRetryTopic(final String topic, final int queueId, long offset,
        Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        String notifyTopic;
        if (KeyBuilder.isPopRetryTopicV2(topic)) {
            notifyTopic = KeyBuilder.parseNormalTopic(topic);
        } else {
            notifyTopic = topic;
        }
        notifyMessageArriving(notifyTopic, queueId, offset, tagsCode, msgStoreTime, filterBitMap, properties);
    }

    /**
     * 按 topic 广播到该主题下全部消费组键, 并尝试唤醒队列级与全队列级请求
     *
     * @param topic 到达消息主题
     * @param queueId 队列编号
     * @param offset 当前偏移量
     * @param tagsCode 标签码
     * @param msgStoreTime 存储时间戳
     * @param filterBitMap 过滤位图
     * @param properties 消息属性
     */
    public void notifyMessageArriving(final String topic, final int queueId, long offset,
        Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        ConcurrentHashMap<String, Byte> cids = topicCidMap.get(topic);
        if (cids == null) {
            return;
        }
        long interval = brokerController.getBrokerConfig().getPopLongPollingForceNotifyInterval();
        boolean force = interval > 0L && offset % interval == 0L;
        for (Map.Entry<String, Byte> cid : cids.entrySet()) {
            if (queueId >= 0) {
                notifyMessageArriving(topic, -1, cid.getKey(), force, tagsCode, msgStoreTime, filterBitMap, properties);
            }
            notifyMessageArriving(topic, queueId, cid.getKey(), force, tagsCode, msgStoreTime, filterBitMap, properties);
        }
    }

    /**
     * 使用默认非强制策略唤醒指定消费组的轮询请求
     *
     * @param topic 主题名
     * @param queueId 队列编号
     * @param cid 消费组名
     * @param tagsCode 标签码
     * @param msgStoreTime 存储时间戳
     * @param filterBitMap 过滤位图
     * @param properties 消息属性
     * @return true 表示至少唤醒一个请求
     */
    public boolean notifyMessageArriving(final String topic, final int queueId, final String cid,
        Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        return notifyMessageArriving(topic, queueId, cid, false, tagsCode, msgStoreTime, filterBitMap, properties, null);
    }

    /**
     * 允许指定强制唤醒标志, 在需要跳过过滤判断时直接释放请求
     *
     * @param topic 主题名
     * @param queueId 队列编号
     * @param cid 消费组名
     * @param force 是否强制唤醒
     * @param tagsCode 标签码
     * @param msgStoreTime 存储时间戳
     * @param filterBitMap 过滤位图
     * @param properties 消息属性
     * @return true 表示至少唤醒一个请求
     */
    public boolean notifyMessageArriving(final String topic, final int queueId, final String cid, boolean force,
        Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        return notifyMessageArriving(topic, queueId, cid, force, tagsCode, msgStoreTime, filterBitMap, properties, null);
    }

    /**
     * 执行单个轮询键的唤醒判断, 过滤失败时将请求重新放回等待队列
     *
     * @param topic 主题名
     * @param queueId 队列编号
     * @param cid 消费组名
     * @param force 是否强制唤醒
     * @param tagsCode 标签码
     * @param msgStoreTime 存储时间戳
     * @param filterBitMap 过滤位图
     * @param properties 消息属性
     * @param callback 可选回调, 在请求被唤醒时附加执行
     * @return true 表示请求成功唤醒
     */
    public boolean notifyMessageArriving(final String topic, final int queueId, final String cid, boolean force,
        Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties, CommandCallback callback) {
        ConcurrentSkipListSet<PopRequest> remotingCommands = pollingMap.get(KeyBuilder.buildPollingKey(topic, cid, queueId));
        if (remotingCommands == null || remotingCommands.isEmpty()) {
            return false;
        }

        PopRequest popRequest = pollRemotingCommands(remotingCommands);
        if (popRequest == null) {
            return false;
        }

        if (!force && popRequest.getMessageFilter() != null && popRequest.getSubscriptionData() != null) {
            boolean match = popRequest.getMessageFilter().isMatchedByConsumeQueue(tagsCode,
                new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
            if (match && properties != null) {
                match = popRequest.getMessageFilter().isMatchedByCommitLog(null, properties);
            }
            if (!match) {
                remotingCommands.add(popRequest);
                totalPollingNum.incrementAndGet();
                return false;
            }
        }

        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("lock release, new msg arrive, wakeUp: {}", popRequest);
        }

        return wakeUp(popRequest, callback);
    }

    /**
     * 使用默认无回调方式唤醒请求
     *
     * @param request 待唤醒请求
     * @return true 表示唤醒流程已提交
     */
    public boolean wakeUp(final PopRequest request) {
        return wakeUp(request, null);
    }

    /**
     * 将挂起请求提交到 Pull 执行线程池处理, 并可附加回调逻辑
     *
     * @param request 待唤醒请求
     * @param callback 可选附加回调
     * @return true 表示请求可执行且已提交
     */
    public boolean wakeUp(final PopRequest request, CommandCallback callback) {
        if (request == null || !request.complete()) {
            return false;
        }

        if (callback != null && request.getRemotingCommand() != null) {
            if (request.getRemotingCommand().getCallbackList() == null) {
                request.getRemotingCommand().setCallbackList(new ArrayList<>());
            }
            request.getRemotingCommand().getCallbackList().add(callback);
        }

        if (!request.getCtx().channel().isActive()) {
            return false;
        }

        Runnable run = () -> {
            try {
                final RemotingCommand response = processor.processRequest(request.getCtx(), request.getRemotingCommand());
                if (response != null) {
                    response.setOpaque(request.getRemotingCommand().getOpaque());
                    response.markResponseType();
                    NettyRemotingAbstract.writeResponse(request.getChannel(), request.getRemotingCommand(), response, future -> {
                        if (!future.isSuccess()) {
                            POP_LOGGER.error("ProcessRequestWrapper response to {} failed", request.getChannel().remoteAddress(), future.cause());
                            POP_LOGGER.error(request.toString());
                            POP_LOGGER.error(response.toString());
                        }
                    });
                }
            } catch (Exception e1) {
                POP_LOGGER.error("ExecuteRequestWhenWakeup run", e1);
            }
        };

        this.brokerController.getPullMessageExecutor().submit(
            new RequestTask(run, request.getChannel(), request.getRemotingCommand()));
        return true;
    }

    /**
     * 将请求加入长轮询队列, 不附带订阅过滤上下文
     *
     * @param ctx 网络上下文
     * @param remotingCommand 原始请求命令
     * @param requestHeader 轮询头信息
     * @return 长轮询入队结果
     */
    public PollingResult polling(final ChannelHandlerContext ctx, RemotingCommand remotingCommand,
        final PollingHeader requestHeader) {
        return this.polling(ctx, remotingCommand, requestHeader, null, null);
    }

    /**
     * 将请求加入长轮询队列并记录过滤信息, 后续由消息到达事件触发唤醒
     *
     * @param ctx 网络上下文
     * @param remotingCommand 原始请求命令
     * @param requestHeader 轮询头信息
     * @param subscriptionData 订阅数据
     * @param messageFilter 消息过滤器
     * @return 长轮询入队结果
     */
    public PollingResult polling(final ChannelHandlerContext ctx, RemotingCommand remotingCommand,
        final PollingHeader requestHeader, SubscriptionData subscriptionData, MessageFilter messageFilter) {
        if (requestHeader.getPollTime() <= 0 || this.isStopped()) {
            return NOT_POLLING;
        }
        ConcurrentHashMap<String, Byte> cids = topicCidMap.get(requestHeader.getTopic());
        if (cids == null) {
            cids = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, Byte> old = topicCidMap.putIfAbsent(requestHeader.getTopic(), cids);
            if (old != null) {
                cids = old;
            }
        }
        cids.putIfAbsent(requestHeader.getConsumerGroup(), Byte.MIN_VALUE);
        long expired = requestHeader.getBornTime() + requestHeader.getPollTime();
        final PopRequest request = new PopRequest(remotingCommand, ctx, expired, subscriptionData, messageFilter);
        boolean isFull = totalPollingNum.get() >= this.brokerController.getBrokerConfig().getMaxPopPollingSize();
        if (isFull) {
            POP_LOGGER.info("polling {}, result POLLING_FULL, total:{}", remotingCommand, totalPollingNum.get());
            return POLLING_FULL;
        }
        boolean isTimeout = request.isTimeout();
        if (isTimeout) {
            if (brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("polling {}, result POLLING_TIMEOUT", remotingCommand);
            }
            return POLLING_TIMEOUT;
        }
        String key = KeyBuilder.buildPollingKey(requestHeader.getTopic(), requestHeader.getConsumerGroup(),
            requestHeader.getQueueId());
        ConcurrentSkipListSet<PopRequest> queue = pollingMap.get(key);
        if (queue == null) {
            queue = new ConcurrentSkipListSet<>(PopRequest.COMPARATOR);
            ConcurrentSkipListSet<PopRequest> old = pollingMap.putIfAbsent(key, queue);
            if (old != null) {
                queue = old;
            }
        } else {
            // check size, 检查单键挂起队列大小是否超过上限
            int size = queue.size();
            if (size > brokerController.getBrokerConfig().getPopPollingSize()) {
                POP_LOGGER.info("polling {}, result POLLING_FULL, singleSize:{}", remotingCommand, size);
                return POLLING_FULL;
            }
        }
        if (queue.add(request)) {
            remotingCommand.setSuspended(true);
            totalPollingNum.incrementAndGet();
            if (brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("polling {}, result POLLING_SUC", remotingCommand);
            }
            return POLLING_SUC;
        } else {
            POP_LOGGER.info("polling {}, result POLLING_FULL, add fail, {}", request, queue);
            return POLLING_FULL;
        }
    }

    public ConcurrentLinkedHashMap<String, ConcurrentSkipListSet<PopRequest>> getPollingMap() {
        return pollingMap;
    }

    /**
     * 清理已删除 topic 或订阅组关联的缓存键, 防止轮询映射长期膨胀
     */
    private void cleanUnusedResource() {
        try {
            {
                Iterator<Map.Entry<String, ConcurrentHashMap<String, Byte>>> topicCidMapIter = topicCidMap.entrySet().iterator();
                while (topicCidMapIter.hasNext()) {
                    Map.Entry<String, ConcurrentHashMap<String, Byte>> entry = topicCidMapIter.next();
                    String topic = entry.getKey();
                    if (brokerController.getTopicConfigManager().selectTopicConfig(topic) == null) {
                        POP_LOGGER.info("remove nonexistent topic {} in topicCidMap!", topic);
                        topicCidMapIter.remove();
                        continue;
                    }
                    Iterator<Map.Entry<String, Byte>> cidMapIter = entry.getValue().entrySet().iterator();
                    while (cidMapIter.hasNext()) {
                        Map.Entry<String, Byte> cidEntry = cidMapIter.next();
                        String cid = cidEntry.getKey();
                        if (!brokerController.getSubscriptionGroupManager().containsSubscriptionGroup(cid)) {
                            POP_LOGGER.info("remove nonexistent subscription group {} of topic {} in topicCidMap!", cid, topic);
                            cidMapIter.remove();
                        }
                    }
                }
            }

            {
                Iterator<Map.Entry<String, ConcurrentSkipListSet<PopRequest>>> pollingMapIter = pollingMap.entrySet().iterator();
                while (pollingMapIter.hasNext()) {
                    Map.Entry<String, ConcurrentSkipListSet<PopRequest>> entry = pollingMapIter.next();
                    if (entry.getKey() == null) {
                        continue;
                    }
                    String[] keyArray = entry.getKey().split(PopAckConstants.SPLIT);
                    if (keyArray.length != 3) {
                        continue;
                    }
                    String topic = keyArray[0];
                    String cid = keyArray[1];
                    if (brokerController.getTopicConfigManager().selectTopicConfig(topic) == null) {
                        POP_LOGGER.info("remove nonexistent topic {} in pollingMap!", topic);
                        pollingMapIter.remove();
                        continue;
                    }
                    if (!brokerController.getSubscriptionGroupManager().containsSubscriptionGroup(cid)) {
                        POP_LOGGER.info("remove nonexistent subscription group {} of topic {} in pollingMap!", cid, topic);
                        pollingMapIter.remove();
                    }
                }
            }
        } catch (Throwable e) {
            POP_LOGGER.error("cleanUnusedResource", e);
        }

        lastCleanTime = System.currentTimeMillis();
    }

    /**
     * 按配置从队列头或尾选择可用请求, 并跳过连接失效请求
     *
     * @param remotingCommands 目标轮询队列
     * @return 可用请求, 若不存在则返回 null
     */
    private PopRequest pollRemotingCommands(ConcurrentSkipListSet<PopRequest> remotingCommands) {
        if (remotingCommands == null || remotingCommands.isEmpty()) {
            return null;
        }

        PopRequest popRequest;
        do {
            if (notifyLast) {
                popRequest = remotingCommands.pollLast();
            } else {
                popRequest = remotingCommands.pollFirst();
            }
            totalPollingNum.decrementAndGet();
        } while (popRequest != null && !popRequest.getChannel().isActive());

        return popRequest;
    }
}
