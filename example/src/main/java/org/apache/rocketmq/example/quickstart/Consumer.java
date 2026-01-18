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
package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.example.Env;

/**
 * This example shows how to subscribe and consume messages using providing {@link DefaultMQPushConsumer}.
 */
public class Consumer {

    public static final String CONSUMER_GROUP = "please_rename_unique_group_name_4";
    public static final String DEFAULT_NAMESRVADDR = Env.DEFAULT_NAMESRVADDR;
    public static final String TOPIC = "TopicTest";

    public static void main(String[] args) throws MQClientException {

        /*
         * Instantiate with specified consumer group name.
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        System.out.println(consumer.getMessageModel());
        System.out.println(consumer.getConsumeThreadMin());
        System.out.println(consumer.getConsumeThreadMax());

        /*
         * Specify name server addresses.
         * <p/>
         *
         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
         * <pre>
         * {@code
         * consumer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
         * }
         * </pre>
         */
        // Uncomment the following line while debugging, namesrvAddr should be set to your local address
        consumer.setNamesrvAddr(DEFAULT_NAMESRVADDR);

        /*
         * Specify where to start in case the specific consumer group is a brand-new one.
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        /*
         * Subscribe one more topic to consume.
         */
        consumer.subscribe(TOPIC, "*");

        /*
         * Register callback to execute on arrival of messages fetched from brokers.
         * 
         * ConsumeConcurrentlyStatus.CONSUME_SUCCESS - 消费成功
         * Broker 会删除消息或标记为已消费
         * 消费位点向前推进
         * 该批消息不会再投递给当前消费组
         * 
         * ConsumeConcurrentlyStatus.RECONSUME_LATER - 稍后重新消费
         * 消息会重新投递（进入重试队列）
         * 消费位点不推进
         * 默认重试 16 次，每次间隔递增（10s → 30s → 1min → ... → 2h）
         * 超过最大重试次数后进入死信队列（DLQ）
         * 
         * 幂等性要求 - 因为消息可能重试，业务逻辑必须支持幂等（使用消息 MsgId 或业务 Key 去重）
         * 异常处理 - 如果 Listener 抛出异常未捕获，RocketMQ 会自动返回 RECONSUME_LATER
         * 批量消息 - 一批消息只能返回一个状态，如果部分成功部分失败，建议拆分消费或全部返回 RECONSUME_LATER
         * 
         * 顺序消费使用的是不同枚举 ConsumeOrderlyStatus
         * SUCCESS - 成功并提交
         * SUSPEND_CURRENT_QUEUE_A_MOMENT - 暂停当前队列消费（失败场景）
         */
        consumer.registerMessageListener((MessageListenerConcurrently) (msg, context) -> {
            System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msg);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        /*
         *  Launch the consumer instance.
         */
        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
