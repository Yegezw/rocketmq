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
package org.apache.rocketmq.common.action;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import org.apache.rocketmq.common.resource.ResourceType;

/**
 * 请求码到鉴权动作的注解定义
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface RocketMQAction {

    /**
     * 对应的请求码
     *
     * @return 请求码值
     */
    int value();

    /**
     * 资源类型
     *
     * @return 资源类型
     */
    ResourceType resource() default ResourceType.UNKNOWN;

    /**
     * 允许执行的动作集合
     *
     * @return 动作数组
     */
    Action[] action();
}
