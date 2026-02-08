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

package org.apache.rocketmq.remoting;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.remoting.protocol.DataVersion;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 远程模块配置聚合器<br>
 * 用于统一注册配置对象, 合并扩展配置, 序列化持久化与版本递增
 */
public class Configuration {

    /**
     * 日志组件<br>
     * 用于记录配置注册, 更新, 持久化过程中的异常与替换信息
     */
    private final Logger log;

    /**
     * 已注册配置对象列表<br>
     * 列表中的对象会在序列化与更新时参与反射映射
     */
    private List<Object> configObjectList = new ArrayList<>(4);
    /**
     * 配置持久化文件路径<br>
     * 当未启用从配置对象读取路径时直接使用该值
     */
    private String storePath;
    /**
     * 持久化路径来源开关<br>
     * true 表示通过 storePathObject 的字段动态读取路径
     */
    private boolean storePathFromConfig = false;
    /**
     * 动态提供存储路径的配置对象<br>
     * 与 storePathField 配合使用
     */
    private Object storePathObject;
    /**
     * 存储路径字段反射句柄<br>
     * 指向 storePathObject 中实际保存路径的字段
     */
    private Field storePathField;
    /**
     * 配置版本信息<br>
     * 每次 update 成功后执行 nextVersion
     */
    private DataVersion dataVersion = new DataVersion();
    /**
     * 读写锁<br>
     * 读操作与写操作通过该锁协调并发访问
     */
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    /**
     * All properties include configs in object and extend properties.<br>
     * 所有配置属性集合, 同时包含配置对象属性与外部扩展属性
     */
    private Properties allConfigs = new Properties();

    /**
     * 构造 Configuration<br>
     * 仅初始化日志实例
     */
    public Configuration(Logger log) {
        this.log = log;
    }

    /**
     * 构造 Configuration 并批量注册配置对象<br>
     * 传入 null 或空数组时仅完成基础初始化
     */
    public Configuration(Logger log, Object... configObjects) {
        this.log = log;
        if (configObjects == null || configObjects.length == 0) {
            return;
        }
        for (Object configObject : configObjects) {
            if (configObject == null) {
                continue;
            }
            registerConfig(configObject);
        }
    }

    /**
     * 构造 Configuration 并指定持久化路径<br>
     * 先执行对象注册流程, 再覆盖 storePath
     */
    public Configuration(Logger log, String storePath, Object... configObjects) {
        this(log, configObjects);
        this.storePath = storePath;
    }

    /**
     * register config object<br>
     * 注册配置对象, 将对象字段反射转换为属性并合并到 allConfigs
     *
     * @return the current Configuration object<br>
     * 返回当前 Configuration 对象, 便于链式调用
     */
    public Configuration registerConfig(Object configObject) {
        try {
            readWriteLock.writeLock().lockInterruptibly();

            try {
                // 将配置对象转换为 Properties 结构
                Properties registerProps = MixAll.object2Properties(configObject);
                // 合并到全量配置, 同名键会被新值覆盖
                merge(registerProps, this.allConfigs);
                // 记录对象, 后续更新与导出时需要再次反射读取
                configObjectList.add(configObject);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("registerConfig lock error");
        }
        return this;
    }

    /**
     * register config properties<br>
     * 注册外部扩展属性, 直接并入全量配置
     *
     * @return the current Configuration object<br>
     * 返回当前 Configuration 对象, 便于链式调用
     */
    public Configuration registerConfig(Properties extProperties) {
        if (extProperties == null) {
            return this;
        }

        try {
            readWriteLock.writeLock().lockInterruptibly();

            try {
                merge(extProperties, this.allConfigs);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("register lock error. {}" + extProperties);
        }

        return this;
    }

    /**
     * The store path will be gotten from the field of object.<br>
     * 存储路径从对象字段中动态读取
     *
     * @throws java.lang.RuntimeException if the field of object is not exist.<br>
     * 当对象中不存在指定字段时抛出运行时异常
     */
    public void setStorePathFromConfig(Object object, String fieldName) {
        assert object != null;

        try {
            readWriteLock.writeLock().lockInterruptibly();

            try {
                this.storePathFromConfig = true;
                this.storePathObject = object;
                // check
                // 校验字段存在且不是静态字段
                this.storePathField = object.getClass().getDeclaredField(fieldName);
                assert this.storePathField != null
                    && !Modifier.isStatic(this.storePathField.getModifiers());
                this.storePathField.setAccessible(true);
            } catch (NoSuchFieldException e) {
                throw new RuntimeException(e);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("setStorePathFromConfig lock error");
        }
    }

    private String getStorePath() {
        String realStorePath = null;
        try {
            readWriteLock.readLock().lockInterruptibly();

            try {
                realStorePath = this.storePath;

                if (this.storePathFromConfig) {
                    try {
                        realStorePath = (String) storePathField.get(this.storePathObject);
                    } catch (IllegalAccessException e) {
                        log.error("getStorePath error, ", e);
                    }
                }
            } finally {
                readWriteLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getStorePath lock error");
        }

        return realStorePath;
    }

    public void setStorePath(final String storePath) {
        this.storePath = storePath;
    }

    /**
     * 更新配置<br>
     * 仅更新 allConfigs 中已存在的键, 并将新值回写到已注册配置对象
     */
    public void update(Properties properties) {
        try {
            readWriteLock.writeLock().lockInterruptibly();

            try {
                // the property must be exist when update
                // 更新时仅允许修改已存在属性
                mergeIfExist(properties, this.allConfigs);

                for (Object configObject : configObjectList) {
                    // not allConfigs to update...
                    // 使用增量属性更新配置对象, 避免无关属性覆盖
                    MixAll.properties2Object(properties, configObject);
                }
                // 配置成功更新后推进数据版本
                this.dataVersion.nextVersion();

            } finally {
                readWriteLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("update lock error, {}", properties);
            return;
        }

        persist();
    }

    /**
     * 持久化全量配置到文件<br>
     * 写入内容由 getAllConfigsInternal 实时组装生成
     */
    public void persist() {
        try {
            readWriteLock.readLock().lockInterruptibly();

            try {
                String allConfigs = getAllConfigsInternal();

                MixAll.string2File(allConfigs, getStorePath());
            } catch (IOException e) {
                log.error("persist string2File error, ", e);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("persist lock error");
        }
    }

    public String getAllConfigsFormatString() {
        try {
            readWriteLock.readLock().lockInterruptibly();

            try {

                return getAllConfigsInternal();

            } finally {
                readWriteLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getAllConfigsFormatString lock error");
        }

        return null;
    }

    public String getClientConfigsFormatString(List<String> clientKeys) {
        try {
            readWriteLock.readLock().lockInterruptibly();

            try {

                return getClientConfigsInternal(clientKeys);

            } finally {
                readWriteLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getAllConfigsFormatString lock error");
        }

        return null;
    }

    public String getDataVersionJson() {
        return this.dataVersion.toJson();
    }

    public Properties getAllConfigs() {
        try {
            readWriteLock.readLock().lockInterruptibly();

            try {

                return this.allConfigs;

            } finally {
                readWriteLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getAllConfigs lock error");
        }

        return null;
    }

    /**
     * 根据已注册配置对象刷新并组装全量配置文本<br>
     * 输出格式为排序后的 key=value 列表
     */
    private String getAllConfigsInternal() {
        StringBuilder stringBuilder = new StringBuilder();

        // reload from config object ?
        // 从配置对象重新提取属性, 保证内存值与对象当前状态一致
        for (Object configObject : this.configObjectList) {
            Properties properties = MixAll.object2Properties(configObject);
            if (properties != null) {
                merge(properties, this.allConfigs);
            } else {
                log.warn("getAllConfigsInternal object2Properties is null, {}", configObject.getClass());
            }
        }

        {
            stringBuilder.append(MixAll.properties2String(this.allConfigs, true));
        }

        return stringBuilder.toString();
    }

    /**
     * 按客户端关注键提取配置并组装文本<br>
     * 仅输出 clientConigKeys 中出现的键值对
     */
    private String getClientConfigsInternal(List<String> clientConigKeys) {
        StringBuilder stringBuilder = new StringBuilder();
        Properties clientProperties = new Properties();

        // reload from config object ?
        // 从配置对象重新提取属性, 再按客户端键集合做过滤
        for (Object configObject : this.configObjectList) {
            Properties properties = MixAll.object2Properties(configObject);

            for (String nameNow : clientConigKeys) {
                if (properties.containsKey(nameNow)) {
                    clientProperties.put(nameNow, properties.get(nameNow));
                }
            }

        }
        stringBuilder.append(MixAll.properties2String(clientProperties));

        return stringBuilder.toString();
    }

    /**
     * 合并属性<br>
     * from 中全部键值都会写入 to, 同名键会被覆盖
     */
    private void merge(Properties from, Properties to) {
        for (Entry<Object, Object> next : from.entrySet()) {
            Object fromObj = next.getValue(), toObj = to.get(next.getKey());
            if (toObj != null && !toObj.equals(fromObj)) {
                log.info("Replace, key: {}, value: {} -> {}", next.getKey(), toObj, fromObj);
            }
            to.put(next.getKey(), fromObj);
        }
    }

    /**
     * 按已存在键合并属性<br>
     * 仅当 to 已包含该键时才执行覆盖
     */
    private void mergeIfExist(Properties from, Properties to) {
        for (Entry<Object, Object> next : from.entrySet()) {
            if (!to.containsKey(next.getKey())) {
                continue;
            }

            Object fromObj = next.getValue(), toObj = to.get(next.getKey());
            if (toObj != null && !toObj.equals(fromObj)) {
                log.info("Replace, key: {}, value: {} -> {}", next.getKey(), toObj, fromObj);
            }
            to.put(next.getKey(), fromObj);
        }
    }

}
