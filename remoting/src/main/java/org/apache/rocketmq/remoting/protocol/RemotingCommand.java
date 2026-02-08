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
package org.apache.rocketmq.remoting.protocol;

import com.alibaba.fastjson.annotation.JSONField;
import com.google.common.base.Stopwatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.CommandCallback;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/*
 * 长度的语义为 "再向后读 length 字节"
 * Message Length (4 byte) | Serialization Type (1 byte) + Header Length (3 byte)
 * Data Header             | Message Body
 * ROCKETMQ 序列化方式下的消息头包括
 * code   (2 byte) | language (1 byte) | version (2 byte)
 * opaque (4 byte) | flag     (4 byte) | remark  (4 + ? byte) | extFields (4 + ? byte)
 */

/**
 * 远程协议命令模型<br>
 * 统一承载请求与响应的头字段, 扩展字段, 序列化类型与消息体
 */
public class RemotingCommand {
    /**
     * 序列化类型系统属性键
     */
    public static final String SERIALIZE_TYPE_PROPERTY = "rocketmq.serialize.type";
    /**
     * 序列化类型环境变量键
     */
    public static final String SERIALIZE_TYPE_ENV = "ROCKETMQ_SERIALIZE_TYPE";
    /**
     * 协议版本系统属性键
     */
    public static final String REMOTING_VERSION_KEY = "rocketmq.remoting.version";
    /**
     * remoting 模块日志器
     */
    static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);
    /**
     * flag 中响应位索引
     */
    private static final int RPC_TYPE = 0; // 0, REQUEST_COMMAND
    // 0, 请求命令
    /**
     * flag 中单向调用位索引
     */
    private static final int RPC_ONEWAY = 1; // 0, RPC
    // 0, 双向调用
    /**
     * 自定义头字段缓存<br>
     * key 为头类型, value 为字段数组
     */
    private static final Map<Class<? extends CommandCustomHeader>, Field[]> CLASS_HASH_MAP =
        new HashMap<>();
    /**
     * 类 canonicalName 缓存<br>
     * 用于减少反射调用成本
     */
    private static final Map<Class, String> CANONICAL_NAME_CACHE = new HashMap<>();
    // 1, Oneway           单向调用
    // 1, RESPONSE_COMMAND 响应命令
    /**
     * 字段可空性缓存<br>
     * key 为字段对象, value 为是否可空
     */
    private static final Map<Field, Boolean> NULLABLE_FIELD_CACHE = new HashMap<>();
    /**
     * String 类型 canonicalName 缓存值
     */
    private static final String STRING_CANONICAL_NAME = String.class.getCanonicalName();
    /**
     * Double 包装类型 canonicalName 缓存值
     */
    private static final String DOUBLE_CANONICAL_NAME_1 = Double.class.getCanonicalName();
    /**
     * double 基本类型 canonicalName 缓存值
     */
    private static final String DOUBLE_CANONICAL_NAME_2 = double.class.getCanonicalName();
    /**
     * Integer 包装类型 canonicalName 缓存值
     */
    private static final String INTEGER_CANONICAL_NAME_1 = Integer.class.getCanonicalName();
    /**
     * int 基本类型 canonicalName 缓存值
     */
    private static final String INTEGER_CANONICAL_NAME_2 = int.class.getCanonicalName();
    /**
     * Long 包装类型 canonicalName 缓存值
     */
    private static final String LONG_CANONICAL_NAME_1 = Long.class.getCanonicalName();
    /**
     * long 基本类型 canonicalName 缓存值
     */
    private static final String LONG_CANONICAL_NAME_2 = long.class.getCanonicalName();
    /**
     * Boolean 包装类型 canonicalName 缓存值
     */
    private static final String BOOLEAN_CANONICAL_NAME_1 = Boolean.class.getCanonicalName();
    /**
     * boolean 基本类型 canonicalName 缓存值
     */
    private static final String BOOLEAN_CANONICAL_NAME_2 = boolean.class.getCanonicalName();
    /**
     * BoundaryType 类型 canonicalName 缓存值
     */
    private static final String BOUNDARY_TYPE_CANONICAL_NAME = BoundaryType.class.getCanonicalName();
    /**
     * 协议版本缓存<br>
     * 小于 0 表示尚未初始化
     */
    private static volatile int configVersion = -1;
    /**
     * 请求序号生成器
     */
    private static AtomicInteger requestId = new AtomicInteger(0);

    /**
     * 消息头的序列化方式
     */
    private static SerializeType serializeTypeConfigInThisServer = SerializeType.JSON;

    /*
     * 静态初始化逻辑
     * 从系统属性或环境变量读取序列化类型并覆盖默认值
     */
    static {
        final String protocol = System.getProperty(SERIALIZE_TYPE_PROPERTY, System.getenv(SERIALIZE_TYPE_ENV));
        if (!StringUtils.isBlank(protocol)) {
            try {
                serializeTypeConfigInThisServer = SerializeType.valueOf(protocol);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("parser specified protocol error. protocol=" + protocol, e);
            }
        }
    }

    /**
     * {@link RequestCode 请求类型} OR {@link ResponseCode 响应类型}
     */
    private int code;
    /**
     * 语言类型<br>
     * 默认使用 JAVA
     */
    private LanguageCode language = LanguageCode.JAVA;
    /**
     * 协议版本
     */
    private int version = 0;
    /**
     * 请求唯一标识<br>
     * 默认由 requestId 自增生成
     */
    private int opaque = requestId.getAndIncrement();
    /**
     * 命令标志位<br>
     * 0 表示请求、1 表示响应、2 表示单向请求
     */
    private int flag = 0;
    /**
     * 备注信息<br>
     * 常用于响应错误描述
     */
    private String remark;
    /**
     * 扩展字段<br>
     * 保存自定义头的扁平化键值
     */
    private HashMap<String, String> extFields;
    /**
     * 发送命令时用到的头对象, 编码时写入 extFields 随命令发送
     */
    private transient CommandCustomHeader customHeader;
    /**
     * 自定义头解析缓存对象<br>
     * 调用 decodeCommandCustomHeader 时从 extFields 反解并缓存, 用于避免重复解析
     */
    private transient CommandCustomHeader cachedHeader;

    /**
     * 消息头的序列化方式
     */
    private SerializeType serializeTypeCurrentRPC = serializeTypeConfigInThisServer;

    /**
     * 请求内容 OR 响应内容<br>
     * transient 表示不经 JSON 直接字段序列化
     */
    private transient byte[] body;
    /**
     * 是否挂起
     */
    private boolean suspended;
    /**
     * 处理耗时计时器
     */
    private transient Stopwatch processTimer;
    /**
     * 命令回调列表
     */
    private transient List<CommandCallback> callbackList;

    /**
     * 默认构造方法<br>
     * 仅供工厂方法与反序列化流程使用
     */
    protected RemotingCommand() {
    }

    /**
     * 创建请求命令<br>
     * 初始化 code, customHeader, version
     */
    public static RemotingCommand createRequestCommand(int code, CommandCustomHeader customHeader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.customHeader = customHeader;
        setCmdVersion(cmd);
        return cmd;
    }

    /**
     * 创建携带自定义头的响应命令
     */
    public static RemotingCommand createResponseCommandWithHeader(int code, CommandCustomHeader customHeader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.markResponseType();
        cmd.customHeader = customHeader;
        setCmdVersion(cmd);
        return cmd;
    }

    /**
     * 设置命令版本<br>
     * 优先使用缓存版本, 首次从系统属性读取并缓存
     */
    protected static void setCmdVersion(RemotingCommand cmd) {
        if (configVersion >= 0) {
            cmd.setVersion(configVersion);
        } else {
            String v = System.getProperty(REMOTING_VERSION_KEY);
            if (v != null) {
                int value = Integer.parseInt(v);
                cmd.setVersion(value);
                configVersion = value;
            }
        }
    }

    /**
     * 创建默认系统错误响应命令
     */
    public static RemotingCommand createResponseCommand(Class<? extends CommandCustomHeader> classHeader) {
        return createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, "not set any response code", classHeader);
    }

    /**
     * 构建错误响应命令<br>
     * 支持指定响应码, 备注, 自定义头类型
     */
    public static RemotingCommand buildErrorResponse(int code, String remark,
        Class<? extends CommandCustomHeader> classHeader) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(classHeader);
        response.setCode(code);
        response.setRemark(remark);
        return response;
    }

    /**
     * 构建错误响应命令<br>
     * 不携带自定义头
     */
    public static RemotingCommand buildErrorResponse(int code, String remark) {
        return buildErrorResponse(code, remark, null);
    }

    /**
     * 创建响应命令<br>
     * classHeader 非空时反射实例化 customHeader
     */
    public static RemotingCommand createResponseCommand(int code, String remark,
        Class<? extends CommandCustomHeader> classHeader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.markResponseType();
        cmd.setCode(code);
        cmd.setRemark(remark);
        setCmdVersion(cmd);

        if (classHeader != null) {
            try {
                CommandCustomHeader objectHeader = classHeader.getDeclaredConstructor().newInstance();
                cmd.customHeader = objectHeader;
            } catch (InstantiationException e) {
                return null;
            } catch (IllegalAccessException e) {
                return null;
            } catch (InvocationTargetException e) {
                return null;
            } catch (NoSuchMethodException e) {
                return null;
            }
        }

        return cmd;
    }

    /**
     * 创建响应命令<br>
     * 不携带自定义头类型
     */
    public static RemotingCommand createResponseCommand(int code, String remark) {
        return createResponseCommand(code, remark, null);
    }

    /**
     * 从字节数组解码命令
     */
    public static RemotingCommand decode(final byte[] array) throws RemotingCommandException {
        ByteBuffer byteBuffer = ByteBuffer.wrap(array);
        return decode(byteBuffer);
    }

    /**
     * 从 ByteBuffer 解码命令<br>
     * 内部转换为 ByteBuf 复用统一解码流程
     */
    public static RemotingCommand decode(final ByteBuffer byteBuffer) throws RemotingCommandException {
        return decode(Unpooled.wrappedBuffer(byteBuffer));
    }

    /**
     * 从 ByteBuf 解码命令<br>
     * 先解析头, 再读取可选消息体
     */
    public static RemotingCommand decode(final ByteBuf byteBuffer) throws RemotingCommandException {
        int length = byteBuffer.readableBytes();
        int oriHeaderLen = byteBuffer.readInt();
        int headerLength = getHeaderLength(oriHeaderLen);
        if (headerLength > length - 4) {
            throw new RemotingCommandException("decode error, bad header length: " + headerLength);
        }

        RemotingCommand cmd = headerDecode(byteBuffer, headerLength, getProtocolType(oriHeaderLen));

        int bodyLength = length - 4 - headerLength;
        byte[] bodyData = null;
        if (bodyLength > 0) {
            bodyData = new byte[bodyLength];
            byteBuffer.readBytes(bodyData);
        }
        cmd.body = bodyData;

        return cmd;
    }

    /**
     * 提取头长度<br>
     * 低 24 位为头长度
     */
    public static int getHeaderLength(int length) {
        return length & 0xFFFFFF;
    }

    /**
     * 解码命令头<br>
     * 按序列化类型分派到不同协议实现
     */
    private static RemotingCommand headerDecode(ByteBuf byteBuffer, int len,
        SerializeType type) throws RemotingCommandException {
        switch (type) {
            case JSON:
                byte[] headerData = new byte[len];
                byteBuffer.readBytes(headerData);
                RemotingCommand resultJson = RemotingSerializable.decode(headerData, RemotingCommand.class);
                resultJson.setSerializeTypeCurrentRPC(type);
                return resultJson;
            case ROCKETMQ:
                RemotingCommand resultRMQ = RocketMQSerializable.rocketMQProtocolDecode(byteBuffer, len);
                resultRMQ.setSerializeTypeCurrentRPC(type);
                return resultRMQ;
            default:
                break;
        }

        return null;
    }

    /**
     * 提取协议类型<br>
     * 高 8 位存储协议枚举码
     */
    public static SerializeType getProtocolType(int source) {
        return SerializeType.valueOf((byte) ((source >> 24) & 0xFF));
    }

    /**
     * 生成新请求 id
     */
    public static int createNewRequestId() {
        return requestId.getAndIncrement();
    }

    public static SerializeType getSerializeTypeConfigInThisServer() {
        return serializeTypeConfigInThisServer;
    }

    /**
     * 标记协议类型与头长度<br>
     * 协议类型写入高 8 位, 长度保留低 24 位
     */
    public static int markProtocolType(int source, SerializeType type) {
        return (type.getCode() << 24) | (source & 0x00FFFFFF);
    }

    /**
     * 标记命令为响应类型
     */
    public void markResponseType() {
        int bits = 1 << RPC_TYPE;
        this.flag |= bits;
    }

    /**
     * 读取自定义头对象
     */
    public CommandCustomHeader readCustomHeader() {
        return customHeader;
    }

    /**
     * 写入自定义头对象
     */
    public void writeCustomHeader(CommandCustomHeader customHeader) {
        this.customHeader = customHeader;
    }

    /**
     * 根据 {@link RemotingCommand#extFields} 创建 {@link RemotingCommand#cachedHeader 自定义头解析缓存对象}
     */
    public <T extends CommandCustomHeader> T decodeCommandCustomHeader(
        Class<T> classHeader) throws RemotingCommandException {
        return decodeCommandCustomHeader(classHeader, false);
    }

    /**
     * 解析自定义头<br>
     * 可选复用缓存结果, 避免重复反射开销
     */
    public <T extends CommandCustomHeader> T decodeCommandCustomHeader(
        Class<T> classHeader, boolean isCached) throws RemotingCommandException {
        if (isCached && cachedHeader != null) {
            return classHeader.cast(cachedHeader);
        }
        cachedHeader = decodeCommandCustomHeaderDirectly(classHeader, true);
        if (cachedHeader == null) {
            return null;
        }
        return classHeader.cast(cachedHeader);
    }

    /**
     * 直接解析自定义头<br>
     * 支持 FastCodesHeader 快速解码路径
     */
    public <T extends CommandCustomHeader> T decodeCommandCustomHeaderDirectly(Class<T> classHeader,
        boolean useFastEncode) throws RemotingCommandException {
        T objectHeader;
        try {
            objectHeader = classHeader.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            return null;
        }

        if (this.extFields != null) {
            if (objectHeader instanceof FastCodesHeader && useFastEncode) {
                ((FastCodesHeader) objectHeader).decode(this.extFields);
                objectHeader.checkFields();
                return objectHeader;
            }

            Field[] fields = getClazzFields(classHeader);
            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    String fieldName = field.getName();
                    if (!fieldName.startsWith("this")) {
                        try {
                            String value = this.extFields.get(fieldName);
                            if (null == value) {
                                if (!isFieldNullable(field)) {
                                    throw new RemotingCommandException("the custom field <" + fieldName + "> is null");
                                }
                                continue;
                            }

                            field.setAccessible(true);
                            String type = getCanonicalName(field.getType());
                            Object valueParsed;

                            if (type.equals(STRING_CANONICAL_NAME)) {
                                valueParsed = value;
                            } else if (type.equals(INTEGER_CANONICAL_NAME_1) || type.equals(INTEGER_CANONICAL_NAME_2)) {
                                valueParsed = Integer.parseInt(value);
                            } else if (type.equals(LONG_CANONICAL_NAME_1) || type.equals(LONG_CANONICAL_NAME_2)) {
                                valueParsed = Long.parseLong(value);
                            } else if (type.equals(BOOLEAN_CANONICAL_NAME_1) || type.equals(BOOLEAN_CANONICAL_NAME_2)) {
                                valueParsed = Boolean.parseBoolean(value);
                            } else if (type.equals(DOUBLE_CANONICAL_NAME_1) || type.equals(DOUBLE_CANONICAL_NAME_2)) {
                                valueParsed = Double.parseDouble(value);
                            } else if (type.equals(BOUNDARY_TYPE_CANONICAL_NAME)) {
                                valueParsed = BoundaryType.getType(value);
                            } else {
                                throw new RemotingCommandException("the custom field <" + fieldName + "> type is not supported");
                            }

                            field.set(objectHeader, valueParsed);

                        } catch (Throwable e) {
                            log.error("Failed field [{}] decoding", fieldName, e);
                        }
                    }
                }
            }

            objectHeader.checkFields();
        }

        return objectHeader;
    }

    /**
     * 获取指定自定义头类型的字段集合<br>
     * 结果会写入 CLASS_HASH_MAP 缓存
     */
    //make it able to test
    // 保持包级可见, 便于测试直接调用
    Field[] getClazzFields(Class<? extends CommandCustomHeader> classHeader) {
        Field[] field = CLASS_HASH_MAP.get(classHeader);

        if (field == null) {
            Set<Field> fieldList = new HashSet<>();
            for (Class className = classHeader; className != Object.class; className = className.getSuperclass()) {
                Field[] fields = className.getDeclaredFields();
                fieldList.addAll(Arrays.asList(fields));
            }
            field = fieldList.toArray(new Field[0]);
            synchronized (CLASS_HASH_MAP) {
                CLASS_HASH_MAP.put(classHeader, field);
            }
        }
        return field;
    }

    /**
     * 判断字段是否可空<br>
     * 依据 CFNotNull 注解并缓存判断结果
     */
    private boolean isFieldNullable(Field field) {
        if (!NULLABLE_FIELD_CACHE.containsKey(field)) {
            Annotation annotation = field.getAnnotation(CFNotNull.class);
            synchronized (NULLABLE_FIELD_CACHE) {
                NULLABLE_FIELD_CACHE.put(field, annotation == null);
            }
        }
        return NULLABLE_FIELD_CACHE.get(field);
    }

    /**
     * 获取类型 canonicalName<br>
     * 通过缓存减少重复反射调用
     */
    private String getCanonicalName(Class clazz) {
        String name = CANONICAL_NAME_CACHE.get(clazz);

        if (name == null) {
            name = clazz.getCanonicalName();
            synchronized (CANONICAL_NAME_CACHE) {
                CANONICAL_NAME_CACHE.put(clazz, name);
            }
        }
        return name;
    }

    /**
     * 编码完整命令<br>
     * 输出结构为总长度 + 头标记 + 头数据 + 可选体数据
     */
    public ByteBuffer encode() {
        // 1> header length size
        // 1> 头长度字段占位
        int length = 4;

        // 2> header data length
        // 2> 头数据长度
        byte[] headerData = this.headerEncode();
        length += headerData.length;

        // 3> body data length
        // 3> 消息体长度
        if (this.body != null) {
            length += body.length;
        }

        ByteBuffer result = ByteBuffer.allocate(4 + length);

        // length
        // 写入总长度
        result.putInt(length);

        // header length
        // 写入协议类型与头长度标记
        result.putInt(markProtocolType(headerData.length, serializeTypeCurrentRPC));

        // header data
        // 写入头数据
        result.put(headerData);

        // body data;
        // 写入消息体数据
        if (this.body != null) {
            result.put(this.body);
        }

        result.flip();

        return result;
    }

    /**
     * 编码命令头<br>
     * 根据序列化类型选择 JSON 或 ROCKETMQ 编码器
     */
    private byte[] headerEncode() {
        this.makeCustomHeaderToNet();
        if (SerializeType.ROCKETMQ == serializeTypeCurrentRPC) {
            return RocketMQSerializable.rocketMQProtocolEncode(this);
        } else {
            return RemotingSerializable.encode(this);
        }
    }

    /**
     * 将 {@link RemotingCommand#customHeader 发送命令时用到的头对象} 中的字段写入 {@link RemotingCommand#extFields} 中<br>
     * 通过反射读取字段并序列化为字符串键值
     */
    public void makeCustomHeaderToNet() {
        if (this.customHeader != null) {
            Field[] fields = getClazzFields(customHeader.getClass());
            if (null == this.extFields) {
                this.extFields = new HashMap<>();
            }

            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    String name = field.getName();
                    if (!name.startsWith("this")) {
                        Object value = null;
                        try {
                            field.setAccessible(true);
                            value = field.get(this.customHeader);
                        } catch (Exception e) {
                            log.error("Failed to access field [{}]", name, e);
                        }

                        if (value != null) {
                            this.extFields.put(name, value.toString());
                        }
                    }
                }
            }
        }
    }

    /**
     * 快速编码命令头<br>
     * 直接向 ByteBuf 写入, 减少中间 ByteBuffer 分配
     */
    public void fastEncodeHeader(ByteBuf out) {
        int bodySize = this.body != null ? this.body.length : 0;
        int beginIndex = out.writerIndex();
        // skip 8 bytes
        // 预留 8 字节用于后续回填总长度与头标记
        out.writeLong(0);
        int headerSize;
        if (SerializeType.ROCKETMQ == serializeTypeCurrentRPC) {
            if (customHeader != null && !(customHeader instanceof FastCodesHeader)) {
                this.makeCustomHeaderToNet(); // 把消息头中的数据存储到 extFields 成员变量中
            }
            // 对当前消息进行编码, 返回编码后的消息头字节长度
            headerSize = RocketMQSerializable.rocketMQProtocolEncode(this, out);
        } else {
            this.makeCustomHeaderToNet();
            byte[] header = RemotingSerializable.encode(this);
            headerSize = header.length;
            out.writeBytes(header);
        }
        out.setInt(beginIndex, 4 + headerSize + bodySize);
        out.setInt(beginIndex + 4, markProtocolType(headerSize, serializeTypeCurrentRPC));
    }

    /**
     * 编码头<br>
     * 自动使用当前 body 长度
     */
    public ByteBuffer encodeHeader() {
        return encodeHeader(this.body != null ? this.body.length : 0);
    }

    /**
     * 编码头<br>
     * 调用方可显式指定 bodyLength
     */
    public ByteBuffer encodeHeader(final int bodyLength) {
        // 1> header length size
        // 1> 头长度字段占位
        int length = 4;

        // 2> header data length
        // 2> 头数据长度
        byte[] headerData;
        headerData = this.headerEncode();

        length += headerData.length;

        // 3> body data length
        // 3> 消息体长度
        length += bodyLength;

        ByteBuffer result = ByteBuffer.allocate(4 + length - bodyLength);

        // length
        // 写入总长度
        result.putInt(length);

        // header length
        // 写入协议类型与头长度标记
        result.putInt(markProtocolType(headerData.length, serializeTypeCurrentRPC));

        // header data
        // 写入头数据
        result.put(headerData);

        ((Buffer) result).flip();

        return result;
    }

    /**
     * 标记命令为单向调用
     */
    public void markOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        this.flag |= bits;
    }

    /**
     * 判断是否为单向调用
     */
    @JSONField(serialize = false)
    public boolean isOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        return (this.flag & bits) == bits;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    /**
     * 获取命令类型<br>
     * 根据 flag 中响应位判断请求或响应
     */
    @JSONField(serialize = false)
    public RemotingCommandType getType() {
        if (this.isResponseType()) {
            return RemotingCommandType.RESPONSE_COMMAND;
        }

        return RemotingCommandType.REQUEST_COMMAND;
    }

    /**
     * 判断当前命令是否为响应
     */
    @JSONField(serialize = false)
    public boolean isResponseType() {
        int bits = 1 << RPC_TYPE;
        return (this.flag & bits) == bits;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public void setLanguage(LanguageCode language) {
        this.language = language;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getOpaque() {
        return opaque;
    }

    public void setOpaque(int opaque) {
        this.opaque = opaque;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    /**
     * 判断命令是否处于挂起状态
     */
    @JSONField(serialize = false)
    public boolean isSuspended() {
        return suspended;
    }

    @JSONField(serialize = false)
    public void setSuspended(boolean suspended) {
        this.suspended = suspended;
    }

    public HashMap<String, String> getExtFields() {
        return extFields;
    }

    public void setExtFields(HashMap<String, String> extFields) {
        this.extFields = extFields;
    }

    /**
     * 添加扩展字段<br>
     * extFields 为空时自动初始化
     */
    public void addExtField(String key, String value) {
        if (null == extFields) {
            extFields = new HashMap<>(256);
        }
        extFields.put(key, value);
    }

    /**
     * 仅在键不存在时添加扩展字段
     */
    public void addExtFieldIfNotExist(String key, String value) {
        extFields.putIfAbsent(key, value);
    }

    /**
     * 输出命令调试字符串<br>
     * 包含核心头字段与当前序列化类型
     */
    @Override
    public String toString() {
        return "RemotingCommand [code=" + code + ", language=" + language + ", version=" + version + ", opaque=" + opaque + ", flag(B)="
            + Integer.toBinaryString(flag) + ", remark=" + remark + ", extFields=" + extFields + ", serializeTypeCurrentRPC="
            + serializeTypeCurrentRPC + "]";
    }

    public SerializeType getSerializeTypeCurrentRPC() {
        return serializeTypeCurrentRPC;
    }

    public void setSerializeTypeCurrentRPC(SerializeType serializeTypeCurrentRPC) {
        this.serializeTypeCurrentRPC = serializeTypeCurrentRPC;
    }

    public Stopwatch getProcessTimer() {
        return processTimer;
    }

    public void setProcessTimer(Stopwatch processTimer) {
        this.processTimer = processTimer;
    }

    public List<CommandCallback> getCallbackList() {
        return callbackList;
    }

    public void setCallbackList(List<CommandCallback> callbackList) {
        this.callbackList = callbackList;
    }
}
