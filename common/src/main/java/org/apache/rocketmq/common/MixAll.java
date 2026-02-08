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
package org.apache.rocketmq.common;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.IOTinyUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * RocketMQ 公共工具与常量集合<br>
 * 该类集中定义环境键, 默认组名, 主题前缀与通用静态工具方法
 */
public class MixAll {
    /**
     * RocketMQ 安装目录环境变量键
     */
    public static final String ROCKETMQ_HOME_ENV = "ROCKETMQ_HOME";
    /**
     * RocketMQ 安装目录系统属性键
     */
    public static final String ROCKETMQ_HOME_PROPERTY = "rocketmq.home.dir";
    /**
     * NameServer 地址环境变量键
     */
    public static final String NAMESRV_ADDR_ENV = "NAMESRV_ADDR";
    /**
     * NameServer 地址系统属性键
     */
    public static final String NAMESRV_ADDR_PROPERTY = "rocketmq.namesrv.addr";
    /**
     * 消息压缩类型系统属性键
     */
    public static final String MESSAGE_COMPRESS_TYPE = "rocketmq.message.compressType";
    /**
     * 消息压缩级别系统属性键
     */
    public static final String MESSAGE_COMPRESS_LEVEL = "rocketmq.message.compressLevel";
    /**
     * NameServer 域名查找默认地址
     */
    public static final String DEFAULT_NAMESRV_ADDR_LOOKUP = "jmenv.tbsite.net";
    /**
     * NameServer 域名配置<br>
     * 优先读取系统属性, 未配置时回退到默认查找地址
     */
    public static final String WS_DOMAIN_NAME = System.getProperty("rocketmq.namesrv.domain", DEFAULT_NAMESRV_ADDR_LOOKUP);
    /**
     * NameServer 域名子分组配置<br>
     * 默认使用 nsaddr
     */
    public static final String WS_DOMAIN_SUBGROUP = System.getProperty("rocketmq.namesrv.domain.subgroup", "nsaddr");
    /**
     * 默认生产者组名
     */
    public static final String DEFAULT_PRODUCER_GROUP = "DEFAULT_PRODUCER";
    /**
     * 默认消费者组名
     */
    public static final String DEFAULT_CONSUMER_GROUP = "DEFAULT_CONSUMER";
    /**
     * 工具类消费者组名
     */
    public static final String TOOLS_CONSUMER_GROUP = "TOOLS_CONSUMER";
    /**
     * 定时消息消费者组名
     */
    public static final String SCHEDULE_CONSUMER_GROUP = "SCHEDULE_CONSUMER";
    /**
     * 过滤服务消费者组名
     */
    public static final String FILTERSRV_CONSUMER_GROUP = "FILTERSRV_CONSUMER";
    /**
     * 监控消费者组名
     */
    public static final String MONITOR_CONSUMER_GROUP = "__MONITOR_CONSUMER";
    /**
     * 客户端内部生产者组名
     */
    public static final String CLIENT_INNER_PRODUCER_GROUP = "CLIENT_INNER_PRODUCER";
    /**
     * 自检生产者组名
     */
    public static final String SELF_TEST_PRODUCER_GROUP = "SELF_TEST_P_GROUP";
    /**
     * 自检消费者组名
     */
    public static final String SELF_TEST_CONSUMER_GROUP = "SELF_TEST_C_GROUP";
    /**
     * ONS HTTP 代理消费者组名
     */
    public static final String ONS_HTTP_PROXY_GROUP = "CID_ONS-HTTP-PROXY";
    /**
     * ONS API 权限消费者组名
     */
    public static final String CID_ONSAPI_PERMISSION_GROUP = "CID_ONSAPI_PERMISSION";
    /**
     * ONS API 所有者消费者组名
     */
    public static final String CID_ONSAPI_OWNER_GROUP = "CID_ONSAPI_OWNER";
    /**
     * ONS API 拉取消费者组名
     */
    public static final String CID_ONSAPI_PULL_GROUP = "CID_ONSAPI_PULL";
    /**
     * 系统消费者组名前缀
     */
    public static final String CID_RMQ_SYS_PREFIX = "CID_RMQ_SYS_";
    /**
     * 心跳 V2 能力标识键
     */
    public static final String IS_SUPPORT_HEART_BEAT_V2 = "IS_SUPPORT_HEART_BEAT_V2";
    /**
     * 订阅变化标识键
     */
    public static final String IS_SUB_CHANGE = "IS_SUB_CHANGE";
    /**
     * 本机网卡地址列表
     */
    public static final List<String> LOCAL_INET_ADDRESS = getLocalInetAddress();
    /**
     * 本机首选地址
     */
    public static final String LOCALHOST = localhost();
    /**
     * 默认字符集名称
     */
    public static final String DEFAULT_CHARSET = "UTF-8";
    /**
     * 主节点 brokerId
     */
    public static final long MASTER_ID = 0L;
    /**
     * 首个从节点 brokerId
     */
    public static final long FIRST_SLAVE_ID = 1L;

    /**
     * 首个 Broker Controller 节点 id
     */
    public static final long FIRST_BROKER_CONTROLLER_ID = 1L;
    /**
     * 当前 JVM 进程号
     */
    public static final long CURRENT_JVM_PID = getPID();
    /**
     * 消息单元预留固定字节数
     */
    public final static int UNIT_PRE_SIZE_FOR_MSG = 28;
    /**
     * 全部 ACK 位于同步副本集时的哨兵值
     */
    public final static int ALL_ACK_IN_SYNC_STATE_SET = -1;

    /**
     * 重试主题前缀
     */
    public static final String RETRY_GROUP_TOPIC_PREFIX = "%RETRY%";
    /**
     * 死信主题前缀
     */
    public static final String DLQ_GROUP_TOPIC_PREFIX = "%DLQ%";
    /**
     * 回复主题后缀
     */
    public static final String REPLY_TOPIC_POSTFIX = "REPLY_TOPIC";
    /**
     * 唯一消息查询标志
     */
    public static final String UNIQUE_MSG_QUERY_FLAG = "_UNIQUE_KEY_QUERY";
    /**
     * 默认链路追踪区域 id
     */
    public static final String DEFAULT_TRACE_REGION_ID = "DefaultRegion";
    /**
     * 消费上下文类型键
     */
    public static final String CONSUME_CONTEXT_TYPE = "ConsumeContextType";
    /**
     * 系统事务消费者组名
     */
    public static final String CID_SYS_RMQ_TRANS = "CID_RMQ_SYS_TRANS";
    /**
     * ACL 工具配置文件相对路径
     */
    public static final String ACL_CONF_TOOLS_FILE = "/conf/tools.yml";
    /**
     * 回复消息标志键
     */
    public static final String REPLY_MESSAGE_FLAG = "reply";
    /**
     * LMQ 主题前缀
     */
    public static final String LMQ_PREFIX = "%LMQ%";
    /**
     * LMQ 默认队列 id
     */
    public static final int LMQ_QUEUE_ID = 0;
    /**
     * LMQ 分发信息分隔符
     */
    public static final String LMQ_DISPATCH_SEPARATOR = ",";
    /**
     * 请求时间标签键
     */
    public static final String REQ_T = "ReqT";
    /**
     * 机房分区环境变量键
     */
    public static final String ROCKETMQ_ZONE_ENV = "ROCKETMQ_ZONE";
    /**
     * 机房分区系统属性键
     */
    public static final String ROCKETMQ_ZONE_PROPERTY = "rocketmq.zone";
    /**
     * 机房分区模式环境变量键
     */
    public static final String ROCKETMQ_ZONE_MODE_ENV = "ROCKETMQ_ZONE_MODE";
    /**
     * 机房分区模式系统属性键
     */
    public static final String ROCKETMQ_ZONE_MODE_PROPERTY = "rocketmq.zone.mode";
    /**
     * RPC Header 中的分区名称字段键
     */
    public static final String ZONE_NAME = "__ZONE_NAME";
    /**
     * RPC Header 中的分区模式字段键
     */
    public static final String ZONE_MODE = "__ZONE_MODE";
    /**
     * RPC Header 中的命名空间透传字段键
     */
    public final static String RPC_REQUEST_HEADER_NAMESPACED_FIELD = "nsd";
    /**
     * RPC Header 中的命名空间字段键
     */
    public final static String RPC_REQUEST_HEADER_NAMESPACE_FIELD = "ns";

    /**
     * 通用日志记录器
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);
    /**
     * 逻辑队列模拟 Broker 名称前缀
     */
    public static final String LOGICAL_QUEUE_MOCK_BROKER_PREFIX = "__syslo__";
    /**
     * 元数据全局作用域标识
     */
    public static final String METADATA_SCOPE_GLOBAL = "__global__";
    /**
     * 逻辑队列模拟 Broker 不存在占位名
     */
    public static final String LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST = "__syslo__none__";
    /**
     * 多存储路径分隔符<br>
     * 默认值可由系统属性覆盖
     */
    public static final String MULTI_PATH_SPLITTER = System.getProperty("rocketmq.broker.multiPathSplitter", ",");

    /**
     * 当前操作系统名称, 已转小写用于平台判断
     */
    private static final String OS = System.getProperty("os.name").toLowerCase();

    /**
     * 预定义消费者组集合<br>
     * 用于系统场景快速识别保留组名
     */
    private static final Set<String> PREDEFINE_GROUP_SET = ImmutableSet.of(
        DEFAULT_CONSUMER_GROUP,
        DEFAULT_PRODUCER_GROUP,
        TOOLS_CONSUMER_GROUP,
        SCHEDULE_CONSUMER_GROUP,
        FILTERSRV_CONSUMER_GROUP,
        MONITOR_CONSUMER_GROUP,
        CLIENT_INNER_PRODUCER_GROUP,
        SELF_TEST_PRODUCER_GROUP,
        SELF_TEST_CONSUMER_GROUP,
        ONS_HTTP_PROXY_GROUP,
        CID_ONSAPI_PERMISSION_GROUP,
        CID_ONSAPI_OWNER_GROUP,
        CID_ONSAPI_PULL_GROUP,
        CID_SYS_RMQ_TRANS
    );

    /**
     * 判断当前 JVM 是否运行在 Windows
     */
    public static boolean isWindows() {
        return OS.contains("win");
    }

    /**
     * 判断当前 JVM 是否运行在 macOS
     */
    public static boolean isMac() {
        return OS.contains("mac");
    }

    /**
     * 判断当前 JVM 是否运行在 Unix Like 平台<br>
     * 当前匹配 nix, nux, aix 三类关键字
     */
    public static boolean isUnix() {
        return OS.contains("nix")
            || OS.contains("nux")
            || OS.contains("aix");
    }

    /**
     * 判断当前 JVM 是否运行在 Solaris
     */
    public static boolean isSolaris() {
        return OS.contains("sunos");
    }

    /**
     * 构造 NameServer HTTP 地址<br>
     * 域名包含端口时采用不追加 8080 的路径格式
     */
    public static String getWSAddr() {
        String wsDomainName = System.getProperty("rocketmq.namesrv.domain", DEFAULT_NAMESRV_ADDR_LOOKUP);
        String wsDomainSubgroup = System.getProperty("rocketmq.namesrv.domain.subgroup", "nsaddr");
        String wsAddr = "http://" + wsDomainName + ":8080/rocketmq/" + wsDomainSubgroup;
        if (wsDomainName.indexOf(":") > 0) {
            wsAddr = "http://" + wsDomainName + "/rocketmq/" + wsDomainSubgroup;
        }
        return wsAddr;
    }

    /**
     * 根据消费者组生成重试主题
     */
    public static String getRetryTopic(final String consumerGroup) {
        return RETRY_GROUP_TOPIC_PREFIX + consumerGroup;
    }

    /**
     * 根据集群名生成回复主题
     */
    public static String getReplyTopic(final String clusterName) {
        return clusterName + "_" + REPLY_TOPIC_POSTFIX;
    }

    /**
     * 判断是否为系统消费者组<br>
     * 规则为组名以系统前缀开头
     */
    public static boolean isSysConsumerGroup(final String consumerGroup) {
        return consumerGroup.startsWith(CID_RMQ_SYS_PREFIX);
    }

    /**
     * 判断是否允许创建系统消费者组<br>
     * 仅在开关开启且组名属于系统组时返回 true
     */
    public static boolean isSysConsumerGroupAndEnableCreate(final String consumerGroup, final boolean isEnableCreateSysGroup) {
        return isEnableCreateSysGroup && isSysConsumerGroup(consumerGroup);
    }

    /**
     * 判断给定组名是否位于预定义组集合
     */
    public static boolean isPredefinedGroup(final String consumerGroup) {
        return PREDEFINE_GROUP_SET.contains(consumerGroup);
    }

    /**
     * 根据消费者组生成死信主题
     */
    public static String getDLQTopic(final String consumerGroup) {
        return DLQ_GROUP_TOPIC_PREFIX + consumerGroup;
    }

    /**
     * 根据 VIP 通道策略转换 Broker 地址<br>
     * 启用转换时端口减 2, 未启用时返回原地址
     */
    public static String brokerVIPChannel(final boolean isChange, final String brokerAddr) {
        if (isChange) {
            int split = brokerAddr.lastIndexOf(":");
            String ip = brokerAddr.substring(0, split);
            String port = brokerAddr.substring(split + 1);
            String brokerAddrNew = ip + ":" + (Integer.parseInt(port) - 2);
            return brokerAddrNew;
        } else {
            return brokerAddr;
        }
    }

    /**
     * 获取当前 JVM 进程号<br>
     * 解析失败时返回 0
     */
    public static long getPID() {
        String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        if (StringUtils.isNotEmpty(processName)) {
            try {
                return Long.parseLong(processName.split("@")[0]);
            } catch (Exception e) {
                return 0;
            }
        }

        return 0;
    }

    /**
     * 安全写字符串到文件<br>
     * 写入前会尝试将旧内容备份为 bak 文件
     */
    public static synchronized void string2File(final String str, final String fileName) throws IOException {

        String bakFile = fileName + ".bak";
        // 先读取旧内容, 仅在旧内容存在时写入备份文件
        String prevContent = file2String(fileName);
        if (prevContent != null) {
            string2FileNotSafe(prevContent, bakFile);
        }

        // 再写入新内容, 保证目标文件最终状态为最新文本
        string2FileNotSafe(str, fileName);
    }

    /**
     * 非安全方式写字符串到文件<br>
     * 该方法不做旧文件备份, 但会确保父目录存在
     */
    public static void string2FileNotSafe(final String str, final String fileName) throws IOException {
        File file = new File(fileName);
        File fileParent = file.getParentFile();
        if (fileParent != null) {
            fileParent.mkdirs();
        }
        IOTinyUtils.writeStringToFile(file, str, DEFAULT_CHARSET);
    }

    /**
     * 通过文件路径读取文本内容
     */
    public static String file2String(final String fileName) throws IOException {
        File file = new File(fileName);
        return file2String(file);
    }

    /**
     * 通过 File 对象读取文本内容<br>
     * 仅在完整读取成功时返回字符串, 否则返回 null
     */
    public static String file2String(final File file) throws IOException {
        if (file.exists()) {
            byte[] data = new byte[(int) file.length()];
            boolean result;

            try (FileInputStream inputStream = new FileInputStream(file)) {
                int len = inputStream.read(data);
                result = len == data.length;
            }

            if (result) {
                return new String(data, DEFAULT_CHARSET);
            }
        }
        return null;
    }

    /**
     * 通过 URL 读取文本内容<br>
     * 读取异常时返回 null
     */
    public static String file2String(final URL url) {
        InputStream in = null;
        try {
            URLConnection urlConnection = url.openConnection();
            urlConnection.setUseCaches(false);
            in = urlConnection.getInputStream();
            int len = in.available();
            byte[] data = new byte[len];
            in.read(data, 0, len);
            return new String(data, StandardCharsets.UTF_8);
        } catch (Exception ignored) {
        } finally {
            if (null != in) {
                try {
                    in.close();
                } catch (IOException ignored) {
                }
            }
        }

        return null;
    }

    /**
     * 打印对象属性<br>
     * 默认打印全部非静态字段
     */
    public static void printObjectProperties(final Logger logger, final Object object) {
        printObjectProperties(logger, object, false);
    }

    /**
     * 打印对象属性<br>
     * onlyImportantField 为 true 时仅打印带 ImportantField 注解的字段
     */
    public static void printObjectProperties(final Logger logger, final Object object,
        final boolean onlyImportantField) {
        Field[] fields = object.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (!Modifier.isStatic(field.getModifiers())) {
                String name = field.getName();
                if (!name.startsWith("this")) {
                    if (onlyImportantField) {
                        Annotation annotation = field.getAnnotation(ImportantField.class);
                        if (null == annotation) {
                            continue;
                        }
                    }

                    Object value = null;
                    try {
                        field.setAccessible(true);
                        value = field.get(object);
                        if (null == value) {
                            value = "";
                        }
                    } catch (IllegalAccessException e) {
                        log.error("Failed to obtain object properties", e);
                    }

                    if (logger != null) {
                        logger.info(name + "=" + value);
                    }
                }
            }
        }
    }

    /**
     * 将 Properties 序列化为文本<br>
     * 默认保持原始遍历顺序
     */
    public static String properties2String(final Properties properties) {
        return properties2String(properties, false);
    }

    /**
     * 将 Properties 序列化为 key=value 文本<br>
     * isSort 为 true 时按 key 排序输出
     */
    public static String properties2String(final Properties properties, final boolean isSort) {
        StringBuilder sb = new StringBuilder();
        Set<Map.Entry<Object, Object>> entrySet = isSort ? new TreeMap<>(properties).entrySet() : properties.entrySet();
        for (Map.Entry<Object, Object> entry : entrySet) {
            if (entry.getValue() != null) {
                sb.append(entry.getKey().toString() + "=" + entry.getValue().toString() + "\n");
            }
        }
        return sb.toString();
    }

    /**
     * 将 key=value 文本反序列化为 Properties<br>
     * 解析失败时返回 null
     */
    public static Properties string2Properties(final String str) {
        Properties properties = new Properties();
        try {
            InputStream in = new ByteArrayInputStream(str.getBytes(DEFAULT_CHARSET));
            properties.load(in);
        } catch (Exception e) {
            log.error("Failed to handle properties", e);
            return null;
        }

        return properties;
    }

    /**
     * 将对象字段映射为 Properties<br>
     * 会遍历当前类及其父类的非静态字段
     */
    public static Properties object2Properties(final Object object) {
        Properties properties = new Properties();

        Class<?> objectClass = object.getClass();
        while (true) {
            Field[] fields = objectClass.getDeclaredFields();
            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    String name = field.getName();
                    if (!name.startsWith("this")) {
                        Object value = null;
                        try {
                            field.setAccessible(true);
                            value = field.get(object);
                        } catch (IllegalAccessException e) {
                            log.error("Failed to handle properties", e);
                        }

                        if (value != null) {
                            properties.setProperty(name, value.toString());
                        }
                    }
                }
            }
            if (objectClass == Object.class || objectClass.getSuperclass() == Object.class) {
                break;
            }
            objectClass = objectClass.getSuperclass();
        }

        return properties;
    }

    /**
     * 将 Properties 映射回对象<br>
     * 通过反射查找 set 方法并按参数类型执行转换
     */
    public static void properties2Object(final Properties p, final Object object) {
        Method[] methods = object.getClass().getMethods();
        for (Method method : methods) {
            String mn = method.getName();
            if (mn.startsWith("set")) {
                try {
                    // 将 setXxx 映射为属性名 xxx
                    String tmp = mn.substring(4);
                    String first = mn.substring(3, 4);

                    String key = first.toLowerCase() + tmp;
                    String property = p.getProperty(key);
                    if (property != null) {
                        Class<?>[] pt = method.getParameterTypes();
                        if (pt.length > 0) {
                            String cn = pt[0].getSimpleName();
                            Object arg;
                            // 按 setter 参数类型执行字符串到目标类型的转换
                            if (cn.equals("int") || cn.equals("Integer")) {
                                arg = Integer.parseInt(property);
                            } else if (cn.equals("long") || cn.equals("Long")) {
                                arg = Long.parseLong(property);
                            } else if (cn.equals("double") || cn.equals("Double")) {
                                arg = Double.parseDouble(property);
                            } else if (cn.equals("boolean") || cn.equals("Boolean")) {
                                arg = Boolean.parseBoolean(property);
                            } else if (cn.equals("float") || cn.equals("Float")) {
                                arg = Float.parseFloat(property);
                            } else if (cn.equals("String")) {
                                property = property.trim();
                                arg = property;
                            } else {
                                // 未支持的参数类型直接跳过
                                continue;
                            }
                            // 使用反射调用 setter 注入配置值
                            method.invoke(object, arg);
                        }
                    }
                } catch (Throwable ignored) {
                }
            }
        }
    }

    /**
     * 判断两个 Properties 是否完全相等
     */
    public static boolean isPropertiesEqual(final Properties p1, final Properties p2) {
        return p1.equals(p2);
    }

    /**
     * 校验指定属性值是否合法<br>
     * 合法性由外部传入的 validator 定义
     */
    public static boolean isPropertyValid(Properties props, String key, Predicate<String> validator) {
        return validator.test(props.getProperty(key));
    }

    /**
     * 获取本机所有网卡地址<br>
     * 遍历所有网络接口并收集 hostAddress
     */
    public static List<String> getLocalInetAddress() {
        List<String> inetAddressList = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
            while (enumeration.hasMoreElements()) {
                NetworkInterface networkInterface = enumeration.nextElement();
                Enumeration<InetAddress> addrs = networkInterface.getInetAddresses();
                while (addrs.hasMoreElements()) {
                    // 逐个收集网络接口地址, 包含 IPv4 与 IPv6
                    inetAddressList.add(addrs.nextElement().getHostAddress());
                }
            }
        } catch (SocketException e) {
            throw new RuntimeException("get local inet address fail", e);
        }

        return inetAddressList;
    }

    /**
     * 获取本机优选地址<br>
     * 优先使用 InetAddress.getLocalHost, 失败后降级到网卡遍历
     */
    private static String localhost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Throwable e) {
            try {
                String candidatesHost = getLocalhostByNetworkInterface();
                if (candidatesHost != null)
                    return candidatesHost;

            } catch (Exception ignored) {
            }

            throw new RuntimeException("InetAddress java.net.InetAddress.getLocalHost() throws UnknownHostException" + FAQUrl.suggestTodo(FAQUrl.UNKNOWN_HOST_EXCEPTION), e);
        }
    }

    //Reverse logic comparing to RemotingUtil method, consider refactor in RocketMQ 5.0
    // 与 RemotingUtil 的筛选顺序相反, 后续可在 RocketMQ 5.0 统一重构
    /**
     * 通过网卡遍历获取本机地址<br>
     * 优先返回 IPv4, 若无 IPv4 则返回首个 IPv6
     */
    public static String getLocalhostByNetworkInterface() throws SocketException {
        List<String> candidatesHost = new ArrayList<>();
        Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();

        while (enumeration.hasMoreElements()) {
            NetworkInterface networkInterface = enumeration.nextElement();
            // Workaround for docker0 bridge
            // 规避 docker0 虚拟网桥干扰
            if ("docker0".equals(networkInterface.getName()) || !networkInterface.isUp()) {
                continue;
            }
            Enumeration<InetAddress> addrs = networkInterface.getInetAddresses();
            while (addrs.hasMoreElements()) {
                InetAddress address = addrs.nextElement();
                if (address.isLoopbackAddress()) {
                    continue;
                }
                //ip4 higher priority
                // IPv4 优先级高于 IPv6
                if (address instanceof Inet6Address) {
                    candidatesHost.add(address.getHostAddress());
                    continue;
                }
                return address.getHostAddress();
            }
        }

        if (!candidatesHost.isEmpty()) {
            return candidatesHost.get(0);
        }

        // Fallback to loopback
        // 回退到本地回环地址
        return localhost();
    }

    /**
     * 仅当新值更大时更新 AtomicLong<br>
     * 更新成功返回 true, 否则返回 false
     */
    public static boolean compareAndIncreaseOnly(final AtomicLong target, final long value) {
        long prev = target.get();
        while (value > prev) {
            boolean updated = target.compareAndSet(prev, value);
            if (updated)
                return true;

            prev = target.get();
        }

        return false;
    }

    /**
     * 将字节数转换为可读字符串<br>
     * si 为 true 时使用 1000 进制, 否则使用 1024 进制
     */
    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit)
            return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    /**
     * 比较两个 int 值
     */
    public static int compareInteger(int x, int y) {
        return Integer.compare(x, y);
    }

    /**
     * 比较两个 long 值
     */
    public static int compareLong(long x, long y) {
        return Long.compare(x, y);
    }

    /**
     * 判断给定元数据是否为 LMQ 主题<br>
     * 规则为非空且以 LMQ 前缀开头
     */
    public static boolean isLmq(String lmqMetaData) {
        return lmqMetaData != null && lmqMetaData.startsWith(LMQ_PREFIX);
    }

    /**
     * 规范化文件路径<br>
     * 消除路径中的冗余分隔或相对片段
     */
    public static String dealFilePath(String aclFilePath) {
        Path path = Paths.get(aclFilePath);
        return path.normalize().toString();
    }

    /**
     * 判断消费者组是否属于系统拉取组<br>
     * 命中系统保留组或系统前缀时返回 true
     */
    public static boolean isSysConsumerGroupPullMessage(String consumerGroup) {
        if (DEFAULT_CONSUMER_GROUP.equals(consumerGroup)
            || TOOLS_CONSUMER_GROUP.equals(consumerGroup)
            || SCHEDULE_CONSUMER_GROUP.equals(consumerGroup)
            || FILTERSRV_CONSUMER_GROUP.equals(consumerGroup)
            || MONITOR_CONSUMER_GROUP.equals(consumerGroup)
            || SELF_TEST_CONSUMER_GROUP.equals(consumerGroup)
            || ONS_HTTP_PROXY_GROUP.equals(consumerGroup)
            || CID_ONSAPI_PERMISSION_GROUP.equals(consumerGroup)
            || CID_ONSAPI_OWNER_GROUP.equals(consumerGroup)
            || CID_ONSAPI_PULL_GROUP.equals(consumerGroup)
            || CID_SYS_RMQ_TRANS.equals(consumerGroup)
            || consumerGroup.startsWith(CID_RMQ_SYS_PREFIX)) {
            return true;
        }
        return false;
    }

    /**
     * 判断主题是否允许 LMQ<br>
     * 重试主题, 系统主题, 调度主题不允许开启 LMQ
     */
    public static boolean topicAllowsLMQ(String topic) {
        return !topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)
            && !topic.startsWith(TopicValidator.SYSTEM_TOPIC_PREFIX)
            && !topic.equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC);
    }
}
