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
package org.apache.rocketmq.common.constant;

/**
 * 权限位定义与权限判定工具
 */
public class PermName {
    /**
     * 优先级权限位索引
     */
    public static final int INDEX_PERM_PRIORITY = 3;
    /**
     * 读权限位索引
     */
    public static final int INDEX_PERM_READ = 2;
    /**
     * 写权限位索引
     */
    public static final int INDEX_PERM_WRITE = 1;
    /**
     * 继承权限位索引
     */
    public static final int INDEX_PERM_INHERIT = 0;


    /**
     * 优先级权限掩码
     */
    public static final int PERM_PRIORITY = 0x1 << INDEX_PERM_PRIORITY;
    /**
     * 读权限掩码
     */
    public static final int PERM_READ = 0x1 << INDEX_PERM_READ;
    /**
     * 写权限掩码
     */
    public static final int PERM_WRITE = 0x1 << INDEX_PERM_WRITE;
    /**
     * 继承权限掩码
     */
    public static final int PERM_INHERIT = 0x1 << INDEX_PERM_INHERIT;

    /**
     * 将权限位转换为 RWX 字符串
     *
     * @param perm 权限位值
     * @return 权限字符串
     */
    public static String perm2String(final int perm) {
        final StringBuilder sb = new StringBuilder("---");
        if (isReadable(perm)) {
            sb.replace(0, 1, "R");
        }

        if (isWriteable(perm)) {
            sb.replace(1, 2, "W");
        }

        if (isInherited(perm)) {
            sb.replace(2, 3, "X");
        }

        return sb.toString();
    }

    public static boolean isReadable(final int perm) {
        return (perm & PERM_READ) == PERM_READ;
    }

    public static boolean isWriteable(final int perm) {
        return (perm & PERM_WRITE) == PERM_WRITE;
    }

    public static boolean isInherited(final int perm) {
        return (perm & PERM_INHERIT) == PERM_INHERIT;
    }

    public static boolean isValid(final String perm) {
        return isValid(Integer.parseInt(perm));
    }

    public static boolean isValid(final int perm) {
        return perm >= 0 && perm < PERM_PRIORITY;
    }
    
    public static boolean isPriority(final int perm) {
        return (perm & PERM_PRIORITY) == PERM_PRIORITY;
    }

    public static boolean isAccessible(final int perm) {
        return isReadable(perm) || isWriteable(perm);
    }
}
