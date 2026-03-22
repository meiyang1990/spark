// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util;

import java.io.ByteArrayOutputStream;

/**
 * 文件级注释：Spark核心工具模块提供的字节数组输出流扩展类
 * 类级注释：ByteArrayOutputStream的子类，直接暴露内部存储缓冲区，避免数组拷贝开销
 * 用于需要直接访问内部缓冲区的高性能场景，减少不必要的内存复制
 */
/** Subclass of ByteArrayOutputStream that exposes `buf` directly. */
public final class ExposedBufferByteArrayOutputStream extends ByteArrayOutputStream {
    /**
     * 构造函数，使用指定初始容量创建输出流
     * @param size 初始缓冲区容量
     */
    public ExposedBufferByteArrayOutputStream(int size) { super(size); }
    /**
     * 获取内部存储缓冲区，直接返回原对象不拷贝
     * @return 内部存储字节数组缓冲区
     */
    public byte[] getBuf() { return buf; }
}