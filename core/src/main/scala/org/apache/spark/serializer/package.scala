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

package org.apache.spark

/**
 * 可插拔的序列化器，用于 RDD 和 shuffle 数据的序列化
 * 
 * 核心接口：
 * - Serializer：序列化器顶层接口
 * - SerializerInstance：序列化器实例（线程独立）
 * - SerializationStream/DeserializationStream：流式序列化
 * 
 * 实现类：
 * - JavaSerializer：基于 Java 内置序列化
 * - KryoSerializer：基于 Kryo 的高性能序列化
 * 
 * @see [[org.apache.spark.serializer.Serializer]]
 */
package object serializer
