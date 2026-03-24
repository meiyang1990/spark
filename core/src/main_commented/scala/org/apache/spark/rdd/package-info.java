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

/**
 * 文件级说明：RDD（弹性分布式数据集）实现包，提供Spark核心分布式数据抽象各类RDD实现类。
 * 包含各种特殊类型RDD的实现，如分区RDD、联合RDD、笛卡尔积RDD、 coalesce 分区调整RDD等，
 * 是Spark核心计算层的数据抽象基础，支撑所有上层组件的分布式计算能力。
 *
 * Provides implementation's of various RDDs.
 */
package org.apache.spark.rdd;