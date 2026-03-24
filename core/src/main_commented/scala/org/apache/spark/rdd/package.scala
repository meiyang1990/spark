// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

import org.apache.hadoop.io.Writable

/**
 * 文件级说明：RDD模块根包对象，定义RDD模块公共类型别名，提供整个RDD模块的公共基础定义
 * 
 * Provides several RDD implementations. See [[org.apache.spark.rdd.RDD]].
 */
package object rdd {
  /** 类型约束别名：表示类型A必须是Hadoop Writable的子类型 */
  type IsWritable[A] = A => Writable
}