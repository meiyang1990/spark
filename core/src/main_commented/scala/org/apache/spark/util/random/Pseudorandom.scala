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

package org.apache.spark.util.random

import org.apache.spark.annotation.DeveloperApi

/**
 * 文件说明: 伪随机数生成器的公共接口，定义所有可设置随机种子的伪随机实现都需要遵守的契约
 * :: DeveloperApi ::
 * 伪随机行为的抽象接口，被Spark中各类随机采样、随机划分组件使用
 */
@DeveloperApi
trait Pseudorandom {
  /**
   * 设置伪随机数生成器的种子，保证随机过程可复现
   * @param seed 随机种子值
   */
  def setSeed(seed: Long): Unit
}