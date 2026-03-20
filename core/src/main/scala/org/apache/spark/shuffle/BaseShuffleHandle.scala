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

// 这个文件已经全部加上中文注释

package org.apache.spark.shuffle

import org.apache.spark.ShuffleDependency

/**
 * 基础 ShuffleHandle 实现，仅捕获 registerShuffle 的参数。
 * 用于保存 Shuffle 操作的句柄信息，包括 shuffleId 和对应的 ShuffleDependency。
 */
private[spark] class BaseShuffleHandle[K, V, C](
    shuffleId: Int,
    val dependency: ShuffleDependency[K, V, C])
  extends ShuffleHandle(shuffleId)
