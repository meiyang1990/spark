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

package org.apache.spark.api.r

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * JVM对象ID包装类，用于SparkR中标识JVM端对象
 * @param id 对象唯一标识字符串
 */
private[r] case class JVMObjectId(id: String) {
  require(id != null, "Object ID cannot be null.")
}

/**
 * 用于跟踪返回给R端的JVM对象的追踪器，在SparkR RPC调用中通过ID引用这些JVM对象
 * 支持并发访问，为多线程场景下的R-JVM交互提供对象管理能力
 */
private[r] class JVMObjectTracker {

  // 存储对象ID到JVM对象的并发映射表
  private[this] val objMap = new ConcurrentHashMap[JVMObjectId, Object]()
  // 对象ID生成计数器，保证生成唯一ID
  private[this] val objCounter = new AtomicInteger()

  /**
   * 根据ID获取对应JVM对象，未找到时返回None
   * @param id 目标对象ID
   * @return 目标对象包装，不存在则为None
   */
  final def get(id: JVMObjectId): Option[Object] = Option(objMap.get(id))

  /**
   * 根据ID获取对应JVM对象，未找到时抛出异常
   * @param id 目标对象ID
   * @return 目标对象
   * @throws NoSuchElementException 当ID不存在时抛出
   */
  @throws[NoSuchElementException]("if key does not exist.")
  final def apply(id: JVMObjectId): Object = {
    get(id).getOrElse(
      throw new NoSuchElementException(s"$id does not exist.")
    )
  }

  /**
   * 添加新JVM对象到追踪器，自动生成并返回唯一ID
   * @param obj 需要追踪的JVM对象
   * @return 分配给该对象的唯一ID
   */
  final def addAndGetId(obj: Object): JVMObjectId = {
    val id = JVMObjectId(objCounter.getAndIncrement().toString)
    objMap.put(id, obj)
    id
  }

  /**
   * 从追踪器移除指定ID的对象
   * @param id 目标对象ID
   * @return 被移除的对象，不存在则为None
   */
  final def remove(id: JVMObjectId): Option[Object] = Option(objMap.remove(id))

  /**
   * 获取当前追踪器中正在跟踪的对象数量
   * @return 正在跟踪的对象总数
   */
  final def size: Int = objMap.size()

  /**
   * 清空追踪器中所有跟踪的对象
   */
  final def clear(): Unit = objMap.clear()
}