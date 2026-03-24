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

package org.apache.spark.util

import java.util.concurrent.TimeUnit

/**
 * 可手动控制时间的时钟实现，主要用于测试场景。
 * 该时钟的时间不会自动随真实时间流逝而变化，仅能通过调用者手动修改。
 * getTimeMillis() 和 nanoTime() 会返回经过单位转换后的同一时间值。
 *
 * @param time 初始时间，单位为自纪元起的毫秒数
 */
private[spark] class ManualClock(private var time: Long) extends Clock {

  /**
   * 创建初始时间为0的手动时钟
   * @return 初始时间为0的ManualClock实例
   */
  def this() = this(0L)

  override def getTimeMillis(): Long = synchronized {
    time
  }

  override def nanoTime(): Long = TimeUnit.MILLISECONDS.toNanos(getTimeMillis())

  /**
   * 将时钟设置为指定时间
   * @param timeToSet 目标时间，单位为毫秒
   */
  def setTime(timeToSet: Long): Unit = synchronized {
    time = timeToSet
    notifyAll()
  }

  /**
   * 将时钟时间向前推进指定时长
   * @param timeToAdd 需要增加的时长，单位为毫秒
   */
  def advance(timeToAdd: Long): Unit = synchronized {
    time += timeToAdd
    notifyAll()
  }

  /**
   * 阻塞等待，直到时钟时间至少达到目标时间后返回
   * @param targetTime 等待目标时间，单位为毫秒
   * @return 等待结束时的当前时钟时间
   */
  override def waitTillTime(targetTime: Long): Long = synchronized {
    while (time < targetTime) {
      wait(10)
    }
    getTimeMillis()
  }
}