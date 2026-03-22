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

package org.apache.spark.metrics.sink

/**
 * 指标输出接收器顶级接口，定义了所有指标接收器必须实现的核心方法
 * Spark指标系统通过不同实现将指标输出到不同外部系统（如JMX、CSV、Graphite等）
 */
private[spark] trait Sink {
  /** 启动接收器，开始接收并输出指标 */
  def start(): Unit
  /** 停止接收器，释放相关资源 */
  def stop(): Unit
  /** 主动上报一次当前指标快照到输出目标 */
  def report(): Unit
}