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

import java.util.Properties

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.jmx.JmxReporter

/**
 * JMX指标接收器，将Spark指标暴露给JMX
 * 实现了Sink接口，负责将Spark内部指标注册到MBean服务器，供JVM监控工具（如JConsole）查看
 */
private[spark] class JmxSink(
    val property: Properties, val registry: MetricRegistry) extends Sink {

  // JMX Reporter实例，负责将指标注册表中的指标注册到JMX
  val reporter: JmxReporter = JmxReporter.forRegistry(registry).build()

  /**
   * 启动JMX接收器，开始向JMX暴露指标
   */
  override def start(): Unit = {
    reporter.start()
  }

  /**
   * 停止JMX接收器，停止暴露指标并清理资源
   */
  override def stop(): Unit = {
    reporter.stop()
  }

  /**
   * 主动报告指标，JMX模式下不需要主动推送，此方法留空
   */
  override def report(): Unit = { }

}