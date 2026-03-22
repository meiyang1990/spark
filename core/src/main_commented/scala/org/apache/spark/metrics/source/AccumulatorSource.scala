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

package org.apache.spark.metrics.source

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.SparkContext
import org.apache.spark.util.{AccumulatorV2, DoubleAccumulator, LongAccumulator}

/**
 * 文件说明: 将Spark累加器当前值导出为监控指标的度量源基类
 * 
 * 仅支持数值类型的LongAccumulator和DoubleAccumulator，不支持集合类型累加器，
 * 因为集合类型无法直接输出为单个监控指标
 */
/**
 * AccumulatorSource is a Spark metric Source that reports the current value
 * of the accumulator as a gauge.
 *
 * It is restricted to the LongAccumulator and the DoubleAccumulator, as those
 * are the current built-in numerical accumulators with Spark, and excludes
 * the CollectionAccumulator, as that is a List of values (hard to report,
 * to a metrics system)
 */
private[spark] class AccumulatorSource extends Source {
  // 指标注册表，存储所有累加器对应的度量Gauge
  private val registry = new MetricRegistry
  /**
   * 将一批累加器注册为度量指标
   * @param accumulators 累加器映射表，key为指标名称，value为累加器对象
   * @tparam T 累加器存储的数值类型
   */
  protected def register[T](accumulators: Map[String, AccumulatorV2[_, T]]): Unit = {
    accumulators.foreach {
      case (name, accumulator) =>
        // 创建Gauge，每次获取指标时读取累加器当前值
        val gauge = new Gauge[T] {
          override def getValue: T = accumulator.value
        }
        registry.register(MetricRegistry.name(name), gauge)
    }
  }

  override def sourceName: String = "AccumulatorSource"
  override def metricRegistry: MetricRegistry = registry
}

/** LongAccumulator类型累加器专属度量源 */
class LongAccumulatorSource extends AccumulatorSource

/** DoubleAccumulator类型累加器专属度量源 */
class DoubleAccumulatorSource extends AccumulatorSource

/**
 * LongAccumulator度量注册单例，用于在Driver端注册长整型累加器到监控系统
 * 
 * 累加器仅在Driver端维护有效值，因此该指标仅由Driver端导出
 * 使用方式: LongAccumulatorSource.register(sc, {"name" -> longAccumulator})
 */
object LongAccumulatorSource {
  /**
   * 将一批LongAccumulator注册到SparkContext的监控系统
   * @param sc SparkContext上下文对象
   * @param accumulators 待注册的累加器映射表
   */
  def register(sc: SparkContext, accumulators: Map[String, LongAccumulator]): Unit = {
    val source = new LongAccumulatorSource
    source.register(accumulators)
    sc.env.metricsSystem.registerSource(source)
  }
}

/**
 * DoubleAccumulator度量注册单例，用于在Driver端注册双精度浮点型累加器到监控系统
 * 
 * 累加器仅在Driver端维护有效值，因此该指标仅由Driver端导出
 * 使用方式: DoubleAccumulatorSource.register(sc, {"name" -> doubleAccumulator})
 */
object DoubleAccumulatorSource {
  /**
   * 将一批DoubleAccumulator注册到SparkContext的监控系统
   * @param sc SparkContext上下文对象
   * @param accumulators 待注册的累加器映射表
   */
  def register(sc: SparkContext, accumulators: Map[String, DoubleAccumulator]): Unit = {
    val source = new DoubleAccumulatorSource
    source.register(accumulators)
    sc.env.metricsSystem.registerSource(source)
  }
}