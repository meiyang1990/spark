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

import java.{lang => jl}
import java.io.ObjectInputStream
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.{InternalAccumulator, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.util.AccumulatorContext.internOption

/**
 * 累加器元数据，存储累加器的ID、名称和是否统计失败任务值的配置
 */
private[spark] case class AccumulatorMetadata(
    id: Long,
    name: Option[String],
    countFailedValues: Boolean) extends Serializable


/**
 * 累加器的抽象基类，支持分布式任务计算中各执行节点结果聚合，支持输入类型IN，输出类型OUT
 * 
 * 输出类型OUT需要支持原子读取（如Int、Long）或线程安全读取（如同步集合），因为会被其他线程读取
 */
abstract class AccumulatorV2[IN, OUT] extends Serializable {
  private[spark] var metadata: AccumulatorMetadata = _
  private[this] var atDriverSide = true

  /**
   * 是否不在心跳中上报此累加器，子类可覆盖
   */
  def excludeFromHeartbeat: Boolean = false

  /**
   * 在SparkContext中注册当前累加器，分配唯一ID并注册到全局上下文
   * @param sc Spark上下文对象
   * @param name 累加器名称
   * @param countFailedValues 是否统计失败任务的值，仅内部指标使用
   */
  private[spark] def register(
      sc: SparkContext,
      name: Option[String] = None,
      countFailedValues: Boolean = false): Unit = {
    if (this.metadata != null) {
      throw new IllegalStateException("Cannot register an Accumulator twice.")
    }
    this.metadata = AccumulatorMetadata(AccumulatorContext.newId(), name, countFailedValues)
    AccumulatorContext.register(this)
    sc.cleaner.foreach(_.registerAccumulatorForCleanup(this))
  }

  /**
   * 检查累加器是否已完成注册，所有累加器必须注册后才能使用
   * @return true表示已注册
   */
  final def isRegistered: Boolean =
    metadata != null && AccumulatorContext.get(metadata.id).isDefined

  private def assertMetadataNotNull(): Unit = {
    if (metadata == null) {
      throw new IllegalStateException("The metadata of this accumulator has not been assigned yet.")
    }
  }

  /**
   * 获取累加器ID，仅注册后可调用
   * @return 累加器全局唯一ID
   */
  final def id: Long = {
    assertMetadataNotNull()
    metadata.id
  }

  /**
   * 获取累加器名称，仅注册后可调用
   * @return 累加器名称Option
   */
  final def name: Option[String] = {
    assertMetadataNotNull()

    if (atDriverSide) {
      metadata.name.orElse(AccumulatorContext.get(id).flatMap(_.metadata.name))
    } else {
      metadata.name
    }
  }

  /**
   * 是否统计失败任务的值，仅内部指标使用，系统时间指标设为true，输入行数等绝对指标设为false
   * @return true表示需要统计失败任务的值
   */
  private[spark] final def countFailedValues: Boolean = {
    assertMetadataNotNull()
    metadata.countFailedValues
  }

  private def isInternal = name.exists(_.startsWith(InternalAccumulator.METRICS_PREFIX))

  /**
   * 将当前累加器转换为AccumulableInfo对象，用于Spark调度器的指标上报
   * @param update 更新值
   * @param value 最终值
   * @return 转换后的AccumulableInfo对象
   */
  private[spark] def toInfo(update: Option[Any], value: Option[Any]): AccumulableInfo = {
    AccumulableInfo(id, name, internOption(update), internOption(value), isInternal,
      countFailedValues)
  }

  /**
   * 将当前累加器转换为更新类型的AccumulableInfo对象
   * @return 转换后的AccumulableInfo对象
   */
  private[spark] def toInfoUpdate: AccumulableInfo = {
    AccumulableInfo(id, name, internOption(Some(value)), None, isInternal, countFailedValues)
  }

  final private[spark] def isAtDriverSide: Boolean = atDriverSide

  /**
   * 检查累加器当前是否为零值（初始状态）
   * @return true表示当前为零值
   */
  def isZero: Boolean

  /**
   * 创建当前累加器的拷贝并重置为零值，拷贝调用isZero必须返回true
   * @return 重置后的新累加器对象
   */
  def copyAndReset(): AccumulatorV2[IN, OUT] = {
    val copyAcc = copy()
    copyAcc.reset()
    copyAcc
  }

  /**
   * 创建当前累加器的深拷贝
   * @return 当前累加器的拷贝对象
   */
  def copy(): AccumulatorV2[IN, OUT]

  /**
   * 将当前累加器重置为零值，重置后调用isZero必须返回true
   */
  def reset(): Unit

  /**
   * 将输入值添加到累加器中
   * @param v 待添加的输入值
   */
  def add(v: IN): Unit

  /**
   * 将另一个同类型累加器合并到当前累加器，原地更新当前累加器状态
   * @param other 待合并的另一个累加器
   */
  def merge(other: AccumulatorV2[IN, OUT]): Unit

  /**
   * 获取累加器当前的输出值
   * @return 当前累加器的结果值
   */
  def value: OUT

  // 发送累加器回Driver前序列化缓冲区，默认不做处理
  protected def withBufferSerialized(): AccumulatorV2[IN, OUT] = this

  // Java序列化时调用，根据是否在Driver端处理序列化逻辑
  final protected def writeReplace(): Any = {
    if (atDriverSide) {
      if (!isRegistered) {
        throw new UnsupportedOperationException(
          "Accumulator must be registered before send to executor")
      }
      val copyAcc = copyAndReset()
      assert(copyAcc.isZero, "copyAndReset must return a zero value copy")
      val isInternalAcc = name.isDefined && name.get.startsWith(InternalAccumulator.METRICS_PREFIX)
      if (isInternalAcc) {
        // 内部累加器不序列化名称，避免发送给Executor
        copyAcc.metadata = metadata.copy(name = None)
      } else {
        // 非内部累加器保留名称，供Executor端或后续反序列化访问
        copyAcc.metadata = metadata
      }
      copyAcc
    } else {
      withBufferSerialized()
    }
  }

  // Java反序列化时调用，反序列化后自动注册到当前任务上下文
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    if (atDriverSide) {
      atDriverSide = false

      // 任务闭包反序列化时自动注册累加器到当前任务
      val taskContext = TaskContext.get()
      if (taskContext != null) {
        taskContext.registerAccumulator(this)
      }
    } else {
      atDriverSide = true
    }
  }

  override def toString: String = {
    // 规避getClass.getSimpleName可能抛出畸形类名错误的问题，使用安全的Utils.getSimpleName
    if (metadata == null) {
      "Un-registered Accumulator: " + Utils.getSimpleName(getClass)
    } else {
      Utils.getSimpleName(getClass) + s"(id: $id, name: $name, value: $value)"
    }
  }
}


/**
 * Spark内部用于跟踪管理所有累加器的全局单例对象
 */
private[spark] object AccumulatorContext extends Logging {

  /**
   * 全局Map保存Driver端创建的原始累加器，使用弱引用，当引用该累加器的RDD和用户代码被清理后，累加器可被GC回收
   * TODO: 不应使用全局Map，应该绑定到SparkContext实例（SPARK-13051）
   */
  private val originals = new ConcurrentHashMap[Long, jl.ref.WeakReference[AccumulatorV2[_, _]]]

  private[this] val nextId = new AtomicLong(0L)

  private[this] val someOfMinusOne = Some(-1L)
  private[this] val someOfZero = Some(0L)

  /**
   * 生成新的全局唯一累加器ID
   * @return 新的唯一ID
   */
  def newId(): Long = nextId.getAndIncrement

  /**
   * 获取已注册累加器数量，仅用于测试
   * @return 已注册累加器数量
   */
  def numAccums: Int = originals.size

  /**
   * 注册Driver端创建的累加器，使得Executor计算的结果可以聚合回Driver端的原始累加器
   * 若相同ID已注册则不做覆盖，仅做安全性检查
   * @param a 待注册的累加器对象
   */
  def register(a: AccumulatorV2[_, _]): Unit = {
    originals.putIfAbsent(a.id, new jl.ref.WeakReference[AccumulatorV2[_, _]](a))
  }

  /**
   * 注销指定ID的累加器
   * @param id 待注销的累加器ID
   */
  def remove(id: Long): Unit = {
    originals.remove(id)
  }

  /**
   * 根据ID获取已注册的累加器
   * @param id 累加器ID
   * @return 累加器Option，不存在或已被GC返回None
   */
  def get(id: Long): Option[AccumulatorV2[_, _]] = {
    val ref = originals.get(id)
    if (ref eq null) {
      None
    } else {
      // 弱引用可能已被GC回收，打印警告日志
      val acc = ref.get
      if (acc eq null) {
        logWarning(log"Attempted to access garbage collected accumulator " +
          log"${MDC(ACCUMULATOR_ID, id)}")
      }
      Option(acc)
    }
  }

  /**
   * 清空所有已注册累加器，仅用于测试
   */
  def clear(): Unit = {
    originals.clear()
  }

  /**
   * 对常见的0L和-1L的Some对象进行去重，减少内存占用
   * 若后续需要支持更多值，可改为使用Guava弱Interner实现
   * @param value 输入的Option对象
   * @return 去重后的Option对象
   */
  def internOption(value: Option[Any]): Option[Any] = {
    value match {
      case Some(0L) => someOfZero
      case Some(-1L) => someOfMinusOne
      case _ => value
    }
  }

  // 区分SQL指标与其他累加器的标识符
  private[spark] val SQL_ACCUM_IDENTIFIER = "sql"
}


/**
 * 用于计算64位整数总和、计数、平均值的累加器
 * @since 2.0.0
 */
class LongAccumulator extends AccumulatorV2[jl.Long, jl.Long] {
  private var _sum = 0L
  private var _count = 0L

  /**
   * 检查累加器是否为零值（无任何值添加）
   * @return 总和和计数都为0返回true
   * 
   * @since 2.0.0
   */
  override def isZero: Boolean = _sum == 0L && _count == 0

  override def copy(): LongAccumulator = {
    val newAcc = new LongAccumulator
    newAcc._count = this._count
    newAcc._sum = this._sum
    newAcc
  }

  override def reset(): Unit = {
    _sum = 0L
    _count = 0L
  }

  /**
   * 添加Java Long类型值到累加器，总和加v，计数加1
   * @param v 待添加的值
   * @since 2.0.0
   */
  override def add(v: jl.Long): Unit = {
    _sum += v
    _count += 1
  }

  /**
   * 添加Scala Long类型值到累加器，总和加v，计数加1
   * @param v 待添加的值
   * @since 2.0.0
   */
  def add(v: Long): Unit = {
    _sum += v
    _count += 1
  }

  /**
   * 获取添加到累加器的元素数量
   * @return 元素计数
   * @since 2.0.0
   */
  def count: Long = _count

  /**
   * 获取所有添加元素的总和
   * @return 总和
   * @since 2.0.0
   */
  def sum: Long = _sum

  /**
   * 获取所有添加元素的平均值
   * @return 平均值
   * @since 2.0.0
   */
  def avg: Double = _sum.toDouble / _count

  override def merge(other: AccumulatorV2[jl.Long, jl.Long]): Unit = other match {
    case o: LongAccumulator =>
      _sum += o.sum
      _count += o.count
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  private[spark] def setValue(newValue: Long): Unit = _sum = newValue

  override def value: jl.Long = _sum
}


/**
 * 用于计算双精度浮点数总和、计数、平均值的累加器
 * @since 2.0.0
 */
class DoubleAccumulator extends AccumulatorV2[jl.Double, jl.Double] {
  private var _sum = 0.0
  private var _count = 0L

  /**
   * 检查累加器是否为零值（无任何值添加）
   * @return 总和和计数都为0返回true
   */
  override def isZero: Boolean = _sum == 0.0 && _count == 0

  override def copy(): DoubleAccumulator = {
    val newAcc = new DoubleAccumulator
    newAcc._count = this._count
    newAcc._sum = this._sum
    newAcc
  }

  override def reset(): Unit = {
    _sum = 0.0
    _count = 0L
  }

  /**
   * 添加Java Double类型值到累加器，总和加v，计数加1
   * @param v 待添加的值
   * @since 2.0.0
   */
  override def add(v: jl.Double): Unit = {
    _sum += v
    _count += 1
  }

  /**
   * 添加Scala Double类型值到累加器，总和加v，计数加1
   * @param v 待添加的值
   * @since 2.0.0
   */
  def add(v: Double): Unit = {
    _sum += v
    _count += 1
  }

  /**
   * 获取添加到累加器的元素数量
   * @return 元素计数
   * @since 2.0.0
   */
  def count: Long = _count

  /**
   * 获取所有添加元素的总和
   * @return 总和
   * @since 2.0.0
   */
  def sum: Double = _sum

  /**
   * 获取所有添加元素的平均值
   * @return 平均值
   * @since 2.0.0
   */
  def avg: Double = _sum / _count

  override def merge(other: AccumulatorV2[jl.Double, jl.Double]): Unit = other match {
    case o: DoubleAccumulator =>
      _sum += o.sum
      _count += o.count
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  private[spark] def setValue(newValue: Double): Unit = _sum = newValue

  override def value: jl.Double = _sum
}


/**
 * 用于收集所有元素到列表的累加器
 * @since 2