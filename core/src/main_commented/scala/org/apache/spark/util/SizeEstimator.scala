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

import java.lang.management.ManagementFactory
import java.lang.reflect.{Field, Modifier}
import java.util.{IdentityHashMap, Random}

import scala.collection.mutable.ArrayBuffer
import scala.runtime.ScalaRunTime

import com.google.common.collect.MapMaker

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Tests.TEST_USE_COMPRESSED_OOPS_KEY
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.OpenHashSet

/**
 * 文件说明: JVM对象内存大小估算工具，用于Spark内存感知缓存、广播变量等场景的内存占用计算
 * 核心功能: 通过反射遍历对象引用图，估算整个对象图占用的堆内存大小，考虑JVM对象布局和对齐规则
 */

/**
 * 自定义大小估算接口，允许类自己提供更精确的内存大小估算，不需要SizeEstimator通过反射计算
 * 与SizeTracker的区别：SizeTracker仍然依赖SizeEstimator估算，本接口允许类直接提供准确值
 */
private[spark] trait KnownSizeEstimation {
  def estimatedSize: Long
}

/**
 * :: DeveloperApi ::
 * JVM对象堆内存大小估算器，用于计算对象及其所有引用对象占用的总内存大小
 * 主要用于内存感知缓存、广播变量大小估算等需要知道对象实际内存占用的场景
 * 基于JavaWorld文章《sizeof for Java》实现，模拟HotSpot JVM对象布局规则进行估算
 */
@DeveloperApi
object SizeEstimator extends Logging {

  /**
   * 估算给定对象在JVM堆上占用的总字节数，包含该对象所有引用的对象递归计算
   * 用于估算广播变量在每个Executor上的占用、缓存对象的反序列化内存占用等场景
   * 注意：这不是序列化后的大小，序列化后通常远小于堆内存大小
   * @param obj 待估算的对象
   * @return 估算的总内存大小，单位字节
   */
  def estimate(obj: AnyRef): Long = estimate(obj, new IdentityHashMap[AnyRef, AnyRef])

  // 基础类型占用字节数
  private val BYTE_SIZE = 1
  private val BOOLEAN_SIZE = 1
  private val CHAR_SIZE = 2
  private val SHORT_SIZE = 2
  private val INT_SIZE = 4
  private val LONG_SIZE = 8
  private val FLOAT_SIZE = 4
  private val DOUBLE_SIZE = 8

  // 字段大小按降序排列，用于模拟HotSpot字段排序布局（大字段优先排列）
  private val fieldSizes = List(8, 4, 2, 1)

  // 对象对齐边界，HotSpot默认按8字节对齐
  private val ALIGN_SIZE = 8

  // 类信息缓存，使用弱键允许动态生成的类被GC回收
  private val classInfos = new MapMaker().weakKeys().makeMap[Class[_], ClassInfo]()

  // JVM架构相关标识和大小参数
  private var is64bit = false
  // 是否开启压缩指针
  private var isCompressedOops = false
  // 对象引用指针大小
  private var pointerSize = 4
  // 基础Object对象头大小
  private var objectSize = 8

  initialize()

  /**
   * 根据当前JVM架构和压缩指针配置初始化对象头、指针等基础大小参数
   */
  private def initialize(): Unit = {
    val arch = Utils.osArch
    is64bit = arch.contains("64") || arch.contains("s390x")
    isCompressedOops = getIsCompressedOops

    objectSize = if (!is64bit) 8 else {
      if (!isCompressedOops) {
        16
      } else {
        12
      }
    }
    pointerSize = if (is64bit && !isCompressedOops) 8 else 4
    classInfos.clear()
    classInfos.put(classOf[Object], new ClassInfo(objectSize, Nil))
  }

  /**
   * 检测当前JVM是否开启了压缩指针（CompressedOops）
   * @return 是否开启压缩指针
   */
  private def getIsCompressedOops: Boolean = {
    // 测试用例支持通过系统属性覆盖检测结果
    if (System.getProperty(TEST_USE_COMPRESSED_OOPS_KEY) != null) {
      return System.getProperty(TEST_USE_COMPRESSED_OOPS_KEY).toBoolean
    }

    // IBM和OpenJ9 JDK通过java.vm.info判断是否开启压缩引用
    val javaVendor = System.getProperty("java.vendor")
    if (javaVendor.contains("IBM") || javaVendor.contains("OpenJ9")) {
      return System.getProperty("java.vm.info").contains("Compressed Ref")
    }

    // HotSpot JVM通过HotSpotDiagnosticMXBean获取UseCompressedOops配置
    try {
      val hotSpotMBeanName = "com.sun.management:type=HotSpotDiagnostic"
      val server = ManagementFactory.getPlatformMBeanServer()

      // scalastyle:off classforname
      val hotSpotMBeanClass = Class.forName("com.sun.management.HotSpotDiagnosticMXBean")
      val getVMMethod = hotSpotMBeanClass.getDeclaredMethod("getVMOption",
          Class.forName("java.lang.String"))
      // scalastyle:on classforname

      val bean = ManagementFactory.newPlatformMXBeanProxy(server,
        hotSpotMBeanName, hotSpotMBeanClass)
      getVMMethod.invoke(bean, "UseCompressedOops").toString.contains("true")
    } catch {
      // 获取失败时根据最大堆大小猜测：堆小于32G则默认开启压缩指针
      case _: Exception =>
        val guess = Runtime.getRuntime.maxMemory < (32L*1024*1024*1024)
        logWarning(log"Failed to check whether UseCompressedOops is set; " +
          log"assuming " + (if (guess) log"yes" else log"not"))
        guess
    }
  }

  /**
   * 大小估算过程的搜索状态，保存待访问对象栈和已访问对象集合，避免重复计算和循环引用
   * @param visited 已访问过的对象集合，使用IdentityHashMap按对象身份去重
   */
  private class SearchState(val visited: IdentityHashMap[AnyRef, AnyRef]) {
    // 待访问对象栈
    val stack = new ArrayBuffer[AnyRef]
    // 当前累计估算的总大小
    var size = 0L

    /**
     * 将对象加入待访问队列，仅处理未访问过的非空对象
     * @param obj 待加入的对象
     */
    def enqueue(obj: AnyRef): Unit = {
      if (obj != null && !visited.containsKey(obj)) {
        visited.put(obj, null)
        stack += obj
      }
    }

    /**
     * 判断是否所有对象都处理完成
     * @return 处理完成返回true
     */
    def isFinished(): Boolean = stack.isEmpty

    /**
     * 弹出一个待处理对象（后进先出）
     * @return 待处理对象
     */
    def dequeue(): AnyRef = {
      val elem = stack.last
      stack.dropRightInPlace(1)
      elem
    }
  }

  /**
   * 缓存的类信息，保存类的shell大小（所有非静态字段加对象头大小）和对象指针类型字段列表
   * @param shellSize 类自身的外壳大小（包含父类所有字段，不包含引用对象本身大小）
   * @param pointerFields 所有引用类型非静态字段列表
   */
  private class ClassInfo(
    val shellSize: Long,
    val pointerFields: List[Field]) {}

  /**
   * 内部估算方法，使用已有的已访问对象集合避免重复计算
   * @param obj 根对象
   * @param visited 已访问对象集合
   * @return 总估算内存大小，单位字节
   */
  private def estimate(obj: AnyRef, visited: IdentityHashMap[AnyRef, AnyRef]): Long = {
    val state = new SearchState(visited)
    state.enqueue(obj)
    while (!state.isFinished()) {
      visitSingleObject(state.dequeue(), state)
    }
    state.size
  }

  /**
   * 处理单个对象，计算其自身大小并将引用对象加入待处理队列
   * @param obj 待处理对象
   * @param state 搜索状态
   */
  private def visitSingleObject(obj: AnyRef, state: SearchState): Unit = {
    val cls = obj.getClass
    // 数组单独处理
    if (cls.isArray) {
      visitArray(obj, cls, state)
    } 
    // scala.reflect包对象引用大量全局反射对象，跳过计算避免估算过大
    else if (cls.getName.startsWith("scala.reflect")) {
    } 
    // ClassLoader和Class对象全局共享，不纳入估算，避免REPL场景下估算整个类加载器的大小
    else if (obj.isInstanceOf[ClassLoader] || obj.isInstanceOf[Class[_]]) {
    } 
    else {
      obj match {
        // 自定义大小估算，直接使用类提供的大小
        case s: KnownSizeEstimation =>
          state.size += s.estimatedSize
        // 通过反射计算大小
        case _ =>
          val classInfo = getClassInfo(cls)
          state.size += alignSize(classInfo.shellSize)
          for (field <- classInfo.pointerFields) {
            state.enqueue(field.get(obj))
          }
      }
    }
  }

  // 大数组采样估算阈值：超过该大小的数组采用采样估算，避免遍历所有元素耗时过长
  private val ARRAY_SIZE_FOR_SAMPLING = 400
  // 大数组采样大小，必须小于采样阈值
  private val ARRAY_SAMPLE_SIZE = 100

  /**
   * 处理数组对象，计算数组自身大小并将元素对象加入待处理队列
   * 大数组采用两次采样取平均的方式估算总大小，避免全遍历开销
   * @param array 数组对象
   * @param arrayClass 数组类
   * @param state 搜索状态
   */
  private def visitArray(array: AnyRef, arrayClass: Class[_], state: SearchState): Unit = {
    val length = ScalaRunTime.array_length(array)
    val elementClass = arrayClass.getComponentType()

    // 数组大小 = 对象头 + 长度字段（int），对齐后得到基础大小
    var arrSize: Long = alignSize(objectSize + INT_SIZE)

    // 基础类型数组：直接计算所有元素大小，不需要处理引用
    if (elementClass.isPrimitive) {
      arrSize += alignSize(length.toLong * primitiveSize(elementClass))
      state.size += arrSize
    } else {
      // 对象数组：先计算数组自身（指针数组）的大小，元素对象后续处理
      arrSize += alignSize(length.toLong * pointerSize)
      state.size += arrSize

      // 小数组直接遍历所有元素
      if (length <= ARRAY_SIZE_FOR_SAMPLING) {
        var arrayIndex = 0
        while (arrayIndex < length) {
          state.enqueue(ScalaRunTime.array_apply(array, arrayIndex).asInstanceOf[AnyRef])
          arrayIndex += 1
        }
      } else {
        // 大数组采用无放回采样，两次采样取较小值减少共享对象影响，外推估算总大小
        val rand = new Random(42)
        val drawn = new OpenHashSet[Int](2 * ARRAY_SAMPLE_SIZE)
        val s1 = sampleArray(array, state, rand, drawn, length)
        val s2 = sampleArray(array, state, rand, drawn, length)
        val size = math.min(s1, s2)
        state.size += math.max(s1, s2) +
          (size * ((length - ARRAY_SAMPLE_SIZE) / ARRAY_SAMPLE_SIZE))
      }
    }
  }

  /**
   * 对大数组进行随机采样，计算采样元素的总大小
   * @param array 原数组
   * @param state 搜索状态
   * @param rand 随机数生成器
   * @param drawn 已采样下标集合，避免重复采样
   * @param length 数组长度
   * @return 采样元素的总估算大小
   */
  private def sampleArray(
      array: AnyRef,
      state: SearchState,
      rand: Random,
      drawn: OpenHashSet[Int],
      length: Int): Long = {
    var size = 0L
    for (i <- 0 until ARRAY_SAMPLE_SIZE) {
      var index = 0
      do {
        index = rand.nextInt(length)
      } while (drawn.contains(index))
      drawn.add(index)
      val obj = ScalaRunTime.array_apply(array, index).asInstanceOf[AnyRef]
      if (obj != null) {
        size += SizeEstimator.estimate(obj, state.visited)
      }
    }
    size
  }

  /**
   * 获取基础类型的字节大小
   * @param cls 基础类型类对象
   * @return 占用字节数
   */
  private def primitiveSize(cls: Class[_]): Int = {
    if (cls == classOf[Byte]) {
      BYTE_SIZE
    } else if (cls == classOf[Boolean]) {
      BOOLEAN_SIZE
    } else if (cls == classOf[Char]) {
      CHAR_SIZE
    } else if (cls == classOf[Short]) {
      SHORT_SIZE
    } else if (cls == classOf[Int]) {
      INT_SIZE
    } else if (cls == classOf[Long]) {
      LONG_SIZE
    } else if (cls == classOf[Float]) {
      FLOAT_SIZE
    } else if (cls == classOf[Double]) {
      DOUBLE_SIZE
    } else {
      throw new IllegalArgumentException(
      "Non-primitive class " + cls + " passed to primitiveSize()")
    }
  }

  /**
   * 获取或计算指定类的ClassInfo缓存
   * @param cls 目标类
   * @return 缓存的ClassInfo
   */
  private def getClassInfo(cls: Class[_]): ClassInfo = {
    // 从缓存返回已计算的类信息
    val info = classInfos.get(cls)
    if (info != null) {
      return info
    }

    // 递归获取父类信息，继承父类的大小和字段
    val parent = getClassInfo(cls.getSuperclass)
    var shellSize = parent.shellSize
    var pointerFields = parent.pointerFields
    // 统计各大小字段的数量
    val sizeCount = Array.ofDim[Int](fieldSizes.max + 1)

    // 遍历当前类声明的所有字段，收集大小和类型信息
    for (field <- cls.getDeclaredFields) {
      if (!Modifier.isStatic(field.getModifiers)) {
        val fieldClass = field.getType
        if (fieldClass.isPrimitive) {
          sizeCount(primitiveSize(fieldClass)) += 1
        } else {
          try {
            // 设置可访问，后续获取字段值时不需要重复处理
            if (field.trySetAccessible()) {
              pointerFields = field :: pointerFields
            }
          } catch {
            // 安全异常无法访问时，仅计入指针大小，不加入引用处理列表
            case _: SecurityException =>
          }
          sizeCount(pointerSize) += 1
        }
      }
    }

    // 按照HotSpot JVM字段布局规则计算最终对齐后的shell大小
    // 遵循：大字段优先排列、按字段大小对齐、父类先布局、实例对齐到指针大小的规则
    var alignedSize = shellSize
    for (size <- fieldSizes if sizeCount(size) > 0) {
      val count = sizeCount(size).toLong
      // 计算对齐后的起始位置，处理内部空隙可以容纳小字段
      alignedSize = math.max(alignedSize, alignSizeUp(shellSize, size) + size * count)
      shellSize += size * count
    }

    // 最终对齐到指针大小，得到当前类的shell大小
    shellSize = alignSizeUp(alignedSize, pointerSize)

    // 缓存并返回结果
    val newInfo = new ClassInfo(shellSize, pointerFields)
    classInfos.put(cls, newInfo)
    newInfo
  }

  private def alignSize(size: Long): Long = alignSizeUp(size, ALIGN_SIZE)

  /**
   * 计算对齐后的大小，要求对齐大小必须是2的幂次
   * 使用位运算快速向上对齐到指定大小的整数倍
   * @param size 原始大小
   * @param alignSize 对齐边界，必须是2^n