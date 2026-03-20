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

package org.apache.spark.serializer

import java.io._
import java.lang.invoke.MethodHandles
import java.lang.reflect.{Field, Method}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging

/**
 * SerializationDebugger - 序列化调试器
 * 
 * 核心功能：当发生 NotSerializableException 时，追踪并报告从根对象到不可序列化对象的完整路径。
 * 
 * 设计思路：
 * - 模拟 OpenJDK 的序列化机制，递归遍历对象图
 * - 支持各种序列化场景：原始类型、数组、Serializable、Externalizable、writeReplace、writeObject
 * - 使用虚拟 ObjectOutputStream 捕获 writeObject() 中的对象
 * - 维护 visited 集合避免循环引用
 * 
 * 使用场景：
 * - 开发时快速定位闭包中的不可序列化字段
 * - 理解复杂对象图的序列化路径
 * 
 * 关闭条件：
 * - 如果 JVM 启用了 sun.io.serialization.extendedDebugInfo，则自动关闭（避免重复）
 * 
 * 典型输出：
 * ```
 * NotSerializableException: com.example.MyClass
 * Serialization stack:
 *   - object (class: MyClass, ...)
 *   - field (class: MyClass, name: myField, type: ...)
 *   - object not serializable (class: NonSerializableClass, ...)
 * ```
 */
private[spark] object SerializationDebugger extends Logging {

  /**
   * 增强 NotSerializableException，添加从根对象到问题对象的序列化路径
   * 
   * 注意：如果 JVM 启用了 `sun.io.serialization.extendedDebugInfo`，
   * 则自动关闭此功能（避免重复信息）
   * 
   * @param obj 尝试序列化的根对象
   * @param e 原始的 NotSerializableException
   * @return 增强后的异常（包含序列化栈信息）
   */
  def improveException(obj: Any, e: NotSerializableException): NotSerializableException = {
    if (enableDebugging && reflect != null) {
      try {
        new NotSerializableException(
          e.getMessage + "\nSerialization stack:\n" + find(obj).map("\t- " + _).mkString("\n"))
      } catch {
        case NonFatal(t) =>
          // Fall back to old exception
          logWarning("Exception in serialization debugger", t)
          e
      }
    } else {
      e
    }
  }

  /**
   * 查找导致对象不可序列化的路径
   * 
   * 此方法模拟 OpenJDK 的序列化机制，处理以下场景：
   * - 原始类型
   * - 原始类型数组
   * - 对象数组
   * - Serializable 对象
   * - Externalizable 对象
   * - writeReplace 方法
   * - writeObject 方法（通过虚拟 ObjectOutputStream 捕获）
   * 
   * @param obj 要检查的对象
   * @return 从根对象到不可序列化对象的路径列表（如果全部可序列化则返回空列表）
   */
  private[serializer] def find(obj: Any): List[String] = {
    new SerializationDebugger().visit(obj, List.empty)
  }

  /**
   * 是否启用调试功能
   * 
   * 控制逻辑：
   * - JDK 24+：读取 sun.io.serialization.extendedDebugInfo 系统属性
   * - JDK 24-：通过反射读取 ObjectOutputStream.extendedDebugInfo 静态字段
   *   （避免跨 JDK 版本的 SecurityManager 变化问题，参见 JEP 486）
   * 
   * 如果 JVM 已启用扩展调试信息，则关闭 Spark 的调试器（避免重复）
   */
  private[serializer] var enableDebugging: Boolean =
    if (Runtime.version().feature() >= 24) {
      // Access plain system property on modern JDKs.
      // https://github.com/openjdk/jdk/commit/9b0ab92b16f682e65e9847e8127b6ce09fc5759c
      !java.lang.Boolean.getBoolean("sun.io.serialization.extendedDebugInfo")
    } else {
      // Try to access the private static boolean ObjectOutputStream.extendedDebugInfo
      // to avoid handling SecurityManager changes across different version of JDKs.
      // See details at - JEP 486: Permanently Disable the Security Manager (JDK 24)
      val clazz = classOf[ObjectOutputStream]
      val lookup = MethodHandles.privateLookupIn(clazz, MethodHandles.lookup())
      val vh = lookup.findStaticVarHandle(clazz, "extendedDebugInfo", java.lang.Boolean.TYPE)
      !vh.get().asInstanceOf[Boolean]
    }

  /**
   * 序列化调试器的内部实现类
   * 
   * 核心思想：深度优先遍历对象图，记录访问路径，直到找到第一个不可序列化的对象
   */
  private class SerializationDebugger {

    /** 
     * 已访问对象集合，用于：
     * 1. 避免循环引用导致的无限递归
     * 2. 避免重复访问同一对象
     */
    private val visited = new mutable.HashSet[Any]

    /**
     * 访问对象及其字段，直到找到不可序列化的对象
     * 
     * 访问策略：
     * - null 和已访问对象：跳过
     * - 原始类型/String/原始数组：可序列化
     * - 对象数组：递归访问每个元素
     * - Externalizable：调用 writeExternal 并访问写入的对象
     * - Serializable：处理 slots（父类字段）和 writeObject/writeReplace
     * - 其他：不可序列化
     * 
     * @param o 要访问的对象
     * @param stack 当前访问路径（用于错误报告）
     * @return 从根对象到不可序列化对象的路径（空列表表示全部可序列化）
     */
    def visit(o: Any, stack: List[String]): List[String] = {
      if (o == null) {
        List.empty
      } else if (visited.contains(o)) {
        List.empty
      } else {
        visited += o
        o match {
          // Primitive value, string, and primitive arrays are always serializable
          case _ if o.getClass.isPrimitive => List.empty
          case _: String => List.empty
          case _ if o.getClass.isArray && o.getClass.getComponentType.isPrimitive => List.empty

          // Traverse non primitive array.
          case a: Array[_] if o.getClass.isArray && !o.getClass.getComponentType.isPrimitive =>
            val elem = s"array (class ${a.getClass.getName}, size ${a.length})"
            visitArray(o.asInstanceOf[Array[_]], elem :: stack)

          case e: java.io.Externalizable =>
            val elem = s"externalizable object (class ${e.getClass.getName}, $e)"
            visitExternalizable(e, elem :: stack)

          case s: Object with java.io.Serializable =>
            val str = try {
              s.toString
            } catch {
              case NonFatal(_) => "exception in toString"
            }
            val elem = s"object (class ${s.getClass.getName}, $str)"
            visitSerializable(s, elem :: stack)

          case _ =>
            // Found an object that is not serializable!
            s"object not serializable (class: ${o.getClass.getName}, value: $o)" :: stack
        }
      }
    }

    private def visitArray(o: Array[_], stack: List[String]): List[String] = {
      var i = 0
      while (i < o.length) {
        val childStack = visit(o(i), s"element of array (index: $i)" :: stack)
        if (childStack.nonEmpty) {
          return childStack
        }
        i += 1
      }
      List.empty
    }

    /**
     * 访问 Externalizable 对象
     * 
     * 实现思路：
     * - writeExternal() 可以写入任意对象，无法静态分析
     * - 使用虚拟 ObjectOutput（ListObjectOutput）捕获所有写入的对象
     * - 递归访问这些捕获的对象
     * 
     * 与 visitSerializableWithWriteObjectMethod 类似，但用于 Externalizable
     */
    private def visitExternalizable(o: java.io.Externalizable, stack: List[String]): List[String] =
    {
      val fieldList = new ListObjectOutput
      o.writeExternal(fieldList)
      val childObjects = fieldList.outputArray
      var i = 0
      while (i < childObjects.length) {
        val childStack = visit(childObjects(i), "writeExternal data" :: stack)
        if (childStack.nonEmpty) {
          return childStack
        }
        i += 1
      }
      List.empty
    }

    /**
     * 访问 Serializable 对象
     * 
     * 核心概念：Slots（插槽）
     * - Java 序列化中，每个类及其父类对应一个 slot
     * - ObjectOutputStream 按照 slot 顺序递归序列化字段
     * 
     * 例如：
     * ```
     * class ParentClass(parentField: Int)
     * class ChildClass(childField: Int) extends ParentClass(1)
     * ```
     * 
     * 序列化 ChildClass 的对象 Obj 时：
     * 1. 先序列化 ParentClass slot 的字段（parentField）
     * 2. 再序列化 ChildClass slot 的字段（childField）
     * 
     * 处理流程：
     * 1. 调用 findObjectAndDescriptor 处理 writeReplace（可能替换对象）
     * 2. 遍历所有 slots（从父类到子类）
     * 3. 对每个 slot：
     *    - 如果定义了 writeObject：使用虚拟 ObjectOutputStream 捕获对象
     *    - 否则：直接访问字段值
     */
    private def visitSerializable(o: Object, stack: List[String]): List[String] = {
      // 获取最终要序列化的对象和描述符（处理 writeReplace）
      val (finalObj, desc) = findObjectAndDescriptor(o)

      // 如果 writeReplace() 替换了对象，递归访问替换后的对象
      if (finalObj.getClass != o.getClass) {
        return visit(finalObj, s"writeReplace data (class: ${finalObj.getClass.getName})" :: stack)
      }

      // 获取所有 slots（类继承层次中的每个类对应一个 slot）
      val slotDescs = desc.getSlotDescs
      var i = 0
      while (i < slotDescs.length) {
        val slotDesc = slotDescs(i)
        if (slotDesc.hasWriteObjectMethod) {
          // If the class type corresponding to current slot has writeObject() defined,
          // then its not obvious which fields of the class will be serialized as the writeObject()
          // can choose arbitrary fields for serialization. This case is handled separately.
          val elem = s"writeObject data (class: ${slotDesc.getName})"
          val childStack = visitSerializableWithWriteObjectMethod(finalObj, elem :: stack)
          if (childStack.nonEmpty) {
            return childStack
          }
        } else {
          // Visit all the fields objects of the class corresponding to the current slot.
          val fields: Array[ObjectStreamField] = slotDesc.getFields
          val objFieldValues: Array[Object] = new Array[Object](slotDesc.getNumObjFields)
          val numPrims = fields.length - objFieldValues.length
          slotDesc.getObjFieldValues(finalObj, objFieldValues)

          var j = 0
          while (j < objFieldValues.length) {
            val fieldDesc = fields(numPrims + j)
            val elem = s"field (class: ${slotDesc.getName}" +
              s", name: ${fieldDesc.getName}" +
              s", type: ${fieldDesc.getType})"
            val childStack = visit(objFieldValues(j), elem :: stack)
            if (childStack.nonEmpty) {
              return childStack
            }
            j += 1
          }
        }
        i += 1
      }
      List.empty
    }

    /**
     * 访问定义了 writeObject() 方法的 Serializable 对象
     * 
     * 实现思路：
     * - writeObject() 可以写入任意对象，无法静态分析字段
     * - 使用虚拟 ObjectOutputStream（ListObjectOutputStream）捕获所有写入的对象
     * - 如果捕获过程中发生 IOException（不可序列化），则递归访问捕获的对象
     * - 如果捕获成功（全部可序列化），则将捕获的对象加入 visited（优化）
     * 
     * 与 visitExternalizable 类似，但用于自定义 writeObject 的 Serializable
     */
    private def visitSerializableWithWriteObjectMethod(
        o: Object, stack: List[String]): List[String] = {
      val innerObjectsCatcher = new ListObjectOutputStream
      var notSerializableFound = false
      try {
        innerObjectsCatcher.writeObject(o)
      } catch {
        case io: IOException =>
          notSerializableFound = true
      }

      // If something was not serializable, then visit the captured objects.
      // Otherwise, all the captured objects are safely serializable, so no need to visit them.
      // As an optimization, just added them to the visited list.
      if (notSerializableFound) {
        val innerObjects = innerObjectsCatcher.outputArray
        var k = 0
        while (k < innerObjects.length) {
          val childStack = visit(innerObjects(k), stack)
          if (childStack.nonEmpty) {
            return childStack
          }
          k += 1
        }
      } else {
        visited ++= innerObjectsCatcher.outputArray
      }
      List.empty
    }
  }

  /**
   * 查找最终要序列化的对象和关联的 ObjectStreamClass
   * 
   * 处理 writeReplace 机制：
   * - Serializable 对象可以定义 writeReplace() 方法来替换序列化对象
   * - 例如：enum 会被替换为 Enum 对象，单例会被替换为占位符
   * 
   * 递归终止条件：
   * - 没有 writeReplace 方法
   * - writeReplace 返回相同类型的对象（防止无限递归）
   * 
   * @return (最终对象, ObjectStreamClass 描述符)
   */
  @tailrec
  private def findObjectAndDescriptor(o: Object): (Object, ObjectStreamClass) = {
    val cl = o.getClass
    val desc = ObjectStreamClass.lookupAny(cl)
    if (!desc.hasWriteReplaceMethod) {
      (o, desc)
    } else {
      val replaced = desc.invokeWriteReplace(o)
      // `writeReplace` recursion stops when the returned object has the same class.
      if (replaced.getClass == o.getClass) {
        (replaced, desc)
      } else {
        findObjectAndDescriptor(replaced)
      }
    }
  }

  /**
   * 虚拟 ObjectOutput，用于捕获 Externalizable.writeExternal() 写入的对象
   * 
   * 工作原理：
   * - 所有原始类型写入操作（writeInt、writeBoolean 等）都是空操作
   * - writeObject() 将对象添加到 output 缓冲区
   * - 通过 outputArray 返回所有捕获的对象
   * 
   * 用途：visitExternalizable 使用此类捕获 writeExternal 写入的对象
   */
  private class ListObjectOutput extends ObjectOutput {
    private val output = new mutable.ArrayBuffer[Any]
    def outputArray: Array[Any] = output.toArray
    override def writeObject(o: Any): Unit = output += o
    override def flush(): Unit = {}
    override def write(i: Int): Unit = {}
    override def write(bytes: Array[Byte]): Unit = {}
    override def write(bytes: Array[Byte], i: Int, i1: Int): Unit = {}
    override def close(): Unit = {}
    override def writeFloat(v: Float): Unit = {}
    override def writeChars(s: String): Unit = {}
    override def writeDouble(v: Double): Unit = {}
    override def writeUTF(s: String): Unit = {}
    override def writeShort(i: Int): Unit = {}
    override def writeInt(i: Int): Unit = {}
    override def writeBoolean(b: Boolean): Unit = {}
    override def writeBytes(s: String): Unit = {}
    override def writeChar(i: Int): Unit = {}
    override def writeLong(l: Long): Unit = {}
    override def writeByte(i: Int): Unit = {}
  }

  /** 
   * 模拟 /dev/null 的输出流，忽略所有写入的字节
   * 
   * 用途：ListObjectOutputStream 使用此流丢弃序列化字节（仅关心对象本身）
   */
  private class NullOutputStream extends OutputStream {
    override def write(b: Int): Unit = { }
  }

  /**
   * 虚拟 ObjectOutputStream，用于捕获 writeObject() 写入的对象
   * 
   * 工作原理：
   * - enableReplaceObject(true)：启用对象替换
   * - replaceObject() 会在每个对象序列化前被调用
   * - 将对象添加到 output 缓冲区，然后返回原对象继续序列化
   * - 序列化的字节被写入 NullOutputStream（丢弃）
   * 
   * 巧妙设计：
   * - 利用 ObjectOutputStream 的 replaceObject hook 捕获所有对象
   * - 不关心序列化字节，只关心对象引用
   * - 避免手动解析 writeObject() 的自定义序列化逻辑
   * 
   * 用途：visitSerializableWithWriteObjectMethod 使用此类捕获 writeObject 写入的对象
   */
  private class ListObjectOutputStream extends ObjectOutputStream(new NullOutputStream) {
    private val output = new mutable.ArrayBuffer[Any]
    this.enableReplaceObject(true)

    def outputArray: Array[Any] = output.toArray

    override def replaceObject(obj: Object): Object = {
      output += obj
      obj
    }
  }

  /** 
   * 隐式类：为 ObjectStreamClass 提供调用私有方法的能力
   * 
   * 背景：ObjectStreamClass 的许多核心方法是私有的，需要通过反射访问
   * 
   * 提供的方法：
   * - getSlotDescs：获取类继承层次的 slots
   * - hasWriteObjectMethod：是否定义了 writeObject
   * - hasWriteReplaceMethod：是否定义了 writeReplace
   * - invokeWriteReplace：调用 writeReplace 方法
   * - getNumObjFields：获取对象字段数量
   * - getObjFieldValues：获取对象字段值
   */
  implicit class ObjectStreamClassMethods(val desc: ObjectStreamClass) extends AnyVal {
    def getSlotDescs: Array[ObjectStreamClass] = {
      reflect.GetClassDataLayout.invoke(desc).asInstanceOf[Array[Object]].map {
        classDataSlot => reflect.DescField.get(classDataSlot).asInstanceOf[ObjectStreamClass]
      }
    }

    def hasWriteObjectMethod: Boolean = {
      reflect.HasWriteObjectMethod.invoke(desc).asInstanceOf[Boolean]
    }

    def hasWriteReplaceMethod: Boolean = {
      reflect.HasWriteReplaceMethod.invoke(desc).asInstanceOf[Boolean]
    }

    def invokeWriteReplace(obj: Object): Object = {
      reflect.InvokeWriteReplace.invoke(desc, obj)
    }

    def getNumObjFields: Int = {
      reflect.GetNumObjFields.invoke(desc).asInstanceOf[Int]
    }

    def getObjFieldValues(obj: Object, out: Array[Object]): Unit = {
      reflect.GetObjFieldValues.invoke(desc, obj, out)
    }
  }

  /**
   * 反射对象持有者，用于访问 ObjectStreamClass 的私有方法
   * 
   * 初始化策略：
   * - 尝试通过反射获取所有需要的私有方法
   * - 如果失败（JVM 不支持），则设为 null 并禁用调试器
   * 
   * 这种设计允许在不支持的 JVM 上优雅降级
   */
  private val reflect: ObjectStreamClassReflection = try {
    new ObjectStreamClassReflection
  } catch {
    case e: Exception =>
      logWarning("Cannot find private methods using reflection", e)
      null
  }

  /**
   * 通过反射访问 ObjectStreamClass 私有方法的封装类
   * 
   * 为什么需要反射：
   * - ObjectStreamClass 的核心方法（getClassDataLayout、hasWriteObjectMethod 等）是私有的
   * - Spark 需要这些方法来模拟 Java 序列化机制
   * 
   * 初始化时机：
   * - 所有方法在类初始化时通过反射获取并设置 accessible
   * - 如果反射失败，外层 try-catch 会捕获并禁用调试器
   */
  private class ObjectStreamClassReflection {
    /** ObjectStreamClass.getClassDataLayout */
    val GetClassDataLayout: Method = {
      val f = classOf[ObjectStreamClass].getDeclaredMethod("getClassDataLayout")
      f.setAccessible(true)
      f
    }

    /** ObjectStreamClass.hasWriteObjectMethod */
    val HasWriteObjectMethod: Method = {
      val f = classOf[ObjectStreamClass].getDeclaredMethod("hasWriteObjectMethod")
      f.setAccessible(true)
      f
    }

    /** ObjectStreamClass.hasWriteReplaceMethod */
    val HasWriteReplaceMethod: Method = {
      val f = classOf[ObjectStreamClass].getDeclaredMethod("hasWriteReplaceMethod")
      f.setAccessible(true)
      f
    }

    /** ObjectStreamClass.invokeWriteReplace */
    val InvokeWriteReplace: Method = {
      val f = classOf[ObjectStreamClass].getDeclaredMethod("invokeWriteReplace", classOf[Object])
      f.setAccessible(true)
      f
    }

    /** ObjectStreamClass.getNumObjFields */
    val GetNumObjFields: Method = {
      val f = classOf[ObjectStreamClass].getDeclaredMethod("getNumObjFields")
      f.setAccessible(true)
      f
    }

    /** ObjectStreamClass.getObjFieldValues */
    val GetObjFieldValues: Method = {
      val f = classOf[ObjectStreamClass].getDeclaredMethod(
        "getObjFieldValues", classOf[Object], classOf[Array[Object]])
      f.setAccessible(true)
      f
    }

    /** ObjectStreamClass$ClassDataSlot.desc field */
    val DescField: Field = {
      // scalastyle:off classforname
      val f = Class.forName("java.io.ObjectStreamClass$ClassDataSlot").getDeclaredField("desc")
      // scalastyle:on classforname
      f.setAccessible(true)
      f
    }
  }
}
