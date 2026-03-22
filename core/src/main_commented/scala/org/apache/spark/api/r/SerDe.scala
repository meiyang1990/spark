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

import java.io.{DataInputStream, DataOutputStream}
import java.nio.charset.StandardCharsets
import java.sql.{Date, Time, Timestamp}

import scala.collection.mutable

import org.apache.spark.util.collection.Utils

/**
 * 文件说明：SparkR模块的序列化/反序列化工具，负责处理JVM与R语言端之间的数据对象互转
 * 核心功能：实现R类型到JVM类型的映射转换，以及JVM类型到R类型的序列化输出，支持基本类型、数组、集合、SQL类型和JVM后端对象追踪
 */
/**
 * Utility functions to serialize, deserialize objects to / from R
 */
private[spark] object SerDe {
  /** SQL类型读取函数类型：从输入流读取指定类型的SQL对象 */
  type SQLReadObject = (DataInputStream, Char) => Object
  /** SQL类型写入函数类型：将SQL对象写入输出流，返回是否成功写入 */
  type SQLWriteObject = (DataOutputStream, Object) => Boolean

  private[this] var sqlReadObject: SQLReadObject = _
  private[this] var sqlWriteObject: SQLWriteObject = _

  /**
   * 设置SQL类型对象读取回调函数
   * @param value SQL读取函数
   * @return 当前对象，支持链式调用
   */
  def setSQLReadObject(value: SQLReadObject): this.type = {
    sqlReadObject = value
    this
  }

  /**
   * 设置SQL类型对象写入回调函数
   * @param value SQL写入函数
   * @return 当前对象，支持链式调用
   */
  def setSQLWriteObject(value: SQLWriteObject): this.type = {
    sqlWriteObject = value
    this
  }

  // Type mapping from R to Java
  //
  // NULL -> void
  // integer -> Int
  // character -> String
  // logical -> Boolean
  // double, numeric -> Double
  // raw -> Array[Byte]
  // Date -> Date
  // POSIXlt/POSIXct -> Time
  //
  // list[T] -> Array[T], where T is one of above mentioned types
  // environment -> Map[String, T], where T is a native type
  // jobj -> Object, where jobj is an object created in the backend

  /**
   * 从输入流读取数据类型标记
   * @param dis 数据输入流
   * @return 数据类型字符标记
   */
  def readObjectType(dis: DataInputStream): Char = {
    dis.readByte().toChar
  }

  /**
   * 从输入流反序列化读取一个R端对象，转换为JVM对象
   * @param dis 数据输入流
   * @param jvmObjectTracker JVM对象追踪器，用于管理后端创建的对象
   * @return 转换后的JVM对象
   */
  def readObject(dis: DataInputStream, jvmObjectTracker: JVMObjectTracker): Object = {
    val dataType = readObjectType(dis)
    readTypedObject(dis, dataType, jvmObjectTracker)
  }

  /**
   * 根据类型标记从输入流反序列化指定类型的对象
   * @param dis 数据输入流
   * @param dataType 数据类型标记
   * @param jvmObjectTracker JVM对象追踪器
   * @return 转换后的JVM对象
   */
  def readTypedObject(
      dis: DataInputStream,
      dataType: Char,
      jvmObjectTracker: JVMObjectTracker): Object = {
    dataType match {
      case 'n' => null
      case 'i' => java.lang.Integer.valueOf(readInt(dis))
      case 'd' => java.lang.Double.valueOf(readDouble(dis))
      case 'b' => java.lang.Boolean.valueOf(readBoolean(dis))
      case 'c' => readString(dis)
      case 'e' => readMap(dis, jvmObjectTracker)
      case 'r' => readBytes(dis)
      case 'a' => readArray(dis, jvmObjectTracker)
      case 'l' => readList(dis, jvmObjectTracker)
      case 'D' => readDate(dis)
      case 't' => readTime(dis)
      case 'j' => jvmObjectTracker(JVMObjectId(readString(dis)))
      case _ =>
        if (sqlReadObject == null) {
          throw new IllegalArgumentException (s"Invalid type $dataType")
        } else {
          val obj = sqlReadObject(dis, dataType)
          if (obj == null) {
            throw new IllegalArgumentException (s"Invalid type $dataType")
          } else {
            obj
          }
        }
    }
  }

  /**
   * 从输入流读取字节数组
   * @param in 数据输入流
   * @return 读取到的字节数组
   */
  def readBytes(in: DataInputStream): Array[Byte] = {
    val len = readInt(in)
    val out = new Array[Byte](len)
    in.readFully(out)
    out
  }

  /**
   * 从输入流读取整数
   * @param in 数据输入流
   * @return 整数值
   */
  def readInt(in: DataInputStream): Int = {
    in.readInt()
  }

  /**
   * 从输入流读取双精度浮点数
   * @param in 数据输入流
   * @return 双精度浮点值
   */
  def readDouble(in: DataInputStream): Double = {
    in.readDouble()
  }

  /**
   * 从输入流读取指定长度的字符串，处理null结尾的字符串格式
   * @param in 数据输入流
   * @param len 字节长度
   * @return 解析后的字符串
   */
  def readStringBytes(in: DataInputStream, len: Int): String = {
    val bytes = new Array[Byte](len)
    in.readFully(bytes)
    assert(bytes(len - 1) == 0)
    val str = new String(bytes.dropRight(1), StandardCharsets.UTF_8)
    str
  }

  /**
   * 从输入流读取字符串，先读长度再读内容
   * @param in 数据输入流
   * @return 解析后的字符串
   */
  def readString(in: DataInputStream): String = {
    val len = in.readInt()
    readStringBytes(in, len)
  }

  /**
   * 从输入流读取布尔值
   * @param in 数据输入流
   * @return 布尔值
   */
  def readBoolean(in: DataInputStream): Boolean = {
    in.readInt() != 0
  }

  /**
   * 从输入流读取日期类型
   * @param in 数据输入流
   * @return 日期对象，NA值返回null
   */
  def readDate(in: DataInputStream): Date = {
    val inStr = readString(in)
    if (inStr == "NA") {
      null
    } else {
      Date.valueOf(inStr)
    }
  }

  /**
   * 从输入流读取时间戳类型
   * @param in 数据输入流
   * @return 时间戳对象，NaN值返回null
   */
  def readTime(in: DataInputStream): Timestamp = {
    val seconds = in.readDouble()
    if (java.lang.Double.isNaN(seconds)) {
      null
    } else {
      val sec = Math.floor(seconds).toLong
      val t = new Timestamp(sec * 1000L)
      t.setNanos(((seconds - sec) * 1e9).toInt)
      t
    }
  }

  /**
   * 从输入流读取字节数组数组
   * @param in 数据输入流
   * @return 字节数组数组
   */
  def readBytesArr(in: DataInputStream): Array[Array[Byte]] = {
    val len = readInt(in)
    (0 until len).map(_ => readBytes(in)).toArray
  }

  /**
   * 从输入流读取整数数组
   * @param in 数据输入流
   * @return 整数数组
   */
  def readIntArr(in: DataInputStream): Array[Int] = {
    val len = readInt(in)
    (0 until len).map(_ => readInt(in)).toArray
  }

  /**
   * 从输入流读取双精度浮点数数组
   * @param in 数据输入流
   * @return 双精度浮点数数组
   */
  def readDoubleArr(in: DataInputStream): Array[Double] = {
    val len = readInt(in)
    (0 until len).map(_ => readDouble(in)).toArray
  }

  /**
   * 从输入流读取布尔数组
   * @param in 数据输入流
   * @return 布尔数组
   */
  def readBooleanArr(in: DataInputStream): Array[Boolean] = {
    val len = readInt(in)
    (0 until len).map(_ => readBoolean(in)).toArray
  }

  /**
   * 从输入流读取字符串数组
   * @param in 数据输入流
   * @return 字符串数组
   */
  def readStringArr(in: DataInputStream): Array[String] = {
    val len = readInt(in)
    (0 until len).map(_ => readString(in)).toArray
  }

  // All elements of an array must be of the same type
  /**
   * 从输入流读取同类型数组，所有元素必须类型一致
   * @param dis 数据输入流
   * @param jvmObjectTracker JVM对象追踪器
   * @return 转换后的JVM数组
   */
  def readArray(dis: DataInputStream, jvmObjectTracker: JVMObjectTracker): Array[_] = {
    val arrType = readObjectType(dis)
    arrType match {
      case 'i' => readIntArr(dis)
      case 'c' => readStringArr(dis)
      case 'd' => readDoubleArr(dis)
      case 'b' => readBooleanArr(dis)
      case 'j' => readStringArr(dis).map(x => jvmObjectTracker(JVMObjectId(x)))
      case 'r' => readBytesArr(dis)
      case 'a' =>
        val len = readInt(dis)
        (0 until len).map(_ => readArray(dis, jvmObjectTracker)).toArray
      case 'l' =>
        val len = readInt(dis)
        (0 until len).map(_ => readList(dis, jvmObjectTracker)).toArray
      case _ =>
        if (sqlReadObject == null) {
          throw new IllegalArgumentException (s"Invalid array type $arrType")
        } else {
          val len = readInt(dis)
          (0 until len).map { _ =>
            val obj = sqlReadObject(dis, arrType)
            if (obj == null) {
              throw new IllegalArgumentException (s"Invalid array type $arrType")
            } else {
              obj
            }
          }.toArray
        }
    }
  }

  // Each element of a list can be of different type. They are all represented
  // as Object on JVM side
  /**
   * 从输入流读取列表，支持元素类型不同，全部转换为Object数组返回
   * @param dis 数据输入流
   * @param jvmObjectTracker JVM对象追踪器
   * @return 元素为Object类型的数组，支持不同类型元素
   */
  def readList(dis: DataInputStream, jvmObjectTracker: JVMObjectTracker): Array[Object] = {
    val len = readInt(dis)
    (0 until len).map(_ => readObject(dis, jvmObjectTracker)).toArray
  }

  /**
   * 从输入流读取Map对象，转换为Java Map返回
   * @param in 数据输入流
   * @param jvmObjectTracker JVM对象追踪器
   * @return 转换后的Java Map对象
   */
  def readMap(
      in: DataInputStream,
      jvmObjectTracker: JVMObjectTracker): java.util.Map[Object, Object] = {
    val len = readInt(in)
    if (len > 0) {
      // Keys is an array of String
      val keys = readArray(in, jvmObjectTracker).asInstanceOf[Array[Object]]
      val values = readList(in, jvmObjectTracker)

      Utils.toJavaMap(keys, values)
    } else {
      new java.util.HashMap[Object, Object]()
    }
  }

  // Methods to write out data from Java to R
  //
  // Type mapping from Java to R
  //
  // void -> NULL
  // Int -> integer
  // String -> character
  // Boolean -> logical
  // Float -> double
  // Double -> double
  // Decimal -> double
  // Long -> double
  // Array[Byte] -> raw
  // Date -> Date
  // Time -> POSIXct
  //
  // Array[T] -> list()
  // Object -> jobj

  /**
   * 将类型名称写入输出流，转换为R对应的类型标记字节
   * @param dos 数据输出流
   * @param typeStr JVM类型名称
   */
  def writeType(dos: DataOutputStream, typeStr: String): Unit = {
    typeStr match {
      case "void" => dos.writeByte('n')
      case "character" => dos.writeByte('c')
      case "double" => dos.writeByte('d')
      case "integer" => dos.writeByte('i')
      case "logical" => dos.writeByte('b')
      case "date" => dos.writeByte('D')
      case "time" => dos.writeByte('t')
      case "raw" => dos.writeByte('r')
      // Array of primitive types
      case "array" => dos.writeByte('a')
      // Array of objects
      case "list" => dos.writeByte('l')
      case "map" => dos.writeByte('e')
      case "jobj" => dos.writeByte('j')
      case _ => throw new IllegalArgumentException(s"Invalid type $typeStr")
    }
  }

  /**
   * 写入Map的单个键值对
   * @param dos 数据输出流
   * @param key 键对象，必须为非null字符串
   * @param value 值对象
   * @param jvmObjectTracker JVM对象追踪器
   */
  private def writeKeyValue(
      dos: DataOutputStream,
      key: Object,
      value: Object,
      jvmObjectTracker: JVMObjectTracker): Unit = {
    if (key == null) {
      throw new IllegalArgumentException("Key in map can't be null.")
    } else if (!key.isInstanceOf[String]) {
      throw new IllegalArgumentException(s"Invalid map key type: ${key.getClass.getName}")
    }

    writeString(dos, key.asInstanceOf[String])
    writeObject(dos, value, jvmObjectTracker)
  }

  /**
   * 将JVM对象序列化写入输出流，供R端读取
   * @param dos 数据输出流
   * @param obj 待序列化的JVM对象
   * @param jvmObjectTracker JVM对象追踪器
   */
  def writeObject(dos: DataOutputStream, obj: Object, jvmObjectTracker: JVMObjectTracker): Unit = {
    if (obj == null) {
      writeType(dos, "void")
    } else {
      // Convert ArrayType collected from DataFrame to Java array
      // Collected data of ArrayType from a DataFrame is observed to be of
      // type "scala.collection.mutable.ArraySeq"
      val value = obj match {
        case wa: mutable.ArraySeq[_] => wa.array
        case other => other
      }

      value match {
        case v: java.lang.Character =>
          writeType(dos, "character")
          writeString(dos, v.toString)
        case v: java.lang.String =>
          writeType(dos, "character")
          writeString(dos, v)
        case v: java.lang.Long =>
          writeType(dos, "double")
          writeDouble(dos, v.toDouble)
        case v: java.lang.Float =>
          writeType(dos, "double")
          writeDouble(dos, v.toDouble)
        case v: java.math.BigDecimal =>
          writeType(dos, "double")
          writeDouble(dos, scala.math.BigDecimal(v).toDouble)
        case v: java.lang.Double =>
          writeType(dos, "double")
          writeDouble(dos, v)
        case v: java.lang.Byte =>
          writeType(dos, "integer")
          writeInt(dos, v.toInt)
        case v: java.lang.S