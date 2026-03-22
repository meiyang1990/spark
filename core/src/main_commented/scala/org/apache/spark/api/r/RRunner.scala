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

import java.io._

import org.apache.spark.broadcast.Broadcast

/**
 * SparkR中执行R用户自定义函数的辅助运行类
 * 负责管理与R进程的通信，完成数据的序列化输入和结果反序列化输出
 * 继承自BaseRRunner实现具体的读写逻辑
 * 
 * @param func 序列化后的R函数字节数组
 * @param deserializer 输入数据反序列化格式
 * @param serializer 输出数据序列化格式
 * @param packageNames 需要安装的R包名称序列化字节数组
 * @param broadcastVars 广播变量数组，用于传递大对象到R执行节点
 * @param numPartitions 分区数量，RDD模式下为-1
 * @param isDataFrame 是否处理DataFrame数据
 * @param colNames DataFrame列名数组
 * @param mode 运行模式，对应RDD、DataFrame等不同模式
 */
private[spark] class RRunner[IN, OUT](
    func: Array[Byte],
    deserializer: String,
    serializer: String,
    packageNames: Array[Byte],
    broadcastVars: Array[Broadcast[Object]],
    numPartitions: Int = -1,
    isDataFrame: Boolean = false,
    colNames: Array[String] = null,
    mode: Int = RRunnerModes.RDD)
  extends BaseRRunner[IN, OUT](
    func,
    deserializer,
    serializer,
    packageNames,
    broadcastVars,
    numPartitions,
    isDataFrame,
    colNames,
    mode) {

  /**
   * 创建从R进程读取输出结果的迭代器
   * 
   * @param dataStream R进程输出输入流
   * @param errThread R进程错误日志读取线程
   * @return 读取R输出结果的迭代器实例
   */
  protected def newReaderIterator(
      dataStream: DataInputStream, errThread: BufferedStreamThread): ReaderIterator = {
    new ReaderIterator(dataStream, errThread) {
      // 根据分区数量和序列化格式选择对应的读取方法
      private val readData = numPartitions match {
        case -1 =>
          serializer match {
            case SerializationFormats.STRING => readStringData _
            case _ => readByteArrayData _
          }
        case _ => readShuffledData _
      }

      /**
       * 读取shuffle聚合后的R输出数据
       * 格式为哈希key + 聚合结果字节数组
       */
      private def readShuffledData(length: Int): (Int, Array[Byte]) = {
        length match {
          case length if length == 2 =>
            val hashedKey = dataStream.readInt()
            val contentPairsLength = dataStream.readInt()
            val contentPairs = new Array[Byte](contentPairsLength)
            dataStream.readFully(contentPairs)
            (hashedKey, contentPairs)
          case _ => null
        }
      }

      /**
       * 读取字节数组格式的R输出结果
       */
      private def readByteArrayData(length: Int): Array[Byte] = {
        length match {
          case length if length > 0 =>
            val obj = new Array[Byte](length)
            dataStream.readFully(obj)
            obj
          case _ => null
        }
      }

      /**
       * 读取字符串格式的R输出结果
       */
      private def readStringData(length: Int): String = {
        length match {
          case length if length > 0 =>
            SerDe.readStringBytes(dataStream, length)
          case _ => null
        }
      }

      /**
       * 从R进程输出流读取下一个结果对象
       * 遇到时间统计数据会先处理日志再继续读取
       * 流结束返回null
       */
      override protected def read(): OUT = {
        try {
          val length = dataStream.readInt()

          length match {
            case SpecialLengths.TIMING_DATA =>
              // 读取并记录R worker各阶段执行时间日志
              val boot = dataStream.readDouble - bootTime
              val init = dataStream.readDouble
              val broadcast = dataStream.readDouble
              val input = dataStream.readDouble
              val compute = dataStream.readDouble
              val output = dataStream.readDouble
              logInfo(
                ("Times: boot = %.3f s, init = %.3f s, broadcast = %.3f s, " +
                  "read-input = %.3f s, compute = %.3f s, write-output = %.3f s, " +
                  "total = %.3f s").format(
                  boot,
                  init,
                  broadcast,
                  input,
                  compute,
                  output,
                  boot + init + broadcast + input + compute + output))
              // 时间数据处理完后继续读取下一个结果
              read()
            case length if length > 0 =>
              // 使用对应格式读取结果并返回
              readData(length).asInstanceOf[OUT]
            case length if length == 0 =>
              // 标记流结束，返回null
              eos = true
              null.asInstanceOf[OUT]
          }
        } catch handleException
      }
    }
  }

  /**
   * 创建向R进程写入输入数据的写线程
   * 
   * @param output 到R进程的输出流
   * @param iter 输入数据迭代器
   * @param partitionIndex 当前分区索引
   * @return 写线程实例
   */
  protected override def newWriterThread(
      output: OutputStream,
      iter: Iterator[IN],
      partitionIndex: Int): WriterThread = {
    new WriterThread(output, iter, partitionIndex) {

      /**
       * 将输入迭代器数据全部写入连接到R worker的输出流
       * 根据反序列化格式处理不同类型输入数据
       */
      override protected def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        // 根据反序列化格式写入单个元素
        def writeElem(elem: Any): Unit = {
          if (deserializer == SerializationFormats.BYTE) {
            val elemArr = elem.asInstanceOf[Array[Byte]]
            dataOut.writeInt(elemArr.length)
            dataOut.write(elemArr)
          } else if (deserializer == SerializationFormats.ROW) {
            dataOut.write(elem.asInstanceOf[Array[Byte]])
          } else if (deserializer == SerializationFormats.STRING) {
            // 字符串RDD场景，按行写入
            // scalastyle:off println
            printOut.println(elem)
            // scalastyle:on println
          }
        }

        // 遍历输入所有元素，按数据结构分情况写入
        for (elem <- iter) {
          elem match {
            // group-by聚合场景：key对应多个元素，写入所有元素后再写入key作为边界
            case (key, innerIter: Iterator[_]) =>
              for (innerElem <- innerIter) {
                writeElem(innerElem)
              }
              // 写入分组结束标记和key，供R端识别分组边界
              dataOut.writeByte('r')
              writeElem(key)
            // 键值对场景：依次写入key和value
            case (key, value) =>
              writeElem(key)
              writeElem(value)
            // 普通单元素场景：直接写入元素
            case _ =>
              writeElem(elem)
          }
        }
      }
    }
  }
}