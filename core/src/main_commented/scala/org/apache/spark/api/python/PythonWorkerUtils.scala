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

package org.apache.spark.api.python

import java.io.{DataInputStream, DataOutputStream, File}
import java.nio.charset.StandardCharsets

import scala.jdk.CollectionConverters._

import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import org.apache.spark._
import org.apache.spark.{SparkEnv, SparkFiles}
import org.apache.spark.api.python.PythonFunction.PythonAccumulator
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging

/**
 * PySpark与Python工作进程通信的工具类，提供数据序列化、读写工具方法
 * 用于JVM核心与Python执行进程之间的命令、数据和上下文信息交互
 */
private[spark] object PythonWorkerUtils extends Logging {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  /**
   * 向输出流写入UTF-8编码的字符串，长度头+内容的格式
   * Python端通过UTF8Deserializer.loads读取
   * @param str 待写入字符串
   * @param dataOut 输出流
   */
  def writeUTF(str: String, dataOut: DataOutputStream): Unit = {
    val bytes = str.getBytes(StandardCharsets.UTF_8)
    writeBytes(bytes, dataOut)
  }

  /**
   * 向输出流写入字节数组，先写长度再写内容
   * Python端通过FramedSerializer._read_with_length读取
   * @param bytes 待写入字节数组
   * @param dataOut 输出流
   */
  def writeBytes(bytes: Array[Byte], dataOut: DataOutputStream): Unit = {
    dataOut.writeInt(bytes.length)
    dataOut.write(bytes)
  }

  /**
   * 向Python工作进程写入Python版本号，用于版本兼容性校验
   * Python端通过worker_util.check_python_version读取并校验
   * @param pythonVer Python版本字符串
   * @param dataOut 输出流
   */
  def writePythonVersion(pythonVer: String, dataOut: DataOutputStream): Unit = {
    writeUTF(pythonVer, dataOut)
  }


  /**
   * 向输出流写入Task上下文信息，供Python工作进程初始化任务上下文使用
   * Python端通过worker_util.setup_task_context读取使用
   * @param context Spark任务上下文对象
   * @param connInfo 连接信息，要么是套接字路径字符串要么是端口号整数
   * @param secret 认证秘钥可选
   * @param dataOut 输出流
   */
  def writeTaskContext(
      context: TaskContext,
      connInfo: Either[String, Int],
      secret: Option[String],
      dataOut: DataOutputStream): Unit = {
    // 将上下文信息序列化为JSON格式
    val json = Serialization.write(Map(
        "isBarrier" -> context.isInstanceOf[BarrierTaskContext],
        "connInfo" -> connInfo.merge,
        "secret" -> secret.orNull,
        "stageId" -> context.stageId(),
        "partitionId" -> context.partitionId(),
        "attemptNumber" -> context.attemptNumber(),
        "taskAttemptId" -> context.taskAttemptId(),
        "cpus" -> context.cpus(),
        "resources" -> context.resources().map { case (k, v) =>
          k -> Map(
            "name" -> v.name,
            "addresses" -> v.addresses
          )
        },
        "localProperties" -> context.getLocalProperties.asScala.toMap
      ))
    writeUTF(json, dataOut)
  }

  /**
   * 向Python工作进程写入Spark文件相关路径信息，用于初始化Python工作环境
   * Python端通过worker_util.setup_spark_files读取使用
   * @param jobArtifactUUID 作业归档UUID可选
   * @param pythonIncludes 需要添加的Python依赖包路径集合
   * @param dataOut 输出流
   */
  def writeSparkFiles(
      jobArtifactUUID: Option[String],
      pythonIncludes: Set[String],
      dataOut: DataOutputStream): Unit = {
    // 获取Spark文件根目录路径，有UUID则拼接UUID子目录
    val root = jobArtifactUUID.map { uuid =>
      new File(SparkFiles.getRootDirectory(), uuid).getAbsolutePath
    }.getOrElse(SparkFiles.getRootDirectory())
    writeUTF(root, dataOut)

    // 写入Python依赖包（zip/egg文件）路径列表
    dataOut.writeInt(pythonIncludes.size)
    for (include <- pythonIncludes) {
      writeUTF(include, dataOut)
    }
  }

  /**
   * 向Python工作进程同步广播变量，增量更新需要添加/移除的广播
   * Python端通过worker_util.setup_broadcasts读取使用
   * @param broadcastVars 当前任务需要的所有Python广播变量序列
   * @param worker Python工作进程对象
   * @param env Spark环境对象
   * @param dataOut 输出流
   */
  def writeBroadcasts(
      broadcastVars: Seq[Broadcast[PythonBroadcast]],
      worker: PythonWorker,
      env: SparkEnv,
      dataOut: DataOutputStream): Unit = {
    // 获取工作进程已缓存的广播ID集合
    val oldBids = PythonRDD.getWorkerBroadcasts(worker)
    val newBids = broadcastVars.map(_.id).toSet
    // 计算需要移除和新增的广播ID
    val toRemove = oldBids.diff(newBids)
    val addedBids = newBids.diff(oldBids)
    val cnt = toRemove.size + addedBids.size
    // 判断是否需要启用解密服务（加密开启且有新增广播）
    val needsDecryptionServer = env.serializerManager.encryptionEnabled && addedBids.nonEmpty
    dataOut.writeBoolean(needsDecryptionServer)
    dataOut.writeInt(cnt)
    // 发送需要移除的广播ID给Python工作进程
    def sendBidsToRemove(): Unit = {
      for (bid <- toRemove) {
        // 负号标记移除，转换保证零也能正确表示
        dataOut.writeLong(-bid - 1)
        oldBids.remove(bid)
      }
    }
    if (needsDecryptionServer) {
      // 加密开启场景：启动解密服务，向Python发送解密服务连接信息
      val idsAndFiles = broadcastVars.flatMap { broadcast =>
        if (!oldBids.contains(broadcast.id)) {
          oldBids.add(broadcast.id)
          Some((broadcast.id, broadcast.value.path))
        } else {
          None
        }
      }
      // 创建加密广播解密服务器
      val server = new EncryptedPythonBroadcastServer(env, idsAndFiles)
      server.connInfo match {
        case portNum: Int =>
          dataOut.writeInt(portNum)
          writeUTF(server.secret, dataOut)
        case sockPath: String =>
          dataOut.writeInt(-1)
          writeUTF(sockPath, dataOut)
      }
      logTrace(s"broadcast decryption server setup on ${server.connInfo}")
      sendBidsToRemove()
      idsAndFiles.foreach { case (id, _) =>
        // 发送新增广播ID
        dataOut.writeLong(id)
      }
      dataOut.flush()
    } else {
      // 无加密场景：直接发送增量更新
      sendBidsToRemove()
      for (broadcast <- broadcastVars) {
        if (!oldBids.contains(broadcast.id)) {
          // 发送新增广播ID和文件路径
          dataOut.writeLong(broadcast.id)
          writeUTF(broadcast.value.path, dataOut)
          oldBids.add(broadcast.id)
        }
      }
    }
    dataOut.flush()
  }

  /**
   * 向Python工作进程写入Python函数序列化后字节数据
   * @param func Python函数对象
   * @param dataOut 输出流
   */
  def writePythonFunction(func: PythonFunction, dataOut: DataOutputStream): Unit = {
    writeBytes(func.command.toArray, dataOut)
  }

  /**
   * 向输出流写入键值对格式的配置信息
   * @param conf 配置键值对Map
   * @param dataOut 输出流
   */
  def writeConf(conf: Map[String, String], dataOut: DataOutputStream): Unit = {
    dataOut.writeInt(conf.size)
    for ((k, v) <- conf) {
      writeUTF(k, dataOut)
      writeUTF(v, dataOut)
    }
  }

  /**
   * 从输入流读取UTF-8编码字符串，先读取长度再读取内容
   * @param dataIn 输入流
   * @return 解码后的字符串
   */
  def readUTF(dataIn: DataInputStream): String = {
    readUTF(dataIn.readInt(), dataIn)
  }

  /**
   * 从输入流读取指定字节长度的UTF-8编码字符串
   * @param length 字节长度
   * @param dataIn 输入流
   * @return 解码后的字符串
   */
  def readUTF(length: Int, dataIn: DataInputStream): String = {
    new String(readBytes(length, dataIn), StandardCharsets.UTF_8)
  }

  /**
   * 从输入流读取字节数组，先读取长度再读取内容
   * @param dataIn 输入流
   * @return 读取到的字节数组
   */
  def readBytes(dataIn: DataInputStream): Array[Byte] = {
    readBytes(dataIn.readInt(), dataIn)
  }

  /**
   * 从输入流读取指定长度的字节数组
   * @param length 字节长度
   * @param dataIn 输入流
   * @return 读取到的字节数组
   */
  def readBytes(length: Int, dataIn: DataInputStream): Array[Byte] = {
    if (length == 0) {
      Array.emptyByteArray
    } else {
      val obj = new Array[Byte](length)
      dataIn.readFully(obj)
      obj
    }
  }

  /**
   * 从Python工作进程接收累加器更新，并更新到对应的累加器
   * 更新数据由Python端worker_util.send_accumulator_updates发送
   * @param maybeAccumulator 目标Python累加器可选，为空则不处理
   * @param dataIn 输入流
   */
  def receiveAccumulatorUpdates(
      maybeAccumulator: Option[PythonAccumulator],
      dataIn: DataInputStream): Unit = {
    val numAccumulatorUpdates = dataIn.readInt()
    (1 to numAccumulatorUpdates).foreach { _ =>
      val update = readBytes(dataIn)
      maybeAccumulator.foreach(_.add(update))
    }
  }
}