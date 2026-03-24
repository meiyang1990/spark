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

package org.apache.spark.util.logging

import java.io.{File, FileOutputStream, InputStream, IOException}

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging, LogKeys}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.util.{IntParam, Utils}

/**
 * 文件日志追加器，持续将输入流中的数据写入指定文件
 * 用于将Executor进程的标准输出/错误输出重定向写入日志文件
 * 
 * @param inputStream 要读取的输入流，通常是子进程的输出流
 * @param file 目标输出日志文件
 * @param bufferSize 读写缓冲区大小，默认8192字节
 * @param closeStreams 停止时是否关闭输入流，默认false
 */
private[spark] class FileAppender(
  inputStream: InputStream,
  file: File,
  bufferSize: Int = 8192,
  closeStreams: Boolean = false
) extends Logging {
  @volatile private var outputStream: FileOutputStream = null
  @volatile private var markedForStop = false     // 标记追加器已请求停止
  // 后台线程，负责从输入流读取数据并写入文件
  private val writingThread = new Thread("File appending thread for " + file) {
    setDaemon(true)
    override def run(): Unit = {
      Utils.logUncaughtExceptions {
        appendStreamToFile()
      }
    }
  }
  writingThread.start()

  /**
   * 等待追加线程终止，直到输入流关闭或出现错误
   */
  def awaitTermination(): Unit = {
    writingThread.join()
  }

  /**
   * 停止日志追加，终止后台写入线程
   */
  def stop(): Unit = {
    markedForStop = true
  }

  /**
   * 持续从输入流读取数据块，追加写入目标文件
   */
  protected def appendStreamToFile(): Unit = {
    try {
      logDebug("Started appending thread")
      Utils.tryWithSafeFinally {
        // 打开输出文件
        openFile()
        val buf = new Array[Byte](bufferSize)
        var n = 0
        // 循环读取直到停止标记被设置或输入流结束
        while (!markedForStop && n != -1) {
          try {
            n = inputStream.read(buf)
          } catch {
            // 当异步停止时，输入流被关闭会抛出IOException，此时忽略该异常直接退出
            case _: IOException if markedForStop =>  // do nothing and proceed to stop appending
          }
          if (n > 0) {
            appendToFile(buf, n)
          }
        }
      } {
        // 结束处理，关闭资源
        try {
          if (closeStreams) {
            inputStream.close()
          }
        } finally {
          closeFile()
        }
      }
    } catch {
      case e: Exception =>
        logError(log"Error writing stream to file ${MDC(PATH, file)}", e)
    }
  }

  /**
   * 将字节数组写入文件输出流
   * @param bytes 待写入的字节数组
   * @param len 要写入的字节长度
   */
  protected def appendToFile(bytes: Array[Byte], len: Int): Unit = {
    if (outputStream == null) {
      openFile()
    }
    outputStream.write(bytes, 0, len)
  }

  /**
   * 以追加模式打开目标文件输出流
   */
  protected def openFile(): Unit = {
    outputStream = new FileOutputStream(file, true)
    logDebug(s"Opened file $file")
  }

  /**
   * 刷新并关闭文件输出流
   */
  protected def closeFile(): Unit = {
    outputStream.flush()
    outputStream.close()
    logDebug(s"Closed file $file")
  }
}

/**
 * FileAppender伴生对象，根据Spark配置选择正确的FileAppender实现
 * 支持根据配置创建普通追加、基于时间滚动、基于大小滚动三种日志追加器
 */
private[spark] object FileAppender extends Logging {

  /**
   * 根据Spark配置创建对应的日志追加器实例
   * @param inputStream 输入流
   * @param file 目标日志文件
   * @param conf Spark配置
   * @param closeStreams 停止时是否关闭输入流
   * @return 匹配配置的FileAppender实例
   */
  def apply(
    inputStream: InputStream,
    file: File,
    conf: SparkConf,
    closeStreams: Boolean = false
  ) : FileAppender = {
    // 从配置中读取滚动策略、最大大小、时间间隔参数
    val rollingStrategy = conf.get(config.EXECUTOR_LOGS_ROLLING_STRATEGY)
    val rollingSizeBytes = conf.get(config.EXECUTOR_LOGS_ROLLING_MAX_SIZE)
    val rollingInterval = conf.get(config.EXECUTOR_LOGS_ROLLING_TIME_INTERVAL)

    /**
     * 创建基于时间滚动的日志追加器
     */
    def createTimeBasedAppender(): FileAppender = {
      val validatedParams: Option[(Long, String)] = rollingInterval match {
        case "daily" =>
          logInfo(log"Rolling executor logs enabled for ${MDC(FILE_NAME, file)} with daily rolling")
          Some((24 * 60 * 60 * 1000L, "--yyyy-MM-dd"))
        case "hourly" =>
          logInfo(log"Rolling executor logs enabled for ${MDC(FILE_NAME, file)}" +
            log" with hourly rolling")
          Some((60 * 60 * 1000L, "--yyyy-MM-dd--HH"))
        case "minutely" =>
          logInfo(log"Rolling executor logs enabled for ${MDC(FILE_NAME, file)}" +
            log" with rolling every minute")
          Some((60 * 1000L, "--yyyy-MM-dd--HH-mm"))
        case IntParam(seconds) =>
          logInfo(log"Rolling executor logs enabled for ${MDC(FILE_NAME, file)}" +
            log" with rolling ${MDC(TIME_UNITS, seconds)} seconds")
          Some((seconds * 1000L, "--yyyy-MM-dd--HH-mm-ss"))
        case _ =>
          logWarning(log"Illegal interval for rolling executor logs [" +
            log"${MDC(TIME_UNITS, rollingInterval)}], " +
            log"rolling logs not enabled")
          None
      }
      validatedParams.map {
        case (interval, pattern) =>
          new RollingFileAppender(
            inputStream, file, new TimeBasedRollingPolicy(interval, pattern), conf,
            closeStreams = closeStreams)
      }.getOrElse {
        new FileAppender(inputStream, file, closeStreams = closeStreams)
      }
    }

    /**
     * 创建基于大小滚动的日志追加器
     */
    def createSizeBasedAppender(): FileAppender = {
      rollingSizeBytes match {
        case IntParam(bytes) =>
          logInfo(log"Rolling executor logs enabled for ${MDC(FILE_NAME, file)}" +
            log" with rolling every ${MDC(NUM_BYTES, bytes)} bytes")
          new RollingFileAppender(
            inputStream, file, new SizeBasedRollingPolicy(bytes), conf, closeStreams = closeStreams)
        case _ =>
          logWarning(
            log"Illegal size [${MDC(LogKeys.NUM_BYTES, rollingSizeBytes)}] " +
            log"for rolling executor logs, rolling logs not enabled")
          new FileAppender(inputStream, file, closeStreams = closeStreams)
      }
    }

    // 根据配置的滚动策略选择对应的追加器实现
    rollingStrategy match {
      case "" =>
        new FileAppender(inputStream, file, closeStreams = closeStreams)
      case "time" =>
        createTimeBasedAppender()
      case "size" =>
        createSizeBasedAppender()
      case _ =>
        logWarning(
          log"Illegal strategy [${MDC(LogKeys.STRATEGY, rollingStrategy)}] for " +
          log"rolling executor logs, rolling logs not enabled")
        new FileAppender(inputStream, file, closeStreams = closeStreams)
    }
  }
}