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

package org.apache.spark.rdd

import java.io.BufferedWriter
import java.io.File
import java.io.FilenameFilter
import java.io.IOException
import java.io.OutputStreamWriter
import java.io.PrintWriter
import java.util.StringTokenizer
import java.util.concurrent.atomic.AtomicReference

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.LogKeys.{COMMAND, ERROR, PATH}
import org.apache.spark.util.Utils


/**
 * 将父RDD每个分区的数据通过管道传递给外部命令处理，并将外部命令输出作为字符串集合返回的RDD。
 * 用于在Spark任务中调用外部脚本或命令行程序处理分区数据。
 *
 * @param prev 父RDD，输入数据来源
 * @param command 需要执行的外部命令序列
 * @param envVars 需要传递给外部进程的环境变量
 * @param printPipeContext 函数，用于将管道上下文信息输出给外部命令，接收一个输出函数作为参数
 * @param printRDDElement 函数，用于将RDD元素格式化输出给外部命令，接收元素和输出函数作为参数
 * @param separateWorkingDir 是否为每个任务使用独立工作目录，避免文件访问冲突
 * @param bufferSize 输出缓冲区大小
 * @param encoding 字符编码
 */
private[spark] class PipedRDD[T: ClassTag](
    prev: RDD[T],
    command: Seq[String],
    envVars: Map[String, String],
    printPipeContext: (String => Unit) => Unit,
    printRDDElement: (T, String => Unit) => Unit,
    separateWorkingDir: Boolean,
    bufferSize: Int,
    encoding: String)
  extends RDD[String](prev) {

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  /**
   * 文件名过滤器，过滤掉名称与指定名称不相等的文件。
   * @param filterName 需要排除的文件/目录名称
   */
  class NotEqualsFileNameFilter(filterName: String) extends FilenameFilter {
    def accept(dir: File, name: String): Boolean = {
      !name.equals(filterName)
    }
  }

  /**
   * 计算指定分区的数据，通过管道调用外部命令处理并返回结果迭代器
   * @param split 待计算的分区
   * @param context 任务上下文
   * @return 外部命令输出的字符串迭代器
   */
  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    // 创建外部进程构建器
    val pb = new ProcessBuilder(command.asJava)
    // 添加环境变量到进程
    val currentEnvVars = pb.environment()
    envVars.foreach { case (variable, value) => currentEnvVars.put(variable, value) }

    // 兼容Hadoop，将Hadoop分区相关环境变量传入，方便用户代码获取输入文件名
    split match {
      case hadoopSplit: HadoopPartition =>
        currentEnvVars.putAll(hadoopSplit.getPipeEnvVars().asJava)
      case _ => // 不做处理
    }

    // 开启独立工作目录时，为每个任务创建唯一工作目录，解决多任务文件访问冲突
    val taskDirectory = "tasks" + File.separator + java.util.UUID.randomUUID.toString
    var workInTaskDirectory = false
    logDebug("taskDirectory = " + taskDirectory)
    if (separateWorkingDir) {
      val currentDir = new File(".")
      logDebug("currentDir = " + currentDir.getAbsolutePath())
      val taskDirFile = new File(taskDirectory)
      // 创建任务工作目录
      Utils.createDirectory(taskDirFile)

      try {
        // 创建过滤器，排除自身创建的tasks目录
        val tasksDirFilter = new NotEqualsFileNameFilter("tasks")

        // 对当前目录下的所有文件/目录创建软链接到任务工作目录，保留分布式缓存等外部文件的访问能力
        for (file <- currentDir.list(tasksDirFilter)) {
          val fileWithDir = new File(currentDir, file)
          Utils.symlink(new File(fileWithDir.getAbsolutePath()),
            new File(taskDirectory + File.separator + fileWithDir.getName()))
        }
        // 设置进程工作目录为任务独立目录
        pb.directory(taskDirFile)
        workInTaskDirectory = true
      } catch {
        case e: Exception =>
          logError(log"Unable to setup task working directory: ${MDC(ERROR, e.getMessage)}" +
          log" (${MDC(PATH, taskDirectory)})", e)
      }
    }

    // 启动外部进程
    val proc = pb.start()
    // 保存子线程抛出的异常
    val childThreadException = new AtomicReference[Throwable](null)

    // 启动线程读取外部进程的标准错误，并输出到当前进程的标准错误
    val stderrReaderThread = new Thread(s"${PipedRDD.STDERR_READER_THREAD_PREFIX} $command") {
      override def run(): Unit = {
        val err = proc.getErrorStream
        try {
          for (line <- Source.fromInputStream(err)(encoding).getLines()) {
            // scalastyle:off println
            System.err.println(line)
            // scalastyle:on println
          }
        } catch {
          case t: Throwable => childThreadException.set(t)
        } finally {
          err.close()
        }
      }
    }
    stderrReaderThread.start()

    // 启动线程将父RDD分区数据写入外部进程的标准输入
    val stdinWriterThread = new Thread(s"${PipedRDD.STDIN_WRITER_THREAD_PREFIX} $command") {
      override def run(): Unit = {
        TaskContext.setTaskContext(context)
        val out = new PrintWriter(new BufferedWriter(
          new OutputStreamWriter(proc.getOutputStream, encoding), bufferSize))
        try {
          // scalastyle:off println
          // 先输出管道上下文信息
          if (printPipeContext != null) {
            printPipeContext(out.println)
          }
          // 遍历父RDD分区元素，写入到外部进程标准输入
          for (elem <- firstParent[T].iterator(split, context)) {
            if (printRDDElement != null) {
              printRDDElement(elem, out.println)
            } else {
              out.println(elem)
            }
          }
          // scalastyle:on println
        } catch {
          case t: Throwable => childThreadException.set(t)
        } finally {
          out.close()
        }
      }
    }
    stdinWriterThread.start()

    // 任务完成后中断读写线程，避免线程泄漏，占用额外资源
    context.addTaskCompletionListener[Unit] { _ =>
      if (proc.isAlive) {
        proc.destroy()
      }

      if (stdinWriterThread.isAlive) {
        stdinWriterThread.interrupt()
      }
      if (stderrReaderThread.isAlive) {
        stderrReaderThread.interrupt()
      }
    }

    // 返回从外部进程标准输出读取行的迭代器
    val lines = Source.fromInputStream(proc.getInputStream)(encoding).getLines()
    new Iterator[String] {
      def next(): String = {
        if (!hasNext) {
          throw SparkCoreErrors.noSuchElementError()
        }
        lines.next()
      }

      def hasNext: Boolean = {
        val result = if (lines.hasNext) {
          true
        } else {
          // 外部进程输出读取完成，等待进程退出并获取退出码
          val exitStatus = proc.waitFor()
          // 清理临时工作目录
          cleanup()
          // 退出码非0表示进程异常，抛出异常
          if (exitStatus != 0) {
            throw new IllegalStateException(s"Subprocess exited with status $exitStatus. " +
              s"Command ran: " + command.mkString(" "))
          }
          false
        }
        // 传播子线程中捕获的异常
        propagateChildException()
        result
      }

      /**
       * 清理任务执行产生的临时工作目录
       */
      private def cleanup(): Unit = {
        // 如果使用了独立工作目录，递归删除目录
        if (workInTaskDirectory) {
          scala.util.control.Exception.ignoring(classOf[IOException]) {
            Utils.deleteRecursively(new File(taskDirectory))
          }
          logDebug(s"Removed task working directory $taskDirectory")
        }
      }

      /**
       * 如果读写子线程抛出异常，传播该异常到主线程
       */
      private def propagateChildException(): Unit = {
        val t = childThreadException.get()
        if (t != null) {
          val commandRan = command.mkString(" ")
          logError(log"Caught exception while running pipe() operator. Command ran: " +
            log"${MDC(COMMAND, commandRan)}. Exception: ${MDC(ERROR, t.getMessage)}")
          proc.destroy()
          cleanup()
          throw t
        }
      }
    }
  }
}

/**
 * PipedRDD的伴生对象，提供命令切分工具方法和线程名称常量
 */
private object PipedRDD {
  /**
   * 将命令字符串按空格切分为命令参数序列
   * @param command 待切分的命令字符串
   * @return 切分后的命令参数序列
   */
  def tokenize(command: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    val tok = new StringTokenizer(command)
    while (tok.hasMoreElements) {
      buf += tok.nextToken()
    }
    buf.toSeq
  }

  val STDIN_WRITER_THREAD_PREFIX = "stdin writer for"
  val STDERR_READER_THREAD_PREFIX = "stderr reader for"
}