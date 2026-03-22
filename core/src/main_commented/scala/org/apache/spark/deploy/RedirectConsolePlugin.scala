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

package org.apache.spark.deploy

import java.io.{ByteArrayOutputStream, PrintStream}
import java.util.{Collections, Map => JMap}

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.{Logging, SparkLoggerFactory}
import org.apache.spark.internal.config._

/**
 * Spark内置插件，用于将标准输出stdout和标准错误stderr重定向到SLF4J日志系统
 */
class RedirectConsolePlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new DriverRedirectConsolePlugin()

  override def executorPlugin(): ExecutorPlugin = new ExecRedirectConsolePlugin()
}

/**
 * 控制台重定向插件的伴随对象，提供通用的重定向方法
 */
object RedirectConsolePlugin {

  /**
   * 将系统标准输出stdout重定向到日志系统
   */
  def redirectStdoutToLog(): Unit = {
    val stdoutLogger = SparkLoggerFactory.getLogger("stdout")
    System.setOut(new LoggingPrintStream(stdoutLogger.info))
  }

  /**
   * 将系统标准错误stderr重定向到日志系统
   */
  def redirectStderrToLog(): Unit = {
    val stderrLogger = SparkLoggerFactory.getLogger("stderr")
    System.setErr(new LoggingPrintStream(stderrLogger.error))
  }
}

/**
 * Driver端控制台重定向插件实现，负责Driver端的控制台输出重定向
 */
class DriverRedirectConsolePlugin extends DriverPlugin with Logging {

  /**
   * 初始化Driver端插件，根据配置决定是否重定向控制台输出
   * @param sc Spark上下文
   * @param ctx 插件上下文
   * @return 空配置映射
   */
  override def init(sc: SparkContext, ctx: PluginContext): JMap[String, String] = {
    val outputs = sc.conf.get(DRIVER_REDIRECT_CONSOLE_OUTPUTS)
    if (outputs.contains("stdout")) {
      logInfo("Redirect driver's stdout to logging system.")
      RedirectConsolePlugin.redirectStdoutToLog()
    }
    if (outputs.contains("stderr")) {
      logInfo("Redirect driver's stderr to logging system.")
      RedirectConsolePlugin.redirectStderrToLog()
    }
    Collections.emptyMap
  }
}

/**
 * Executor端控制台重定向插件实现，负责Executor端的控制台输出重定向
 */
class ExecRedirectConsolePlugin extends ExecutorPlugin with Logging {

  /**
   * 初始化Executor端插件，根据配置决定是否重定向控制台输出
   * @param ctx 插件上下文
   * @param extraConf 额外配置参数
   */
  override def init(ctx: PluginContext, extraConf: JMap[String, String]): Unit = {
    val outputs = ctx.conf.get(EXEC_REDIRECT_CONSOLE_OUTPUTS)
    if (outputs.contains("stdout")) {
      logInfo("Redirect executor's stdout to logging system.")
      RedirectConsolePlugin.redirectStdoutToLog()
    }
    if (outputs.contains("stderr")) {
      logInfo("Redirect executor's stderr to logging system.")
      RedirectConsolePlugin.redirectStderrToLog()
    }
  }
}

/**
 * 自定义打印流，将写入的内容按行缓存，满行或达到阈值后输出到日志系统
 * @param redirect 日志输出函数，将生成的行字符串传入日志系统
 */
private[spark] class LoggingPrintStream(redirect: String => Unit)
  extends PrintStream(new LineBuffer(4 * 1024 * 1024)) {

  override def write(b: Int): Unit = {
    super.write(b)
    tryLogCurrentLine()
  }

  override def write(buf: Array[Byte], off: Int, len: Int): Unit = {
    super.write(buf, off, len)
    tryLogCurrentLine()
  }

  private def tryLogCurrentLine(): Unit = this.synchronized {
    out.asInstanceOf[LineBuffer].tryGenerateContext.foreach { logContext =>
      redirect(logContext)
    }
  }
}

/**
 * 行缓存缓冲区，在换行符出现前缓存字节内容，当遇到行结束或缓冲区达到阈值时生成完整行
 */
private[spark] object LineBuffer {
  // 系统换行符对应的字节数组
  private val LF_BYTES = System.lineSeparator.getBytes
  // 换行符字节长度
  private val LF_LENGTH = LF_BYTES.length
}

/**
 * 行缓存缓冲区实现，继承ByteArrayOutputStream实现动态扩容缓存
 * @param lineMaxBytes 单条日志行最大字节数，超过该阈值直接输出并清空缓冲区
 */
private[spark] class LineBuffer(lineMaxBytes: Long) extends ByteArrayOutputStream {

  import LineBuffer._

  /**
   * 尝试从缓冲区生成完整日志行
   * @return 完整日志行，无完整行则返回None
   */
  def tryGenerateContext: Option[String] =
    if (isLineEnded) {
      // 遇到行结束，去掉末尾换行符后生成日志行并清空缓冲区
      try Some(new String(buf, 0, count - LF_LENGTH)) finally reset()
    } else if (count >= lineMaxBytes) {
      // 达到最大字节限制，直接输出全部缓存内容并清空缓冲区
      try Some(new String(buf, 0, count)) finally reset()
    } else {
      // 无完整行可输出
      None
    }

  /**
   * 检查当前缓冲区末尾是否以系统换行符结束
   * @return 是则返回true，否则false
   */
  private def isLineEnded: Boolean = {
    // 缓冲区长度小于换行符长度，不可能是行结束
    if (count < LF_LENGTH) return false
    // 类Unix系统换行符只有一个字节'\n'，直接比较最后一个字节
    if (LF_LENGTH == 1) return LF_BYTES(0) == buf(count - 1)

    // 多字节换行符（如Windows的\r\n）逐字节比较末尾
    var i = 0
    do {
      if (LF_BYTES(i) != buf(count - LF_LENGTH + i)) {
        return false
      }
      i = i + 1
    } while (i < LF_LENGTH)
    true
  }
}