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

package org.apache.spark.deploy.worker

import java.io.{File, FileOutputStream, InputStream, IOException}

import scala.collection.Map
import scala.jdk.CollectionConverters._

import org.apache.spark.{SecurityManager, SSLOptions}
import org.apache.spark.deploy.Command
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.launcher.WorkerCommandBuilder
import org.apache.spark.util.Utils

/**
 * 文件说明：Spark Worker节点执行器启动命令构建工具类，提供基于Spark类路径构建执行进程的工具方法
 * 用于Worker节点为Executor进程准备启动命令和环境配置
 */
private[deploy]
object CommandUtils extends Logging {

  /**
   * 根据给定参数构建Executor进程启动的ProcessBuilder
   * @param command 原始执行命令描述
   * @param securityMgr 安全管理器，用于认证和SSL配置
   * @param memory 分配给Executor的内存大小
   * @param sparkHome Spark安装目录路径
   * @param substituteArguments 参数占位符替换函数
   * @param classPaths 额外添加的类路径
   * @param env 系统环境变量，暴露该参数用于单元测试
   * @return 配置完成的ProcessBuilder对象
   */
  def buildProcessBuilder(
      command: Command,
      securityMgr: SecurityManager,
      memory: Int,
      sparkHome: String,
      substituteArguments: String => String,
      classPaths: Seq[String] = Seq.empty,
      env: Map[String, String] = sys.env): ProcessBuilder = {
    val localCommand = buildLocalCommand(
      command, securityMgr, substituteArguments, classPaths, env)
    val commandSeq = buildCommandSeq(localCommand, memory, sparkHome)
    val builder = new ProcessBuilder(commandSeq: _*)
    val environment = builder.environment()
    for ((key, value) <- localCommand.environment) {
      environment.put(key, value)
    }
    builder
  }

  /**
   * 构建进程启动命令列表，使用WorkerCommandBuilder组装命令
   */
  private def buildCommandSeq(command: Command, memory: Int, sparkHome: String): Seq[String] = {
    // SPARK-698: 不使用run.cmd脚本包装，避免Windows上process.destroy()无法杀死整个进程树
    val cmd = new WorkerCommandBuilder(sparkHome, memory, command).buildCommand()
    (cmd.asScala ++ Seq(command.mainClass) ++ command.arguments).toSeq
  }

  /**
   * 根据原始命令，结合本地运行环境，生成适配Worker节点的本地执行命令，处理环境变量、占位符替换和额外类路径
   */
  private def buildLocalCommand(
      command: Command,
      securityMgr: SecurityManager,
      substituteArguments: String => String,
      classPath: Seq[String] = Seq.empty,
      env: Map[String, String]): Command = {
    val libraryPathName = Utils.libraryPathEnvName
    val libraryPathEntries = command.libraryPathEntries
    val cmdLibraryPath = command.environment.get(libraryPathName)

    var newEnvironment = if (libraryPathEntries.nonEmpty && libraryPathName.nonEmpty) {
      val libraryPaths = libraryPathEntries ++ cmdLibraryPath ++ env.get(libraryPathName)
      command.environment ++ Map(libraryPathName -> libraryPaths.mkString(File.pathSeparator))
    } else {
      command.environment
    }

    // 如果启用认证，将认证秘钥写入环境变量
    if (securityMgr.isAuthenticationEnabled()) {
      newEnvironment = newEnvironment ++
        Map(SecurityManager.ENV_AUTH_SECRET -> securityMgr.getSecretKey())
    }
    // 如果启用SSL，将SSL RPC密码相关配置写入环境变量
    newEnvironment ++= securityMgr.getEnvironmentForSslRpcPasswords

    Command(
      command.mainClass,
      command.arguments.map(substituteArguments),
      newEnvironment,
      command.classPathEntries ++ classPath,
      Seq.empty, // 本地库路径已经合并到环境变量，不再单独保存
      // 从Java选项中过滤掉明文密钥配置，避免泄露
      command.javaOpts.filterNot(opts =>
        opts.startsWith("-D" + SecurityManager.SPARK_AUTH_SECRET_CONF) ||
        SSLOptions.SPARK_RPC_SSL_PASSWORD_FIELDS.exists(
          field => opts.startsWith("-D" + field)
        )
      ))
  }

  /**
   * 启动一个后台线程，将输入流的内容重定向输出到指定文件，用于Executor日志重定向
   * @param in 要重定向的输入流，通常是Executor进程的标准输出/错误流
   * @param file 目标日志文件
   */
  def redirectStream(in: InputStream, file: File): Unit = {
    val out = new FileOutputStream(file, true)
    // TODO: 可以添加关闭钩子，在Worker退出时记录日志终止原因，避免Executor日志无提示中断
    new Thread("redirect output to " + file) {
      override def run(): Unit = {
        try {
          Utils.copyStream(in, out, true)
        } catch {
          case e: IOException =>
            logInfo(log"Redirection to ${MDC(LogKeys.FILE_NAME, file)} closed: " +
              log"${MDC(LogKeys.ERROR, e.getMessage)}")
        }
      }
    }.start()
  }
}