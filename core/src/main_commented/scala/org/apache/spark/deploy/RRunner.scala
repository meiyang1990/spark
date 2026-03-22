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

import java.io._
import java.util.concurrent.{Semaphore, TimeUnit}

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkException, SparkUserAppException}
import org.apache.spark.api.r.{RBackend, RUtils}
import org.apache.spark.internal.config.R._
import org.apache.spark.internal.config.SUBMIT_DEPLOY_MODE
import org.apache.spark.util.RedirectThread

/**
 * 文件级注释：SparkR应用启动入口类，供spark-submit启动R语言应用使用
 * 核心功能：启动R子进程执行用户应用，同时在JVM中启动R后端服务供R进程连接，传递系统配置等信息
 */
/**
 * Main class used to launch SparkR applications using spark-submit. It executes R as a
 * subprocess and then has it connect back to the JVM to access system properties etc.
 */
/**
 * RRunner 对象：SparkR应用启动器，负责在JVM中准备R后端并启动R用户应用子进程
 * 核心职责：对接spark-submit，启动R后端服务，配置环境变量，重定向输出，管理应用生命周期
 */
object RRunner {
  /**
   * RRunner主入口方法，由spark-submit调用启动SparkR应用
   * @param args 启动参数，第一个参数为R脚本文件路径，后续为传递给R脚本的参数
   * @return 无返回值，应用异常则抛出异常退出
   */
  def main(args: Array[String]): Unit = {
    val rFile = PythonRunner.formatPath(args(0))

    val otherArgs = args.slice(1, args.length)

    // Time to wait for SparkR backend to initialize in seconds
    val backendTimeout = sys.env.getOrElse("SPARKR_BACKEND_TIMEOUT", "120").toInt
    // 按优先级获取R可执行命令路径，兼容旧配置项
    val rCommand = {
      // "spark.sparkr.r.command" is deprecated and replaced by "spark.r.command",
      // but kept here for backward compatibility.
      var cmd = sys.props.getOrElse(SPARKR_COMMAND.key, SPARKR_COMMAND.defaultValue.get)
      cmd = sys.props.getOrElse(R_COMMAND.key, cmd)
      // client模式下优先使用driver端指定的R命令
      if (sys.props.getOrElse(SUBMIT_DEPLOY_MODE.key, "client") == "client") {
        cmd = sys.props.getOrElse("spark.r.driver.command", cmd)
      }
      cmd
    }

    //  Connection timeout set by R process on its connection to RBackend in seconds.
    val backendConnectionTimeout = sys.props.getOrElse(
      R_BACKEND_CONNECTION_TIMEOUT.key, R_BACKEND_CONNECTION_TIMEOUT.defaultValue.get.toString)

    // Check if the file path exists.
    // If not, change directory to current working directory for YARN cluster mode
    val rF = new File(rFile)
    // YARN集群模式下脚本不存在当前路径，仅保留文件名使用工作目录
    val rFileNormalized = if (!rF.exists()) {
      new Path(rFile).getName
    } else {
      rFile
    }

    // Launch a SparkR backend server for the R process to connect to; this will let it see our
    // Java system properties etc.
    // 创建R后端服务实例，供R子进程连接
    val sparkRBackend = new RBackend()
    @volatile var sparkRBackendPort = 0
    @volatile var sparkRBackendSecret: String = null
    // 信号量用于同步等待R后端初始化完成
    val initialized = new Semaphore(0)
    // 单独线程启动R后端服务
    val sparkRBackendThread = new Thread("SparkR backend") {
      override def run(): Unit = {
        // 初始化R后端，获取监听端口和认证信息
        val (port, authHelper) = sparkRBackend.init()
        sparkRBackendPort = port
        sparkRBackendSecret = authHelper.secret
        // 初始化完成释放信号量，主线程继续执行
        initialized.release()
        // 开始监听R连接，进入服务循环
        sparkRBackend.run()
      }
    }

    sparkRBackendThread.start()
    // Wait for RBackend initialization to finish
    // 等待R后端初始化完成，超时则抛出异常
    if (initialized.tryAcquire(backendTimeout, TimeUnit.SECONDS)) {
      // Launch R
      // 启动R子进程执行用户脚本
      val returnCode = try {
        // 构建R进程启动命令
        val builder = new ProcessBuilder((Seq(rCommand, rFileNormalized) ++ otherArgs).asJava)
        val env = builder.environment()
        // 传递R后端端口给R子进程
        env.put("EXISTING_SPARKR_BACKEND_PORT", sparkRBackendPort.toString)
        // 传递R连接超时时间给R子进程
        env.put("SPARKR_BACKEND_CONNECTION_TIMEOUT", backendConnectionTimeout)
        // 获取SparkR包所在路径
        val rPackageDir = RUtils.sparkRPackagePath(isDriver = true)
        // Put the R package directories into an env variable of comma-separated paths
        env.put("SPARKR_PACKAGE_DIR", rPackageDir.mkString(","))
        // 设置R环境配置文件路径
        env.put("R_PROFILE_USER",
          Seq(rPackageDir(0), "SparkR", "profile", "general.R").mkString(File.separator))
        // 传递认证密钥给R子进程
        env.put("SPARKR_BACKEND_AUTH_SECRET", sparkRBackendSecret)
        // 合并标准错误和标准输出，保证输出顺序一致
        builder.redirectErrorStream(true) // Ugly but needed for stdout and stderr to synchronize
        // 启动R子进程
        val process = builder.start()

        // 启动线程重定向R子进程的输出到当前JVM的标准输出
        new RedirectThread(process.getInputStream, System.out, "redirect R output").start()

        // 等待R子进程执行结束，获取退出码
        process.waitFor()
      } finally {
        // R进程退出后关闭R后端服务
        sparkRBackend.close()
      }
      // R进程退出码非0则抛出用户应用异常
      if (returnCode != 0) {
        throw new SparkUserAppException(returnCode)
      }
    } else {
      // R后端初始化超时，输出错误信息并抛出异常
      val errorMessage = s"SparkR backend did not initialize in $backendTimeout seconds"
      // scalastyle:off println
      System.err.println(errorMessage)
      // scalastyle:on println
      throw new SparkException(errorMessage)
    }
  }
}