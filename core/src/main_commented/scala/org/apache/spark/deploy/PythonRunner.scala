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

import java.io.File
import java.net.URI
import java.nio.file.Files
import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.Try

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.{SparkConf, SparkUserAppException}
import org.apache.spark.api.python.{Py4JServer, PythonUtils}
import org.apache.spark.internal.config._
import org.apache.spark.util.{RedirectThread, Utils}

/**
 * 文件级注释：Python应用启动入口类，负责在JVM中启动Python子进程，建立Py4J通信通道供Python客户端连接JVM核心。
 * 是PySpark应用在集群模式下的启动入口，负责环境准备、路径处理和进程管理。
 */

/**
 * 启动Python PySpark应用的主入口对象，负责：
 * 1. 解析启动参数和配置，确定Python可执行文件路径
 * 2. 格式化处理Python文件路径，构建正确的PYTHONPATH
 * 3. 启动Py4J网关服务器供Python客户端连接
 * 4. 启动Python子进程执行用户代码，重定向输出并管理生命周期
 * 5. 处理不同API模式（经典/Connect）的环境变量配置
 */
object PythonRunner {
  /**
   * PythonRunner主方法，入口函数，负责启动PySpark Python应用
   * @param args 输入参数，格式：[python主文件, 附加python文件逗号分隔列表, 其他用户参数]
   */
  def main(args: Array[String]): Unit = {
    val pythonFile = args(0)
    val pyFiles = args(1)
    val otherArgs = args.slice(2, args.length)
    val sparkConf = new SparkConf()
    // 按优先级获取Python可执行文件路径：驱动配置 -> 通用配置 -> 环境变量 -> 默认python3
    val pythonExec = sparkConf.get(PYSPARK_DRIVER_PYTHON)
      .orElse(sparkConf.get(PYSPARK_PYTHON))
      .orElse(sys.env.get("PYSPARK_DRIVER_PYTHON"))
      .orElse(sys.env.get("PYSPARK_PYTHON"))
      .getOrElse("python3")

    // 格式化Python文件路径，准备添加到PYTHONPATH
    val formattedPythonFile = formatPath(pythonFile)
    val formattedPyFiles = resolvePyFiles(formatPaths(pyFiles))
    // 解析API模式，判断是经典模式还是Spark Connect模式
    val apiMode = sparkConf.get(SPARK_API_MODE).toLowerCase(Locale.ROOT)
    val isAPIModeClassic = apiMode == "classic"
    val isAPIModeConnect = apiMode == "connect"

    var gatewayServer: Option[Py4JServer] = None
    // 非远程场景或经典API模式下，启动Py4J网关服务器
    if (sparkConf.getOption("spark.remote").isEmpty || isAPIModeClassic) {
      gatewayServer = Some(new Py4JServer(sparkConf))

      // 启动守护线程初始化Py4J网关
      val thread = new Thread(() => Utils.logUncaughtExceptions { gatewayServer.get.start() })
      thread.setName("py4j-gateway-init")
      thread.setDaemon(true)
      thread.start()

      // 等待网关启动完成，获取绑定的端口号后再继续
      // `gatewayServer.start()`初始化socket后才会结束线程，确保端口已准备好
      thread.join()
    }

    // 构建PYTHONPATH，包含：用户附加文件、Spark Python目录、SPARK_HOME路径、原有PYTHONPATH
    val pathElements = new ArrayBuffer[String]
    pathElements ++= formattedPyFiles
    pathElements += PythonUtils.sparkPythonPath
    pathElements += sys.env.getOrElse("PYTHONPATH", "")
    val pythonPath = PythonUtils.mergePythonPaths(pathElements.toSeq: _*)

    // 创建Python进程启动器，组装启动命令
    val builder = new ProcessBuilder((Seq(pythonExec, formattedPythonFile) ++ otherArgs).asJava)
    val env = builder.environment()
    // Spark Connect模式或远程场景下，将Spark配置批量编码为JSON写入环境变量传递给Python客户端
    if (sparkConf.getOption("spark.remote").nonEmpty || isAPIModeConnect) {
      // 每10个配置一组拆分，避免环境变量长度超限
      val grouped = sparkConf.getAll.toMap.grouped(10).toSeq
      env.put("PYSPARK_REMOTE_INIT_CONF_LEN", grouped.length.toString)
      grouped.zipWithIndex.foreach { case (group, idx) =>
        env.put(s"PYSPARK_REMOTE_INIT_CONF_$idx", compact(render(group)))
      }
    }
    // 根据API模式传递对应主地址环境变量
    if (isAPIModeClassic) {
      sparkConf.getOption("spark.master").foreach(url => env.put("MASTER", url))
    } else {
      sparkConf.getOption("spark.remote").foreach(url => env.put("SPARK_REMOTE", url))
    }
    // 设置PYTHONPATH环境变量
    env.put("PYTHONPATH", pythonPath)
    // 设置Python输出无缓冲，等价于-u参数，兼容ipython不支持-u的场景
    env.put("PYTHONUNBUFFERED", "YES") // value is needed to be set to a non-empty string
    // 传递Py4J网关端口和秘钥给Python进程
    gatewayServer.foreach(s => env.put("PYSPARK_GATEWAY_PORT", s.getListeningPort.toString))
    gatewayServer.foreach(s => env.put("PYSPARK_GATEWAY_SECRET", s.secret))
    // 通过环境变量传递PYSPARK_PYTHON配置给Python进程
    sparkConf.get(PYSPARK_PYTHON).foreach(env.put("PYSPARK_PYTHON", _))
    // 传递PYTHONHASHSEED环境变量保持一致性
    sys.env.get("PYTHONHASHSEED").foreach(env.put("PYTHONHASHSEED", _))
    // 如果未显式设置OMP_NUM_THREADS，则使用驱动核心数设置，避免pandas/numpy使用过多线程导致高内存占用
    if (sparkConf.getOption("spark.yarn.appMasterEnv.OMP_NUM_THREADS").isEmpty &&
        sparkConf.getOption("spark.kubernetes.driverEnv.OMP_NUM_THREADS").isEmpty) {
      // SPARK-28843: 将OpenMP线程池限制为分配给驱动的核心数
      // 避免pandas/numpy因为默认大线程池导致高内存消耗
      sparkConf.getOption("spark.driver.cores").foreach(env.put("OMP_NUM_THREADS", _))
    }
    // 合并标准错误和标准输出，保证输出顺序一致
    builder.redirectErrorStream(true) // Ugly but needed for stdout and stderr to synchronize
    try {
      // 启动Python进程
      val process = builder.start()

      // 启动线程将Python进程的标准输出重定向到当前进程标准输出
      new RedirectThread(process.getInputStream, System.out, "redirect output").start()

      // 等待Python进程退出，非0退出码则抛出异常
      val exitCode = process.waitFor()
      if (exitCode != 0) {
        throw new SparkUserAppException(exitCode)
      }
    } finally {
      // 退出前关闭Py4J网关服务器释放资源
      gatewayServer.foreach(_.shutdown())
    }
  }

  /**
   * 格式化Python文件路径，处理URI格式，转换为Python能识别的本地路径
   * 目前仅支持本地Python文件，不支持非本地路径
   * @param path 原始输入路径
   * @param testWindows 测试用参数，是否模拟Windows环境
   * @return 格式化后的可被Python识别的路径
   */
  def formatPath(path: String, testWindows: Boolean = false): String = {
    if (Utils.nonLocalPaths(path, testWindows).nonEmpty) {
      throw new IllegalArgumentException("Launching Python applications through " +
        s"spark-submit is currently only supported for local files: $path")
    }
    // 解析URI，提取本地路径部分
    val uri = Try(new URI(path)).getOrElse(new File(path).toURI)
    var formattedPath = uri.getScheme match {
      case null => path
      case "file" | "local" => uri.getPath
      case _ => null
    }

    // 路径格式异常检查
    if (formattedPath == null) {
      throw new IllegalArgumentException(s"Python file path is malformed: $path")
    }

    // Windows环境下移除盘符前多余的斜杠，Python不识别"/C:/xxx"格式
    if (Utils.isWindows && formattedPath.matches("/[a-zA-Z]:/.*")) {
      formattedPath = formattedPath.stripPrefix("/")
    }
    formattedPath
  }

  /**
   * 批量格式化逗号分隔的Python文件路径列表
   * @param paths 逗号分隔的原始路径字符串
   * @param testWindows 测试用参数，是否模拟Windows环境
   * @return 格式化后的路径数组
   */
  def formatPaths(paths: String, testWindows: Boolean = false): Array[String] = {
    Option(paths).getOrElse("")
      .split(",")
      .filter(_.nonEmpty)
      .map { p => formatPath(p, testWindows) }
  }

  /**
   * 处理单个.py文件路径，将.py文件复制到临时目录，把临时目录加入PYTHONPATH
   * 因为PYTHONPATH需要添加目录而非单个文件，所以单独处理.py文件（见SPARK-24384）
   * @param pyFiles 格式化后的路径数组
   * @return 处理后的路径数组，单个.py会替换为其所在临时目录
   */
  private def resolvePyFiles(pyFiles: Array[String]): Array[String] = {
    // 延迟创建临时目录，仅当有.py文件需要处理时才创建
    lazy val dest = Utils.createTempDir(namePrefix = "localPyFiles")
    pyFiles.flatMap { pyFile =>
      // 客户端提交场景下，需要在上下文初始化前设置好python路径
      // 将本地.py文件复制到临时目录，将临时目录加入PYTHONPATH，符合Python导入规则
      if (pyFile.endsWith(".py")) {
        val source = new File(pyFile)
        if (source.exists() && source.isFile && source.canRead) {
          Files.copy(source.toPath, new File(dest, source.getName).toPath)
          Some(dest.getAbsolutePath)
        } else {
          // 文件不存在或不可读则跳过
          None
        }
      } else {
        // 目录直接保留
        Some(pyFile)
      }
    }.distinct
  }
}