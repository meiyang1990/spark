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

import java.io.File
import java.nio.file.Paths
import java.util.{List => JList}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.sys.process.Process

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{PATH, PYTHON_PACKAGES, PYTHON_VERSION}
import org.apache.spark.util.ArrayImplicits.SparkArrayOps
import org.apache.spark.util.Utils

/**
 * PySpark Python 交互工具类，提供Python路径处理、集合类型转换、Python环境信息日志等公共能力
 * 供PySpark核心交互流程使用，仅在Spark内部开放使用
 */
private[spark] object PythonUtils extends Logging {
  /** Py4J依赖压缩包名称，用于Python与JVM通信 */
  val PY4J_ZIP_NAME = "py4j-0.10.9.9-src.zip"

  /**
   * 获取PySpark所需的PYTHONPATH环境变量，优先从SPARK_HOME获取，否则从当前JAR路径推导
   * @return 拼接完成的PYTHONPATH字符串
   */
  def sparkPythonPath: String = {
    val pythonPath = new ArrayBuffer[String]
    for (sparkHome <- sys.env.get("SPARK_HOME")) {
      pythonPath += Seq(sparkHome, "python", "lib", "pyspark.zip").mkString(File.separator)
      pythonPath +=
        Seq(sparkHome, "python", "lib", PY4J_ZIP_NAME).mkString(File.separator)
    }
    pythonPath ++= SparkContext.jarOfObject(this)
    pythonPath.mkString(File.pathSeparator)
  }

  /**
   * 合并多个PYTHONPATH路径，自动忽略空字符串，使用系统路径分隔符拼接
   * @param paths 待合并的多个路径
   * @return 合并完成的路径字符串
   */
  def mergePythonPaths(paths: String*): String = {
    paths.filter(_ != "").mkString(File.pathSeparator)
  }

  /**
   * 生成包含null值的测试RDD，用于Python端边界测试
   * @param sc JavaSparkContext上下文
   * @return 包含"a"、null、"b"三个元素的JavaRDD
   */
  def generateRDDWithNull(sc: JavaSparkContext): JavaRDD[String] = {
    sc.parallelize(List("a", null, "b"))
  }

  /**
   * 将Java列表转换为Scala序列，用于Java调用Scala可变参数API
   * @param vs Java列表对象
   * @tparam T 元素类型
   * @return Scala序列
   */
  def toSeq[T](vs: JList[T]): Seq[T] = {
    vs.asScala.toSeq
  }

  /**
   * 将Java列表转换为Scala List
   * @param vs Java列表对象
   * @tparam T 元素类型
   * @return Scala List
   */
  def toList[T](vs: JList[T]): List[T] = {
    vs.asScala.toList
  }

  /**
   * 将Java列表转换为Java数组，用于Java调用Scala数组参数API
   * @param vs Java列表对象
   * @tparam T 元素类型
   * @return 类型转换后的数组
   */
  def toArray[T](vs: JList[T]): Array[T] = {
    vs.toArray().asInstanceOf[Array[T]]
  }

  /**
   * 将Java Map转换为Scala不可变Map，用于Java调用Scala参数API
   * @param jm Java Map对象
   * @tparam K Key类型
   * @tparam V Value类型
   * @return Scala不可变Map
   */
  def toScalaMap[K, V](jm: java.util.Map[K, V]): Map[K, V] = {
    jm.asScala.toMap
  }

  /**
   * 从Spark配置获取IO加密是否开启
   * @param sc JavaSparkContext上下文
   * @return IO加密开启状态
   */
  def isEncryptionEnabled(sc: JavaSparkContext): Boolean = {
    sc.conf.get(org.apache.spark.internal.config.IO_ENCRYPTION_ENABLED)
  }

  /**
   * 从Spark配置获取UDF广播压缩阈值
   * @param sc JavaSparkContext上下文
   * @return 广播压缩阈值（字节）
   */
  def getBroadcastThreshold(sc: JavaSparkContext): Long = {
    sc.conf.get(org.apache.spark.internal.config.BROADCAST_FOR_UDF_COMPRESSION_THRESHOLD)
  }

  /**
   * 从Spark配置获取Python认证Socket超时时间
   * @param sc JavaSparkContext上下文
   * @return 超时时间（毫秒）
   */
  def getPythonAuthSocketTimeout(sc: JavaSparkContext): Long = {
    sc.conf.get(org.apache.spark.internal.config.Python.PYTHON_AUTH_SOCKET_TIMEOUT)
  }

  /**
   * 从Spark配置获取Spark内部缓存缓冲区大小
   * @param sc JavaSparkContext上下文
   * @return 缓冲区大小（字节）
   */
  def getSparkBufferSize(sc: JavaSparkContext): Int = {
    sc.conf.get(org.apache.spark.internal.config.BUFFER_SIZE)
  }

  /**
   * 记录Python环境版本和已安装包信息到日志，用于问题排查
   * 仅在配置开启PYTHON_LOG_INFO时执行
   * @param pythonExec Python可执行文件路径
   */
  def logPythonInfo(pythonExec: String): Unit = {
    if (SparkEnv.get.conf.get(org.apache.spark.internal.config.Python.PYTHON_LOG_INFO)) {
      import scala.sys.process._
      /**
       * 执行外部命令并捕获标准输出，异常或执行失败返回None
       */
      def runCommand(process: ProcessBuilder): Option[String] = {
        try {
          val stdout = new StringBuilder
          val processLogger = ProcessLogger(line => stdout.append(line).append(" "), _ => ())
          if (process.run(processLogger).exitValue() == 0) {
            Some(stdout.toString.trim)
          } else {
            None
          }
        } catch {
          case _: Throwable => None
        }
      }

      // 构造获取Python版本的命令
      val pythonVersionCMD = Seq(pythonExec, "-VV")
      // 拼接完整PYTHONPATH
      val pythonPath = PythonUtils.mergePythonPaths(
        PythonUtils.sparkPythonPath,
        sys.env.getOrElse("PYTHONPATH", ""))
      // 设置环境变量
      val environment = Map("PYTHONPATH" -> pythonPath)
      logInfo(log"Python path ${MDC(PATH, pythonPath)}")

      // 执行获取版本命令并记录日志
      val processPythonVer = Process(pythonVersionCMD, None, environment.toSeq: _*)
      val output = runCommand(processPythonVer)
      logInfo(log"Python version: ${MDC(PYTHON_VERSION, output.getOrElse("Unable to determine"))}")

      // Python代码：获取所有已安装包列表并打印
      val pythonCode =
        """
          |import pkg_resources
          |
          |installed_packages = pkg_resources.working_set
          |installed_packages_list = sorted(["%s:%s" % (i.key, i.version)
          |                                 for i in installed_packages])
          |
          |for package in installed_packages_list:
          |    print(package)
          |""".stripMargin

      // 执行获取已安装包命令
      val listPackagesCMD = Process(Seq(pythonExec, "-c", pythonCode))
      val listOfPackages = runCommand(listPackagesCMD)

      // 格式化输出：将空白字符替换为逗号分隔
      def formatOutput(output: String): String = {
        output.replaceAll("\\s+", ", ")
      }
      // 记录已安装包日志
      listOfPackages.foreach(x => logInfo(log"List of Python packages :-" +
        log" ${MDC(PYTHON_PACKAGES, formatOutput(x))}"))
    }
  }

  // 仅用于测试，额外测试路径，供开发测试时添加自定义Python包路径
  private[spark] var additionalTestingPath: Option[String] = None

  /** 默认Python可执行文件路径，优先读取PYSPARK_DRIVER_PYTHON，其次PYSPARK_PYTHON，默认python3 */
  private[spark] val defaultPythonExec: String = sys.env.getOrElse(
    "PYSPARK_DRIVER_PYTHON", sys.env.getOrElse("PYSPARK_PYTHON", "python3"))

  /**
   * 创建SimplePythonFunction对象，封装Python用户定义函数，用于Python UDF执行
   * 自动处理开发测试环境和生产环境的PYTHONPATH路径
   * @param command 序列化后的Python函数字节码
   * @return 封装完成的SimplePythonFunction对象
   */
  private[spark] def createPythonFunction(command: Array[Byte]): SimplePythonFunction = {
    val sourcePython = if (Utils.isTesting) {
      // 测试环境使用源码路径，不需要每次打包PySpark，加速开发测试
      val sparkHome: String = {
        require(
          sys.props.contains("spark.test.home") || sys.env.contains("SPARK_HOME"),
          "spark.test.home or SPARK_HOME is not set.")
        sys.props.getOrElse("spark.test.home", sys.env("SPARK_HOME"))
      }
      val sourcePath = Paths.get(sparkHome, "python").toAbsolutePath
      val py4jPath = Paths.get(
        sparkHome, "python", "lib", PythonUtils.PY4J_ZIP_NAME).toAbsolutePath
      val merged = mergePythonPaths(sourcePath.toString, py4jPath.toString)
      // 添加测试额外路径
      additionalTestingPath.map(mergePythonPaths(_, merged)).getOrElse(merged)
    } else {
      // 生产环境使用打包后的PYTHONPATH
      PythonUtils.sparkPythonPath
    }
    // 拼接系统PYTHONPATH
    val pythonPath = PythonUtils.mergePythonPaths(
      sourcePython, sys.env.getOrElse("PYTHONPATH", ""))

    // 获取Python主版本号和次版本号
    val pythonVer: String =
      Process(
        Seq(defaultPythonExec, "-c", "import sys; print('%d.%d' % sys.version_info[:2])"),
        None,
        "PYTHONPATH" -> pythonPath).!!.trim()

    // 构造并返回SimplePythonFunction对象
    SimplePythonFunction(
      command = command.toImmutableArraySeq,
      envVars = mutable.Map("PYTHONPATH" -> pythonPath).asJava,
      pythonIncludes = List.empty.asJava,
      pythonExec = defaultPythonExec,
      pythonVer = pythonVer,
      broadcastVars = List.empty.asJava,
      accumulator = null)
  }
}