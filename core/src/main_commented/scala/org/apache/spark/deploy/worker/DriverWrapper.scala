// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the license.  You may obtain a copy of the License at
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

import java.io.File

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys.RPC_ADDRESS
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.util._

/**
 * 文件: DriverWrapper.scala
 * 所属模块: Spark Core 部署模块
 * 核心职责: 在Standalone集群模式下，包装Driver进程，保证Driver与Worker进程同生共死，用于Worker启动Driver时使用
 */
/**
 * 启动Driver程序的工具类，确保Driver与Worker进程生命周期绑定，仅在Standalone集群模式下使用。
 * 当Worker在本地启动Driver时，通过该包装类启动用户Driver，保证Worker退出时Driver也随之退出。
 */
object DriverWrapper extends Logging {
  /**
   * DriverWrapper入口方法，解析参数并启动用户Driver进程
   * @param args 启动参数：依次为Worker地址、用户Jar包路径、Driver主类全限定名、额外参数
   */
  def main(args: Array[String]): Unit = {
    args.toList match {
      /*
       * IMPORTANT: Spark 1.3 provides a stable application submission gateway that is both
       * backward and forward compatible across future Spark versions. Because this gateway
       * uses this class to launch the driver, the ordering and semantics of the arguments
       * here must also remain consistent across versions.
       */
      case workerUrl :: userJar :: mainClass :: extraArgs =>
        val conf = new SparkConf()
        // 获取本地主机名
        val host: String = Utils.localHostName()
        // 从系统属性获取Driver端口，默认0表示随机分配端口
        val port: Int = sys.props.getOrElse(config.DRIVER_PORT.key, "0").toInt
        // 创建Driver端的RPC环境
        val rpcEnv = RpcEnv.create("Driver", host, port, conf, new SecurityManager(conf))
        // 记录Driver的RPC地址日志
        logInfo(log"Driver address: ${MDC(RPC_ADDRESS, rpcEnv.address)}")
        // 注册WorkerWatcher端点，用于监控Worker状态，保证Driver随Worker退出而退出
        rpcEnv.setupEndpoint("workerWatcher", new WorkerWatcher(rpcEnv, workerUrl))

        // 获取当前线程上下文类加载器
        val currentLoader = Thread.currentThread.getContextClassLoader
        // 将用户Jar包转换为URL
        val userJarUrl = new File(userJar).toURI().toURL()
        // 根据配置选择类加载器：优先用户Jar还是双亲委派优先还是系统类加载器优先
        val loader =
          if (sys.props.getOrElse(config.DRIVER_USER_CLASS_PATH_FIRST.key, "false").toBoolean) {
            new ChildFirstURLClassLoader(Array(userJarUrl), currentLoader)
          } else {
            new MutableURLClassLoader(Array(userJarUrl), currentLoader)
          }
        // 设置当前线程上下文类加载器为创建好的用户类加载器
        Thread.currentThread.setContextClassLoader(loader)
        // 处理依赖，添加Maven依赖解析结果到类路径
        setupDependencies(loader, userJar)

        // 反射加载用户Driver主类
        val clazz = Utils.classForName(mainClass)
        // 获取主类的main方法
        val mainMethod = clazz.getMethod("main", classOf[Array[String]])
        // 调用用户Driver的main方法，启动Driver
        mainMethod.invoke(null, extraArgs.toArray[String])

        // 用户Driver退出后关闭RPC环境
        rpcEnv.shutdown()

      case _ =>
        // scalastyle:off println
        // 参数格式不正确，输出使用说明并退出
        System.err.println("Usage: DriverWrapper <workerUrl> <userJar> <driverMainClass> [options]")
        // scalastyle:on println
        System.exit(-1)
    }
  }

  /**
   * 解析并添加用户Driver依赖的Jar包到类路径，支持Maven坐标依赖自动下载
   * @param loader 用于添加依赖的类加载器
   * @param userJar 用户提交的主Jar包路径
   */
  private def setupDependencies(loader: MutableURLClassLoader, userJar: String): Unit = {
    val sparkConf = new SparkConf()
    // 创建Hadoop配置对象
    val hadoopConf = SparkHadoopUtil.newConfiguration(sparkConf)

    // 从系统属性获取Ivy相关配置
    val ivyProperties = DependencyUtils.getIvyProperties()

    // 解析Maven坐标，下载依赖的Jar包
    val resolvedMavenCoordinates = DependencyUtils.resolveMavenDependencies(true,
      ivyProperties.packagesExclusions, ivyProperties.packages, ivyProperties.repositories,
      ivyProperties.ivyRepoPath, Option(ivyProperties.ivySettingsPath))
    // 合并已配置的Jar列表和Maven解析得到的Jar列表
    val jars = {
      val jarsProp = sys.props.get(config.JARS.key).orNull
      if (resolvedMavenCoordinates.nonEmpty) {
        DependencyUtils.mergeFileLists(jarsProp,
          DependencyUtils.mergeFileLists(resolvedMavenCoordinates: _*))
      } else {
        jarsProp
      }
    }
    // 解析并下载所有依赖Jar到本地
    val localJars = DependencyUtils.resolveAndDownloadJars(jars, userJar, sparkConf, hadoopConf)
    // 将所有本地Jar添加到类加载器的类路径中
    DependencyUtils.addJarsToClassPath(localJars, loader)
  }
}