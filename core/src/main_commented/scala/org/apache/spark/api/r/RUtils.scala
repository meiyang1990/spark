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

import java.io.File
import java.util.Arrays

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.config._

/**
 * SparkR相关工具类，提供R包路径查找、R环境检查等工具能力，供SparkR模块内部使用
 */
private[spark] object RUtils {
  // 通过--packages或--jars参数引入的R源码编译二进制包的本地存储路径
  var rPackages: Option[String] = None

  /**
   * 获取本地Spark分发中SparkR包的安装路径
   * @return SparkR包路径，未找到SPARK_HOME时返回None
   */
  def localSparkRPackagePath: Option[String] = {
    val sparkHome = sys.env.get("SPARK_HOME").orElse(sys.props.get("spark.test.home"))
    sparkHome.map(
      Seq(_, "R", "lib").mkString(File.separator)
    )
  }

  /**
   * 检查SparkR是否已经正确安装，用于依赖SparkR的测试前环境校验
   * @return true表示SparkR已安装，false表示未安装
   */
  def isSparkRInstalled: Boolean = {
    localSparkRPackagePath.exists { pkgDir =>
      new File(Seq(pkgDir, "SparkR").mkString(File.separator)).exists
    }
  }

  /**
   * 根据部署模式获取R包的搜索路径列表，第一个路径是SparkR本身，第二个路径是用户通过Spark Packages添加的第三方R包（如果存在）
   * @param isDriver 是否是Driver节点
   * @return R包搜索路径序列
   */
  def sparkRPackagePath(isDriver: Boolean): Seq[String] = {
    val (master, deployMode) =
      if (isDriver) {
        // Driver端从系统属性获取Master和部署模式
        (sys.props("spark.master"), sys.props(SUBMIT_DEPLOY_MODE.key))
      } else {
        // Executor端从SparkEnv获取配置
        val sparkConf = SparkEnv.get.conf
        (sparkConf.get("spark.master"), sparkConf.get(SUBMIT_DEPLOY_MODE))
      }

    // 判断是否是YARN集群部署模式
    val isYarnCluster = master != null && master.contains("yarn") && deployMode == "cluster"
    // 判断是否是YARN客户端部署模式
    val isYarnClient = master != null && master.contains("yarn") && deployMode == "client"

    // YARN模式下，SparkR包会作为归档分发到当前目录的sparkr链接，第三方R包分发到rpkg链接
    // YARN客户端模式下Driver运行在集群外，所以该规则不适用Driver
    if (isYarnCluster || (isYarnClient && !isDriver)) {
      val sparkRPkgPath = new File("sparkr").getAbsolutePath
      val rPkgPath = new File("rpkg")
      if (rPkgPath.exists()) {
        Seq(sparkRPkgPath, rPkgPath.getAbsolutePath)
      } else {
        Seq(sparkRPkgPath)
      }
    } else {
      // 非YARN模式下，使用本地Spark分发中的R包路径
      val sparkRPkgPath = localSparkRPackagePath.getOrElse {
          throw new SparkException("SPARK_HOME not set. Can't locate SparkR package.")
      }
      // 如果存在用户添加的第三方R包，追加到路径列表
      if (!rPackages.isEmpty) {
        Seq(sparkRPkgPath, rPackages.get)
      } else {
        Seq(sparkRPkgPath)
      }
    }
  }

  /**
   * 检查系统是否安装了R环境，用于依赖R命令的测试前环境校验
   * @return true表示R已安装，false表示未安装或执行出错
   */
  def isRInstalled: Boolean = {
    try {
      // 执行R --version命令检测R是否可用
      val builder = new ProcessBuilder(Arrays.asList("R", "--version"))
      builder.start().waitFor() == 0
    } catch {
      case e: Exception => false
    }
  }

  /**
   * 检查IO加密是否已启用
   * @param sc JavaSparkContext上下文实例
   * @return true表示IO加密已启用，false表示未启用
   */
  def isEncryptionEnabled(sc: JavaSparkContext): Boolean = {
    sc.conf.get(org.apache.spark.internal.config.IO_ENCRYPTION_ENABLED)
  }

  /**
   * 获取当前Spark作业的标签数组
   * @param sc JavaSparkContext上下文实例
   * @return 作业标签数组
   */
  def getJobTags(sc: JavaSparkContext): Array[String] = {
    sc.getJobTags().toArray().map(_.asInstanceOf[String])
  }
}