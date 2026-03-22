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

package org.apache.spark.launcher

import java.io.File
import java.util.{HashMap => JHashMap, List => JList, Map => JMap}

import scala.jdk.CollectionConverters._

import org.apache.spark.deploy.Command

/**
 * 文件级：Standalone集群Worker启动命令构建器，负责生成Worker进程的Java启动命令
 * 
 * 该类供CommandUtils工具使用，由于需要访问SparkLauncher的包私有API，且不希望对外暴露公共API，
 * 因此放置在launcher包中。用于构建Standalone集群模式下启动Worker节点的Java命令行参数。
 */
/**
 * Worker启动命令构建器，负责构造Worker进程的Java启动命令
 * 
 * @param sparkHome Spark安装根目录路径
 * @param memoryMb 分配给Worker进程的堆内存大小，单位MB
 * @param command 部署层的Worker命令描述对象，包含类路径、Java选项和环境变量信息
 */
private[spark] class WorkerCommandBuilder(sparkHome: String, memoryMb: Int, command: Command)
    extends AbstractCommandBuilder {

  // 将Worker命令中定义的环境变量添加到子进程环境
  childEnv.putAll(command.environment.asJava)
  // 设置SPARK_HOME环境变量
  childEnv.put(CommandBuilderUtils.ENV_SPARK_HOME, sparkHome)

  /**
   * 构建Worker进程的Java启动命令列表
   * 
   * @param env 额外的环境变量映射
   * @return 完整的启动命令参数列表，适配Java进程启动要求
   */
  override def buildCommand(env: JMap[String, String]): JList[String] = {
    // 构建基础Java命令，拼接完整的类路径
    val cmd = buildJavaCommand(command.classPathEntries.mkString(File.pathSeparator))
    // 添加堆内存大小参数
    cmd.add(s"-Xmx${memoryMb}M")
    // 添加用户自定义的Java选项
    command.javaOpts.foreach(cmd.add)
    cmd
  }

  /**
   * 不携带额外环境变量的快捷构建方法
   * 
   * @return 完整的启动命令参数列表
   */
  def buildCommand(): JList[String] = buildCommand(new JHashMap[String, String]())

}