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

import scala.collection.Map

/**
 * Spark部署流程中用于描述可执行进程启动命令的数据结构
 * 封装了启动Java进程所需的所有信息，用于Worker节点启动Driver或Executor进程
 * @param mainClass 要启动的主类全限定名
 * @param arguments 传给主类的命令行参数序列
 * @param environment 启动进程需要设置的环境变量
 * @param classPathEntries 额外添加到Classpath的路径列表
 * @param libraryPathEntries 额外添加到库路径的路径列表
 * @param javaOpts JVM启动参数选项
 */
private[spark] case class Command(
    mainClass: String,
    arguments: Seq[String],
    environment: Map[String, String],
    classPathEntries: Seq[String],
    libraryPathEntries: Seq[String],
    javaOpts: Seq[String]) {
}