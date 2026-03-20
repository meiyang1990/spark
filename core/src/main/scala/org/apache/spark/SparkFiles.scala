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

package org.apache.spark

import java.io.File

/**
 * 解析通过 SparkContext.addFile() 添加的文件的绝对路径。
 */
object SparkFiles {

  /**
   * 获取通过 SparkContext.addFile() 添加的文件的绝对路径。
   * 如果有活跃的 Spark Connect 会话，会加入 UUID 子目录以隔离不同会话的文件。
   */
  def get(filename: String): String = {
    val jobArtifactUUID = JobArtifactSet
      .getCurrentJobArtifactState.map(_.uuid).getOrElse("default")
    val withUuid = if (jobArtifactUUID == "default") filename else s"$jobArtifactUUID/$filename"
    new File(getRootDirectory(), withUuid).getAbsolutePath
  }

  /**
   * Get the root directory that contains files added through `SparkContext.addFile()`.
   */
  def getRootDirectory(): String =
    SparkEnv.get.driverTmpDir.getOrElse(".")

}
