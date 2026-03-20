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

package org.apache

import org.apache.spark.util.VersionUtils

/**
 * Spark 核心功能包。SparkContext 是 Spark 的主入口点，
 * RDD 是表示分布式集合的数据类型，提供大部分并行操作。
 *
 * PairRDDFunctions 包含仅适用于键值对 RDD 的操作（如 groupByKey、join）；
 * DoubleRDDFunctions 包含仅适用于 Double RDD 的操作；
 * SequenceFileRDDFunctions 包含可保存为 SequenceFile 的操作。
 * 这些操作通过隐式转换自动可用。
 *
 * Java 开发者请参考 org.apache.spark.api.java 包。
 */
package object spark {
  // Spark 构建版本信息常量
  val SPARK_VERSION: String = SparkBuildInfo.spark_version
  val SPARK_VERSION_SHORT: String = VersionUtils.shortVersion(SparkBuildInfo.spark_version)
  val SPARK_BRANCH: String = SparkBuildInfo.spark_branch
  val SPARK_REVISION: String = SparkBuildInfo.spark_revision
  val SPARK_BUILD_USER: String = SparkBuildInfo.spark_build_user
  val SPARK_REPO_URL: String = SparkBuildInfo.spark_repo_url
  val SPARK_BUILD_DATE: String = SparkBuildInfo.spark_build_date
  val SPARK_DOC_ROOT: String = SparkBuildInfo.spark_doc_root
}
