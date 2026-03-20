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

package org.apache.spark.resource

import scala.util.control.NonFatal

import org.json4s.{DefaultFormats, Extraction, Formats, JValue}
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkException
import org.apache.spark.annotation.Evolving
import org.apache.spark.util.ArrayImplicits._

/**
 * ResourceInformation - 资源信息类
 * 
 * 保存某种类型资源的信息。资源可以是 GPU、FPGA 等。
 * 
 * 地址数组是资源特定的，由用户解释其含义。
 * 例如对于 GPU，addresses 可能是 GPU 的索引号（如 ["0", "1"]）。
 * 
 * 提供 JSON 序列化/反序列化支持，用于在 Spark 组件间传递资源信息。
 *
 * @param name 资源名称
 * @param addresses 描述资源地址的字符串数组
 *
 * @since 3.0.0
 */
@Evolving
class ResourceInformation(
    val name: String,
    val addresses: Array[String]) extends Serializable {

  override def toString: String = s"[name: ${name}, addresses: ${addresses.mkString(",")}]"

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ResourceInformation =>
        that.getClass == this.getClass &&
        that.name == name &&
        that.addresses.toImmutableArraySeq == addresses.toImmutableArraySeq
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Seq(name, addresses.toImmutableArraySeq).hashCode()

  // TODO(SPARK-39658): reconsider whether we want to expose a third-party library's
  // symbols as part of a public API:
  final def toJson(): JValue = ResourceInformationJson(name, addresses.toImmutableArraySeq).toJValue
}

private[spark] object ResourceInformation {

  private lazy val exampleJson: String = compact(render(
    ResourceInformationJson("gpu", Seq("0", "1")).toJValue))

  /**
   * Parses a JSON string into a [[ResourceInformation]] instance.
   */
  def parseJson(json: String): ResourceInformation = {
    implicit val formats: Formats = DefaultFormats
    try {
      parse(json).extract[ResourceInformationJson].toResourceInformation
    } catch {
      case NonFatal(e) =>
        throw new SparkException(s"Error parsing JSON into ResourceInformation:\n$json\n" +
          s"Here is a correct example: $exampleJson.", e)
    }
  }

  def parseJson(json: JValue): ResourceInformation = {
    implicit val formats: Formats = DefaultFormats
    try {
      json.extract[ResourceInformationJson].toResourceInformation
    } catch {
      case NonFatal(e) =>
        throw new SparkException(s"Error parsing JSON into ResourceInformation:\n$json\n", e)
    }
  }
}

/** 
 * ResourceInformationJson - 简化 ResourceInformation 的 JSON 序列化
 * 
 * 用于在发现脚本和 Spark 之间传递资源信息。
 */
private case class ResourceInformationJson(name: String, addresses: Seq[String]) {

  def toJValue: JValue = {
    Extraction.decompose(this)(DefaultFormats)
  }

  def toResourceInformation: ResourceInformation = {
    new ResourceInformation(name, addresses.toArray)
  }
}
