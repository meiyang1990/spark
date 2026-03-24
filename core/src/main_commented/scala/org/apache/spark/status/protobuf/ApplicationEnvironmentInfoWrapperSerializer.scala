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

package org.apache.spark.status.protobuf

import scala.jdk.CollectionConverters._

import org.apache.spark.resource.{ExecutorResourceRequest, TaskResourceRequest}
import org.apache.spark.status.ApplicationEnvironmentInfoWrapper
import org.apache.spark.status.api.v1.{ApplicationEnvironmentInfo, ResourceProfileInfo, RuntimeInfo}
import org.apache.spark.status.protobuf.Utils.{getStringField, setStringField}

/**
 * 应用环境信息包装器的Protobuf序列化器，实现Spark状态存储中应用环境信息的Protobuf编解码
 */
private[protobuf] class ApplicationEnvironmentInfoWrapperSerializer
  extends ProtobufSerDe[ApplicationEnvironmentInfoWrapper] {

  /**
   * 将应用环境信息包装器序列化为Protobuf字节数组
   * @param input 待序列化的应用环境信息包装器
   * @return 序列化后的字节数组
   */
  override def serialize(input: ApplicationEnvironmentInfoWrapper): Array[Byte] = {
    val builder = StoreTypes.ApplicationEnvironmentInfoWrapper.newBuilder()
    builder.setInfo(serializeApplicationEnvironmentInfo(input.info))
    builder.build().toByteArray
  }

  /**
   * 将Protobuf字节数组反序列化为应用环境信息包装器
   * @param bytes 待反序列化的Protobuf字节数组
   * @return 反序列化得到的应用环境信息包装器
   */
  def deserialize(bytes: Array[Byte]): ApplicationEnvironmentInfoWrapper = {
    val wrapper = StoreTypes.ApplicationEnvironmentInfoWrapper.parseFrom(bytes)
    new ApplicationEnvironmentInfoWrapper(
      info = deserializeApplicationEnvironmentInfo(wrapper.getInfo)
    )
  }

  /**
   * 序列化应用环境信息对象为Protobuf结构
   * @param info 应用环境信息对象
   * @return 序列化后的Protobuf应用环境信息对象
   */
  private def serializeApplicationEnvironmentInfo(info: ApplicationEnvironmentInfo):
    StoreTypes.ApplicationEnvironmentInfo = {

    // 构建运行时信息Protobuf对象
    val runtimeBuilder = StoreTypes.RuntimeInfo.newBuilder()
    val runtime = info.runtime
    setStringField(runtime.javaHome, runtimeBuilder.setJavaHome)
    setStringField(runtime.javaVersion, runtimeBuilder.setJavaVersion)
    setStringField(runtime.scalaVersion, runtimeBuilder.setScalaVersion)

    // 构建应用环境信息Protobuf对象
    val builder = StoreTypes.ApplicationEnvironmentInfo.newBuilder()
    builder.setRuntime(runtimeBuilder.build())
    // 序列化各类配置属性
    info.sparkProperties.foreach { pair =>
      builder.addSparkProperties(serializePairStrings(pair))
    }
    info.hadoopProperties.foreach { pair =>
      builder.addHadoopProperties(serializePairStrings(pair))
    }
    info.systemProperties.foreach { pair =>
      builder.addSystemProperties(serializePairStrings(pair))
    }
    info.metricsProperties.foreach { pair =>
      builder.addMetricsProperties(serializePairStrings(pair))
    }
    info.classpathEntries.foreach { pair =>
      builder.addClasspathEntries(serializePairStrings(pair))
    }
    // 序列化资源配置信息
    info.resourceProfiles.foreach { profile =>
      builder.addResourceProfiles(serializeResourceProfileInfo(profile))
    }
    builder.build()
  }

  /**
   * 从Protobuf结构反序列化出应用环境信息对象
   * @param info Protobuf格式的应用环境信息
   * @return 反序列化得到的应用环境信息对象
   */
  private def deserializeApplicationEnvironmentInfo(info: StoreTypes.ApplicationEnvironmentInfo):
    ApplicationEnvironmentInfo = {
    val rt = info.getRuntime
    // 反序列化运行时信息
    val runtime = new RuntimeInfo (
      javaVersion = getStringField(rt.hasJavaVersion, () => rt.getJavaVersion),
      javaHome = getStringField(rt.hasJavaHome, () => rt.getJavaHome),
      scalaVersion = getStringField(rt.hasScalaVersion, () => rt.getScalaVersion)
    )
    // Protobuf键值对转Scala元组转换函数
    val pairSSToTuple = (pair: StoreTypes.PairStrings) => {
      (getStringField(pair.hasValue1, pair.getValue1),
        getStringField(pair.hasValue2, pair.getValue2))
    }
    new ApplicationEnvironmentInfo(
      runtime = runtime,
      sparkProperties = info.getSparkPropertiesList.asScala.map(pairSSToTuple),
      hadoopProperties = info.getHadoopPropertiesList.asScala.map(pairSSToTuple),
      systemProperties = info.getSystemPropertiesList.asScala.map(pairSSToTuple),
      metricsProperties = info.getMetricsPropertiesList.asScala.map(pairSSToTuple),
      classpathEntries = info.getClasspathEntriesList.asScala.map(pairSSToTuple),
      resourceProfiles =
        info.getResourceProfilesList.asScala.map(deserializeResourceProfileInfo)
    )
  }

  /**
   * 序列化字符串键值对为Protobuf结构
   * @param pair 输入的字符串键值对元组
   * @return 序列化后的Protobuf键值对对象
   */
  private def serializePairStrings(pair: (String, String)): StoreTypes.PairStrings = {
    val builder = StoreTypes.PairStrings.newBuilder()
    setStringField(pair._1, builder.setValue1)
    setStringField(pair._2, builder.setValue2)
    builder.build()
  }

  /**
   * 序列化资源配置信息为Protobuf结构
   * @param info 资源配置信息对象
   * @return 序列化后的Protobuf资源配置信息对象
   */
  private[status] def serializeResourceProfileInfo(info: ResourceProfileInfo):
    StoreTypes.ResourceProfileInfo = {
    val builder = StoreTypes.ResourceProfileInfo.newBuilder()
    builder.setId(info.id)
    // 序列化执行器资源请求
    info.executorResources.foreach{case (k, resource) =>
      val requestBuilder = StoreTypes.ExecutorResourceRequest.newBuilder()
      setStringField(resource.resourceName, requestBuilder.setResourceName)
      requestBuilder.setAmount(resource.amount)
      setStringField(resource.discoveryScript, requestBuilder.setDiscoveryScript)
      setStringField(resource.vendor, requestBuilder.setVendor)
      builder.putExecutorResources(k, requestBuilder.build())
    }
    // 序列化任务资源请求
    info.taskResources.foreach { case (k, resource) =>
      val requestBuilder = StoreTypes.TaskResourceRequest.newBuilder()
      setStringField(resource.resourceName, requestBuilder.setResourceName)
      requestBuilder.setAmount(resource.amount)
      builder.putTaskResources(k, requestBuilder.build())
    }
    builder.build()
  }

  /**
   * 从Protobuf结构反序列化出资源配置信息对象
   * @param info Protobuf格式的资源配置信息
   * @return 反序列化得到的资源配置信息对象
   */
  private[status] def deserializeResourceProfileInfo(info: StoreTypes.ResourceProfileInfo):
    ResourceProfileInfo = {

    new ResourceProfileInfo(
      id = info.getId,
      executorResources = info.getExecutorResourcesMap.asScala.toMap
        .transform((_, v) => deserializeExecutorResourceRequest(v)),
      taskResources = info.getTaskResourcesMap.asScala.toMap
        .transform((_, v) => deserializeTaskResourceRequest(v))
    )
  }

  /**
   * 从Protobuf结构反序列化出执行器资源请求对象
   * @param info Protobuf格式的执行器资源请求
   * @return 反序列化得到的执行器资源请求对象
   */
  private def deserializeExecutorResourceRequest(info: StoreTypes.ExecutorResourceRequest):
    ExecutorResourceRequest = {
    new ExecutorResourceRequest(
      resourceName = getStringField(info.hasResourceName, () => info.getResourceName),
      amount = info.getAmount,
      discoveryScript = getStringField(info.hasDiscoveryScript, () => info.getDiscoveryScript),
      vendor = getStringField(info.hasVendor, () => info.getVendor)
    )
  }

  /**
   * 从Protobuf结构反序列化出任务资源请求对象
   * @param info Protobuf格式的任务资源请求
   * @return 反序列化得到的任务资源请求对象
   */
  private def deserializeTaskResourceRequest(info: StoreTypes.TaskResourceRequest):
    TaskResourceRequest = {
    new TaskResourceRequest(
      resourceName = getStringField(info.hasResourceName, () => info.getResourceName),
      amount = info.getAmount
    )
  }
}