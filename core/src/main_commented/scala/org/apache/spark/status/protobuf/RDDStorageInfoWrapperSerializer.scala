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

import org.apache.spark.status.RDDStorageInfoWrapper
import org.apache.spark.status.api.v1.{RDDDataDistribution, RDDPartitionInfo, RDDStorageInfo}
import org.apache.spark.status.protobuf.Utils.{getOptional, getStringField, setStringField}
import org.apache.spark.util.Utils.weakIntern

/**
 * RDD存储信息包装类的Protobuf序列化与反序列化处理器
 * 用于将Spark UI状态存储中的RDD存储信息转换为Protobuf格式进行持久化，以及反向解析
 */
private[protobuf] class RDDStorageInfoWrapperSerializer
  extends ProtobufSerDe[RDDStorageInfoWrapper] {

  /**
   * 将RDD存储信息包装对象序列化为Protobuf字节数组
   * @param input 待序列化的RDD存储信息包装对象
   * @return 序列化后的字节数组
   */
  override def serialize(input: RDDStorageInfoWrapper): Array[Byte] = {
    val builder = StoreTypes.RDDStorageInfoWrapper.newBuilder()
    builder.setInfo(serializeRDDStorageInfo(input.info))
    builder.build().toByteArray
  }

  /**
   * 将Protobuf字节数组反序列化为RDD存储信息包装对象
   * @param bytes 待反序列化的Protobuf字节数组
   * @return 反序列化得到的RDD存储信息包装对象
   */
  def deserialize(bytes: Array[Byte]): RDDStorageInfoWrapper = {
    val wrapper = StoreTypes.RDDStorageInfoWrapper.parseFrom(bytes)
    new RDDStorageInfoWrapper(
      info = deserializeRDDStorageInfo(wrapper.getInfo)
    )
  }

  /**
   * 将RDD存储信息对象序列化为Protobuf对象
   * @param info 待序列化的RDD存储信息对象
   * @return 序列化后的Protobuf RDD存储信息对象
   */
  private def serializeRDDStorageInfo(info: RDDStorageInfo): StoreTypes.RDDStorageInfo = {
    val builder = StoreTypes.RDDStorageInfo.newBuilder()
    // 设置RDD ID
    builder.setId(info.id)
    // 设置RDD名称
    setStringField(info.name, builder.setName)
    // 设置RDD总分区数
    builder.setNumPartitions(info.numPartitions)
    // 设置已缓存分区数
    builder.setNumCachedPartitions(info.numCachedPartitions)
    // 设置存储级别描述
    setStringField(info.storageLevel, builder.setStorageLevel)
    // 设置内存使用量
    builder.setMemoryUsed(info.memoryUsed)
    // 设置磁盘使用量
    builder.setDiskUsed(info.diskUsed)

    // 序列化各执行节点的数据分布信息（存在时才处理）
    if (info.dataDistribution.isDefined) {
      info.dataDistribution.get.foreach { dd =>
        val dataDistributionBuilder = StoreTypes.RDDDataDistribution.newBuilder()
        setStringField(dd.address, dataDistributionBuilder.setAddress)
        dataDistributionBuilder.setMemoryUsed(dd.memoryUsed)
        dataDistributionBuilder.setMemoryRemaining(dd.memoryRemaining)
        dataDistributionBuilder.setDiskUsed(dd.diskUsed)
        dd.onHeapMemoryUsed.foreach(dataDistributionBuilder.setOnHeapMemoryUsed)
        dd.offHeapMemoryUsed.foreach(dataDistributionBuilder.setOffHeapMemoryUsed)
        dd.onHeapMemoryRemaining.foreach(dataDistributionBuilder.setOnHeapMemoryRemaining)
        dd.offHeapMemoryRemaining.foreach(dataDistributionBuilder.setOffHeapMemoryRemaining)
        builder.addDataDistribution(dataDistributionBuilder.build())
      }
    }

    // 序列化各分区存储信息（存在时才处理）
    if (info.partitions.isDefined) {
      info.partitions.get.foreach { p =>
        val partitionsBuilder = StoreTypes.RDDPartitionInfo.newBuilder()
        setStringField(p.blockName, partitionsBuilder.setBlockName)
        setStringField(p.storageLevel, partitionsBuilder.setStorageLevel)
        partitionsBuilder.setMemoryUsed(p.memoryUsed)
        partitionsBuilder.setDiskUsed(p.diskUsed)
        p.executors.foreach(partitionsBuilder.addExecutors)
        builder.addPartitions(partitionsBuilder.build())
      }
    }

    builder.build()
  }

  /**
   * 将Protobuf RDD存储信息对象反序列化为API层RDDStorageInfo对象
   * @param info Protobuf格式的RDD存储信息对象
   * @return 反序列化得到的API层RDD存储信息对象
   */
  private def deserializeRDDStorageInfo(info: StoreTypes.RDDStorageInfo): RDDStorageInfo = {
    new RDDStorageInfo(
      id = info.getId,
      name = getStringField(info.hasName, info.getName),
      numPartitions = info.getNumPartitions,
      numCachedPartitions = info.getNumCachedPartitions,
      storageLevel = getStringField(info.hasStorageLevel, info.getStorageLevel),
      memoryUsed = info.getMemoryUsed,
      diskUsed = info.getDiskUsed,
      dataDistribution =
        if (info.getDataDistributionList.isEmpty) {
          None
        } else {
          Some(info.getDataDistributionList.asScala.map(deserializeRDDDataDistribution))
        },
      partitions =
        Some(info.getPartitionsList.asScala.map(deserializeRDDPartitionInfo))
    )
  }

  /**
   * 将Protobuf数据分布对象反序列化为API层RDDDataDistribution对象
   * @param info Protobuf格式的RDD数据分布对象
   * @return 反序列化得到的API层RDD数据分布对象
   */
  private def deserializeRDDDataDistribution(info: StoreTypes.RDDDataDistribution):
    RDDDataDistribution = {

    new RDDDataDistribution(
      address = getStringField(info.hasAddress, info.getAddress),
      memoryUsed = info.getMemoryUsed,
      memoryRemaining = info.getMemoryRemaining,
      diskUsed = info.getDiskUsed,
      onHeapMemoryUsed = getOptional(info.hasOnHeapMemoryUsed, info.getOnHeapMemoryUsed),
      offHeapMemoryUsed = getOptional(info.hasOffHeapMemoryUsed, info.getOffHeapMemoryUsed),
      onHeapMemoryRemaining =
        getOptional(info.hasOnHeapMemoryRemaining, info.getOnHeapMemoryRemaining),
      offHeapMemoryRemaining =
        getOptional(info.hasOffHeapMemoryRemaining, info.getOffHeapMemoryRemaining)
    )
  }

  /**
   * 将Protobuf分区信息对象反序列化为API层RDDPartitionInfo对象
   * @param info Protobuf格式的RDD分区信息对象
   * @return 反序列化得到的API层RDD分区信息对象
   */
  private def deserializeRDDPartitionInfo(info: StoreTypes.RDDPartitionInfo): RDDPartitionInfo = {
    new RDDPartitionInfo(
      blockName = getStringField(info.hasBlockName, info.getBlockName),
      storageLevel = getStringField(info.hasStorageLevel, () => weakIntern(info.getStorageLevel)),
      memoryUsed = info.getMemoryUsed,
      diskUsed = info.getDiskUsed,
      executors = info.getExecutorsList.asScala
    )
  }
}