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

import org.apache.spark.rdd.DeterministicLevel
import org.apache.spark.status.{RDDOperationClusterWrapper, RDDOperationGraphWrapper}
import org.apache.spark.status.protobuf.StoreTypes.{DeterministicLevel => GDeterministicLevel}
import org.apache.spark.status.protobuf.Utils.{getStringField, setStringField}
import org.apache.spark.ui.scope.{RDDOperationEdge, RDDOperationNode}

/**
 * RDD操作图包装对象的Protobuf序列化/反序列化器
 * 负责将Spark UI使用的RDD操作图结构转换为Protobuf二进制格式进行持久化存储，以及反向解析
 */
private[protobuf] class RDDOperationGraphWrapperSerializer
  extends ProtobufSerDe[RDDOperationGraphWrapper] {

  /**
   * 将RDD操作图包装对象序列化为Protobuf字节数组
   * @param op 待序列化的RDD操作图包装对象
   * @return 序列化后的二进制字节数组
   */
  override def serialize(op: RDDOperationGraphWrapper): Array[Byte] = {
    val builder = StoreTypes.RDDOperationGraphWrapper.newBuilder()
    // 设置Stage ID
    builder.setStageId(op.stageId.toLong)
    // 序列化所有内部边
    op.edges.foreach { e =>
      builder.addEdges(serializeRDDOperationEdge(e))
    }
    // 序列化所有出边（指向其他Stage）
    op.outgoingEdges.foreach { e =>
      builder.addOutgoingEdges(serializeRDDOperationEdge(e))
    }
    // 序列化所有入边（来自其他Stage）
    op.incomingEdges.foreach { e =>
      builder.addIncomingEdges(serializeRDDOperationEdge(e))
    }
    // 序列化根操作聚类
    builder.setRootCluster(serializeRDDOperationClusterWrapper(op.rootCluster))
    builder.build().toByteArray
  }

  /**
   * 将Protobuf字节数组反序列化为RDD操作图包装对象
   * @param bytes 待反序列化的二进制字节数组
   * @return 反序列化得到的RDD操作图包装对象
   */
  def deserialize(bytes: Array[Byte]): RDDOperationGraphWrapper = {
    val wrapper = StoreTypes.RDDOperationGraphWrapper.parseFrom(bytes)
    new RDDOperationGraphWrapper(
      stageId = wrapper.getStageId.toInt,
      edges = wrapper.getEdgesList.asScala.map(deserializeRDDOperationEdge),
      outgoingEdges = wrapper.getOutgoingEdgesList.asScala.map(deserializeRDDOperationEdge),
      incomingEdges = wrapper.getIncomingEdgesList.asScala.map(deserializeRDDOperationEdge),
      rootCluster = deserializeRDDOperationClusterWrapper(wrapper.getRootCluster)
    )
  }

  /**
   * 序列化RDD操作聚类包装对象为Protobuf结构
   * @param op 待序列化的RDD操作聚类包装对象
   * @return 序列化后的Protobuf结构对象
   */
  private def serializeRDDOperationClusterWrapper(op: RDDOperationClusterWrapper):
    StoreTypes.RDDOperationClusterWrapper = {
    val builder = StoreTypes.RDDOperationClusterWrapper.newBuilder()
    setStringField(op.id, builder.setId)
    setStringField(op.name, builder.setName)
    // 序列化所有子操作节点
    op.childNodes.foreach { node =>
      builder.addChildNodes(serializeRDDOperationNode(node))
    }
    // 递归序列化所有子聚类
    op.childClusters.foreach { cluster =>
      builder.addChildClusters(serializeRDDOperationClusterWrapper(cluster))
    }
    builder.build()
  }

  /**
   * 从Protobuf结构反序列化得到RDD操作聚类包装对象
   * @param op 待反序列化的Protobuf结构对象
   * @return 反序列化得到的RDD操作聚类包装对象
   */
  private def deserializeRDDOperationClusterWrapper(op: StoreTypes.RDDOperationClusterWrapper):
    RDDOperationClusterWrapper = {
    new RDDOperationClusterWrapper(
      id = getStringField(op.hasId, op.getId),
      name = getStringField(op.hasName, op.getName),
      childNodes = op.getChildNodesList.asScala.map(deserializeRDDOperationNode),
      childClusters =
        op.getChildClustersList.asScala.map(deserializeRDDOperationClusterWrapper)
    )
  }

  /**
   * 序列化RDD操作节点为Protobuf结构
   * @param node 待序列化的RDD操作节点
   * @return 序列化后的Protobuf结构对象
   */
  private def serializeRDDOperationNode(node: RDDOperationNode): StoreTypes.RDDOperationNode = {
    val outputDeterministicLevel = DeterministicLevelSerializer.serialize(
      node.outputDeterministicLevel)
    val builder = StoreTypes.RDDOperationNode.newBuilder()
    builder.setId(node.id)
    setStringField(node.name, builder.setName)
    setStringField(node.callsite, builder.setCallsite)
    builder.setCached(node.cached)
    builder.setBarrier(node.barrier)
    builder.setOutputDeterministicLevel(outputDeterministicLevel)
    builder.build()
  }

  /**
   * 从Protobuf结构反序列化得到RDD操作节点
   * @param node 待反序列化的Protobuf结构对象
   * @return 反序列化得到的RDD操作节点
   */
  private def deserializeRDDOperationNode(node: StoreTypes.RDDOperationNode): RDDOperationNode = {
    RDDOperationNode(
      id = node.getId,
      name = getStringField(node.hasName, node.getName),
      cached = node.getCached,
      barrier = node.getBarrier,
      callsite = getStringField(node.hasCallsite, node.getCallsite),
      outputDeterministicLevel = DeterministicLevelSerializer.deserialize(
        node.getOutputDeterministicLevel)
    )
  }

  /**
   * 序列化RDD操作边为Protobuf结构
   * @param edge 待序列化的RDD操作边
   * @return 序列化后的Protobuf结构对象
   */
  private def serializeRDDOperationEdge(edge: RDDOperationEdge): StoreTypes.RDDOperationEdge = {
    val builder = StoreTypes.RDDOperationEdge.newBuilder()
    builder.setFromId(edge.fromId)
    builder.setToId(edge.toId)
    builder.build()
  }

  /**
   * 从Protobuf结构反序列化得到RDD操作边
   * @param edge 待反序列化的Protobuf结构对象
   * @return 反序列化得到的RDD操作边
   */
  private def deserializeRDDOperationEdge(edge: StoreTypes.RDDOperationEdge): RDDOperationEdge = {
    RDDOperationEdge(
      fromId = edge.getFromId,
      toId = edge.getToId)
  }
}

/**
 * 确定性级别枚举的Protobuf序列化/反序列化工具
 * 负责在Spark内部枚举和Protobuf枚举定义之间进行转换
 */
private[protobuf] object DeterministicLevelSerializer {

  /**
   * 将Spark内部确定性级别枚举转换为Protobuf枚举
   * @param input Spark内部确定性级别枚举值
   * @return 对应Protobuf枚举值
   */
  def serialize(input: DeterministicLevel.Value): GDeterministicLevel = {
    input match {
      case DeterministicLevel.DETERMINATE =>
        GDeterministicLevel.DETERMINISTIC_LEVEL_DETERMINATE
      case DeterministicLevel.UNORDERED =>
        GDeterministicLevel.DETERMINISTIC_LEVEL_UNORDERED
      case DeterministicLevel.INDETERMINATE =>
        GDeterministicLevel.DETERMINISTIC_LEVEL_INDETERMINATE
    }
  }

  /**
   * 将Protobuf枚举转换为Spark内部确定性级别枚举
   * @param binary Protobuf确定性级别枚举值
   * @return 对应Spark内部枚举值，未知枚举返回null
   */
  def deserialize(binary: GDeterministicLevel): DeterministicLevel.Value = {
    binary match {
      case GDeterministicLevel.DETERMINISTIC_LEVEL_DETERMINATE =>
        DeterministicLevel.DETERMINATE
      case GDeterministicLevel.DETERMINISTIC_LEVEL_UNORDERED =>
        DeterministicLevel.UNORDERED
      case GDeterministicLevel.DETERMINISTIC_LEVEL_INDETERMINATE =>
        DeterministicLevel.INDETERMINATE
      case _ => null
    }
  }
}