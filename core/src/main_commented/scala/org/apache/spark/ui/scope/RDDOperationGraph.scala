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

package org.apache.spark.ui.scope

import java.util.Objects

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, StringBuilder}
import scala.xml.Utility

import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.rdd.DeterministicLevel
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.storage.StorageLevel

/**
 * 文件说明：RDD操作图的核心数据结构与生成工具，为Spark Web UI展示RDD依赖DAG图提供数据支持
 * 核心功能：根据Stage信息构建RDD操作层次图，并生成Graphviz DOT格式的图描述文件
 */

/**
 * RDD操作图的整体表示，用于存储RDD操作的层次结构和依赖关系
 * @param edges 图内部节点之间的依赖边
 * @param outgoingEdges 从本图指向其他图的出边（跨Stage依赖）
 * @param incomingEdges 从其他图指向本图的入边（跨Stage依赖）
 * @param rootCluster 根集群，代表整个图的顶级容器（通常对应一个Stage）
 */
private[spark] case class RDDOperationGraph(
    edges: collection.Seq[RDDOperationEdge],
    outgoingEdges: collection.Seq[RDDOperationEdge],
    incomingEdges: collection.Seq[RDDOperationEdge],
    rootCluster: RDDOperationCluster)

/**
 * RDD操作图中的节点，每个节点对应一个RDD
 * @param id RDD ID
 * @param name RDD名称
 * @param cached RDD是否被缓存
 * @param barrier RDD是否是Barrier阶段（用于Barrier执行模式）
 * @param callsite RDD创建的调用位置信息
 * @param outputDeterministicLevel RDD输出确定性级别
 */
private[spark] case class RDDOperationNode(
    id: Int,
    name: String,
    cached: Boolean,
    barrier: Boolean,
    callsite: String,
    outputDeterministicLevel: DeterministicLevel.Value)

/**
 * RDD操作图中的有向边，代表两个RDD之间的依赖关系
 * @param fromId 父RDD的ID
 * @param toId 子RDD的ID
 */
private[spark] case class RDDOperationEdge(fromId: Int, toId: Int)

/**
 * RDD操作图中的集群，用于分组聚合节点或嵌套子集群，代表RDD操作范围、Stage、Job等层级结构
 * 支持嵌套，可以包含子节点和子集群，用于构建层次化的操作范围
 * @param id 集群唯一ID
 * @param barrier 集群是否包含Barrier操作
 * @param _name 集群名称
 */
private[spark] class RDDOperationCluster(
    val id: String,
    val barrier: Boolean,
    private var _name: String) {
  private val _childNodes = new ListBuffer[RDDOperationNode]
  private val _childClusters = new ListBuffer[RDDOperationCluster]

  def name: String = _name
  def setName(n: String): Unit = { _name = n }

  def childNodes: Seq[RDDOperationNode] = _childNodes.iterator.toSeq
  def childClusters: Seq[RDDOperationCluster] = _childClusters.iterator.toSeq
  def attachChildNode(childNode: RDDOperationNode): Unit = { _childNodes += childNode }
  def attachChildCluster(childCluster: RDDOperationCluster): Unit = {
    _childClusters += childCluster
  }

  /** 返回当前集群及其所有子集群中所有被缓存的节点 */
  def getCachedNodes: Seq[RDDOperationNode] = {
    (_childNodes.filter(_.cached) ++ _childClusters.flatMap(_.getCachedNodes)).toSeq
  }

  /** 返回当前集群及其所有子集群中所有Barrier集群 */
  def getBarrierClusters: Seq[RDDOperationCluster] = {
    (_childClusters.filter(_.barrier) ++ _childClusters.flatMap(_.getBarrierClusters)).toSeq
  }

  /** 返回当前集群及其所有子集群中所有输出不确定的节点 */
  def getIndeterminateNodes: Seq[RDDOperationNode] = {
    (_childNodes.filter(_.outputDeterministicLevel == DeterministicLevel.INDETERMINATE) ++
      _childClusters.flatMap(_.getIndeterminateNodes)).toSeq
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[RDDOperationCluster]

  override def equals(other: Any): Boolean = other match {
    case that: RDDOperationCluster =>
      (that canEqual this) &&
          _childClusters == that._childClusters &&
          id == that.id &&
          _name == that._name
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(_childClusters, id, _name)
    state.map(Objects.hashCode).foldLeft(0)((a, b) => 31 * a + b)
  }
}

/**
 * RDD操作图生成工具类，为Spark Web UI构建RDD DAG图并生成DOT格式描述
 */
private[spark] object RDDOperationGraph extends Logging {

  val STAGE_CLUSTER_PREFIX = "stage_"

  /**
   * 根据给定Stage信息构建RDD操作图
   * @param stage 目标Stage信息
   * @param retainedNodes 保留的根节点最大数量，避免节点过多导致UI渲染卡顿
   * @return 构建完成的RDD操作图
   */
  def makeOperationGraph(stage: StageInfo, retainedNodes: Int): RDDOperationGraph = {
    val edges = new ListBuffer[RDDOperationEdge]
    val nodes = new mutable.HashMap[Int, RDDOperationNode]
    val clusters = new mutable.HashMap[String, RDDOperationCluster] // indexed by cluster ID

    // 根集群对应整个Stage
    // 使用特殊前缀区分Stage集群和其他操作集群
    val stageClusterId = STAGE_CLUSTER_PREFIX + stage.stageId
    val stageClusterName = s"Stage ${stage.stageId}" +
      { if (stage.attemptNumber() == 0) "" else s" (attempt ${stage.attemptNumber()})" }
    val rootCluster = new RDDOperationCluster(stageClusterId, false, stageClusterName)

    var rootNodeCount = 0
    val addRDDIds = new mutable.HashSet[Int]()
    val dropRDDIds = new mutable.HashSet[Int]()

    // 遍历当前Stage包含的所有RDD，收集节点、边和操作范围
    stage.rddInfos.sortBy(_.id).foreach { rdd =>
      val parentIds = rdd.parentIds
      // 判断当前RDD是否需要保留，避免根节点超过限制
      val isAllowed =
        if (parentIds.isEmpty) {
          rootNodeCount += 1
          rootNodeCount <= retainedNodes
        } else {
          parentIds.exists(id => addRDDIds.contains(id) || !dropRDDIds.contains(id))
        }

      if (isAllowed) {
        addRDDIds += rdd.id
        // 添加所有未被丢弃的父RDD到当前RDD的依赖边
        edges ++= parentIds.filter(id => !dropRDDIds.contains(id)).map(RDDOperationEdge(_, rdd.id))
      } else {
        dropRDDIds += rdd.id
      }

      // TODO: 区分缓存意图和实际是否缓存
      val node = nodes.getOrElseUpdate(rdd.id, RDDOperationNode(
        rdd.id, rdd.name, rdd.storageLevel != StorageLevel.NONE, rdd.isBarrier, rdd.callSite,
        rdd.outputDeterministicLevel))
      if (rdd.scope.isEmpty) {
        // RDD没有外层操作范围，直接添加到根集群
        // 这种情况通常发生在RDD在公开RDD API之外被实例化
        if (isAllowed) {
          rootCluster.attachChildNode(node)
        }
      } else {
        // RDD属于内层操作集群，可能嵌套在其他集群中
        // 获取从最外层到最内层的所有作用域链
        val rddScopes = rdd.scope.map { scope => scope.getAllScopes }.getOrElse(Seq.empty)
        // 为每个作用域创建对应的集群
        val rddClusters = rddScopes.map { scope =>
          val clusterId = scope.id
          val clusterName = scope.name.replaceAll("\\n", "\\\\n")
          clusters.getOrElseUpdate(
            clusterId, new RDDOperationCluster(clusterId, false, clusterName))
        }
        // 构建集群的嵌套层次结构
        rddClusters.sliding(2).foreach { pc =>
          if (pc.size == 2) {
            val parentCluster = pc(0)
            val childCluster = pc(1)
            parentCluster.attachChildCluster(childCluster)
          }
        }
        // 将最外层集群附加到根集群，将RDD附加到最内层集群
        rddClusters.headOption.foreach { cluster =>
          if (!rootCluster.childClusters.contains(cluster)) {
            rootCluster.attachChildCluster(cluster)
          }
        }
        if (isAllowed) {
          rddClusters.lastOption.foreach { cluster => cluster.attachChildNode(node) }
        }
      }
    }

    // 将边分类为内部边、出边、入边
    // 该分类用于描述不同Stage之间的依赖关系
    val internalEdges = new ListBuffer[RDDOperationEdge]
    val outgoingEdges = new ListBuffer[RDDOperationEdge]
    val incomingEdges = new ListBuffer[RDDOperationEdge]
    edges.foreach { case e: RDDOperationEdge =>
      val fromThisGraph = nodes.contains(e.fromId)
      val toThisGraph = nodes.contains(e.toId)
      (fromThisGraph, toThisGraph) match {
        case (true, true) => internalEdges += e
        case (true, false) => outgoingEdges += e
        case (false, true) => incomingEdges += e
        // 不应该出现这种情况
        case _ => logWarning(log"Found an orphan edge in stage " +
          log"${MDC(STAGE_ID, stage.stageId)}: ${MDC(ERROR, e)}")
      }
    }

    RDDOperationGraph(internalEdges.toSeq, outgoingEdges.toSeq, incomingEdges.toSeq, rootCluster)
  }

  /**
   * 根据RDD操作图生成Graphviz DOT格式的图描述文件内容
   * @param graph 输入RDD操作图
   * @return DOT格式的图描述字符串
   */
  def makeDotFile(graph: RDDOperationGraph): String = {
    val dotFile = new StringBuilder
    dotFile.append("digraph G {\n")
    val indent = "  "
    val graphId = s"graph_${graph.rootCluster.id.replaceAll(STAGE_CLUSTER_PREFIX, "")}"
    dotFile.append(indent).append(s"""id="$graphId";\n""")
    // 递归生成所有子图（集群）
    makeDotSubgraph(dotFile, graph.rootCluster, indent = indent)
    // 添加所有内部边
    graph.edges.foreach { edge => dotFile.append(s"""  ${edge.fromId}->${edge.toId};\n""") }
    dotFile.append("}")
    val result = dotFile.toString()
    logDebug(result)
    result
  }

  /**
   * 生成单个RDD节点的DOT表示
   * @param node RDD节点
   * @return DOT格式的节点定义字符串
   */
  private def makeDotNode(node: RDDOperationNode): String = {
    val isCached = if (node.cached) {
      " [Cached]"
    } else {
      ""
    }
    val isBarrier = if (node.barrier) {
      " [Barrier]"
    } else {
      ""
    }
    val outputDeterministicLevel = node.outputDeterministicLevel match {
      case DeterministicLevel.DETERMINATE => ""
      case DeterministicLevel.INDETERMINATE => " [Indeterminate]"
      case DeterministicLevel.UNORDERED => " [Unordered]"
      case _ => ""
    }
    // 转义调用位置中的特殊字符
    val escapedCallsite = Utility.escape(node.callsite)
    // 构建HTML格式的节点标签，包含RDD基本信息和调用位置
    val label = StringEscapeUtils.escapeJava(
      s"${node.name} [${node.id}]$isCached$isBarrier$outputDeterministicLevel" +
        s"<br>$escapedCallsite")
    s"""${node.id} [id="node_${node.id}" labelType="html" label="$label"]"""
  }

  /**
   * 递归生成集群的DOT子图表示
   * @param subgraph 用于拼接结果的StringBuilder
   * @param cluster 当前处理的集群
   * @param indent 当前缩进层级
   * @param prefix 子图ID前缀
   */
  private def makeDotSubgraph(
      subgraph: StringBuilder,
      cluster: RDDOperationCluster,
      indent: String,
      prefix: String = "graph_"): Unit = {
    val clusterId = s"$prefix${cluster.id}"
    // 写入子图头部和基本属性
    subgraph.append(indent).append(s"subgraph $clusterId {\n")
      .append(indent).append(s"""  id="$clusterId";\n""")
      .append(indent).append(s"""  isCluster="true";\n""")
      .append(indent).append(s"""  label="${StringEscapeUtils.escapeJava(cluster.name)}";\n""")
    // 添加当前集群包含的所有节点
    cluster.childNodes.foreach { node =>
      subgraph.append(indent).append(s"  ${makeDotNode(node)};\n")
    }
    // 递归处理所有子集群
    cluster.childClusters.foreach { cscope =>
      makeDotSubgraph(subgraph, cscope, indent + "  ", "cluster_")
    }
    // 写入子图尾部
    subgraph.append(indent).append("}\n")
  }
}