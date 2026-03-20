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

// 这个文件已经全部加上中文注释

package org.apache.spark.scheduler

import java.io.{DataInputStream, DataOutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Properties

import scala.collection.immutable
import scala.collection.mutable.{HashMap, Map}
import scala.jdk.CollectionConverters._

import org.apache.spark.{JobArtifactSet, JobArtifactState}
import org.apache.spark.util.{ByteBufferInputStream, ByteBufferOutputStream, Utils}

/**
 * 传递给Executor执行的任务描述信息，通常由`TaskSetManager.resourceOffer`创建。
 *
 * TaskDescription和关联的Task需要仔细序列化，原因有两个：
 *
 *     (1) 当Executor收到TaskDescription时，需要先获取JAR和文件列表并添加到类路径，
 *         设置属性，然后才能反序列化Task对象（serializedTask）。
 *         这就是为什么Properties也包含在TaskDescription中，尽管它们也在序列化的Task中。
 *
 *     (2) 由于每个任务都需要序列化并发送一个TaskDescription到Executor，
 *         高效的序列化（序列化时间和缓冲区大小）非常重要。
 *         因此使用自定义的encode/decode方法，避免序列化Map对象中的不必要字段。
 *
 * @param taskId 任务唯一ID
 * @param attemptNumber 尝试编号
 * @param executorId 目标Executor ID
 * @param name 任务名称
 * @param index 此任务在TaskSet中的索引
 * @param partitionId 分区ID
 * @param artifacts 作业Artifact集合
 * @param properties 调度属性
 * @param cpus 分配的CPU核心数
 * @param resources 分配给任务的总资源，如 Map("gpu" -> Map("0" -> 0.7的内部表示))
 * @param serializedTask 序列化后的Task对象
 */
private[spark] class TaskDescription(
    val taskId: Long,
    val attemptNumber: Int,
    val executorId: String,
    val name: String,
    val index: Int,    // Index within this task's TaskSet
    val partitionId: Int,
    val artifacts: JobArtifactSet,
    val properties: Properties,
    val cpus: Int,
    // resources is the total resources assigned to the task
    // Eg, Map("gpu" -> Map("0" -> ResourceAmountUtils.toInternalResource(0.7))):
    // assign 0.7 of the gpu address "0" to this task
    val resources: immutable.Map[String, immutable.Map[String, Long]],
    val serializedTask: ByteBuffer) {

  assert(cpus > 0, "CPUs per task should be > 0")

  override def toString: String = s"TaskDescription($name)"
}

private[spark] object TaskDescription {
  private def serializeStringLongMap(map: Map[String, Long], dataOut: DataOutputStream): Unit = {
    dataOut.writeInt(map.size)
    map.foreach { case (key, value) =>
      dataOut.writeUTF(key)
      dataOut.writeLong(value)
    }
  }

  private def serializeResources(map: immutable.Map[String, immutable.Map[String, Long]],
      dataOut: DataOutputStream): Unit = {
    dataOut.writeInt(map.size)
    map.foreach { case (rName, addressAmountMap) =>
      dataOut.writeUTF(rName)
      dataOut.writeInt(addressAmountMap.size)
      addressAmountMap.foreach { case (address, amount) =>
        dataOut.writeUTF(address)
        dataOut.writeLong(amount)
      }
    }
  }

  def encode(taskDescription: TaskDescription): ByteBuffer = {
    val bytesOut = new ByteBufferOutputStream(4096)
    val dataOut = new DataOutputStream(bytesOut)

    dataOut.writeLong(taskDescription.taskId)
    dataOut.writeInt(taskDescription.attemptNumber)
    dataOut.writeUTF(taskDescription.executorId)
    dataOut.writeUTF(taskDescription.name)
    dataOut.writeInt(taskDescription.index)
    dataOut.writeInt(taskDescription.partitionId)

    // Write artifacts
    serializeArtifacts(taskDescription.artifacts, dataOut)

    // Write properties.
    dataOut.writeInt(taskDescription.properties.size())
    taskDescription.properties.asScala.foreach { case (key, value) =>
      dataOut.writeUTF(key)
      // SPARK-19796 -- writeUTF doesn't work for long strings, which can happen for property values
      val bytes = value.getBytes(StandardCharsets.UTF_8)
      dataOut.writeInt(bytes.length)
      dataOut.write(bytes)
    }

    // Write cpus.
    dataOut.writeInt(taskDescription.cpus)

    // Write resources.
    serializeResources(taskDescription.resources, dataOut)

    // Write the task. The task is already serialized, so write it directly to the byte buffer.
    Utils.writeByteBuffer(taskDescription.serializedTask, bytesOut)

    dataOut.close()
    bytesOut.close()
    bytesOut.toByteBuffer
  }

  private def deserializeOptionString(in: DataInputStream): Option[String] = {
    if (in.readBoolean()) {
      Some(in.readUTF())
    } else {
      None
    }
  }

  private def deserializeArtifacts(dataIn: DataInputStream): JobArtifactSet = {
    new JobArtifactSet(
      state = deserializeOptionString(dataIn).map { uuid =>
        JobArtifactState(
          uuid = uuid,
          replClassDirUri = deserializeOptionString(dataIn))
      },
      jars = immutable.Map(deserializeStringLongMap(dataIn).toSeq: _*),
      files = immutable.Map(deserializeStringLongMap(dataIn).toSeq: _*),
      archives = immutable.Map(deserializeStringLongMap(dataIn).toSeq: _*))
  }

  private def serializeOptionString(str: Option[String], out: DataOutputStream): Unit = {
    out.writeBoolean(str.isDefined)
    if (str.isDefined) {
      out.writeUTF(str.get)
    }
  }

  private def serializeArtifacts(artifacts: JobArtifactSet, dataOut: DataOutputStream): Unit = {
    serializeOptionString(artifacts.state.map(_.uuid), dataOut)
    artifacts.state.foreach { state =>
      serializeOptionString(state.replClassDirUri, dataOut)
    }
    serializeStringLongMap(Map(artifacts.jars.toSeq: _*), dataOut)
    serializeStringLongMap(Map(artifacts.files.toSeq: _*), dataOut)
    serializeStringLongMap(Map(artifacts.archives.toSeq: _*), dataOut)
  }

  private def deserializeStringLongMap(dataIn: DataInputStream): HashMap[String, Long] = {
    val map = new HashMap[String, Long]()
    val mapSize = dataIn.readInt()
    var i = 0
    while (i < mapSize) {
      map(dataIn.readUTF()) = dataIn.readLong()
      i += 1
    }
    map
  }

  private def deserializeResources(dataIn: DataInputStream):
      immutable.Map[String, immutable.Map[String, Long]] = {
    val map = new HashMap[String, immutable.Map[String, Long]]()
    val mapSize = dataIn.readInt()
    var i = 0
    while (i < mapSize) {
      val resType = dataIn.readUTF()
      val addressAmountMap = new HashMap[String, Long]()
      val addressAmountSize = dataIn.readInt()
      var j = 0
      while (j < addressAmountSize) {
        val address = dataIn.readUTF()
        val amount = dataIn.readLong()
        addressAmountMap(address) = amount
        j += 1
      }
      map.put(resType, addressAmountMap.toMap)
      i += 1
    }
    map.toMap
  }

  def decode(byteBuffer: ByteBuffer): TaskDescription = {
    val dataIn = new DataInputStream(new ByteBufferInputStream(byteBuffer))
    val taskId = dataIn.readLong()
    val attemptNumber = dataIn.readInt()
    val executorId = dataIn.readUTF()
    val name = dataIn.readUTF()
    val index = dataIn.readInt()
    val partitionId = dataIn.readInt()

    // Read artifacts.
    val artifacts = deserializeArtifacts(dataIn)

    // Read properties.
    val properties = new Properties()
    val numProperties = dataIn.readInt()
    for (i <- 0 until numProperties) {
      val key = dataIn.readUTF()
      val valueLength = dataIn.readInt()
      val valueBytes = new Array[Byte](valueLength)
      dataIn.readFully(valueBytes)
      properties.setProperty(key, new String(valueBytes, StandardCharsets.UTF_8))
    }

    // Read cpus.
    val cpus = dataIn.readInt()

    // Read resources.
    val resources = deserializeResources(dataIn)

    // Create a sub-buffer for the serialized task into its own buffer (to be deserialized later).
    val serializedTask = byteBuffer.slice()

    new TaskDescription(taskId, attemptNumber, executorId, name, index, partitionId, artifacts,
      properties, cpus, resources, serializedTask)
  }
}
