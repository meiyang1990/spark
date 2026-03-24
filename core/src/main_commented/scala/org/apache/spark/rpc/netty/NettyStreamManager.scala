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
package org.apache.spark.rpc.netty

import java.io.File
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.server.StreamManager
import org.apache.spark.rpc.RpcEnvFileServer
import org.apache.spark.util.Utils

/**
 * 文件流管理器实现，用于在NettyRpcEnv中提供文件下载服务
 * 
 * 本管理器支持注册三类文件资源，都对应实际物理文件：
 * - "/files": 平面文件列表，为[[SparkContext.addFile]]提供后端支持，供节点下载用户添加的文件
 * - "/jars": 平面jar包列表，为[[SparkContext.addJar]]提供后端支持，供节点下载依赖的jar包
 * - 任意目录，目录下所有文件均可通过本管理器访问，保持原目录层级结构
 * 
 * 仅支持打开流（openStream）操作，不支持分块获取
 */
private[netty] class NettyStreamManager(rpcEnv: NettyRpcEnv)
  extends StreamManager with RpcEnvFileServer {

  // 存储已注册的文件，key为文件名，value为对应文件对象
  private val files = new ConcurrentHashMap[String, File]()
  // 存储已注册的jar包，key为文件名，value为对应文件对象
  private val jars = new ConcurrentHashMap[String, File]()
  // 存储已注册的目录，key为基础URI路径，value为对应目录对象
  private val dirs = new ConcurrentHashMap[String, File]()

  override def removeFile(key: String): Unit = files.remove(key)

  override def removeJar(key: String): Unit = jars.remove(key)

  /**
   * 获取流的分块数据，本实现不支持该操作
   * @param streamId 流ID
   * @param chunkIndex 分块索引
   * @return 永远抛出异常
   */
  override def getChunk(streamId: Long, chunkIndex: Int): ManagedBuffer = {
    throw new UnsupportedOperationException()
  }

  /**
   * 根据流ID打开对应文件，返回可传输的ManagedBuffer
   * @param streamId 请求的流标识，格式为/类型/文件名
   * @return 对应文件的ManagedBuffer，未找到则返回null
   */
  override def openStream(streamId: String): ManagedBuffer = {
    // 分割流ID为资源类型和文件名两部分
    val Array(ftype, fname) = streamId.stripPrefix("/").split("/", 2)
    // 根据资源类型查找对应文件
    val file = ftype match {
      case "files" => files.get(fname)
      case "jars" => jars.get(fname)
      case other =>
        // 其他类型视为目录基路径，拼接得到完整文件路径
        val dir = dirs.get(ftype)
        require(dir != null, s"Invalid stream URI: $ftype not found.")
        new File(dir, fname)
    }

    // 文件存在且为普通文件，构造文件段缓冲区返回
    if (file != null && file.isFile()) {
      new FileSegmentManagedBuffer(rpcEnv.transportConf, file, 0, file.length())
    } else {
      null
    }
  }

  /**
   * 注册文件供远程下载
   * @param file 要注册的本地文件
   * @return 可访问该文件的完整Spark URL
   */
  override def addFile(file: File): String = {
    val canonicalFile = file.getCanonicalFile
    // 不存在同名文件时才注册
    val existingPath = files.putIfAbsent(file.getName, canonicalFile)
    // 检查重复注册是否为同一个文件，不允许同一个文件名对应不同路径
    require(existingPath == null || existingPath == canonicalFile,
      s"File ${file.getName} was already registered with a different path " +
        s"(old path = $existingPath, new path = $file")
    s"${rpcEnv.address.toSparkURL}/files/${Utils.encodeFileNameToURIRawPath(file.getName())}"
  }

  /**
   * 注册jar包供远程下载
   * @param file 要注册的本地jar文件
   * @return 可访问该jar包的完整Spark URL
   */
  override def addJar(file: File): String = {
    val canonicalFile = file.getCanonicalFile
    // 不存在同名jar包时才注册
    val existingPath = jars.putIfAbsent(file.getName, canonicalFile)
    // 检查重复注册是否为同一个文件，不允许同一个文件名对应不同路径
    require(existingPath == null || existingPath == canonicalFile,
      s"File ${file.getName} was already registered with a different path " +
        s"(old path = $existingPath, new path = $file")
    s"${rpcEnv.address.toSparkURL}/jars/${Utils.encodeFileNameToURIRawPath(file.getName())}"
  }

  /**
   * 注册目录供远程下载，目录下所有文件均可通过对应URL访问
   * @param baseUri 访问该目录的基础URI
   * @param path 本地目录路径
   * @return 可访问该目录的完整基础Spark URL
   */
  override def addDirectory(baseUri: String, path: File): String = {
    val fixedBaseUri = validateDirectoryUri(baseUri)
    // 不允许重复注册同一个URI
    require(dirs.putIfAbsent(fixedBaseUri.stripPrefix("/"), path.getCanonicalFile) == null,
      s"URI '$fixedBaseUri' already registered.")
    s"${rpcEnv.address.toSparkURL}$fixedBaseUri"
  }

  /**
   * 如果URI未注册则注册目录，已存在则不做操作
   * @param baseUri 访问该目录的基础URI
   * @param path 本地目录路径
   * @return 可访问该目录的完整基础Spark URL
   */
  override def addDirectoryIfAbsent(baseUri: String, path: File): String = {
    val fixedBaseUri = validateDirectoryUri(baseUri)
    dirs.putIfAbsent(fixedBaseUri.stripPrefix("/"), path.getCanonicalFile)
    s"${rpcEnv.address.toSparkURL}$fixedBaseUri"
  }
}