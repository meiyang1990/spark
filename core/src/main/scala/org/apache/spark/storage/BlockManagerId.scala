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

package org.apache.spark.storage

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import com.google.common.cache.{CacheBuilder, CacheLoader}

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 * BlockManager 的唯一标识符。
 *
 * 该类的前两个构造函数被设为私有，以确保 BlockManagerId 对象只能通过伴生对象的 apply 方法创建。
 * 这样可以实现 ID 对象的去重。同时，构造参数也是私有的，以确保参数不能从类外部修改。
 */
@DeveloperApi
class BlockManagerId private (
    private var executorId_ : String,
    private var host_ : String,
    private var port_ : Int,
    private var topologyInfo_ : Option[String])
  extends Externalizable {

  private def this() = this(null, null, 0, None)  // 仅用于反序列化

  def executorId: String = executorId_

  if (null != host_) {
    Utils.checkHost(host_)
    assert (port_ > 0)
  }

  def hostPort: String = {
    // 调试代码
    Utils.checkHost(host)
    assert (port > 0)
    host + ":" + port
  }

  def host: String = host_

  def port: Int = port_

  def topologyInfo: Option[String] = topologyInfo_

  // 判断是否为 Driver 的 BlockManager
  def isDriver: Boolean = {
    executorId == SparkContext.DRIVER_IDENTIFIER
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeUTF(executorId_)
    out.writeUTF(host_)
    out.writeInt(port_)
    out.writeBoolean(topologyInfo_.isDefined)
    // 只有在有拓扑信息时才写入
    topologyInfo.foreach(out.writeUTF)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    executorId_ = in.readUTF()
    host_ = in.readUTF()
    port_ = in.readInt()
    val isTopologyInfoAvailable = in.readBoolean()
    topologyInfo_ = if (isTopologyInfoAvailable) Option(in.readUTF()) else None
  }

  // 反序列化后返回缓存中的对象（去重）
  @throws(classOf[IOException])
  private def readResolve(): Object = BlockManagerId.getCachedBlockManagerId(this)

  override def toString: String = s"BlockManagerId($executorId, $host, $port, $topologyInfo)"

  override def hashCode: Int =
    ((executorId.hashCode * 41 + host.hashCode) * 41 + port) * 41 + topologyInfo.hashCode

  override def equals(that: Any): Boolean = that match {
    case id: BlockManagerId =>
      executorId == id.executorId &&
        port == id.port &&
        host == id.host &&
        topologyInfo == id.topologyInfo
    case _ =>
      false
  }
}


private[spark] object BlockManagerId {

  /**
   * 根据给定的配置返回一个 [[org.apache.spark.storage.BlockManagerId]]。
   *
   * @param execId executor 的 ID
   * @param host BlockManager 的主机名
   * @param port BlockManager 的端口
   * @param topologyInfo BlockManager 的拓扑信息（如果可用）
   *                     这可以是网络拓扑信息，用于在复制数据块时选择节点。
   *                     更多信息参见 [[org.apache.spark.storage.TopologyMapper]]
   * @return 新的 [[org.apache.spark.storage.BlockManagerId]]
   */
  def apply(
      execId: String,
      host: String,
      port: Int,
      topologyInfo: Option[String] = None): BlockManagerId =
    getCachedBlockManagerId(new BlockManagerId(execId, host, port, topologyInfo))

  def apply(in: ObjectInput): BlockManagerId = {
    val obj = new BlockManagerId()
    obj.readExternal(in)
    getCachedBlockManagerId(obj)
  }

  /**
   * 最大缓存大小硬编码为 10000，因为 BlockManagerId 对象的大小约为 48B，
   * 总内存开销应该在 1MB 以下，这是可以接受的。
   */
  val blockManagerIdCache = CacheBuilder.newBuilder()
    .maximumSize(10000)
    .build(new CacheLoader[BlockManagerId, BlockManagerId]() {
      override def load(id: BlockManagerId) = id
    })

  // 从缓存中获取 BlockManagerId（去重）
  def getCachedBlockManagerId(id: BlockManagerId): BlockManagerId = {
    blockManagerIdCache.get(id)
  }

  // Shuffle 合并器的特殊标识符
  private[spark] val SHUFFLE_MERGER_IDENTIFIER = "shuffle-push-merger"

  // 无效 executor 的标识符
  private[spark] val INVALID_EXECUTOR_ID = "invalid"
}
