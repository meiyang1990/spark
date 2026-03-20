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

package org.apache.spark.shuffle

import java.io.{DataInputStream, File, FileInputStream}
import java.util.zip.CheckedInputStream

import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper
import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage.{BlockId, ShuffleChecksumBlockId, ShuffleDataBlockId}

/** Shuffle 校验和工具类，用于校验和数据块的生成和验证 */
object ShuffleChecksumUtils {

  /**
   * 返回 Shuffle 数据块 ID 对应的校验和文件名。如果不是 Shuffle 数据块，返回 null。
   */
  def getChecksumFileName(blockId: BlockId, algorithm: String): String = blockId match {
    case ShuffleDataBlockId(shuffleId, mapId, _) =>
      ShuffleChecksumHelper.getChecksumFileName(
        ShuffleChecksumBlockId(shuffleId, mapId, NOOP_REDUCE_ID).name, algorithm)
    case _ =>
      null
  }

  /**
   * 校验校验和值是否与索引文件和数据文件一致。
   * 通过重新计算数据文件中各分区的校验和并与存储的校验和比较，验证数据完整性。
   *
   * @param numPartition 分区数
   * @param algorithm 校验和算法
   * @param checksum 校验和文件
   * @param data 数据文件
   * @param index 索引文件
   * @return true 表示校验通过
   */
  def compareChecksums(
      numPartition: Int,
      algorithm: String,
      checksum: File,
      data: File,
      index: File): Boolean = {
    var checksumIn: DataInputStream = null
    val expectChecksums = Array.ofDim[Long](numPartition)
    try {
      checksumIn = new DataInputStream(new FileInputStream(checksum))
      (0 until numPartition).foreach(i => expectChecksums(i) = checksumIn.readLong())
    } finally {
      if (checksumIn != null) {
        checksumIn.close()
      }
    }

    var dataIn: FileInputStream = null
    var indexIn: DataInputStream = null
    var checkedIn: CheckedInputStream = null
    try {
      dataIn = new FileInputStream(data)
      indexIn = new DataInputStream(new FileInputStream(index))
      var prevOffset = indexIn.readLong
      (0 until numPartition).foreach { i =>
        val curOffset = indexIn.readLong
        val limit = (curOffset - prevOffset).toInt
        val bytes = new Array[Byte](limit)
        val checksumCal = ShuffleChecksumHelper.getChecksumByAlgorithm(algorithm)
        checkedIn = new CheckedInputStream(
          new LimitedInputStream(dataIn, curOffset - prevOffset), checksumCal)
        checkedIn.read(bytes, 0, limit)
        prevOffset = curOffset
        // 校验和在写入端和读取端必须一致
        if (checkedIn.getChecksum.getValue != expectChecksums(i)) return false
      }
    } finally {
      if (dataIn != null) {
        dataIn.close()
      }
      if (indexIn != null) {
        indexIn.close()
      }
      if (checkedIn != null) {
        checkedIn.close()
      }
    }
    true
  }
}
