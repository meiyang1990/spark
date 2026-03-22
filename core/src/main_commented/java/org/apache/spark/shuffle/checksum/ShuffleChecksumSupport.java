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

package org.apache.spark.shuffle.checksum;

import java.util.zip.Checksum;

import org.apache.spark.SparkConf;
import org.apache.spark.internal.config.package$;
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper;

/**
 * Shuffle校验和计算支持接口，为Shuffle计算提供分区级校验和工具方法，
 * 用于验证Shuffle输出数据的完整性，检测数据 corruption 问题。
 */
public interface ShuffleChecksumSupport {

  /**
   * 根据分区数量创建对应数量的校验和对象，根据配置决定是否启用校验和
   * @param numPartitions 分区数量，每个分区对应一个校验和
   * @param conf Spark配置，读取校验和相关配置项
   * @return 对应每个分区的校验和对象数组，未启用时返回空数组
   */
  default Checksum[] createPartitionChecksums(int numPartitions, SparkConf conf) {
    // 检查校验和功能是否开启
    if ((boolean) conf.get(package$.MODULE$.SHUFFLE_CHECKSUM_ENABLED())) {
      // 获取配置指定的校验和算法
      String checksumAlgorithm = conf.get(package$.MODULE$.SHUFFLE_CHECKSUM_ALGORITHM());
      // 为每个分区创建对应算法的校验和对象
      return ShuffleChecksumHelper.createPartitionChecksums(numPartitions, checksumAlgorithm);
    } else {
      // 未开启校验和功能，返回空数组
      return ShuffleChecksumHelper.EMPTY_CHECKSUM;
    }
  }

  /**
   * 从所有分区的校验和对象中提取最终计算得到的校验和数值
   * @param partitionChecksums 每个分区对应的校验和对象数组
   * @return 每个分区的校验和数值数组
   */
  default long[] getChecksumValues(Checksum[] partitionChecksums) {
    int numPartitions = partitionChecksums.length;
    long[] checksumValues = new long[numPartitions];
    // 遍历所有分区，提取每个分区的校验和值
    for (int i = 0; i < numPartitions; i++) {
      checksumValues[i] = partitionChecksums[i].getValue();
    }
    return checksumValues;
  }
}