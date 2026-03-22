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

package org.apache.spark.util.collection.unsafe.sort;

import java.io.File;
import java.io.IOException;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.serializer.DummySerializerInstance;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.DiskBlockObjectWriter;
import org.apache.spark.storage.TempLocalBlockId;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.internal.config.package$;

/**
 * 文件级注释：不安全排序溢出写入器，负责将内存中已排序的记录溢出写入磁盘，用于外部排序过程中处理内存不足的情况
 * 溢出文件格式：
 *   [记录总数(int)] [[记录长度(int)][排序前缀(long)][记录数据(bytes)]...]
 */
public final class UnsafeSorterSpillWriter {

  private final SparkConf conf = new SparkConf();

  /**
   * 向磁盘文件写入排序记录时使用的缓冲区大小，前缀+长度占用固定12字节，因此缓冲区剩余空间必须大于0
   */
  private final int diskWriteBufferSize =
    (int) (long) conf.get(package$.MODULE$.SHUFFLE_DISK_WRITE_BUFFER_SIZE());

  // 直接向DiskBlockObjectWriter写入小块数据效率较低，由于没有直接从堆外内存转移到磁盘写入器的API
  // 因此通过字节数组在堆内缓冲数据，再批量写入磁盘
  private byte[] writeBuffer = new byte[diskWriteBufferSize];

  private final File file;
  private final BlockId blockId;
  private final int numRecordsToWrite;
  private DiskBlockObjectWriter writer;
  private int numRecordsSpilled = 0;

  /**
   * 构造溢出写入器，创建临时溢出文件并初始化写入器，写入记录总数头信息
   * @param blockManager 块管理器，用于创建临时磁盘块
   * @param fileBufferSize 文件缓冲区大小
   * @param writeMetrics Shuffle写入指标统计对象
   * @param numRecordsToWrite 本次要写入的总记录数
   * @throws IOException 创建文件或写入头信息失败时抛出异常
   */
  public UnsafeSorterSpillWriter(
      BlockManager blockManager,
      int fileBufferSize,
      ShuffleWriteMetrics writeMetrics,
      int numRecordsToWrite) throws IOException {
    // 创建本地临时块和对应的磁盘文件
    final Tuple2<TempLocalBlockId, File> spilledFileInfo =
      blockManager.diskBlockManager().createTempLocalBlock();
    this.file = spilledFileInfo._2();
    this.blockId = spilledFileInfo._1();
    this.numRecordsToWrite = numRecordsToWrite;
    // 需要序列化实例才能构造DiskBlockObjectWriter，但我们实际不使用它的序列化能力
    // 只调用底层OutputStream的write方法，因此传入一个空操作的哑序列化器绕过检查
    writer = blockManager.getDiskWriter(
      blockId, file, DummySerializerInstance.INSTANCE, fileBufferSize, writeMetrics);
    // 在文件开头写入本次溢出的总记录数
    writeIntToBuffer(numRecordsToWrite, 0);
    writer.write(writeBuffer, 0, 4);
  }

  /**
   * 将long类型值按大端序写入缓冲区的指定偏移位置，模仿DataOutputStream.writeLong实现
   * @param v 要写入的long值
   * @param offset 缓冲区起始偏移
   */
  private void writeLongToBuffer(long v, int offset) {
    writeBuffer[offset + 0] = (byte)(v >>> 56);
    writeBuffer[offset + 1] = (byte)(v >>> 48);
    writeBuffer[offset + 2] = (byte)(v >>> 40);
    writeBuffer[offset + 3] = (byte)(v >>> 32);
    writeBuffer[offset + 4] = (byte)(v >>> 24);
    writeBuffer[offset + 5] = (byte)(v >>> 16);
    writeBuffer[offset + 6] = (byte)(v >>>  8);
    writeBuffer[offset + 7] = (byte)(v >>>  0);
  }

  /**
   * 将int类型值按大端序写入缓冲区的指定偏移位置，模仿DataOutputStream.writeInt实现
   * @param v 要写入的int值
   * @param offset 缓冲区起始偏移
   */
  private void writeIntToBuffer(int v, int offset) {
    writeBuffer[offset + 0] = (byte)(v >>> 24);
    writeBuffer[offset + 1] = (byte)(v >>> 16);
    writeBuffer[offset + 2] = (byte)(v >>>  8);
    writeBuffer[offset + 3] = (byte)(v >>>  0);
  }

  /**
   * 将单条已排序记录写入溢出文件
   *
   * @param baseObject 包含记录的基础对象/内存页
   * @param baseOffset 记录数据在内存中的起始偏移
   * @param recordLength 记录长度
   * @param keyPrefix 排序键前缀，用于排序时快速比较
   */
  public void write(
      Object baseObject,
      long baseOffset,
      int recordLength,
      long keyPrefix) throws IOException {
    // 检查写入记录数不超过预期总数
    if (numRecordsSpilled == numRecordsToWrite) {
      throw new IllegalStateException(
        "Number of records written exceeded numRecordsToWrite = " + numRecordsToWrite);
    } else {
      numRecordsSpilled++;
    }
    // 写入记录长度和排序前缀到缓冲区开头
    writeIntToBuffer(recordLength, 0);
    writeLongToBuffer(keyPrefix, 4);
    // 剩余需要拷贝的数据量
    int dataRemaining = recordLength;
    // 缓冲区剩余可用空间 = 总缓冲区大小 - 前缀长度(12字节)
    int freeSpaceInWriteBuffer = diskWriteBufferSize - 4 - 8;
    // 当前从源内存读取的位置
    long recordReadPosition = baseOffset;
    // 循环分批拷贝数据，写满缓冲区就刷新到磁盘
    while (dataRemaining > 0) {
      // 本次拷贝字节数取剩余数据和缓冲区剩余空间的较小值
      final int toTransfer = Math.min(freeSpaceInWriteBuffer, dataRemaining);
      // 从堆外内存拷贝数据到写入缓冲区
      Platform.copyMemory(
        baseObject,
        recordReadPosition,
        writeBuffer,
        Platform.BYTE_ARRAY_OFFSET + (diskWriteBufferSize - freeSpaceInWriteBuffer),
        toTransfer);
      // 将缓冲区中已填充的数据写入磁盘
      writer.write(writeBuffer, 0, (diskWriteBufferSize - freeSpaceInWriteBuffer) + toTransfer);
      // 更新读位置和剩余数据量
      recordReadPosition += toTransfer;
      dataRemaining -= toTransfer;
      // 重置缓冲区剩余空间
      freeSpaceInWriteBuffer = diskWriteBufferSize;
    }
    // 如果缓冲区还有剩余数据未写满，将已填充部分写入磁盘
    if (freeSpaceInWriteBuffer < diskWriteBufferSize) {
      writer.write(writeBuffer, 0, (diskWriteBufferSize - freeSpaceInWriteBuffer));
    }
    // 更新写入指标，记录一条记录写入完成
    writer.recordWritten();
  }

  /**
   * 关闭写入器，提交写入内容并释放资源
   * @throws IOException 关闭或提交失败时抛出异常
   */
  public void close() throws IOException {
    writer.commitAndGet();
    writer.close();
    writer = null;
    writeBuffer = null;
  }

  /**
   * 获取溢出文件对象
   * @return 溢出写入的磁盘文件
   */
  public File getFile() {
    return file;
  }

  /**
   * 创建对应于此溢出文件的读取器，用于后续读取已溢出的排序记录
   * @param serializerManager 序列化管理器
   * @return 溢出文件读取器实例
   * @throws IOException 创建读取器或打开文件失败时抛出异常
   */
  public UnsafeSorterSpillReader getReader(SerializerManager serializerManager) throws IOException {
    return new UnsafeSorterSpillReader(serializerManager, file, blockId);
  }

  /**
   * 获取已溢出的记录总数
   * @return 已写入磁盘的记录数
   */
  public int recordsSpilled() {
    return numRecordsSpilled;
  }
}