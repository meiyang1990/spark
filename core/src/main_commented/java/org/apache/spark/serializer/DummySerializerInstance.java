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

package org.apache.spark.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import scala.reflect.ClassTag;

import org.apache.spark.annotation.Private;
import org.apache.spark.unsafe.Platform;

/**
 * 空操作序列化器实例，用于Shuffle写入场景的占位使用
 * <p>
 * 由于构造DiskBlockObjectWriter必须传入一个SerializerInstance实例，而Shuffle写入流程实际上
 * 并不使用该序列化器（直接调用OutputStream的write方法写入），但DiskBlockObjectWriter仍会
 * 调用序列化实例的部分方法。此类作为占位实现，仅提供必要的空/转发方法满足接口要求，
 * 真正的序列化操作不被支持。
 */
@Private
public final class DummySerializerInstance extends SerializerInstance {

  /** 单例实例，全局复用 */
  public static final DummySerializerInstance INSTANCE = new DummySerializerInstance();

  private DummySerializerInstance() { }

  /**
   * 创建空操作序列化流，仅实现flush和close方法代理底层输出流，禁止写入对象操作
   * @param s 底层输出流
   * @return 序列化流实例
   */
  @Override
  public SerializationStream serializeStream(final OutputStream s) {
    return new SerializationStream() {
      @Override
      public void flush() {
        // 需要实现该方法，因为DiskObjectWriter会用它刷新压缩流
        try {
          s.flush();
        } catch (IOException e) {
          Platform.throwException(e);
        }
      }

      @Override
      public <T> SerializationStream writeObject(T t, ClassTag<T> ev1) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void close() {
        // 需要实现该方法，因为DiskObjectWriter会用它关闭压缩流
        try {
          s.close();
        } catch (IOException e) {
          Platform.throwException(e);
        }
      }
    };
  }

  @Override
  public <T> ByteBuffer serialize(T t, ClassTag<T> ev1) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DeserializationStream deserializeStream(InputStream s) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T deserialize(ByteBuffer bytes, ClassLoader loader, ClassTag<T> ev1) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T deserialize(ByteBuffer bytes, ClassTag<T> ev1) {
    throw new UnsupportedOperationException();
  }
}