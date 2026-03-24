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

import java.lang.reflect.ParameterizedType
import java.util.ServiceLoader

import scala.jdk.CollectionConverters._

import org.apache.spark.status.KVUtils.KVStoreScalaSerializer

/**
 * KV存储的Protobuf序列化实现，基于Protobuf格式序列化Spark状态信息，替代默认Java序列化机制
 * 扩展KVStoreScalaSerializer接口，通过Java ServiceLoader动态发现加载Protobuf序列化器
 */
private[spark] class KVStoreProtobufSerializer extends KVStoreScalaSerializer {
  /**
   * 序列化对象为字节数组，如果存在对应Protobuf序列化器则使用Protobuf序列化，否则回退到父类默认序列化
   * @param o 需要序列化的对象
   * @return 序列化后的字节数组
   */
  override def serialize(o: Object): Array[Byte] =
    KVStoreProtobufSerializer.getSerializer(o.getClass) match {
      case Some(serializer) => serializer.serialize(o)
      case _ => super.serialize(o)
    }

  /**
   * 从字节数组反序列化对象，如果存在对应Protobuf序列化器则使用Protobuf反序列化，否则回退到父类默认反序列化
   * @param data 序列化字节数组
   * @param klass 目标对象类型
   * @return 反序列化得到的对象
   */
  override def deserialize[T](data: Array[Byte], klass: Class[T]): T =
    KVStoreProtobufSerializer.getSerializer(klass) match {
      case Some(serializer) =>
        serializer.deserialize(data).asInstanceOf[T]
      case _ => super.deserialize(data, klass)
    }
}

/**
 * KVStoreProtobufSerializer单例对象，负责通过ServiceLoader预加载所有Protobuf序列化器
 * 维护类型到序列化器的映射，供序列化方法查询使用
 */
private[spark] object KVStoreProtobufSerializer {

  /**
   * 懒加载初始化类型-序列化器映射表，通过Java ServiceLoader发现所有实现类
   * 键为Protobuf序列化器处理的实体类型，值为对应的序列化器实例
   */
  private[this] lazy val serializerMap: Map[Class[_], ProtobufSerDe[Any]] = {
    /**
     * 从ProtobufSerDe实现类的泛型参数中提取该序列化器处理的实体类型
     * @param klass 序列化器实现类
     * @return 该序列化器处理的实体类
     */
    def getGenericsType(klass: Class[_]): Class[_] = {
      klass.getGenericInterfaces.head.asInstanceOf[ParameterizedType]
        .getActualTypeArguments.head.asInstanceOf[Class[_]]
    }
    // 通过ServiceLoader加载所有ProtobufSerDe实现，转换为类型-序列化器映射
    ServiceLoader.load(classOf[ProtobufSerDe[Any]]).asScala.map { serDe =>
      getGenericsType(serDe.getClass) -> serDe
    }.toMap
  }

  /**
   * 根据目标类型获取对应Protobuf序列化器
   * @param klass 目标实体类型
   * @return 存在则返回Option包装的序列化器，不存在返回None
   */
  def getSerializer(klass: Class[_]): Option[ProtobufSerDe[Any]] =
    serializerMap.get(klass)
}