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

package org.apache.spark.internal.config

import org.apache.spark.network.util.ByteUnit

/**
 * Kryo序列化相关配置项定义，集中管理所有Kryo序列化器的可配置参数
 */
private[spark] object Kryo {

  /**
   * 是否要求所有类必须提前注册才能序列化，未注册类会抛异常
   */
  val KRYO_REGISTRATION_REQUIRED = ConfigBuilder("spark.kryo.registrationRequired")
    .version("1.1.0")
    .booleanConf
    .createWithDefault(false)

  /**
   * 用户自定义的Kryo注册器类名列表，用于用户自定义类的序列化注册
   */
  val KRYO_USER_REGISTRATORS = ConfigBuilder("spark.kryo.registrator")
    .version("0.5.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  /**
   * 需要提前注册到Kryo的自定义类名列表
   */
  val KRYO_CLASSES_TO_REGISTER = ConfigBuilder("spark.kryo.classesToRegister")
    .version("1.2.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  /**
   * 是否开启Kryo的Unsafe内存序列化优化，提升序列化性能
   */
  val KRYO_USE_UNSAFE = ConfigBuilder("spark.kryo.unsafe")
    .version("2.1.0")
    .booleanConf
    .createWithDefault(true)

  /**
   * 是否开启Kryo实例池复用，减少重复创建Kryo对象的开销
   */
  val KRYO_USE_POOL = ConfigBuilder("spark.kryo.pool")
    .version("3.0.0")
    .booleanConf
    .createWithDefault(true)

  /**
   * 是否开启Kryo引用跟踪，处理循环引用和重复对象序列化
   */
  val KRYO_REFERENCE_TRACKING = ConfigBuilder("spark.kryo.referenceTracking")
    .version("0.8.0")
    .booleanConf
    .createWithDefault(true)

  /**
   * Kryo序列化缓冲区初始大小
   */
  val KRYO_SERIALIZER_BUFFER_SIZE = ConfigBuilder("spark.kryoserializer.buffer")
    .version("1.4.0")
    .bytesConf(ByteUnit.KiB)
    .createWithDefaultString("64k")

  /**
   * Kryo序列化缓冲区最大允许大小
   */
  val KRYO_SERIALIZER_MAX_BUFFER_SIZE = ConfigBuilder("spark.kryoserializer.buffer.max")
    .version("1.4.0")
    .bytesConf(ByteUnit.MiB)
    .createWithDefaultString("64m")

}