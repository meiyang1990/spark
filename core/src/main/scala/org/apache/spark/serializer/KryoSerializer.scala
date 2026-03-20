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

package org.apache.spark.serializer

import java.io._
import java.lang.invoke.SerializedLambda
import java.nio.ByteBuffer
import java.util.Locale
import javax.annotation.Nullable

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import com.esotericsoftware.kryo.{Kryo, KryoException, Serializer => KryoClassSerializer}
import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}
import com.esotericsoftware.kryo.io.{UnsafeInput => KryoUnsafeInput, UnsafeOutput => KryoUnsafeOutput}
import com.esotericsoftware.kryo.pool.{KryoCallback, KryoFactory, KryoPool}
import com.esotericsoftware.kryo.serializers.{JavaSerializer => KryoJavaSerializer}
import com.twitter.chill.{AllScalaRegistrar, EmptyScalaKryoInstantiator}
import org.apache.avro.generic.{GenericContainer, GenericData, GenericRecord}
import org.roaringbitmap.RoaringBitmap

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.python.PythonBroadcast
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.CLASS_NAME
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.internal.io.FileCommitProtocol._
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.scheduler.{CompressedMapStatus, HighlyCompressedMapStatus}
import org.apache.spark.storage._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{BoundedPriorityQueue, ByteBufferInputStream, NextIterator, SerializableConfiguration, SerializableJobConf, Utils}
import org.apache.spark.util.collection.{BitSet, CompactBuffer}
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * KryoSerializer - 基于 Kryo 库的高性能序列化器
 * 
 * 核心特点：
 * 1. 性能：比 JavaSerializer 快 10 倍，体积小 10 倍
 * 2. 类注册：通过注册类提升性能（避免写入完整类名）
 * 3. 缓冲区管理：支持自动扩容，可配置初始和最大大小
 * 4. 引用跟踪：可选的对象引用跟踪（处理循环引用）
 * 5. Unsafe 模式：使用 sun.misc.Unsafe 提升性能
 * 6. 对象池：可选的 Kryo 实例池（减少创建开销）
 * 
 * 配置要点：
 * - spark.kryo.registrator：自定义类注册器
 * - spark.kryo.classesToRegister：要注册的类列表
 * - spark.kryo.registrationRequired：是否强制注册（未注册类会报错）
 * - spark.kryo.referenceTracking：是否跟踪引用（循环引用必须启用）
 * - spark.kryo.unsafe：是否使用 Unsafe（更快但不安全）
 * - spark.serializer.objectStreamReset：auto-reset（影响 relocation）
 * 
 * 预注册类：
 * - Spark 核心类（StorageLevel、BlockManagerId、ChunkedByteBuffer 等）
 * - 原始类型数组
 * - Tuple 数组（Tuple1 到 Tuple22）
 * - Scala 集合类
 * - SQL/ML/MLlib 类（如果可用）
 * - Avro GenericContainer 类
 * 
 * 注意：不保证跨 Spark 版本的线路兼容性，仅用于单个应用内部。
 * 
 * @see <a href="https://github.com/EsotericSoftware/kryo">Kryo 序列化库</a>
 */
class KryoSerializer(conf: SparkConf)
  extends org.apache.spark.serializer.Serializer
  with Logging
  with Serializable {

  /** 初始缓冲区大小（KB），写入超过此大小时自动扩容 */
  private val bufferSizeKb = conf.get(KRYO_SERIALIZER_BUFFER_SIZE)

  if (bufferSizeKb >= ByteUnit.GiB.toKiB(2)) {
    throw new SparkIllegalArgumentException(
      errorClass = "INVALID_KRYO_SERIALIZER_BUFFER_SIZE",
      messageParameters = Map(
        "bufferSizeConfKey" -> KRYO_SERIALIZER_BUFFER_SIZE.key,
        "bufferSizeConfValue" -> ByteUnit.KiB.toMiB(bufferSizeKb).toString))
  }
  private val bufferSize = ByteUnit.KiB.toBytes(bufferSizeKb).toInt

  /** 最大缓冲区大小（MB），超过此大小会抛出 Buffer overflow 异常 */
  val maxBufferSizeMb = conf.get(KRYO_SERIALIZER_MAX_BUFFER_SIZE).toInt
  if (maxBufferSizeMb >= ByteUnit.GiB.toMiB(2)) {
    throw new SparkIllegalArgumentException(
      errorClass = "INVALID_KRYO_SERIALIZER_BUFFER_SIZE",
      messageParameters = Map(
        "bufferSizeConfKey" -> KRYO_SERIALIZER_MAX_BUFFER_SIZE.key,
        "bufferSizeConfValue" -> maxBufferSizeMb.toString))
  }
  private val maxBufferSize = ByteUnit.MiB.toBytes(maxBufferSizeMb).toInt

  /** 是否跟踪对象引用（处理循环引用，禁用可提升性能） */
  private val referenceTracking = conf.get(KRYO_REFERENCE_TRACKING)
  /** 是否强制要求类注册（未注册的类会抛出异常） */
  private val registrationRequired = conf.get(KRYO_REGISTRATION_REQUIRED)
  /** 用户自定义的类注册器列表 */
  private val userRegistrators = conf.get(KRYO_USER_REGISTRATORS)
    .map(_.trim)
    .filter(!_.isEmpty)
  /** 要注册的类名列表 */
  private val classesToRegister = conf.get(KRYO_CLASSES_TO_REGISTER)
    .map(_.trim)
    .filter(!_.isEmpty)

  /** Avro schema 配置（用于 Avro 序列化） */
  private val avroSchemas = conf.getAvroSchema
  /** 是否使用 Unsafe 模式（更快但不检查边界） */
  private val useUnsafe = conf.get(KRYO_USE_UNSAFE)
  /** 是否使用对象池（复用 Kryo 实例） */
  private val usePool = conf.get(KRYO_USE_POOL)

  /** 创建 KryoOutput 实例（根据 useUnsafe 配置选择实现） */
  def newKryoOutput(): KryoOutput =
    if (useUnsafe) {
      new KryoUnsafeOutput(bufferSize, math.max(bufferSize, maxBufferSize))
    } else {
      new KryoOutput(bufferSize, math.max(bufferSize, maxBufferSize))
    }

  @transient
  private lazy val factory: KryoFactory = new KryoFactory() {
    override def create: Kryo = {
      newKryo()
    }
  }

  private class PoolWrapper extends KryoPool {
    private var pool: KryoPool = getPool

    override def borrow(): Kryo = pool.borrow()

    override def release(kryo: Kryo): Unit = pool.release(kryo)

    override def run[T](kryoCallback: KryoCallback[T]): T = pool.run(kryoCallback)

    def reset(): Unit = {
      pool = getPool
    }

    private def getPool: KryoPool = {
      new KryoPool.Builder(factory).softReferences.build
    }
  }

  @transient
  private lazy val internalPool = new PoolWrapper

  def pool: KryoPool = internalPool

  /**
   * 创建新的 Kryo 实例并注册 Spark 所需的类
   * 
   * 注册顺序：
   * 1. 配置 referenceTracking（用户注册器可覆盖）
   * 2. 注册 Spark 核心类（toRegister 和 toRegisterSerializer）
   * 3. 注册 Java Iterable 包装器（asJavaIterable）
   * 4. 注册自定义 Java 序列化的类（SerializableWritable、SerializableConfiguration 等）
   * 5. 注册 Avro GenericContainer 类
   * 6. 注册用户配置的类（spark.kryo.classesToRegister）
   * 7. 调用用户注册器（spark.kryo.registrator）
   * 8. 注册 Chill 的 Scala 类（AllScalaRegistrar）
   * 9. 注册 Chill 遗漏的类（Tuple 数组、Scala 集合类）
   * 10. 注册 SQL/ML/MLlib 类（如果可用）
   * 
   * 设计考虑：
   * - Spark 的注册在 Chill 之前，允许覆盖 Chill 的通用序列化器（如 Seq）
   * - 用户注册器可以覆盖所有默认注册
   * - SQL/ML/MLlib 类动态加载，避免不必要的依赖
   */
  def newKryo(): Kryo = {
    val instantiator = new EmptyScalaKryoInstantiator
    val kryo = instantiator.newKryo()
    kryo.setRegistrationRequired(registrationRequired)

    val classLoader = defaultClassLoader.getOrElse(Thread.currentThread.getContextClassLoader)

    // Allow disabling Kryo reference tracking if user knows their object graphs don't have loops.
    // Do this before we invoke the user registrator so the user registrator can override this.
    kryo.setReferences(referenceTracking)

    for (cls <- KryoSerializer.toRegister) {
      kryo.register(cls)
    }
    for ((cls, ser) <- KryoSerializer.toRegisterSerializer) {
      kryo.register(cls, ser)
    }

    // For results returned by asJavaIterable. See JavaIterableWrapperSerializer.
    kryo.register(JavaIterableWrapperSerializer.wrapperClass, new JavaIterableWrapperSerializer)

    // Allow sending classes with custom Java serializers
    kryo.register(classOf[SerializableWritable[_]], new KryoJavaSerializer())
    kryo.register(classOf[SerializableConfiguration], new KryoJavaSerializer())
    kryo.register(classOf[SerializableJobConf], new KryoJavaSerializer())
    kryo.register(classOf[PythonBroadcast], new KryoJavaSerializer())

    // Register serializers for Avro GenericContainer classes
    // We do not handle SpecificRecordBase and SpecificFixed here. They are abstract classes and
    // we will need to register serializers for their concrete implementations individually.
    // Also, their serialization requires the use of SpecificDatum(Reader|Writer) instead of
    // GenericDatum(Reader|Writer).
    def registerAvro[T <: GenericContainer]()(implicit ct: ClassTag[T]): Unit =
      kryo.register(ct.runtimeClass, new GenericAvroSerializer[T](avroSchemas))
    registerAvro[GenericRecord]()
    registerAvro[GenericData.Record]()
    registerAvro[GenericData.Array[_]]()
    registerAvro[GenericData.EnumSymbol]()
    registerAvro[GenericData.Fixed]()

    // Use the default classloader when calling the user registrator.
    Utils.withContextClassLoader(classLoader) {
      try {
        // Register classes given through spark.kryo.classesToRegister.
        classesToRegister.foreach { className =>
          kryo.register(Utils.classForName(className, noSparkClassLoader = true))
        }
        // Allow the user to register their own classes by setting spark.kryo.registrator.
        userRegistrators
          .map(Utils.classForName[KryoRegistrator](_, noSparkClassLoader = true).
            getConstructor().newInstance())
          .foreach { reg => reg.registerClasses(kryo) }
      } catch {
        case e: Exception =>
          throw new SparkException(
            errorClass = "FAILED_REGISTER_CLASS_WITH_KRYO",
            messageParameters = Map.empty,
            cause = e)
      }
    }

    // Register Chill's classes; we do this after our ranges and the user's own classes to let
    // our code override the generic serializers in Chill for things like Seq
    new AllScalaRegistrar().apply(kryo)

    // Register types missed by Chill.
    // scalastyle:off
    kryo.register(classOf[Array[Tuple1[Any]]])
    kryo.register(classOf[Array[Tuple2[Any, Any]]])
    kryo.register(classOf[Array[Tuple3[Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple4[Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple5[Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple6[Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple7[Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple8[Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple9[Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple11[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple12[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple13[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple14[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple15[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple16[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple17[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple18[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple19[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple20[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple21[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple22[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])

    // scalastyle:on

    kryo.register(Utils.classForName("scala.collection.immutable.ArraySeq$ofRef"))
    kryo.register(Utils.classForName("scala.collection.immutable.Map$EmptyMap$"))
    kryo.register(Utils.classForName("scala.math.Ordering$Reverse"))
    kryo.register(Utils.classForName("scala.reflect.ClassTag$GenericClassTag"))
    kryo.register(classOf[ArrayBuffer[Any]])
    kryo.register(classOf[Array[Array[Byte]]])
    kryo.register(classOf[UTF8String])

    // We can't load those class directly in order to avoid unnecessary jar dependencies.
    // We load them safely, ignore it if the class not found.
    KryoSerializer.loadableSparkClasses.foreach { clazz =>
      try {
        kryo.register(clazz)
      } catch {
        case NonFatal(_) => // do nothing
        case _: NoClassDefFoundError if Utils.isTesting => // See SPARK-23422.
      }
    }

    kryo.setClassLoader(classLoader)
    kryo
  }

  override def setDefaultClassLoader(classLoader: ClassLoader): Serializer = {
    super.setDefaultClassLoader(classLoader)
    internalPool.reset()
    this
  }

  override def newInstance(): SerializerInstance = {
    new KryoSerializerInstance(this, useUnsafe, usePool)
  }

  /**
   * Kryo 是否支持序列化对象的重新排列（relocation）
   * 
   * 依赖条件：auto-reset 必须启用
   * 
   * 原因：
   * - 如果禁用 auto-reset，Kryo 可能会在流中存储重复对象的引用（而不是完整序列化）
   * - 这会导致重新排列字节流时引用失效
   * 
   * 影响：
   * - 如果返回 false，sort-based shuffle 无法使用字节流重排序优化
   * - 必须完全反序列化才能重新排序
   * 
   * @see <a href="https://groups.google.com/d/msg/kryo-users/6ZUSyfjjtdo/FhGG1KHDXPgJ">Kryo 讨论</a>
   */
  private[spark] override lazy val supportsRelocationOfSerializedObjects: Boolean = {
    newInstance().asInstanceOf[KryoSerializerInstance].getAutoReset()
  }
}

/**
 * KryoSerializationStream - Kryo 序列化输出流
 * 
 * 资源管理：
 * - 从 KryoSerializerInstance 借用 Kryo 实例
 * - close() 时释放 Kryo 实例回池
 * - 支持 Unsafe 模式（KryoUnsafeOutput）
 * 
 * 线程安全：不是线程安全的，每个线程应使用独立的流
 */
private[spark]
class KryoSerializationStream(
    serInstance: KryoSerializerInstance,
    outStream: OutputStream,
    useUnsafe: Boolean) extends SerializationStream {

  private[this] var output: KryoOutput =
    if (useUnsafe) new KryoUnsafeOutput(outStream) else new KryoOutput(outStream)

  private[this] var kryo: Kryo = serInstance.borrowKryo()

  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    kryo.writeClassAndObject(output, t)
    this
  }

  override def flush(): Unit = {
    if (output == null) {
      throw new IOException("Stream is closed")
    }
    output.flush()
  }

  override def close(): Unit = {
    if (output != null) {
      try {
        output.close()
      } finally {
        serInstance.releaseKryo(kryo)
        kryo = null
        output = null
      }
    }
  }
}

/**
 * KryoDeserializationStream - Kryo 反序列化输入流
 * 
 * 资源管理：
 * - 从 KryoSerializerInstance 借用 Kryo 实例
 * - close() 时释放 Kryo 实例回池
 * - 支持 Unsafe 模式（KryoUnsafeInput）
 * 
 * EOF 处理：
 * - Kryo 抛出 "buffer underflow" KryoException 表示 EOF
 * - 转换为 EOFException 以符合 DeserializationStream 接口
 * 
 * 线程安全：不是线程安全的，每个线程应使用独立的流
 */
private[spark]
class KryoDeserializationStream(
    serInstance: KryoSerializerInstance,
    inStream: InputStream,
    useUnsafe: Boolean) extends DeserializationStream {

  private[this] var input: KryoInput =
    if (useUnsafe) new KryoUnsafeInput(inStream) else new KryoInput(inStream)

  private[this] var kryo: Kryo = serInstance.borrowKryo()

  private[this] def hasNext: Boolean = {
    if (input == null) {
      return false
    }

    val eof = input.eof()
    if (eof) close()
    !eof
  }

  override def readObject[T: ClassTag](): T = {
    try {
      kryo.readClassAndObject(input).asInstanceOf[T]
    } catch {
      // Kryo 使用 "buffer underflow" 异常表示 EOF，转换为标准的 EOFException
      case e: KryoException
        if e.getMessage.toLowerCase(Locale.ROOT).contains("buffer underflow") =>
        throw new EOFException
    }
  }

  override def close(): Unit = {
    if (input != null) {
      try {
        // Kryo 的 Input 自动关闭底层输入流
        input.close()
      } finally {
        serInstance.releaseKryo(kryo)
        kryo = null
        input = null
      }
    }
  }

  final override def asIterator: Iterator[Any] = new NextIterator[Any] {
    override protected def getNext(): Any = {
      if (KryoDeserializationStream.this.hasNext) {
        try {
          return readObject[Any]()
        } catch {
          case eof: EOFException =>
        }
      }
      finished = true
      null
    }

    override protected def close(): Unit = {
      KryoDeserializationStream.this.close()
    }
  }

  final override def asKeyValueIterator: Iterator[(Any, Any)] = new NextIterator[(Any, Any)] {
    override protected def getNext(): (Any, Any) = {
      if (KryoDeserializationStream.this.hasNext) {
        try {
          return (readKey[Any](), readValue[Any]())
        } catch {
          case eof: EOFException =>
        }
      }
      finished = true
      null
    }

    override protected def close(): Unit = {
      KryoDeserializationStream.this.close()
    }
  }
}

/**
 * KryoSerializerInstance - Kryo 序列化器实例
 * 
 * 核心设计：Kryo 实例的借用/释放机制
 * 
 * 两种模式：
 * 1. usePool=true：使用对象池（KryoPool），适合高并发场景
 *    - 每次借用从池中获取并 reset()
 *    - 每次释放归还到池中
 * 
 * 2. usePool=false：使用缓存的 Kryo 实例（cachedKryo），适合低并发场景
 *    - 逻辑上是大小为 1 的缓存池
 *    - 借用时：返回 cachedKryo 并置为 null，如果为 null 则新建
 *    - 释放时：保存到 cachedKryo（如果为空），否则丢弃
 * 
 * 注意：SerializerInstance 不是线程安全的，不同步访问 cachedKryo
 * 
 * @param useUnsafe 是否使用 Unsafe 模式（KryoUnsafe(Input|Output)）
 * @param usePool 是否使用对象池
 */
private[spark] class KryoSerializerInstance(
   ks: KryoSerializer, useUnsafe: Boolean, usePool: Boolean)
  extends SerializerInstance {
  /**
   * 缓存的 Kryo 实例（仅在 usePool=false 时使用）
   * 
   * 逻辑上是大小为 1 的对象池：
   * - 空闲时：cachedKryo 持有实例
   * - 使用中：cachedKryo 为 null
   * 
   * 注意：不是线程安全的，因为 SerializerInstance 本身不线程安全
   */
  @Nullable private[this] var cachedKryo: Kryo = if (usePool) null else borrowKryo()

  /**
   * 借用 Kryo 实例
   * 
   * 策略：
   * - usePool=true：从池中借用并 reset()
   * - usePool=false：返回 cachedKryo（并置为 null），如果为 null 则新建
   * 
   * 防御性措施：每次借用都调用 reset() 清理状态（SPARK-7766）
   */
  private[serializer] def borrowKryo(): Kryo = {
    if (usePool) {
      val kryo = ks.pool.borrow()
      kryo.reset()
      kryo
    } else {
      if (cachedKryo != null) {
        val kryo = cachedKryo
        // As a defensive measure, call reset() to clear any Kryo state that might have
        // been modified by the last operation to borrow this instance
        // (see SPARK-7766 for discussion of this issue)
        kryo.reset()
        cachedKryo = null
        kryo
      } else {
        ks.newKryo()
      }
    }
  }

  /**
   * 释放 Kryo 实例
   * 
   * 策略：
   * - usePool=true：归还到池中
   * - usePool=false：保存到 cachedKryo（如果为空），否则丢弃
   * 
   * 设计考虑：
   * - 不保存多个实例（逻辑上是大小为 1 的池）
   * - 如果 cachedKryo 已有实例，说明调用者错误（不释放就再次借用）
   */
  private[serializer] def releaseKryo(kryo: Kryo): Unit = {
    if (usePool) {
      ks.pool.release(kryo)
    } else {
      if (cachedKryo == null) {
        cachedKryo = kryo
      }
    }
  }

  // 延迟初始化 output 和 input，避免不必要的缓冲区分配
  private lazy val output = ks.newKryoOutput()
  private lazy val input = if (useUnsafe) new KryoUnsafeInput() else new KryoInput()

  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    output.clear()
    val kryo = borrowKryo()
    try {
      kryo.writeClassAndObject(output, t)
    } catch {
      // Buffer overflow：缓冲区超过 maxBufferSize
      case e: KryoException if e.getMessage.startsWith("Buffer overflow") =>
        throw new SparkException(
          errorClass = "KRYO_BUFFER_OVERFLOW",
          messageParameters = Map(
            "exceptionMsg" -> e.getMessage,
            "bufferSizeConfKey" -> KRYO_SERIALIZER_MAX_BUFFER_SIZE.key),
          cause = e)
    } finally {
      releaseKryo(kryo)
    }
    ByteBuffer.wrap(output.toBytes)
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    val kryo = borrowKryo()
    try {
      // 优化：如果 ByteBuffer 有数组支持，直接使用数组
      if (bytes.hasArray) {
        input.setBuffer(bytes.array(), bytes.arrayOffset() + bytes.position(), bytes.remaining())
      } else {
        // 否则使用 InputStream 包装
        input.setBuffer(new Array[Byte](4096))
        input.setInputStream(new ByteBufferInputStream(bytes))
      }
      kryo.readClassAndObject(input).asInstanceOf[T]
    } finally {
      releaseKryo(kryo)
    }
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    val kryo = borrowKryo()
    val oldClassLoader = kryo.getClassLoader
    try {
      kryo.setClassLoader(loader)
      if (bytes.hasArray) {
        input.setBuffer(bytes.array(), bytes.arrayOffset() + bytes.position(), bytes.remaining())
      } else {
        input.setBuffer(new Array[Byte](4096))
        input.setInputStream(new ByteBufferInputStream(bytes))
      }
      kryo.readClassAndObject(input).asInstanceOf[T]
    } finally {
      kryo.setClassLoader(oldClassLoader)
      releaseKryo(kryo)
    }
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    new KryoSerializationStream(this, s, useUnsafe)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new KryoDeserializationStream(this, s, useUnsafe)
  }

  /**
   * 检查 auto-reset 是否启用
   * 
   * 用途：判断是否支持序列化对象重新排列（supportsRelocationOfSerializedObjects）
   * 
   * 实现：通过反射读取 Kryo.autoReset 私有字段
   * 
   * 注意：
   * - 通常 auto-reset 默认启用
   * - 用户注册器可以显式关闭（但会影响 relocation）
   */
  def getAutoReset(): Boolean = {
    val field = classOf[Kryo].getDeclaredField("autoReset")
    field.setAccessible(true)
    val kryo = borrowKryo()
    try {
      field.get(kryo).asInstanceOf[Boolean]
    } finally {
      releaseKryo(kryo)
    }
  }
}

/**
 * KryoRegistrator - Kryo 类注册器接口
 * 
 * 用途：用户自定义类注册，提升序列化性能
 * 
 * 使用方式：
 * 1. 实现此接口：
 *    ```scala
 *    class MyRegistrator extends KryoRegistrator {
 *      override def registerClasses(kryo: Kryo): Unit = {
 *        kryo.register(classOf[MyClass])
 *      }
 *    }
 *    ```
 * 
 * 2. 配置 Spark：
 *    ```
 *    spark.kryo.registrator=com.example.MyRegistrator
 *    ```
 * 
 * 注意：
 * - 可以覆盖 Spark 的默认序列化器（包括 Chill 的）
 * - 可以控制 Kryo 的 referenceTracking、autoReset 等设置
 * - 未注册的类会使用完整类名（更慢、更大）
 */
@DeveloperApi
trait KryoRegistrator {
  def registerClasses(kryo: Kryo): Unit
}

/**
 * KryoSerializer 伴生对象
 * 
 * 包含预注册的类列表：
 * - toRegister：常用类（StorageLevel、BlockManagerId、原始数组等）
 * - toRegisterSerializer：需要自定义序列化器的类（RoaringBitmap）
 * - loadableSparkClasses：SQL/ML/MLlib 类（动态加载，避免不必要依赖）
 */
private[serializer] object KryoSerializer {
  // 常用类列表（无需自定义序列化器）
  private val toRegister: Seq[Class[_]] = Seq(
    ByteBuffer.allocate(1).getClass,
    classOf[Array[ByteBuffer]],
    classOf[StorageLevel],
    classOf[CompressedMapStatus],
    classOf[HighlyCompressedMapStatus],
    classOf[ChunkedByteBuffer],
    classOf[CompactBuffer[_]],
    classOf[BlockManagerId],
    classOf[Array[Boolean]],
    classOf[Array[Byte]],
    classOf[Array[Short]],
    classOf[Array[Int]],
    classOf[Array[Long]],
    classOf[Array[Float]],
    classOf[Array[Double]],
    classOf[Array[Char]],
    classOf[Array[String]],
    classOf[Array[Array[String]]],
    classOf[BoundedPriorityQueue[_]],
    classOf[SparkConf],
    classOf[TaskCommitMessage],
    classOf[SerializedLambda],
    classOf[BitSet]
  )

  private val toRegisterSerializer = Map[Class[_], KryoClassSerializer[_]](
    classOf[RoaringBitmap] -> new KryoClassSerializer[RoaringBitmap]() {
      override def write(kryo: Kryo, output: KryoOutput, bitmap: RoaringBitmap): Unit = {
        bitmap.serialize(new KryoOutputObjectOutputBridge(kryo, output))
      }
      override def read(kryo: Kryo, input: KryoInput, cls: Class[RoaringBitmap]): RoaringBitmap = {
        val ret = new RoaringBitmap
        ret.deserialize(new KryoInputObjectInputBridge(kryo, input))
        ret
      }
    }
  )

  // classForName() is expensive in case the class is not found, so we filter the list of
  // SQL / ML / MLlib classes once and then re-use that filtered list in newInstance() calls.
  private lazy val loadableSparkClasses: Seq[Class[_]] = {
    Seq(
      "org.apache.spark.sql.catalyst.expressions.BoundReference",
      "org.apache.spark.sql.catalyst.expressions.SortOrder",
      "[Lorg.apache.spark.sql.catalyst.expressions.SortOrder;",
      "org.apache.spark.sql.catalyst.expressions.GenericInternalRow",
      "org.apache.spark.sql.catalyst.InternalRow",
      "org.apache.spark.sql.catalyst.InternalRow$",
      "[Lorg.apache.spark.sql.catalyst.InternalRow;",
      "org.apache.spark.sql.catalyst.expressions.UnsafeRow",
      "org.apache.spark.sql.catalyst.expressions.UnsafeArrayData",
      "org.apache.spark.sql.catalyst.expressions.UnsafeMapData",
      "org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering",
      "org.apache.spark.sql.catalyst.expressions.Ascending$",
      "org.apache.spark.sql.catalyst.expressions.NullsFirst$",
      "org.apache.spark.sql.catalyst.trees.Origin",
      "org.apache.spark.sql.types.IntegerType",
      "org.apache.spark.sql.types.IntegerType$",
      "org.apache.spark.sql.types.LongType$",
      "org.apache.spark.sql.types.DoubleType",
      "org.apache.spark.sql.types.DoubleType$",
      "org.apache.spark.sql.types.Metadata",
      "org.apache.spark.sql.types.StringType$",
      "org.apache.spark.sql.types.StructField",
      "[Lorg.apache.spark.sql.types.StructField;",
      "org.apache.spark.sql.types.StructType",
      "[Lorg.apache.spark.sql.types.StructType;",
      "org.apache.spark.sql.types.DateType$",
      "org.apache.spark.sql.types.DecimalType",
      "org.apache.spark.sql.types.Decimal$DecimalAsIfIntegral$",
      "org.apache.spark.sql.types.Decimal$DecimalIsFractional$",
      "org.apache.spark.sql.execution.command.PartitionStatistics",
      "org.apache.spark.sql.execution.datasources.BasicWriteTaskStats",
      "org.apache.spark.sql.execution.datasources.ExecutedWriteSummary",
      "org.apache.spark.sql.execution.datasources.WriteTaskResult",
      "org.apache.spark.sql.execution.datasources.v2.DataWritingSparkTaskResult",
      "org.apache.spark.sql.execution.joins.EmptyHashedRelation$",
      "org.apache.spark.sql.execution.joins.LongHashedRelation",
      "org.apache.spark.sql.execution.joins.LongToUnsafeRowMap",
      "org.apache.spark.sql.execution.joins.UnsafeHashedRelation",
      "org.apache.spark.sql.columnar.CachedBatch",
      "org.apache.spark.sql.columnar.SimpleMetricsCachedBatch",
      "org.apache.spark.sql.execution.columnar.DefaultCachedBatch",
      "org.apache.spark.sql.columnar.CachedBatchSerializer",
      "org.apache.spark.sql.columnar.SimpleMetricsCachedBatchSerializer",
      "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer",

      "org.apache.spark.ml.attribute.Attribute",
      "org.apache.spark.ml.attribute.AttributeGroup",
      "org.apache.spark.ml.attribute.BinaryAttribute",
      "org.apache.spark.ml.attribute.NominalAttribute",
      "org.apache.spark.ml.attribute.NumericAttribute",

      "org.apache.spark.ml.feature.Instance",
      "org.apache.spark.ml.feature.InstanceBlock",
      "org.apache.spark.ml.feature.LabeledPoint",
      "org.apache.spark.ml.feature.OffsetInstance",
      "org.apache.spark.ml.linalg.DenseMatrix",
      "org.apache.spark.ml.linalg.DenseVector",
      "org.apache.spark.ml.linalg.Matrix",
      "org.apache.spark.ml.linalg.SparseMatrix",
      "org.apache.spark.ml.linalg.SparseVector",
      "org.apache.spark.ml.linalg.Vector",
      "org.apache.spark.ml.stat.distribution.MultivariateGaussian",
      "org.apache.spark.ml.tree.impl.TreePoint",
      "org.apache.spark.mllib.clustering.VectorWithNorm",
      "org.apache.spark.mllib.linalg.DenseMatrix",
      "org.apache.spark.mllib.linalg.DenseVector",
      "org.apache.spark.mllib.linalg.Matrix",
      "org.apache.spark.mllib.linalg.SparseMatrix",
      "org.apache.spark.mllib.linalg.SparseVector",
      "org.apache.spark.mllib.linalg.Vector",
      "org.apache.spark.mllib.regression.LabeledPoint",
      "org.apache.spark.mllib.stat.distribution.MultivariateGaussian"
    ).flatMap { name =>
      try {
        Some[Class[_]](Utils.classForName(name))
      } catch {
        case NonFatal(_) => None // do nothing
        case _: NoClassDefFoundError if Utils.isTesting => None // See SPARK-23422.
      }
    }
  }
}

/**
 * KryoInputObjectInputBridge - Kryo 输入流桥接器
 * 
 * 用途：将 KryoInput 包装为 ObjectInput 接口
 * 
 * 使用场景：
 * - API 需要 ObjectInput，但想使用 Kryo
 * - 例如：RoaringBitmap.deserialize(ObjectInput)
 * 
 * 实现：转发所有方法到 KryoInput（readObject 使用 kryo.readClassAndObject）
 */
private[spark] class KryoInputObjectInputBridge(
    kryo: Kryo, input: KryoInput) extends FilterInputStream(input) with ObjectInput {
  override def readLong(): Long = input.readLong()
  override def readChar(): Char = input.readChar()
  override def readFloat(): Float = input.readFloat()
  override def readByte(): Byte = input.readByte()
  override def readShort(): Short = input.readShort()
  override def readUTF(): String = input.readString() // Kryo 的 readString 使用 UTF-8
  override def readInt(): Int = input.readInt()
  override def readUnsignedShort(): Int = input.readShortUnsigned()
  override def skipBytes(n: Int): Int = {
    input.skip(n)
    n
  }
  override def readFully(b: Array[Byte]): Unit = input.read(b)
  override def readFully(b: Array[Byte], off: Int, len: Int): Unit = input.read(b, off, len)
  override def readLine(): String = throw new UnsupportedOperationException("readLine")
  override def readBoolean(): Boolean = input.readBoolean()
  override def readUnsignedByte(): Int = input.readByteUnsigned()
  override def readDouble(): Double = input.readDouble()
  override def readObject(): AnyRef = kryo.readClassAndObject(input)
}

/**
 * KryoOutputObjectOutputBridge - Kryo 输出流桥接器
 * 
 * 用途：将 KryoOutput 包装为 ObjectOutput 接口
 * 
 * 使用场景：
 * - API 需要 ObjectOutput，但想使用 Kryo
 * - 例如：RoaringBitmap.serialize(ObjectOutput)
 * 
 * 实现：转发所有方法到 KryoOutput（writeObject 使用 kryo.writeClassAndObject）
 * 
 * 注意：writeChars 不支持（没有对应的 readChars）
 */
private[spark] class KryoOutputObjectOutputBridge(
    kryo: Kryo, output: KryoOutput) extends FilterOutputStream(output) with ObjectOutput  {
  override def writeFloat(v: Float): Unit = output.writeFloat(v)
  // 没有 "readChars" 对应方法（除了 readLine，但不支持）
  override def writeChars(s: String): Unit = throw new UnsupportedOperationException("writeChars")
  override def writeDouble(v: Double): Unit = output.writeDouble(v)
  override def writeUTF(s: String): Unit = output.writeString(s) // Kryo 的 writeString 使用 UTF-8
  override def writeShort(v: Int): Unit = output.writeShort(v)
  override def writeInt(v: Int): Unit = output.writeInt(v)
  override def writeBoolean(v: Boolean): Unit = output.writeBoolean(v)
  override def write(b: Int): Unit = output.write(b)
  override def write(b: Array[Byte]): Unit = output.write(b)
  override def write(b: Array[Byte], off: Int, len: Int): Unit = output.write(b, off, len)
  override def writeBytes(s: String): Unit = output.writeString(s)
  override def writeChar(v: Int): Unit = output.writeChar(v.toChar)
  override def writeLong(v: Long): Unit = output.writeLong(v)
  override def writeByte(v: Int): Unit = output.writeByte(v)
  override def writeObject(obj: AnyRef): Unit = kryo.writeClassAndObject(output, obj)
}

/**
 * JavaIterableWrapperSerializer - Java Iterable 包装器序列化器
 * 
 * 问题：
 * - scala.collection.asJava 返回 scala.collection.convert.Wrappers$IterableWrapper
 * - Kryo 反序列化为 AbstractCollection（行为不正确）
 * 
 * 解决方案：
 * - 写入时：如果是 IterableWrapper，提取底层 Scala Iterable 并序列化
 * - 读取时：反序列化后用 asJava 重新包装
 * 
 * 效果：避免 Kryo 的默认 AbstractCollection 序列化器
 */
private class JavaIterableWrapperSerializer
  extends com.esotericsoftware.kryo.Serializer[java.lang.Iterable[_]] {

  import JavaIterableWrapperSerializer._

  override def write(kryo: Kryo, out: KryoOutput, obj: java.lang.Iterable[_]): Unit = {
    // 如果是包装器且有 underlying 方法，序列化底层 Scala Iterable
    if (obj.getClass == wrapperClass && underlyingMethodOpt.isDefined) {
      kryo.writeClassAndObject(out, underlyingMethodOpt.get.invoke(obj))
    } else {
      kryo.writeClassAndObject(out, obj)
    }
  }

  override def read(kryo: Kryo, in: KryoInput, clz: Class[java.lang.Iterable[_]])
    : java.lang.Iterable[_] = {
    kryo.readClassAndObject(in) match {
      case scalaIterable: Iterable[_] => scalaIterable.asJava
      case javaIterable: java.lang.Iterable[_] => javaIterable
    }
  }
}

private object JavaIterableWrapperSerializer extends Logging {
  // CollectionConverters.asJava 返回的包装器类
  // (scala.collection.convert.Wrappers$IterableWrapper)
  import scala.jdk.CollectionConverters._
  val wrapperClass = Seq(1).asJava.getClass

  // 获取 underlying 方法，用于提取底层 Scala 集合
  private val underlyingMethodOpt = {
    try Some(wrapperClass.getDeclaredMethod("underlying")) catch {
      case e: Exception =>
        logError(log"Failed to find the underlying field in ${MDC(CLASS_NAME, wrapperClass)}", e)
        None
    }
  }
}
