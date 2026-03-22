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

package org.apache.spark.executor

import java.io.{ByteArrayOutputStream, FileNotFoundException, FilterInputStream, InputStream}
import java.net.{URI, URL, URLEncoder}
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets.UTF_8

import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.xbean.asm9.{ClassReader, ClassVisitor, ClassWriter, MethodVisitor, Opcodes}

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.util.ParentClassLoader

/**
 * 文件说明: Executor端自定义类加载器，用于从远程（Driver端或HDFS）加载动态生成的类文件
 *
 * 核心用途：主要为Spark REPL（交互式命令行）场景提供类加载能力，加载Driver端动态生成的类
 * 支持配置用户类路径优先加载策略，适配动态代码执行场景。
 */
/**
 * Executor 端的类加载器，用于从 Hadoop 文件系统或 Spark RPC 端点读取类文件。
 * 
 * 此类加载器主要用于 REPL（交互式命令行）场景，加载解释器动态定义的类。
 * 允许用户指定是否优先加载用户类路径中的类。
 *
 * == 资源加载策略 ==
 * 此类加载器将获取资源的操作委托给父加载器，这是合理的，因为 REPL 不会动态生成资源。
 * 但有一个例外：当以资源流的形式获取 Class 文件时，我们会尝试用与加载类相同的方式获取，
 * 以便能够获取到 REPL 动态生成的类文件。
 *
 * == 父类加载器设置 ==
 * 注意：[[ClassLoader]] 会优先从父加载器加载类。只有当父加载器为 null 或加载失败时，
 * 才会调用重写的 `findClass` 函数。为了避免使用不当的类加载器加载类可能导致的潜在问题，
 * 我们将 ClassLoader 的父加载器设置为 null，这样可以完全控制使用哪个类加载器。
 * 详细讨论见 SPARK-18646。
 *
 * @param conf Spark 配置
 * @param env SparkEnv 环境
 * @param classUri 类文件的 URI（可以是 HDFS 路径或 spark:// 协议）
 * @param parent 父类加载器
 * @param userClassPathFirst 是否优先加载用户类路径中的类
 */
class ExecutorClassLoader(
    conf: SparkConf,
    env: SparkEnv,
    classUri: String,
    parent: ClassLoader,
    userClassPathFirst: Boolean) extends ClassLoader(null) with Logging {
  // 解析类文件的基础URI
  val uri = new URI(classUri)
  // 获取类文件所在目录路径
  val directory = uri.getPath

  // 封装父类加载器，提供统一的加载接口
  val parentLoader = new ParentClassLoader(parent)

  // 根据URI协议选择类文件获取函数：spark协议走RPC，其他协议走Hadoop文件系统
  private val fetchFn: (String) => InputStream = uri.getScheme() match {
    case "spark" => getClassFileInputStreamFromSparkRPC
    case _ =>
      val fileSystem = FileSystem.get(uri, SparkHadoopUtil.get.newConfiguration(conf))
      getClassFileInputStreamFromFileSystem(fileSystem)
  }

  /** 获取资源，委托给父加载器处理 */
  override def getResource(name: String): URL = {
    parentLoader.getResource(name)
  }

  /** 获取所有同名资源，委托给父加载器处理 */
  override def getResources(name: String): java.util.Enumeration[URL] = {
    parentLoader.getResources(name)
  }

  /**
   * 以输入流方式获取资源。
   * 根据userClassPathFirst配置决定加载顺序，类文件会尝试从远程动态位置加载。
   */
  override def getResourceAsStream(name: String): InputStream = {
    if (userClassPathFirst) {
      val res = getClassResourceAsStreamLocally(name)
      if (res != null) res else parentLoader.getResourceAsStream(name)
    } else {
      val res = parentLoader.getResourceAsStream(name)
      if (res != null) res else getClassResourceAsStreamLocally(name)
    }
  }

  /**
   * 尝试从本地动态位置加载类文件输入流，仅处理.class后缀的类文件。
   * @return 类文件输入流，找不到返回null
   */
  private def getClassResourceAsStreamLocally(name: String): InputStream = {
    try {
      if (name.endsWith(".class")) fetchFn(name) else null
    } catch {
      // 捕获类找不到异常，遵循ClassLoader规范返回null
      case _: ClassNotFoundException => null
    }
  }

  /**
   * 根据类名查找类，按照userClassPathFirst配置决定加载优先级。
   * @param name 待加载类名
   * @return 加载完成的Class对象
   */
  override def findClass(name: String): Class[_] = {
    if (userClassPathFirst) {
      // 用户类优先：先尝试从远程动态位置加载，失败后回退到父加载器
      findClassLocally(name).getOrElse(parentLoader.loadClass(name))
    } else {
      // 父加载器优先：先尝试父加载器加载，失败后尝试远程动态加载
      try {
        parentLoader.loadClass(name)
      } catch {
        case e: ClassNotFoundException =>
          val classOption = try {
            findClassLocally(name)
          } catch {
            case e: RemoteClassLoaderError =>
              throw e
            case NonFatal(e) =>
              // 包装加载异常，保留类名信息
              // scalastyle:off throwerror
              throw new RemoteClassLoaderError(name, e)
              // scalastyle:on throwerror
          }
          classOption match {
            case None => throw new ClassNotFoundException(name, e)
            case Some(a) => a
          }
      }
    }
  }

  // 匹配Spark RPC端"流未找到"错误的正则表达式
  private val STREAM_NOT_FOUND_REGEX = s"Stream '.*' was not found.".r.pattern

  /**
   * 通过Spark RPC从Driver端获取类文件输入流。
   * 适配spark://协议，将RPC流异常转换为类加载器规范的异常。
   * @param path 类文件路径
   * @return 类文件输入流
   */
  private def getClassFileInputStreamFromSparkRPC(path: String): InputStream = {
    // 通过RPC环境打开到Driver的传输通道
    val channel = env.rpcEnv.openChannel(s"$classUri/${urlEncode(path)}")
    // 包装输入流，转换RPC特定异常为标准类加载异常
    new FilterInputStream(Channels.newInputStream(channel)) {

      override def read(): Int = toClassNotFound(super.read())

      override def read(b: Array[Byte], offset: Int, len: Int) =
        toClassNotFound(super.read(b, offset, len))

      // 将RPC的"流未找到"异常转换为ClassNotFoundException
      private def toClassNotFound(fn: => Int): Int = {
        try {
          fn
        } catch {
          case e: RuntimeException if e.getMessage != null
            && STREAM_NOT_FOUND_REGEX.matcher(e.getMessage).matches() =>
            // Driver明确返回流不存在，转换为标准类找不到异常
            throw new ClassNotFoundException(path, e)
          case NonFatal(e) =>
            // 其他异常封装为RemoteClassLoaderError抛出
            // scalastyle:off throwerror
            throw new RemoteClassLoaderError(path, e)
            // scalastyle:on throwerror
        }
      }
    }
  }

  /**
   * 从Hadoop文件系统获取类文件输入流，适配HDFS等存储协议。
   * @param fileSystem Hadoop文件系统实例
   * @return 类文件输入流
   */
  private def getClassFileInputStreamFromFileSystem(fileSystem: FileSystem)(
      pathInDirectory: String): InputStream = {
    val path = new Path(directory, pathInDirectory)
    try {
      fileSystem.open(path)
    } catch {
      case _: FileNotFoundException =>
        throw new ClassNotFoundException(s"Class file not found at path $path")
    }
  }

  /**
   * 在本地（远程源）查找并加载类。
   * 从远程获取类字节码，转换后定义为JVM中的Class对象。
   * @param name 待加载类名
   * @return 加载成功返回Some(Class)，找不到返回None
   */
  def findClassLocally(name: String): Option[Class[_]] = {
    val pathInDirectory = name.replace('.', '/') + ".class"
    var inputStream: InputStream = null
    try {
      inputStream = fetchFn(pathInDirectory)
      val bytes = readAndTransformClass(name, inputStream)
      Some(defineClass(name, bytes, 0, bytes.length))
    } catch {
      case e: ClassNotFoundException =>
        // 未找到类，返回None
        logDebug(s"Did not load class $name from REPL class server at $uri", e)
        None
      case e: Exception =>
        // 加载过程发生异常，根据策略决定返回None还是抛出异常
        logError(log"Failed to check existence of class ${MDC(LogKeys.CLASS_NAME, name)} " +
          log"on REPL class server at ${MDC(LogKeys.URI, uri)}", e)
        if (userClassPathFirst) {
          // 用户类优先模式下，允许回退到父加载器，返回None
          None
        } else {
          throw e
        }
    } finally {
      // 确保输入流关闭
      if (inputStream != null) {
        try {
          inputStream.close()
        } catch {
          case e: Exception =>
            logError("Exception while closing inputStream", e)
        }
      }
    }
  }

  /**
   * 读取并转换类字节码。
   * 对REPL生成的包装类（存储val/var的iw类）进行字节码修改，清除原有构造函数的初始化逻辑，
   * 实现延迟初始化；其他类保持原样返回。
   * @param name 类名
   * @param in 类字节输入流
   * @return 转换后的字节码数组
   */
  def readAndTransformClass(name: String, in: InputStream): Array[Byte] = {
    if (name.startsWith("line") && name.endsWith("$iw$")) {
      // REPL生成的交互式包装类，需要清理构造函数
      val cr = new ClassReader(in)
      val cw = new ClassWriter(
        ClassWriter.COMPUTE_FRAMES + ClassWriter.COMPUTE_MAXS)
      val cleaner = new ConstructorCleaner(name, cw)
      cr.accept(cleaner, 0)
      cw.toByteArray
    } else {
      // 非包装类，直接读取字节码返回
      val bos = new ByteArrayOutputStream
      val bytes = new Array[Byte](4096)
      var done = false
      while (!done) {
        val num = in.read(bytes)
        if (num >= 0) {
          bos.write(bytes, 0, num)
        } else {
          done = true
        }
      }
      bos.toByteArray
    }
  }

  /**
   * 对路径进行URL编码，保留斜杠不编码以保证路径结构正确。
   * @param str 待编码路径字符串
   * @return 编码后的字符串
   */
  def urlEncode(str: String): String = {
    str.split('/').map(part => URLEncoder.encode(part, UTF_8.name())).mkString("/")
  }
}

/**
 * ASM字节码访问器，用于清理REPL包装类的构造函数。
 * REPL生成的包装类（存储val/var）构造函数包含初始化代码，需要替换为空构造函数，
 * 只调用父类Object的构造函数，实现延迟初始化，避免在Executor端提前执行初始化逻辑。
 */
class ConstructorCleaner(className: String, cv: ClassVisitor)
extends ClassVisitor(Opcodes.ASM9, cv) {
  /**
   * 访问类方法，对非静态构造函数进行替换处理。
   */
  override def visitMethod(access: Int, name: String, desc: String,
      sig: String, exceptions: Array[String]): MethodVisitor = {
    val mv = cv.visitMethod(access, name, desc, sig, exceptions)
    // 识别目标：非静态构造函数<init>
    if (name == "<init>" && (access & Opcodes.ACC_STATIC) == 0) {
      // 生成空构造函数字节码，仅调用父类Object构造函数
      mv.visitCode()
      mv.visitVarInsn(Opcodes.ALOAD, 0) // 加载this引用
      mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false)
      mv.visitVarInsn(Opcodes.ALOAD, 0) // 加载this引用
      mv.visitInsn(Opcodes.RETURN)
      mv.visitMaxs(-1, -1) // 自动计算栈帧和局部变量大小
      mv.visitEnd()
      null // 返回null，不处理原构造函数字节码
    } else {
      // 其他方法保持不变
      mv
    }
  }
}

/**
 * 远程类加载异常，用于包装远程加载过程中发生的非类找不到异常。
 * 此异常既不是LinkageError也不是ClassNotFoundException，JVM会在后续重新尝试加载类，
 * 处理不确定类是否存在的异常场景。
 *
 * @param className 加载失败的类名
 * @param cause 原始异常
 */
private[executor] class RemoteClassLoaderError(className: String, cause: Throwable)
  extends Error(className, cause)