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
  // 解析类 URI
  val uri = new URI(classUri)
  // 类文件所在的目录路径
  val directory = uri.getPath

  // 父类加载器的包装器
  val parentLoader = new ParentClassLoader(parent)

  // 根据 URI 协议选择获取类文件的方法
  // spark:// 协议使用 RPC 方式获取，其他协议使用文件系统方式获取
  private val fetchFn: (String) => InputStream = uri.getScheme() match {
    case "spark" => getClassFileInputStreamFromSparkRPC
    case _ =>
      val fileSystem = FileSystem.get(uri, SparkHadoopUtil.get.newConfiguration(conf))
      getClassFileInputStreamFromFileSystem(fileSystem)
  }

  /** 获取资源，委托给父加载器 */
  override def getResource(name: String): URL = {
    parentLoader.getResource(name)
  }

  /** 获取所有同名资源，委托给父加载器 */
  override def getResources(name: String): java.util.Enumeration[URL] = {
    parentLoader.getResources(name)
  }

  /**
   * 以流的形式获取资源。
   * 对于类文件（.class），会尝试从本地（REPL 生成的类）获取。
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
   * 尝试从本地获取类文件资源流。
   * 类文件可以由 REPL 动态生成，此加载器允许加载这些文件用于类加载以外的目的。
   */
  private def getClassResourceAsStreamLocally(name: String): InputStream = {
    try {
      // 只处理 .class 文件
      if (name.endsWith(".class")) fetchFn(name) else null
    } catch {
      // fetchFn 引用的辅助函数使用 CNFE 表示获取类失败
      // 这与 IOException 的预期用途匹配
      // ClassLoader.getResourceAsStream() 捕获 IOException 并返回 null
      // 所以我们遵循该模式在这里处理 CNFE
      case _: ClassNotFoundException => null
    }
  }

  /**
   * 查找并加载类。
   * 根据 userClassPathFirst 配置决定加载顺序。
   */
  override def findClass(name: String): Class[_] = {
    if (userClassPathFirst) {
      // 用户类优先：先尝试本地加载，失败后使用父加载器
      findClassLocally(name).getOrElse(parentLoader.loadClass(name))
    } else {
      // 父类优先：先尝试父加载器，失败后尝试本地加载
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
              // 包装错误以包含类名
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

  // 用于匹配"流未找到"错误消息的正则表达式
  // 参见 org.apache.spark.network.server.TransportRequestHandler.processStreamRequest
  private val STREAM_NOT_FOUND_REGEX = s"Stream '.*' was not found.".r.pattern

  /**
   * 通过 Spark RPC 从 Driver 获取类文件输入流。
   * 用于 spark:// 协议。
   */
  private def getClassFileInputStreamFromSparkRPC(path: String): InputStream = {
    // 通过 RPC 打开与 Driver 的通道获取类文件
    val channel = env.rpcEnv.openChannel(s"$classUri/${urlEncode(path)}")
    // 返回包装后的输入流，将 RPC 错误转换为 ClassNotFoundException
    new FilterInputStream(Channels.newInputStream(channel)) {

      override def read(): Int = toClassNotFound(super.read())

      override def read(b: Array[Byte], offset: Int, len: Int) =
        toClassNotFound(super.read(b, offset, len))

      // 将流未找到错误转换为 ClassNotFoundException
      private def toClassNotFound(fn: => Int): Int = {
        try {
          fn
        } catch {
          case e: RuntimeException if e.getMessage != null
            && STREAM_NOT_FOUND_REGEX.matcher(e.getMessage).matches() =>
            // 将流未找到错误转换为 ClassNotFoundException
            // Driver 发送此明确确认告诉我们类不存在
            throw new ClassNotFoundException(path, e)
          case NonFatal(e) =>
            // scalastyle:off throwerror
            throw new RemoteClassLoaderError(path, e)
            // scalastyle:on throwerror
        }
      }
    }
  }

  /**
   * 从 Hadoop 文件系统获取类文件输入流。
   * 用于 HDFS 等文件系统协议。
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
   * 在本地查找并加载类。
   * 从远程源获取类字节码，可选地进行转换，然后定义类。
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
        // We did not find the class
        logDebug(s"Did not load class $name from REPL class server at $uri", e)
        None
      case e: Exception =>
        // Something bad happened while checking if the class exists
        logError(log"Failed to check existence of class ${MDC(LogKeys.CLASS_NAME, name)} " +
          log"on REPL class server at ${MDC(LogKeys.URI, uri)}", e)
        if (userClassPathFirst) {
          // Allow to try to load from "parentLoader"
          None
        } else {
          throw e
        }
    } finally {
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
   * 读取并可选地转换类字节码。
   * 对于 REPL 生成的"包装"对象（存储 val 或 var 的类），
   * 需要替换其构造函数以避免执行 REPL 放置的初始化代码。
   * val 或 var 将在任务中使用时通过反射延迟初始化。
   */
  def readAndTransformClass(name: String, in: InputStream): Array[Byte] = {
    if (name.startsWith("line") && name.endsWith("$iw$")) {
      // 类似乎是解释器的"包装"对象，存储 val 或 var
      // 使用不执行 REPL 放置的初始化代码的空构造函数替换原构造函数
      val cr = new ClassReader(in)
      val cw = new ClassWriter(
        ClassWriter.COMPUTE_FRAMES + ClassWriter.COMPUTE_MAXS)
      val cleaner = new ConstructorCleaner(name, cw)
      cr.accept(cleaner, 0)
      cw.toByteArray
    } else {
      // 原样传递类，不做修改
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
   * URL 编码字符串，但保留斜杠不编码。
   */
  def urlEncode(str: String): String = {
    str.split('/').map(part => URLEncoder.encode(part, UTF_8.name())).mkString("/")
  }
}

/**
 * 构造函数清理器 - 用于清理 REPL 生成的包装类的构造函数。
 * 
 * 在 REPL 中定义的 val/var 会被包装在特殊的对象中，
 * 其构造函数包含初始化代码。为了延迟初始化（在任务中使用时才初始化），
 * 需要将构造函数替换为只调用 Object 构造函数的空实现。
 */
class ConstructorCleaner(className: String, cv: ClassVisitor)
extends ClassVisitor(Opcodes.ASM9, cv) {
  /**
   * 访问类的方法。对于构造函数，生成一个空的替代实现。
   */
  override def visitMethod(access: Int, name: String, desc: String,
      sig: String, exceptions: Array[String]): MethodVisitor = {
    val mv = cv.visitMethod(access, name, desc, sig, exceptions)
    // 检查是否是非静态构造函数
    if (name == "<init>" && (access & Opcodes.ACC_STATIC) == 0) {
      // 这是构造函数，需要清理它
      // 输出创建对象并设置静态 MODULE$ 字段的指令，但不做其他操作
      mv.visitCode()
      mv.visitVarInsn(Opcodes.ALOAD, 0) // 加载 this
      // 调用父类 Object 的构造函数
      mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false)
      mv.visitVarInsn(Opcodes.ALOAD, 0) // 加载 this
      // val classType = className.replace('.', '/')
      // mv.visitFieldInsn(Opcodes.PUTSTATIC, classType, "MODULE$", "L" + classType + ";")
      mv.visitInsn(Opcodes.RETURN)
      mv.visitMaxs(-1, -1) // 栈大小和局部变量将自动计算
      mv.visitEnd()
      null // 返回 null 表示不再需要原始方法访问器
    } else {
      mv // 非构造函数方法原样返回
    }
  }
}

/**
 * 远程类加载错误。
 * 当因异常无法加载类时抛出。我们不知道此类是否存在，
 * 所以抛出一个既不是 [[LinkageError]] 也不是 [[ClassNotFoundException]] 的特殊异常，
 * 让 JVM 稍后重试加载此类。
 *
 * @param className 尝试加载的类名
 * @param cause 原始异常
 */
private[executor] class RemoteClassLoaderError(className: String, cause: Throwable)
  extends Error(className, cause)
