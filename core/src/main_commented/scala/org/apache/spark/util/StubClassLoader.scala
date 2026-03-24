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
package org.apache.spark.util

import org.apache.xbean.asm9.{ClassWriter, Opcodes}

import org.apache.spark.internal.Logging

/**
 * 存根类加载器，用于在找不到指定类时生成占位存根替代缺失类，仅对标记为需要存根化的类生效
 * 
 * 主要用于加载lambda表达式场景：当lambda捕获的类包含未知且不需要实际使用的类时，
 * lambda本身执行不依赖该缺失类，因此可以安全地用存根替代，避免类加载失败
 * 
 * @param parent 父类加载器，委托实际类加载
 * @param shouldStub 判断给定类全限定名是否需要生成存根的断言函数
 */
private[spark] class StubClassLoader(parent: ClassLoader, shouldStub: String => Boolean)
  extends ClassLoader(parent) with Logging {

  /**
   * 查找并加载类，如果类需要存根化则动态生成存根类返回
   * @param name 要加载的类全限定名
   * @return 加载完成的类对象，要么是父加载器结果，要么是生成的存根类
   * @throws ClassNotFoundException 当类不需要存根化且找不到时抛出
   */
  override def findClass(name: String): Class[_] = {
    if (!shouldStub(name)) {
      throw new ClassNotFoundException(name)
    }
    logDebug(s"Generating stub for $name")
    val bytes = StubClassLoader.generateStub(name)
    defineClass(name, bytes, 0, bytes.length)
  }
}

/**
 * 存根类加载器的伴生对象，提供工厂方法和存根类字节码生成逻辑
 */
private[spark] object StubClassLoader {
  /**
   * 基于指定前缀列表创建存根类加载器，所有以指定前缀开头的类都会生成存根
   * @param parent 父类加载器
   * @param binaryName 需要生成存根的类前缀列表（二进制格式全限定名）
   * @return 配置完成的存根类加载器实例
   */
  def apply(parent: ClassLoader, binaryName: Seq[String]): StubClassLoader = {
    new StubClassLoader(parent, name => binaryName.exists(p => name.startsWith(p)))
  }

  /**
   * 使用ASM字节码生成工具动态生成缺失类的存根字节码，存根类构造方法会直接抛出异常
   * @param binaryName 要生成存根的类二进制全限定名
   * @return 生成好的存根类字节数组
   */
  def generateStub(binaryName: String): Array[Byte] = {
    // 将点分隔的全限定名转换为JVM内部斜杠分隔格式
    val name = binaryName.replace('.', '/')
    val classWriter = new ClassWriter(0)
    // 访问类头，声明版本、访问标志、类名、父类为Object
    classWriter.visit(
      49,
      Opcodes.ACC_PUBLIC + Opcodes.ACC_SUPER,
      name,
      null,
      "java/lang/Object",
      null)
    // 设置源码信息
    classWriter.visitSource(name + ".java", null)

    // 生成公共无参构造方法
    val ctorWriter = classWriter.visitMethod(
      Opcodes.ACC_PUBLIC,
      "<init>",
      "()V",
      null,
      null)
    // 加载this引用到操作数栈
    ctorWriter.visitVarInsn(Opcodes.ALOAD, 0)
    // 调用父类Object的构造方法
    ctorWriter.visitMethodInsn(
      Opcodes.INVOKESPECIAL,
      "java/lang/Object",
      "<init>",
      "()V",
      false)

    val internalException: String = "java/lang/ClassNotFoundException"
    // 创建ClassNotFoundException实例
    ctorWriter.visitTypeInsn(Opcodes.NEW, internalException)
    // 复制引用，为调用构造方法和抛出做准备
    ctorWriter.visitInsn(Opcodes.DUP)
    // 加载异常提示信息常量
    ctorWriter.visitLdcInsn(
      s"Fail to initiate the class $binaryName because it is stubbed. " +
        "Please install the artifact of the missing class by calling session.addArtifact.")
    // 调用异常构造方法初始化
    ctorWriter.visitMethodInsn(
      Opcodes.INVOKESPECIAL,
      internalException,
      "<init>",
      "(Ljava/lang/String;)V",
      false)

    // 抛出初始化异常
    ctorWriter.visitInsn(Opcodes.ATHROW)
    // 设置栈帧和局部变量大小
    ctorWriter.visitMaxs(3, 3)
    // 结束构造方法生成
    ctorWriter.visitEnd()
    // 结束类生成
    classWriter.visitEnd()
    // 转换为字节数组返回
    classWriter.toByteArray
  }
}