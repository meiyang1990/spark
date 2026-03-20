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

package org.apache.spark

import java.io.File
import java.security.NoSuchAlgorithmException
import java.util.HashMap
import java.util.Map
import javax.net.ssl.SSLContext

import org.apache.hadoop.conf.Configuration
import org.eclipse.jetty.util.ssl.SslContextFactory

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.ConfigProvider
import org.apache.spark.network.util.MapConfigProvider

/**
 * SSL 配置选项的通用容器类。
 * 提供方法来生成适用于不同通信协议（如 Jetty、Netty）的 SSL 配置对象。
 * 旨在提供协议所支持的最大公共 SSL 设置集合。
 *
 * @param namespace           配置命名空间（如 spark.ssl.rpc）
 * @param enabled             是否启用 SSL；为 false 时忽略其余设置
 * @param port                SSL 服务器绑定端口
 * @param keyStore            密钥库文件路径
 * @param keyStorePassword    密钥库访问密码
 * @param privateKey          PKCS#8 PEM 格式的私钥文件
 * @param privateKeyPassword  私钥文件密码
 * @param keyPassword         密钥库中私钥的访问密码
 * @param keyStoreType        密钥库类型
 * @param needClientAuth      是否需要客户端证书认证（mTLS）
 * @param certChain           PEM 格式的 X.509 证书链文件
 * @param trustStore          信任库文件路径
 * @param trustStorePassword  信任库访问密码
 * @param trustStoreType      信任库类型
 * @param trustStoreReloadingEnabled 是否启用信任库自动重加载
 * @param trustStoreReloadIntervalMs 信任库重加载间隔（毫秒）
 * @param openSslEnabled      是否启用 OpenSSL 实现（需提供 certChain 和 keyFile）
 * @param protocol            SSL 协议名称
 * @param enabledAlgorithms   允许使用的加密算法集合
 */
private[spark] case class SSLOptions(
    namespace: Option[String] = None,
    enabled: Boolean = false,
    port: Option[Int] = None,
    keyStore: Option[File] = None,
    keyStorePassword: Option[String] = None,
    privateKey: Option[File] = None,
    keyPassword: Option[String] = None,
    keyStoreType: Option[String] = None,
    needClientAuth: Boolean = false,
    certChain: Option[File] = None,
    trustStore: Option[File] = None,
    trustStorePassword: Option[String] = None,
    trustStoreType: Option[String] = None,
    trustStoreReloadingEnabled: Boolean = false,
    trustStoreReloadIntervalMs: Int = 10000,
    openSslEnabled: Boolean = false,
    protocol: Option[String] = None,
    enabledAlgorithms: Set[String] = Set.empty,
    privateKeyPassword: Option[String] = None)
    extends Logging {

  /**
   * 根据当前 SSL 配置创建 Jetty SslContextFactory.Server 实例。
   * SSL 未启用时返回 None。
   */
  def createJettySslContextFactoryServer(): Option[SslContextFactory.Server] = {
    if (enabled) {
      val sslContextFactory = new SslContextFactory.Server()

      // 配置密钥库
      keyStore.foreach(file => sslContextFactory.setKeyStorePath(file.getAbsolutePath))
      keyStorePassword.foreach(sslContextFactory.setKeyStorePassword)
      keyPassword.foreach(sslContextFactory.setKeyManagerPassword)
      keyStoreType.foreach(sslContextFactory.setKeyStoreType)
      // 配置客户端证书认证（mTLS）
      if (needClientAuth) {
        trustStore.foreach(file => sslContextFactory.setTrustStorePath(file.getAbsolutePath))
        trustStorePassword.foreach(sslContextFactory.setTrustStorePassword)
        trustStoreType.foreach(sslContextFactory.setTrustStoreType)
        sslContextFactory.setNeedClientAuth(needClientAuth)

      }
      protocol.foreach(sslContextFactory.setProtocol)
      // 仅包含当前 Java 安全提供者支持的加密算法
      if (supportedAlgorithms.nonEmpty) {
        sslContextFactory.setIncludeCipherSuites(supportedAlgorithms.toSeq: _*)
      }

      Some(sslContextFactory)
    } else {
      None
    }
  }

  /**
   * 计算当前 Java 安全提供者实际支持的加密算法子集。
   * 过滤掉用户配置但不被当前环境支持的算法。
   */
  private val supportedAlgorithms: Set[String] = if (enabledAlgorithms.isEmpty) {
    Set.empty
  } else {
    var context: SSLContext = null
    if (protocol.isEmpty) {
      logDebug("No SSL protocol specified")
      context = SSLContext.getDefault
    } else {
      try {
        context = SSLContext.getInstance(protocol.get)
        /* The set of supported algorithms does not depend upon the keys, trust, or
         rng, although they will influence which algorithms are eventually used. */
        context.init(null, null, null)
      } catch {
        case nsa: NoSuchAlgorithmException =>
          logDebug(s"No support for requested SSL protocol ${protocol.get}")
          context = SSLContext.getDefault
      }
    }

    val providerAlgorithms = context.getServerSocketFactory.getSupportedCipherSuites.toSet

    // Log which algorithms we are discarding
    (enabledAlgorithms &~ providerAlgorithms).foreach { cipher =>
      logDebug(s"Discarding unsupported cipher $cipher")
    }

    val supported = enabledAlgorithms & providerAlgorithms
    require(supported.nonEmpty || sys.env.contains("SPARK_TESTING"),
      "SSLContext does not support any of the enabled algorithms: " +
        enabledAlgorithms.mkString(","))
    supported
  }

  /** 将 SSL 配置项导出为 Spark 网络层使用的 ConfigProvider（MapConfigProvider） */
  def createConfigProvider(conf: SparkConf): ConfigProvider = {
    val nsp = namespace.getOrElse("spark.ssl")
    val confMap: Map[String, String] = new HashMap[String, String]
    conf.getAll.foreach(tuple => confMap.put(tuple._1, tuple._2))
    confMap.put(s"$nsp.enabled", enabled.toString)
    confMap.put(s"$nsp.trustStoreReloadingEnabled", trustStoreReloadingEnabled.toString)
    confMap.put(s"$nsp.openSslEnabled", openSslEnabled.toString)
    confMap.put(s"$nsp.trustStoreReloadIntervalMs", trustStoreReloadIntervalMs.toString)
    keyStore.map(_.getAbsolutePath).foreach(confMap.put(s"$nsp.keyStore", _))
    keyStorePassword.foreach(confMap.put(s"$nsp.keyStorePassword", _))
    privateKey.map(_.getAbsolutePath).foreach(confMap.put(s"$nsp.privateKey", _))
    keyPassword.foreach(confMap.put(s"$nsp.keyPassword", _))
    certChain.map(_.getAbsolutePath).foreach(confMap.put(s"$nsp.certChain", _))
    trustStore.map(_.getAbsolutePath).foreach(confMap.put(s"$nsp.trustStore", _))
    trustStorePassword.foreach(confMap.put(s"$nsp.trustStorePassword", _))
    protocol.foreach(confMap.put(s"$nsp.protocol", _))
    confMap.put(s"$nsp.enabledAlgorithms", enabledAlgorithms.mkString(","))
    privateKeyPassword.foreach(confMap.put(s"$nsp.privateKeyPassword", _))

    new MapConfigProvider(confMap)
  }

  /** Returns a string representation of this SSLOptions with all the passwords masked. */
  override def toString: String = s"SSLOptions{enabled=$enabled, port=$port, " +
      s"keyStore=$keyStore, keyStorePassword=${keyStorePassword.map(_ => "xxx")}, " +
      s"privateKey=$privateKey, keyPassword=${keyPassword.map(_ => "xxx")}, " +
      s"privateKeyPassword=${privateKeyPassword.map(_ => "xxx")}, keyStoreType=$keyStoreType, " +
      s"needClientAuth=$needClientAuth, certChain=$certChain, trustStore=$trustStore, " +
      s"trustStorePassword=${trustStorePassword.map(_ => "xxx")}, " +
      s"trustStoreReloadIntervalMs=$trustStoreReloadIntervalMs, " +
      s"trustStoreReloadingEnabled=$trustStoreReloadingEnabled, openSSLEnabled=$openSslEnabled, " +
      s"protocol=$protocol, enabledAlgorithms=$enabledAlgorithms}"
}

/** SSLOptions 伴生对象，提供配置解析和环境变量常量 */
private[spark] object SSLOptions extends Logging {

  /**
   * 从 SparkConf 中解析指定命名空间的 SSL 配置。
   * 每个配置项可回退到默认 SSLOptions（如有提供）。
   * RPC 命名空间不继承默认的 enabled 设置（向后兼容）。
   *
   * @param conf 用于读取配置的 SparkConf
   * @param hadoopConf 用于读取 Hadoop 密码配置
   * @param ns 命名空间（如 spark.ssl.rpc）
   * @param defaults 可选的默认 SSLOptions
   * @return 解析后的 SSLOptions 实例
   */
  def parse(
      conf: SparkConf,
      hadoopConf: Configuration,
      ns: String,
      defaults: Option[SSLOptions] = None): SSLOptions = {

    // RPC 命名空间出于向后兼容不继承默认 enabled 值
    val enabledDefault = if (ns == "spark.ssl.rpc") {
      false
    } else {
      defaults.exists(_.enabled)
    }

    val enabled = conf.getBoolean(s"$ns.enabled", defaultValue = enabledDefault)
    if (!enabled) {
      return new SSLOptions()
    }
    val port = conf.getWithSubstitution(s"$ns.port").map(_.toInt)
    port.foreach { p =>
      require(p >= 0, "Port number must be a non-negative value.")
    }

    val keyStore = conf.getWithSubstitution(s"$ns.keyStore").map(new File(_))
        .orElse(defaults.flatMap(_.keyStore))

    val keyStorePassword = conf.getWithSubstitution(s"$ns.keyStorePassword")
        .orElse(Option(hadoopConf.getPassword(s"$ns.keyStorePassword")).map(new String(_)))
        .orElse(Option(conf.getenv(ENV_RPC_SSL_KEY_STORE_PASSWORD)).filter(_.trim.nonEmpty))
        .orElse(defaults.flatMap(_.keyStorePassword))

    val privateKey = conf.getOption(s"$ns.privateKey").map(new File(_))
        .orElse(defaults.flatMap(_.privateKey))

    val privateKeyPassword = conf.getWithSubstitution(s"$ns.privateKeyPassword")
      .orElse(Option(conf.getenv(ENV_RPC_SSL_PRIVATE_KEY_PASSWORD)).filter(_.trim.nonEmpty))
      .orElse(defaults.flatMap(_.privateKeyPassword))

    val keyPassword = conf.getWithSubstitution(s"$ns.keyPassword")
        .orElse(Option(hadoopConf.getPassword(s"$ns.keyPassword")).map(new String(_)))
        .orElse(Option(conf.getenv(ENV_RPC_SSL_KEY_PASSWORD)).filter(_.trim.nonEmpty))
        .orElse(defaults.flatMap(_.keyPassword))

    val keyStoreType = conf.getWithSubstitution(s"$ns.keyStoreType")
        .orElse(defaults.flatMap(_.keyStoreType))

    val certChain = conf.getOption(s"$ns.certChain").map(new File(_))
        .orElse(defaults.flatMap(_.certChain))

    val needClientAuth =
      conf.getBoolean(s"$ns.needClientAuth", defaultValue = defaults.exists(_.needClientAuth))

    val trustStore = conf.getWithSubstitution(s"$ns.trustStore").map(new File(_))
        .orElse(defaults.flatMap(_.trustStore))

    val trustStorePassword = conf.getWithSubstitution(s"$ns.trustStorePassword")
        .orElse(Option(hadoopConf.getPassword(s"$ns.trustStorePassword")).map(new String(_)))
        .orElse(Option(conf.getenv(ENV_RPC_SSL_TRUST_STORE_PASSWORD)).filter(_.trim.nonEmpty))
        .orElse(defaults.flatMap(_.trustStorePassword))

    val trustStoreType = conf.getWithSubstitution(s"$ns.trustStoreType")
        .orElse(defaults.flatMap(_.trustStoreType))

    val trustStoreReloadingEnabled = conf.getBoolean(s"$ns.trustStoreReloadingEnabled",
        defaultValue = defaults.exists(_.trustStoreReloadingEnabled))

    val trustStoreReloadIntervalMs = conf.getInt(s"$ns.trustStoreReloadIntervalMs",
      defaultValue = defaults.map(_.trustStoreReloadIntervalMs).getOrElse(10000))

    val openSslEnabled = conf.getBoolean(s"$ns.openSslEnabled",
        defaultValue = defaults.exists(_.openSslEnabled))

    val protocol = conf.getWithSubstitution(s"$ns.protocol")
        .orElse(defaults.flatMap(_.protocol))

    val enabledAlgorithms = conf.getWithSubstitution(s"$ns.enabledAlgorithms")
        .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSet)
        .orElse(defaults.map(_.enabledAlgorithms))
        .getOrElse(Set.empty)

    new SSLOptions(
      Some(ns),
      enabled,
      port,
      keyStore,
      keyStorePassword,
      privateKey,
      keyPassword,
      keyStoreType,
      needClientAuth,
      certChain,
      trustStore,
      trustStorePassword,
      trustStoreType,
      trustStoreReloadingEnabled,
      trustStoreReloadIntervalMs,
      openSslEnabled,
      protocol,
      enabledAlgorithms,
      privateKeyPassword)
  }

  // ===== SSL 密码的配置键和环境变量常量，用于在进程间传播密码 =====
  val SPARK_RPC_SSL_KEY_PASSWORD_CONF = "spark.ssl.rpc.keyPassword"
  val SPARK_RPC_SSL_PRIVATE_KEY_PASSWORD_CONF = "spark.ssl.rpc.privateKeyPassword"
  val SPARK_RPC_SSL_KEY_STORE_PASSWORD_CONF = "spark.ssl.rpc.keyStorePassword"
  val SPARK_RPC_SSL_TRUST_STORE_PASSWORD_CONF = "spark.ssl.rpc.trustStorePassword"
  val SPARK_RPC_SSL_PASSWORD_FIELDS: Seq[String] = Seq(
    SPARK_RPC_SSL_KEY_PASSWORD_CONF,
    SPARK_RPC_SSL_PRIVATE_KEY_PASSWORD_CONF,
    SPARK_RPC_SSL_KEY_STORE_PASSWORD_CONF,
    SPARK_RPC_SSL_TRUST_STORE_PASSWORD_CONF
  )

  val ENV_RPC_SSL_KEY_PASSWORD = "_SPARK_SSL_RPC_KEY_PASSWORD"
  val ENV_RPC_SSL_PRIVATE_KEY_PASSWORD = "_SPARK_SSL_RPC_PRIVATE_KEY_PASSWORD"
  val ENV_RPC_SSL_KEY_STORE_PASSWORD = "_SPARK_SSL_RPC_KEY_STORE_PASSWORD"
  val ENV_RPC_SSL_TRUST_STORE_PASSWORD = "_SPARK_SSL_RPC_TRUST_STORE_PASSWORD"
  val SPARK_RPC_SSL_PASSWORD_ENVS: Seq[String] = Seq(
    ENV_RPC_SSL_KEY_PASSWORD,
    ENV_RPC_SSL_PRIVATE_KEY_PASSWORD,
    ENV_RPC_SSL_KEY_STORE_PASSWORD,
    ENV_RPC_SSL_TRUST_STORE_PASSWORD
  )
}

