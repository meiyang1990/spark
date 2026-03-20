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
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.Base64

import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.UI._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.network.sasl.SecretKeyHolder
import org.apache.spark.util.Utils

/**
 * Spark 安全管理器，负责认证、ACL 权限控制、SSL/加密等安全相关功能。
 *
 * 通常由 SparkEnv 实例化，大部分组件通过 SparkEnv 访问。
 * 在 SparkEnv 尚未初始化的场景下也可直接实例化。
 *
 * 本类实现了 Spark "Security" 文档中描述的所有安全配置功能。
 */
private[spark] class SecurityManager(
    sparkConf: SparkConf,
    val ioEncryptionKey: Option[Array[Byte]] = None,
    authSecretFileConf: ConfigEntry[Option[String]] = AUTH_SECRET_FILE)
  extends Logging with SecretKeyHolder {

  import SecurityManager._

  // 通配符 ACL，表示允许所有用户/组
  private val WILDCARD_ACL = "*"

  // 是否启用网络认证
  private val authOn = sparkConf.get(NETWORK_AUTH_ENABLED)
  // 是否启用 ACL 权限控制
  private var aclsOn = sparkConf.get(ACLS_ENABLE)

  // 管理员 ACL 用户集合（需在 view/modify ACL 之前设置）
  private var adminAcls: Set[String] = sparkConf.get(ADMIN_ACLS).toSet

  // 管理员 ACL 用户组集合（需在 view/modify 组 ACL 之前设置）
  private var adminAclsGroups: Set[String] = sparkConf.get(ADMIN_ACLS_GROUPS).toSet

  // 拥有查看权限的用户集合
  private var viewAcls: Set[String] = _

  // 拥有查看权限的用户组集合
  private var viewAclsGroups: Set[String] = _

  // 拥有修改权限的用户集合（适用于 UI 和 CLI，如 kill 应用）
  private var modifyAcls: Set[String] = _

  // 拥有修改权限的用户组集合
  private var modifyAclsGroups: Set[String] = _

  // 默认 ACL 用户：始终包含当前系统用户和 SPARK_USER
  private val defaultAclUsers = Set[String](System.getProperty("user.name", ""),
    Utils.getCurrentUserName())

  // 初始化查看和修改 ACL
  setViewAcls(defaultAclUsers, sparkConf.get(UI_VIEW_ACLS))
  setModifyAcls(defaultAclUsers, sparkConf.get(MODIFY_ACLS))

  setViewAclsGroups(sparkConf.get(UI_VIEW_ACLS_GROUPS))
  setModifyAclsGroups(sparkConf.get(MODIFY_ACLS_GROUPS))

  // 认证密钥（本地变量，解决 UGI 刷新后密钥丢失的问题）
  private var secretKey: String = _

  // 是否启用 RPC SSL 加密
  private val sslRpcEnabled = sparkConf.getBoolean(
    "spark.ssl.rpc.enabled", false)

  // 打印安全配置摘要
  logInfo(log"SecurityManager: authentication ${MDC(LogKeys.AUTH_ENABLED,
    if (authOn) "enabled" else "disabled")}" +
    log"; ui acls ${MDC(LogKeys.UI_ACLS, if (aclsOn) "enabled" else "disabled")}" +
    log"; users with view permissions: ${MDC(LogKeys.VIEW_ACLS,
      if (viewAcls.nonEmpty) viewAcls.mkString(", ")
    else "EMPTY")} groups with view permissions: ${MDC(LogKeys.VIEW_ACLS_GROUPS,
      if (viewAclsGroups.nonEmpty) viewAclsGroups.mkString(", ") else "EMPTY")}" +
    log"; users with modify permissions: ${MDC(LogKeys.MODIFY_ACLS,
      if (modifyAcls.nonEmpty) modifyAcls.mkString(", ") else "EMPTY")}" +
    log"; groups with modify permissions: ${MDC(LogKeys.MODIFY_ACLS_GROUPS,
      if (modifyAclsGroups.nonEmpty) modifyAclsGroups.mkString(", ") else "EMPTY")}" +
    log"; RPC SSL ${MDC(LogKeys.RPC_SSL_ENABLED, if (sslRpcEnabled) "enabled" else "disabled")}")

  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
  // 默认 SSL 配置，作为所有模块 SSL 配置的基准
  private val defaultSSLOptions =
    SSLOptions.parse(sparkConf, hadoopConf, "spark.ssl", defaults = None)
  // RPC 通信专用的 SSL 配置
  private val rpcSSLOptions = getSSLOptions("rpc")

  /** 获取指定模块的 SSL 配置选项，会以默认 SSL 配置作为回退 */
  def getSSLOptions(module: String): SSLOptions = {
    val opts =
      SSLOptions.parse(sparkConf, hadoopConf, s"spark.ssl.$module", Some(defaultSSLOptions))
    logDebug(s"Created SSL options for $module: $opts")
    opts
  }

  /**
   * 设置查看 ACL。管理员 ACL 需先于本方法设置，修改管理员 ACL 后需重新调用本方法以生效。
   */
  def setViewAcls(defaultUsers: Set[String], allowedUsers: Seq[String]): Unit = {
    viewAcls = adminAcls ++ defaultUsers ++ allowedUsers
    logInfo(log"Changing view acls to: ${MDC(LogKeys.VIEW_ACLS, viewAcls.mkString(","))}")
  }

  /** 设置查看 ACL（单用户版本） */
  def setViewAcls(defaultUser: String, allowedUsers: Seq[String]): Unit = {
    setViewAcls(Set[String](defaultUser), allowedUsers)
  }

  /**
   * 设置查看 ACL 用户组。管理员组 ACL 需先于本方法设置。
   */
  def setViewAclsGroups(allowedUserGroups: Seq[String]): Unit = {
    viewAclsGroups = adminAclsGroups ++ allowedUserGroups
    logInfo(log"Changing view acls groups to: ${MDC(LogKeys.VIEW_ACLS, viewAcls.mkString(","))}")
  }

  /**
   * 获取查看 ACL 字符串。需检查通配符"*"，因为 YARN 无法识别 "defaultuser,*" 格式。
   */
  def getViewAcls: String = {
    if (viewAcls.contains(WILDCARD_ACL)) {
      WILDCARD_ACL
    } else {
      viewAcls.mkString(",")
    }
  }

  /** 获取查看 ACL 用户组字符串 */
  def getViewAclsGroups: String = {
    if (viewAclsGroups.contains(WILDCARD_ACL)) {
      WILDCARD_ACL
    } else {
      viewAclsGroups.mkString(",")
    }
  }

  /**
   * 设置修改 ACL。管理员 ACL 需先于本方法设置。
   */
  def setModifyAcls(defaultUsers: Set[String], allowedUsers: Seq[String]): Unit = {
    modifyAcls = adminAcls ++ defaultUsers ++ allowedUsers
    logInfo(log"Changing modify acls to: ${MDC(LogKeys.MODIFY_ACLS, modifyAcls.mkString(","))}")
  }

  /**
   * 设置修改 ACL 用户组。管理员组 ACL 需先于本方法设置。
   */
  def setModifyAclsGroups(allowedUserGroups: Seq[String]): Unit = {
    modifyAclsGroups = adminAclsGroups ++ allowedUserGroups
    logInfo(log"Changing modify acls groups to: ${MDC(LogKeys.MODIFY_ACLS,
      modifyAcls.mkString(","))}")
  }

  /** 获取修改 ACL 字符串 */
  def getModifyAcls: String = {
    if (modifyAcls.contains(WILDCARD_ACL)) {
      WILDCARD_ACL
    } else {
      modifyAcls.mkString(",")
    }
  }

  /** 获取修改 ACL 用户组字符串 */
  def getModifyAclsGroups: String = {
    if (modifyAclsGroups.contains(WILDCARD_ACL)) {
      WILDCARD_ACL
    } else {
      modifyAclsGroups.mkString(",")
    }
  }

  /** 设置管理员 ACL 用户集合 */
  def setAdminAcls(adminUsers: Seq[String]): Unit = {
    adminAcls = adminUsers.toSet
    logInfo(log"Changing admin acls to: ${MDC(LogKeys.ADMIN_ACLS, adminAcls.mkString(","))}")
  }

  /** 设置管理员 ACL 用户组集合 */
  def setAdminAclsGroups(adminUserGroups: Seq[String]): Unit = {
    adminAclsGroups = adminUserGroups.toSet
    logInfo(log"Changing admin acls groups to: ${MDC(LogKeys.ADMIN_ACLS, adminAcls.mkString(","))}")
  }

  /** 动态开关 ACL 权限控制 */
  def setAcls(aclSetting: Boolean): Unit = {
    aclsOn = aclSetting
    logInfo(log"Changing acls enabled to: ${MDC(LogKeys.AUTH_ENABLED, aclsOn)}")
  }

  /** 获取 IO 加密密钥 */
  def getIOEncryptionKey(): Option[Array[Byte]] = ioEncryptionKey

  /** 检查 UI ACL 是否启用 */
  def aclsEnabled(): Boolean = aclsOn

  /**
   * 检查指定用户是否为管理员。管理员同时拥有查看和修改权限，
   * 还可在 UI 请求中模拟其他用户。
   */
  def checkAdminPermissions(user: String): Boolean = {
    isUserInACL(user, adminAcls, adminAclsGroups)
  }

  /**
   * 检查指定用户是否拥有 UI 查看权限。
   * ACL 关闭时所有用户均有权限；用户为 null 视为未开启认证；
   * ACL 或组中包含通配符"*"则所有用户均有权限。
   */
  def checkUIViewPermissions(user: String): Boolean = {
    logDebug("user=" + user + " aclsEnabled=" + aclsEnabled() + " viewAcls=" +
      viewAcls.mkString(",") + " viewAclsGroups=" + viewAclsGroups.mkString(","))
    isUserInACL(user, viewAcls, viewAclsGroups)
  }

  /**
   * 检查指定用户是否拥有应用修改权限（如 kill 应用等操作）。
   * 逻辑同 checkUIViewPermissions，对 modify ACL 进行匹配。
   */
  def checkModifyPermissions(user: String): Boolean = {
    logDebug("user=" + user + " aclsEnabled=" + aclsEnabled() + " modifyAcls=" +
      modifyAcls.mkString(",") + " modifyAclsGroups=" + modifyAclsGroups.mkString(","))
    isUserInACL(user, modifyAcls, modifyAclsGroups)
  }

  /** 检查 Spark 通信协议的认证是否启用 */
  def isAuthenticationEnabled(): Boolean = authOn

  /**
   * 检查是否应启用网络加密。
   * 如果同时启用了 RPC SSL，则网络加密会被禁用以避免冲突。
   */
  def isEncryptionEnabled(): Boolean = {
    val encryptionEnabled = sparkConf.get(Network.NETWORK_CRYPTO_ENABLED) ||
      sparkConf.get(SASL_ENCRYPTION_ENABLED)
    if (encryptionEnabled && sslRpcEnabled) {
      logWarning("Network encryption disabled as RPC SSL encryption is enabled")
      false
    } else {
      encryptionEnabled
    }
  }

  /** 检查 RPC SSL 是否启用 */
  def isSslRpcEnabled(): Boolean = sslRpcEnabled

  /** 获取 RPC 命名空间的 SSLOptions */
  def getRpcSSLOptions(): SSLOptions = rpcSSLOptions

  /** 获取 SASL 认证用户名（当前使用固定硬编码值） */
  def getSaslUser(): String = "sparkSaslUser"

  /**
   * 获取认证密钥。认证启用时按以下优先级查找密钥：
   * 1. Hadoop UGI 凭证中的密钥
   * 2. 本地缓存的 secretKey（解决 UGI 刷新丢失问题）
   * 3. 环境变量 _SPARK_AUTH_SECRET
   * 4. SparkConf 中的配置
   * 5. 从文件读取（仅 Kubernetes 模式）
   */
  def getSecretKey(): String = {
    if (isAuthenticationEnabled()) {
      val creds = UserGroupInformation.getCurrentUser().getCredentials()
      Option(creds.getSecretKey(SECRET_LOOKUP_KEY))
        .map { bytes => new String(bytes, UTF_8) }
        // UGI 被 loginFromKeytab 刷新后可能丢失密钥（ThriftServer 场景），
        // 因此将密钥保存在本地变量中作为备份
        .orElse(Option(secretKey))
        .orElse(Option(sparkConf.getenv(ENV_AUTH_SECRET)))
        .orElse(sparkConf.getOption(SPARK_AUTH_SECRET_CONF))
        .orElse(secretKeyFromFile())
        .getOrElse {
          throw new IllegalArgumentException(
            s"A secret key must be specified via the $SPARK_AUTH_SECRET_CONF config")
        }
    } else {
      null
    }
  }

  /**
   * 初始化认证密钥。
   * - 认证关闭时直接返回
   * - YARN/local 模式：生成新密钥并存入当前用户的 UGI 凭证
   * - Kubernetes 模式：生成密钥但不存入 UGI（避免与 k8s delegation token 机制冲突）
   * - 其他模式：要求配置中必须提供密钥
   */
  def initializeAuth(): Unit = {
    import SparkMasterRegex._

    if (!sparkConf.get(NETWORK_AUTH_ENABLED)) {
      return
    }

    val master = sparkConf.get(SparkLauncher.SPARK_MASTER, "")
    // 根据 master 类型决定是否将密钥存入 UGI
    val storeInUgi = master match {
      case "yarn" | "local" | LOCAL_N_REGEX(_) | LOCAL_N_FAILURES_REGEX(_, _) =>
        true

      case KUBERNETES_REGEX(_) =>
        // k8s 模式不通过 UGI 传播密钥，避免与 delegation token 机制冲突
        false

      case _ =>
        require(sparkConf.contains(SPARK_AUTH_SECRET_CONF),
          s"A secret key must be specified via the $SPARK_AUTH_SECRET_CONF config.")
        return
    }

    // Driver 和 Executor 的密钥文件配置必须同时存在或同时缺省
    if (sparkConf.get(AUTH_SECRET_FILE_DRIVER).isDefined !=
        sparkConf.get(AUTH_SECRET_FILE_EXECUTOR).isDefined) {
      throw new IllegalArgumentException(
        "Invalid secret configuration: Secret files must be specified for both the driver and the" +
          " executors, not only one or the other.")
    }

    // 优先从文件读取密钥，否则自动生成随机密钥
    secretKey = secretKeyFromFile().getOrElse(Utils.createSecret(sparkConf))

    // 将密钥存入 Hadoop UGI 凭证，以便 Executor 通过 UGI 获取
    if (storeInUgi) {
      val creds = new Credentials()
      creds.addSecretKey(SECRET_LOOKUP_KEY, secretKey.getBytes(UTF_8))
      UserGroupInformation.getCurrentUser().addCredentials(creds)
    }
  }

  /** 从配置的密钥文件中读取密钥（仅 Kubernetes 模式支持） */
  private def secretKeyFromFile(): Option[String] = {
    sparkConf.get(authSecretFileConf).flatMap { secretFilePath =>
      sparkConf.getOption(SparkLauncher.SPARK_MASTER).map {
        case SparkMasterRegex.KUBERNETES_REGEX(_) =>
          val secretFile = new File(secretFilePath)
          require(secretFile.isFile, s"No file found containing the secret key at $secretFilePath.")
          val base64Key = Base64.getEncoder.encodeToString(Files.readAllBytes(secretFile.toPath))
          require(!base64Key.isEmpty, s"Secret key from file located at $secretFilePath is empty.")
          base64Key
        case _ =>
          throw new IllegalArgumentException(
            "Secret keys provided via files is only allowed in Kubernetes mode.")
      }
    }
  }

  /**
   * 检查用户是否在 ACL 列表中。满足以下任一条件即返回 true：
   * - 用户为 null（未开启认证）
   * - ACL 未启用
   * - ACL 用户/组列表包含通配符
   * - 用户直接在 ACL 列表中
   * - 用户所属的组在 ACL 组列表中
   */
  private def isUserInACL(
      user: String,
      aclUsers: Set[String],
      aclGroups: Set[String]): Boolean = {
    if (user == null ||
        !aclsEnabled() ||
        aclUsers.contains(WILDCARD_ACL) ||
        aclUsers.contains(user) ||
        aclGroups.contains(WILDCARD_ACL)) {
      true
    } else {
      // 查询用户所属组并检查是否与 ACL 组有交集
      val userGroups = Utils.getCurrentUserGroups(sparkConf, user)
      logDebug(s"user $user is in groups ${userGroups.mkString(",")}")
      aclGroups.exists(userGroups.contains(_))
    }
  }

  // 默认安全管理器只有一个密钥，忽略 appId 参数
  override def getSaslUser(appId: String): String = getSaslUser()
  override def getSecretKey(appId: String): String = getSecretKey()

  /**
   * 当 RPC SSL 启用时，收集所有密码类配置项为环境变量 Map，
   * 用于传递给 Executor 或其他子进程。
   */
  def getEnvironmentForSslRpcPasswords: Map[String, String] = {
    if (rpcSSLOptions.enabled) {
      val map = scala.collection.mutable.Map[String, String]()
      rpcSSLOptions.keyPassword.foreach(password =>
        map += (SSLOptions.ENV_RPC_SSL_KEY_PASSWORD -> password))
      rpcSSLOptions.privateKeyPassword.foreach(password =>
        map += (SSLOptions.ENV_RPC_SSL_PRIVATE_KEY_PASSWORD -> password))
      rpcSSLOptions.keyStorePassword.foreach(password =>
        map += (SSLOptions.ENV_RPC_SSL_KEY_STORE_PASSWORD -> password))
      rpcSSLOptions.trustStorePassword.foreach(password =>
        map += (SSLOptions.ENV_RPC_SSL_TRUST_STORE_PASSWORD -> password))
      map.toMap
    } else {
      Map()
    }
  }
}

/** SecurityManager 伴生对象，定义认证相关的常量 */
private[spark] object SecurityManager {

  // 认证开关配置键
  val SPARK_AUTH_CONF = NETWORK_AUTH_ENABLED.key
  // 认证密钥配置键
  val SPARK_AUTH_SECRET_CONF = AUTH_SECRET.key
  // 用于将认证密钥传递给 Executor 的环境变量名
  val ENV_AUTH_SECRET = "_SPARK_AUTH_SECRET"

  // 在 Hadoop UGI 中存储 Spark 密钥所使用的 key
  val SECRET_LOOKUP_KEY = new Text("sparkCookie")
}
