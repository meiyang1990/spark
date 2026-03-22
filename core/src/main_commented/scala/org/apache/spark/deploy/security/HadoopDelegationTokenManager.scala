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

package org.apache.spark.deploy.security

import java.io.File
import java.net.URI
import java.security.PrivilegedExceptionAction
import java.util.ServiceLoader
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.config._
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.UpdateDelegationTokens
import org.apache.spark.security.HadoopDelegationTokenProvider
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Spark应用中Hadoop代理令牌管理器
 *
 * 当启用代理令牌续订功能后，该管理器可以保证长时间运行的Spark应用在访问安全服务时不会中断。
 * 它会定期使用用户提供的凭证向KDC重新登录，并联系所有配置的安全服务获取新的代理令牌，分发到应用的所有进程。
 *
 * 新代理令牌会在原始令牌有效期剩余25%（即已过去75%）时提前获取，新令牌会发送给Driver，由Driver分发给所有需要的进程。
 *
 * 令牌续订可通过两种方式启用：给Spark提供用户名和keytab，或者基于本地凭证缓存启用续订。
 * 第二种方式的缺点是Spark无法自动更新TGT票据，需要用户手动更新外部的Kerberos票据缓存。
 *
 * 该类也可以仅用于获取代理令牌，调用`obtainDelegationTokens`方法即可。这种使用方式不需要调用`start`方法，
 * 也不需要提供Driver引用，由调用者自行分发生成的令牌。
 */
private[spark] class HadoopDelegationTokenManager(
    protected val sparkConf: SparkConf,
    protected val hadoopConf: Configuration,
    protected val schedulerRef: RpcEndpointRef) extends Logging {

  private val principal = sparkConf.get(PRINCIPAL).orNull

  // Keytab路径支持cluster模式下的local: URI格式，需要转换为普通文件路径，后续使用时会检查文件是否存在
  private val keytab = sparkConf.get(KEYTAB).map { uri => new URI(uri).getPath() }.orNull

  require((principal == null) == (keytab == null),
    "Both principal and keytab must be defined, or neither.")

  private val delegationTokenProviders = loadProviders()
  logDebug("Using the following builtin delegation token providers: " +
    s"${delegationTokenProviders.keys.mkString(", ")}.")

  private var renewalExecutor: ScheduledExecutorService = _

  /** @return 是否启用代理令牌续订功能 */
  def renewalEnabled: Boolean = sparkConf.get(KERBEROS_RENEWAL_CREDENTIALS) match {
    case "keytab" => principal != null
    case "ccache" => UserGroupInformation.getCurrentUser().hasKerberosCredentials()
    case _ => false
  }

  /**
   * 启动令牌续订器。需要配置principal和keytab。启动后，续订器会为所有配置服务获取代理令牌并发送给Driver，
   * 同时设置定时任务定期获取新令牌。
   *
   * 该方法要求已经提供了keytab，在管理器生命周期内会保持登录用户的TGT票据有效。
   *
   * @return 为配置principal创建的新代理令牌集合序列化字节数组
   */
  def start(): Array[Byte] = {
    require(renewalEnabled, "Token renewal must be enabled to start the renewer.")
    require(schedulerRef != null, "Token renewal requires a scheduler endpoint.")
    // 创建后台单线程定时执行器用于凭证续订任务
    renewalExecutor =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("Credential Renewal Thread")

    val ugi = UserGroupInformation.getCurrentUser()
    if (ugi.isFromKeytab()) {
      // 在Hadoop 2.x中，基于keytab的登录会自动续订；但Hadoop 3.x中该功能是可配置的。
      // 该定时任务无论配置是什么都保证用户保持登录状态。checkTGTAndReloginFromKeytab()在不需要续订时是空操作
      val tgtRenewalTask = new Runnable {
        override def run(): Unit = {
          ugi.checkTGTAndReloginFromKeytab()
        }
      }
      val tgtRenewalPeriod = sparkConf.get(KERBEROS_RELOGIN_PERIOD)
      renewalExecutor.scheduleAtFixedRate(tgtRenewalTask, tgtRenewalPeriod, tgtRenewalPeriod,
        TimeUnit.SECONDS)
    }

    updateTokensTask()
  }

  def stop(): Unit = {
    if (renewalExecutor != null) {
      renewalExecutor.shutdownNow()
    }
  }

  /**
   * 为配置的服务获取新代理令牌，并存储到给定的凭证对象中。
   *
   * @param creds 用于存储新获取代理令牌的凭证对象
   */
  def obtainDelegationTokens(creds: Credentials): Unit = {
    val currentUser = UserGroupInformation.getCurrentUser()
    val hasKerberosCreds = principal != null ||
      Option(currentUser.getRealUser()).getOrElse(currentUser).hasKerberosCredentials()

    // 只有真实用户拥有Kerberos凭证时才能获取代理令牌，没有凭证则跳过创建
    if (hasKerberosCreds) {
      val freshUGI = doLogin()
      freshUGI.doAs(new PrivilegedExceptionAction[Unit]() {
        override def run(): Unit = {
          val (newTokens, _) = obtainDelegationTokens()
          creds.addAll(newTokens)
        }
      })
      if (!currentUser.equals(freshUGI)) {
        FileSystem.closeAllForUGI(freshUGI)
      }
    }
  }

  /**
   * 为配置的服务获取新代理令牌。
   *
   * @return 二元组(包含新令牌的凭证对象, 令牌必须被续订的时间戳)
   */
  private def obtainDelegationTokens(): (Credentials, Long) = {
    val creds = new Credentials()
    val nextRenewal = delegationTokenProviders.values.flatMap { provider =>
      if (provider.delegationTokensRequired(sparkConf, hadoopConf)) {
        provider.obtainDelegationTokens(hadoopConf, sparkConf, creds)
      } else {
        logDebug(s"Service ${provider.serviceName} does not require a token." +
          s" Check your configuration to see if security is disabled or not.")
        None
      }
    }.foldLeft(Long.MaxValue)(math.min)
    (creds, nextRenewal)
  }

  // Visible for testing.
  def isProviderLoaded(serviceName: String): Boolean = {
    delegationTokenProviders.contains(serviceName)
  }

  // 调度下一次令牌续订任务
  private def scheduleRenewal(delay: Long): Unit = {
    val _delay = math.max(0, delay)
    logInfo(log"Scheduling renewal in ${MDC(LogKeys.TIME_UNITS, UIUtils.formatDuration(_delay))}.")

    val renewalTask = new Runnable() {
      override def run(): Unit = {
        updateTokensTask()
      }
    }
    renewalExecutor.schedule(renewalTask, _delay, TimeUnit.MILLISECONDS)
  }

  /**
   * 定期任务：登录KDC并创建新的代理令牌，完成后重新调度自己等待下一次获取。
   */
  private def updateTokensTask(): Array[Byte] = {
    try {
      // 重新登录获取最新UGI
      val freshUGI = doLogin()
      // 获取新令牌并调度下次续订
      val creds = obtainTokensAndScheduleRenewal(freshUGI)
      // 序列化凭证
      val tokens = SparkHadoopUtil.get.serialize(creds)

      logInfo("Updating delegation tokens.")
      // 发送更新后的令牌给Driver
      schedulerRef.send(UpdateDelegationTokens(tokens))
      tokens
    } catch {
      case _: InterruptedException =>
        // 关闭过程可能触发该异常，直接忽略
        null
      case e: Exception =>
        // 更新失败，等待重试间隔后重试
        val delay = TimeUnit.SECONDS.toMillis(sparkConf.get(CREDENTIALS_RENEWAL_RETRY_WAIT))
        logWarning(log"Failed to update tokens, will try again in " +
          log"${MDC(LogKeys.TIME_UNITS, UIUtils.formatDuration(delay))}!" +
          log" If this happens too often tasks will fail.", e)
        scheduleRenewal(delay)
        null
    }
  }

  /**
   * 从可用提供者获取新代理令牌，并在新令牌过期前调度下一次获取任务。
   *
   * @return 包含新令牌的凭证对象
   */
  private def obtainTokensAndScheduleRenewal(ugi: UserGroupInformation): Credentials = {
    ugi.doAs(new PrivilegedExceptionAction[Credentials]() {
      override def run(): Credentials = {
        val (creds, nextRenewal) = obtainDelegationTokens()

        // 根据配置的比率计算需要创建新凭证的时间
        val now = System.currentTimeMillis
        val ratio = sparkConf.get(CREDENTIALS_RENEWAL_INTERVAL_RATIO)
        val delay = (ratio * (nextRenewal - now)).toLong
        logInfo(log"Calculated delay on renewal is ${MDC(LogKeys.DELAY, delay)}," +
          log" based on next renewal ${MDC(LogKeys.NEXT_RENEWAL_TIME, nextRenewal)}" +
          log" and the ratio ${MDC(LogKeys.CREDENTIALS_RENEWAL_INTERVAL_RATIO, ratio)}," +
          log" and current time ${MDC(LogKeys.CURRENT_TIME, now)}")
        scheduleRenewal(delay)
        creds
      }
    })
  }

  // 执行Kerberos登录，返回登录后的UGI对象
  private def doLogin(): UserGroupInformation = {
    if (principal != null) {
      logInfo(log"Attempting to login to KDC using principal: ${MDC(LogKeys.PRINCIPAL, principal)}")
      require(new File(keytab).isFile(), s"Cannot find keytab at $keytab.")
      val ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab)
      logInfo("Successfully logged into KDC.")
      ugi
    } else if (!SparkHadoopUtil.get.isProxyUser(UserGroupInformation.getCurrentUser())) {
      logInfo("Attempting to load user's ticket cache.")
      // 从环境变量获取票据缓存路径和用户名
      val ccache = sparkConf.getenv("KRB5CCNAME")
      val user = Option(sparkConf.getenv("KRB5PRINCIPAL")).getOrElse(
        UserGroupInformation.getCurrentUser().getUserName())
      UserGroupInformation.getUGIFromTicketCache(ccache, user)
    } else {
      // 代理用户场景直接使用当前UGI
      UserGroupInformation.getCurrentUser()
    }
  }

  // 通过Java ServiceLoader加载所有启用的代理令牌提供者
  private def loadProviders(): Map[String, HadoopDelegationTokenProvider] = {
    val loader = ServiceLoader.load(classOf[HadoopDelegationTokenProvider],
      Utils.getContextOrSparkClassLoader)
    val providers = mutable.ArrayBuffer[HadoopDelegationTokenProvider]()

    val iterator = loader.iterator
    var keepLoading = true
    while (keepLoading) {
      try {
        if (iterator.hasNext) {
          providers += iterator.next()
        } else {
          keepLoading = false
        }
      } catch {
        // 加载单个提供者失败不影响整体，仅记录调试日志跳过
        case t: Throwable =>
          logDebug(s"Failed to load built in provider.", t)
      }
    }

    // 过滤掉配置中未启用的提供者
    providers
      .filter { p => HadoopDelegationTokenManager.isServiceEnabled(sparkConf, p.serviceName) }
      .map { p => (p.serviceName, p) }
      .toMap
  }
}

/**
 * Hadoop代理令牌管理器单例对象，提供公共配置检查工具方法
 */
private[spark] object HadoopDelegationTokenManager extends Logging {
  // 服务提供者启用状态配置键格式
  private val providerEnabledConfig = "spark.security.credentials.%s.enabled"

  // 已废弃的旧版配置键格式，保持兼容用于提示用户迁移
  private val deprecatedProviderEnabledConfigs = List(
    "spark.yarn.security.tokens.%s.enabled",
    "spark.yarn.security.credentials.%s.enabled")

  /**
   * 检查指定服务的令牌提供者是否启用
   * @param sparkConf Spark配置
   * @param serviceName 服务名称
   * @return 是否启用
   */
  def isServiceEnabled(sparkConf: SparkConf, serviceName: String): Boolean = {
    val key = providerEnabledConfig.format(serviceName)

    // 检查是否使用了废弃配置，输出警告提示迁移
    deprecatedProviderEnabledConfigs.foreach { pattern =>
      val deprecatedKey = pattern.format(serviceName)
      if (sparkConf.contains(deprecatedKey)) {
        logWarning(log"${MDC(LogKeys.DEPRECATED_KEY, deprecatedKey)} is deprecated. " +
          log"Please use ${MDC(LogKeys.CONFIG, key)} instead.")
      }
    }

    // 所有废弃配置都没设置时默认启用
    val isEnabledDeprecated = deprecatedProviderEnabledConfigs.forall { pattern =>
      sparkConf
        .getOption(pattern.format(serviceName))
        .map(_.toBoolean)
        .getOrElse(true)
    }

    // 优先使用新配置，没有配置则使用废弃配置的默认结果
    sparkConf
      .getOption(key)
      .map(_.toBoolean)
      .getOrElse(isEnabledDeprecated)
  }
}