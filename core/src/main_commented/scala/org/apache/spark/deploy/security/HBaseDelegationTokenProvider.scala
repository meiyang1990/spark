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

import java.io.Closeable

import scala.reflect.runtime.universe
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.token.{Token, TokenIdentifier}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.security.HadoopDelegationTokenProvider
import org.apache.spark.util.Utils

/**
 * HBase 服务的Hadoop委托令牌提供者，负责在Spark安全认证流程中获取HBase的委托令牌
 * 用于支持Spark在Kerberos认证环境下安全访问HBase服务
 */
private[security] class HBaseDelegationTokenProvider
  extends HadoopDelegationTokenProvider with Logging {

  override def serviceName: String = "hbase"

  /**
   * 从HBase服务获取委托令牌，并添加到凭证中
   * @param hadoopConf Hadoop配置
   * @param sparkConf Spark配置
   * @param creds 要添加令牌的凭证对象
   * @return 令牌过期时间（返回None表示不跟踪过期时间）
   */
  override def obtainDelegationTokens(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    try {
      // 获取当前类加载器对应的反射镜像
      val mirror = universe.runtimeMirror(Utils.getContextOrSparkClassLoader)
      // 通过反射加载HBase TokenUtil类并获取旧版obtainToken方法
      val obtainToken = mirror.classLoader.
        loadClass("org.apache.hadoop.hbase.security.token.TokenUtil")
        .getMethod("obtainToken", classOf[Configuration])

      logDebug("Attempting to fetch HBase security token.")
      // 创建HBase配置并调用获取令牌方法
      val token = obtainToken.invoke(null, hbaseConf(hadoopConf))
        .asInstanceOf[Token[_ <: TokenIdentifier]]
      logInfo(log"Get token from HBase: ${MDC(TOKEN, token.toString)}")
      // 将获取到的令牌添加到凭证中
      creds.addToken(token.getService, token)
    } catch {
      // 旧版API获取失败，说明HBase版本较高（2.x+），改用兼容API重新获取
      case NonFatal(e) =>
        logWarning(Utils.createFailedToGetTokenMessage(serviceName) + log" Retrying to fetch " +
          log"HBase security token with ${MDC(SERVICE_NAME, serviceName)} connection parameter.", e)
        obtainDelegationTokensWithHBaseConn(hadoopConf, creds)
    }
    None
  }

  /**
   * 使用HBase连接API获取HBase委托令牌，兼容HBase 2.x+版本
   * HBase 2.x版本移除了旧版的单参数Configuration方法，改用需要Connection参数的API
   * @param hadoopConf Hadoop配置
   * @param creds 要添加令牌的凭证对象
   */
  private def obtainDelegationTokensWithHBaseConn(
      hadoopConf: Configuration,
      creds: Credentials): Unit = {
    var hbaseConnection : Closeable = null
    try {
      val mirror = universe.runtimeMirror(Utils.getContextOrSparkClassLoader)
      // 反射加载ConnectionFactory并创建HBase连接
      val connectionFactoryClass = mirror.classLoader
        .loadClass("org.apache.hadoop.hbase.client.ConnectionFactory")
        .getMethod("createConnection", classOf[Configuration])
      hbaseConnection = connectionFactoryClass.invoke(null, hbaseConf(hadoopConf))
        .asInstanceOf[Closeable]
      // 获取Connection类类型引用
      val connectionParamTypeClassRef = mirror.classLoader
        .loadClass("org.apache.hadoop.hbase.client.Connection")
      // 加载新版obtainToken方法
      val obtainTokenMethod = mirror.classLoader
        .loadClass("org.apache.hadoop.hbase.security.token.TokenUtil")
        .getMethod("obtainToken", connectionParamTypeClassRef)
      logDebug("Attempting to fetch HBase security token.")
      // 调用新版API获取令牌
      val token = obtainTokenMethod.invoke(null, hbaseConnection)
        .asInstanceOf[Token[_ <: TokenIdentifier]]
      logInfo(log"Get token from HBase: ${MDC(TOKEN, token.toString)}")
      // 将令牌添加到凭证中
      creds.addToken(token.getService, token)
    } catch {
      case NonFatal(e) =>
        logWarning(Utils.createFailedToGetTokenMessage(serviceName), e)
    } finally {
      // 关闭HBase连接释放资源
      if (null != hbaseConnection) {
        hbaseConnection.close()
      }
    }
  }

  /**
   * 检查是否需要获取HBase委托令牌，仅当HBase开启Kerberos认证时才需要
   * @param sparkConf Spark配置
   * @param hadoopConf Hadoop配置
   * @return true表示需要获取令牌，false不需要
   */
  override def delegationTokensRequired(
      sparkConf: SparkConf,
      hadoopConf: Configuration): Boolean = {
    hbaseConf(hadoopConf).get("hbase.security.authentication") == "kerberos"
  }

  /**
   * 通过反射创建HBase配置对象，继承自传入的Hadoop配置
   * @param conf 原始Hadoop配置
   * @return HBase配置对象，若HBase类不存在则返回原始配置
   */
  private def hbaseConf(conf: Configuration): Configuration = {
    try {
      val mirror = universe.runtimeMirror(Utils.getContextOrSparkClassLoader)
      // 反射调用HBaseConfiguration.create方法创建HBase配置
      val confCreate = mirror.classLoader.
        loadClass("org.apache.hadoop.hbase.HBaseConfiguration").
        getMethod("create", classOf[Configuration])
      confCreate.invoke(null, conf).asInstanceOf[Configuration]
    } catch {
      case NonFatal(e) =>
        // 加载失败使用debug级别日志，避免未使用HBase时产生大量警告
        logDebug("Unable to load HBaseConfiguration.", e)
        conf
    }
  }
}