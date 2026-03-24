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

/**
 * Spark状态API v1版本 - 单个Spark应用REST资源实现
 * 提供单个Spark应用及其内部各类实体（Job、Stage、Executor等）的REST查询接口
 */
package org.apache.spark.status.api.v1

import java.io.OutputStream
import java.util.{List => JList}
import java.util.zip.ZipOutputStream

import scala.util.control.NonFatal

import jakarta.ws.rs.{NotFoundException => _, _}
import jakarta.ws.rs.core.{MediaType, Response, StreamingOutput}

import org.apache.spark.{JobExecutionStatus, SparkContext}
import org.apache.spark.status.api.v1
import org.apache.spark.util.Utils

/**
 * 单个Spark应用API资源的抽象基类
 * 封装单个应用内各类资源（Job、Executor、RDD等）的公共查询逻辑，被具体应用和尝试资源继承
 */
@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class AbstractApplicationResource extends BaseAppResource {

  /**
   * 获取应用所有Job列表，支持按状态过滤
   * @param statuses 过滤的状态列表
   * @return 符合条件的Job数据列表
   */
  @GET
  @Path("jobs")
  def jobsList(@QueryParam("status") statuses: JList[JobExecutionStatus]): Seq[JobData] = {
    withUI(_.store.jobsList(statuses))
  }

  /**
   * 获取指定ID的单个Job详情
   * @param jobId Job ID
   * @return 对应Job的详细数据
   */
  @GET
  @Path("jobs/{jobId: \\d+}")
  def oneJob(@PathParam("jobId") jobId: Int): JobData = withUI { ui =>
    try {
      ui.store.job(jobId)
    } catch {
      case _: NoSuchElementException =>
        throw new NotFoundException("unknown job: " + jobId)
    }
  }

  /**
   * 获取当前活跃的Executor列表
   * @return 活跃Executor摘要列表
   */
  @GET
  @Path("executors")
  def executorList(): Seq[ExecutorSummary] = withUI(_.store.executorList(true))

  /**
   * 获取指定Executor的线程栈转储
   * @param execId Executor ID
   * @return 线程栈信息数组
   */
  @GET
  @Path("executors/{executorId}/threads")
  def threadDump(@PathParam("executorId") execId: String): Array[ThreadStackTrace] = withUI { ui =>
    // 验证Executor ID格式合法性
    checkExecutorId(execId)
    // 获取可用的SparkContext实例，历史服务器模式下不可用
    val safeSparkContext = checkAndGetSparkContext()
    ui.store.asOption(ui.store.executorSummary(execId)) match {
      case Some(executorSummary) if executorSummary.isActive =>
          // 从SparkContext获取线程转储，不存在则返回404
          val safeThreadDump = safeSparkContext.getExecutorThreadDump(execId).getOrElse {
            throw new NotFoundException("No thread dump is available.")
          }
          safeThreadDump
      case Some(_) => throw new BadParameterException("Executor is not active.")
      case _ => throw new NotFoundException("Executor does not exist.")
    }
  }

  /**
   * 获取指定运行中任务的线程栈转储
   * @param taskId 任务ID
   * @param execId 运行任务的Executor ID
   * @return 任务对应的线程栈信息
   */
  @GET
  @Path("threads")
  def getTaskThreadDump(
      @QueryParam("taskId") taskId: Long,
      @QueryParam("executorId") execId: String): ThreadStackTrace = {
    checkExecutorId(execId)
    val safeSparkContext = checkAndGetSparkContext()
    safeSparkContext
      .getTaskThreadDump(taskId, execId)
      .getOrElse {
        throw new NotFoundException(
          s"Task '$taskId' is not running on Executor '$execId' right now")
      }
  }

  /**
   * 获取所有Executor（包括已移除的）列表
   * @return 所有Executor摘要列表
   */
  @GET
  @Path("allexecutors")
  def allExecutorList(): Seq[ExecutorSummary] = withUI(_.store.executorList(false))

  /**
   * 获取所有杂项进程列表
   * @return 所有杂项进程摘要列表
   */
  @GET
  @Path("allmiscellaneousprocess")
  def allProcessList(): Seq[ProcessSummary] = withUI(_.store.miscellaneousProcessList(false))

  /**
   * 将Stages相关请求转发到StagesResource处理
   * @return StagesResource类对象
   */
  @Path("stages")
  def stages(): Class[StagesResource] = classOf[StagesResource]

  /**
   * 获取存储层面所有RDD的存储信息列表
   * @return RDD存储信息列表
   */
  @GET
  @Path("storage/rdd")
  def rddList(): Seq[RDDStorageInfo] = withUI(_.store.rddList())

  /**
   * 获取指定ID RDD的存储详情
   * @param rddId RDD ID
   * @return 对应RDD的存储详细信息
   */
  @GET
  @Path("storage/rdd/{rddId: \\d+}")
  def rddData(@PathParam("rddId") rddId: Int): RDDStorageInfo = withUI { ui =>
    try {
      ui.store.rdd(rddId)
    } catch {
      case _: NoSuchElementException =>
        throw new NotFoundException(s"no rdd found w/ id $rddId")
    }
  }

  /**
   * 获取应用运行环境信息
   * @return 脱敏后的应用环境完整信息
   */
  @GET
  @Path("environment")
  def environmentInfo(): ApplicationEnvironmentInfo = withUI { ui =>
    val envInfo = ui.store.environmentInfo()
    val resourceProfileInfo = ui.store.resourceProfileInfo()
    // 对敏感配置信息脱敏，然后按键排序返回
    new v1.ApplicationEnvironmentInfo(
      envInfo.runtime,
      Utils.redact(ui.conf, envInfo.sparkProperties).sortBy(_._1),
      Utils.redact(ui.conf, envInfo.hadoopProperties).sortBy(_._1),
      Utils.redact(ui.conf, envInfo.systemProperties).sortBy(_._1),
      Utils.redact(ui.conf, envInfo.metricsProperties).sortBy(_._1),
      envInfo.classpathEntries.sortBy(_._1),
      resourceProfileInfo)
  }

  /**
   * 下载应用事件日志压缩包
   * @return 流式下载响应，返回ZIP压缩的事件日志
   */
  @GET
  @Path("logs")
  @Produces(Array(MediaType.APPLICATION_OCTET_STREAM))
  def getEventLogs(): Response = {
    // 向后兼容：如果未指定尝试ID找不到，尝试使用默认尝试ID "1"
    try {
      checkUIViewPermissions()
    } catch {
      case _: NotFoundException if attemptId == null =>
        attemptId = "1"
        checkUIViewPermissions()
        attemptId = null
    }

    try {
      // 根据是否指定尝试ID生成压缩包文件名
      val fileName = if (attemptId != null) {
        s"eventLogs-$appId-$attemptId.zip"
      } else {
        s"eventLogs-$appId.zip"
      }

      // 定义流式输出，边压缩边写入响应，避免加载全量日志到内存
      val stream = new StreamingOutput {
        override def write(output: OutputStream): Unit = {
          val zipStream = new ZipOutputStream(output)
          try {
            uiRoot.writeEventLogs(appId, Option(attemptId), zipStream)
          } finally {
            zipStream.close()
          }

        }
      }

      // 构建下载响应，设置对应头信息
      Response.ok(stream)
        .header("Content-Disposition", s"attachment; filename=$fileName")
        .header("Content-Type", MediaType.APPLICATION_OCTET_STREAM)
        .build()
    } catch {
      case NonFatal(_) =>
        throw new ServiceUnavailable(s"Event logs are not available for app: $appId.")
    }
  }

  /**
   * 绑定指定尝试ID的路由，转发到OneApplicationAttemptResource处理
   * 必须放在最后，避免匹配覆盖其他路径
   * @return OneApplicationAttemptResource类对象
   */
  @Path("{attemptId}")
  def applicationAttempt(): Class[OneApplicationAttemptResource] = {
    if (attemptId != null) {
      throw new NotFoundException(httpRequest.getRequestURI())
    }
    classOf[OneApplicationAttemptResource]
  }

  /**
   * 验证Executor ID格式是否合法，必须是driver或纯数字ID
   * @param execId 待验证的Executor ID
   */
  private def checkExecutorId(execId: String): Unit = {
    if (execId != SparkContext.DRIVER_IDENTIFIER && !execId.forall(Character.isDigit)) {
      throw new BadParameterException(
        s"Invalid executorId: neither '${SparkContext.DRIVER_IDENTIFIER}' nor number.")
    }
  }

  /**
   * 检查并获取可用的SparkContext，历史服务器模式下无法获取
   * @return 可用的SparkContext实例
   */
  private def checkAndGetSparkContext(): SparkContext = withUI { ui =>
    ui.sc.getOrElse {
      throw new ServiceUnavailable("Thread dumps not available through the history server.")
    }
  }
}

/**
 * 单个Spark应用（不区分尝试）的根资源
 * 用于获取应用整体信息，路由内部各类子资源
 */
private[v1] class OneApplicationResource extends AbstractApplicationResource {

  /**
   * 获取当前应用的基本信息
   * @return 应用整体信息
   */
  @GET
  def getApp(): ApplicationInfo = {
    val app = uiRoot.getApplicationInfo(appId)
    app.getOrElse(throw new NotFoundException("unknown app: " + appId))
  }

}

/**
 * 单个Spark应用指定尝试的资源
 * 用于获取特定尝试的信息，继承抽象基类提供的所有子资源查询能力
 */
private[v1] class OneApplicationAttemptResource extends AbstractApplicationResource {

  /**
   * 获取当前指定尝试的详细信息
   * @return 应用尝试信息
   */
  @GET
  def getAttempt(): ApplicationAttemptInfo = {
    uiRoot.getApplicationInfo(appId)
      .flatMap { app =>
        app.attempts.find(_.attemptId.contains(attemptId))
      }
      .getOrElse {
        throw new NotFoundException(s"unknown app $appId, attempt $attemptId")
      }
  }

}