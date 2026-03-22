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

package org.apache.spark.deploy.rest

import java.io.File

import jakarta.servlet.http.HttpServletResponse

import org.apache.spark.{SPARK_VERSION => sparkVersion, SparkConf}
import org.apache.spark.deploy.{Command, DeployMessages, DriverDescription, SparkSubmit}
import org.apache.spark.deploy.ClientArguments._
import org.apache.spark.internal.config
import org.apache.spark.launcher.{JavaModuleOptions, SparkLauncher}
import org.apache.spark.resource.ResourceUtils
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * Standalone 模式下的REST提交服务器，用于响应RestSubmissionClient的提交、终止、查询等Driver操作请求
 * 嵌入在Standalone集群的Master节点中运行，仅用于集群模式
 *
 * 响应HTTP状态码约定：
 *   200 OK - 请求处理成功
 *   400 BAD REQUEST - 请求格式错误、验证失败或类型不匹配
 *   468 UNKNOWN PROTOCOL VERSION - 请求使用的协议版本服务器不支持
 *   500 INTERNAL SERVER ERROR - 服务器处理请求时发生内部异常
 *
 * 服务器始终在HTTP响应体中返回JSON格式的SubmitRestProtocolResponse，错误时返回ErrorResponse
 * 如果构造错误响应本身失败，则返回空响应体并设置500状态码
 *
 * @param host 服务器绑定的地址
 * @param requestedPort 服务器尝试绑定的端口
 * @param masterConf Master节点使用的配置
 * @param masterEndpoint Master端点的Rpc引用，用于发送请求
 * @param masterUrl 新Driver将连接的Master地址
 */
private[deploy] class StandaloneRestServer(
    host: String,
    requestedPort: Int,
    masterConf: SparkConf,
    masterEndpoint: RpcEndpointRef,
    masterUrl: String)
  extends RestSubmissionServer(host, requestedPort, masterConf) {

  // 提交请求处理器
  protected override val submitRequestServlet =
    new StandaloneSubmitRequestServlet(masterEndpoint, masterUrl, masterConf)
  // 终止Driver请求处理器
  protected override val killRequestServlet =
    new StandaloneKillRequestServlet(masterEndpoint, masterConf)
  // 终止所有Driver请求处理器
  protected override val killAllRequestServlet =
    new StandaloneKillAllRequestServlet(masterEndpoint, masterConf)
  // 查询Driver状态请求处理器
  protected override val statusRequestServlet =
    new StandaloneStatusRequestServlet(masterEndpoint, masterConf)
  // 清理已完成Driver请求处理器
  protected override val clearRequestServlet =
    new StandaloneClearRequestServlet(masterEndpoint, masterConf)
  // 健康就绪检查请求处理器
  protected override val readyzRequestServlet =
    new StandaloneReadyzRequestServlet(masterEndpoint, masterConf)
}

/**
 * 处理终止指定Driver请求的Servlet，转发请求到Standalone Master
 */
private[rest] class StandaloneKillRequestServlet(masterEndpoint: RpcEndpointRef, conf: SparkConf)
  extends KillRequestServlet {

  /**
   * 处理终止Driver请求，转发到Master并构造响应
   * @param submissionId 要终止的Driver提交ID
   * @return 终止操作响应结果
   */
  protected def handleKill(submissionId: String): KillSubmissionResponse = {
    val response = masterEndpoint.askSync[DeployMessages.KillDriverResponse](
      DeployMessages.RequestKillDriver(submissionId))
    val k = new KillSubmissionResponse
    k.serverSparkVersion = sparkVersion
    k.message = response.message
    k.submissionId = submissionId
    k.success = response.success
    k
  }
}

/**
 * 处理终止所有Driver请求的Servlet，转发请求到Standalone Master
 */
private[rest] class StandaloneKillAllRequestServlet(masterEndpoint: RpcEndpointRef, conf: SparkConf)
  extends KillAllRequestServlet {

  /**
   * 处理终止所有Driver请求，转发到Master并构造响应
   * @return 批量终止操作响应结果
   */
  protected def handleKillAll() : KillAllSubmissionResponse = {
    val response = masterEndpoint.askSync[DeployMessages.KillAllDriversResponse](
      DeployMessages.RequestKillAllDrivers)
    val k = new KillAllSubmissionResponse
    k.serverSparkVersion = sparkVersion
    k.message = response.message
    k.success = response.success
    k
  }
}

/**
 * 处理查询Driver状态请求的Servlet，转发请求到Standalone Master
 */
private[rest] class StandaloneStatusRequestServlet(masterEndpoint: RpcEndpointRef, conf: SparkConf)
  extends StatusRequestServlet {

  /**
   * 处理Driver状态查询请求，转发到Master并构造响应
   * @param submissionId 要查询的Driver提交ID
   * @return Driver状态响应结果
   */
  protected def handleStatus(submissionId: String): SubmissionStatusResponse = {
    val response = masterEndpoint.askSync[DeployMessages.DriverStatusResponse](
      DeployMessages.RequestDriverStatus(submissionId))
    // 如果查询过程中发生异常，格式化异常信息
    val message = response.exception.map { s"Exception from the cluster:\n" + formatException(_) }
    val d = new SubmissionStatusResponse
    d.serverSparkVersion = sparkVersion
    d.submissionId = submissionId
    d.success = response.found
    d.driverState = response.state.map(_.toString).orNull
    d.workerId = response.workerId.orNull
    d.workerHostPort = response.workerHostPort.orNull
    d.message = message.orNull
    d
  }
}

/**
 * 处理清理已完成Driver请求的Servlet，转发请求到Standalone Master
 */
private[rest] class StandaloneClearRequestServlet(masterEndpoint: RpcEndpointRef, conf: SparkConf)
  extends ClearRequestServlet {

  /**
   * 处理清理已完成Driver和应用请求，转发到Master并构造响应
   * @return 清理操作响应结果
   */
  protected def handleClear(): ClearResponse = {
    val response = masterEndpoint.askSync[Boolean](
      DeployMessages.RequestClearCompletedDriversAndApps)
    val c = new ClearResponse
    c.serverSparkVersion = sparkVersion
    c.message = ""
    c.success = response
    c
  }
}

/**
 * 处理Master就绪检查请求的Servlet，转发请求到Standalone Master
 */
private[rest] class StandaloneReadyzRequestServlet(masterEndpoint: RpcEndpointRef, conf: SparkConf)
  extends ReadyzRequestServlet {

  /**
   * 处理就绪检查请求，查询Master状态并构造响应
   * @return 就绪检查响应结果
   */
  protected def handleReadyz(): ReadyzResponse = {
    val success = masterEndpoint.askSync[Boolean](DeployMessages.RequestReadyz)
    val r = new ReadyzResponse
    r.serverSparkVersion = sparkVersion
    r.message = ""
    r.success = success
    r
  }
}

/**
 * 处理提交新Driver请求的Servlet，构造Driver描述并转发请求到Standalone Master
 */
private[rest] class StandaloneSubmitRequestServlet(
    masterEndpoint: RpcEndpointRef,
    masterUrl: String,
    conf: SparkConf)
  extends SubmitRequestServlet {

  // 替换占位符为环境变量值，如果占位符对应环境变量不存在则返回原始字符串
  private def replacePlaceHolder(variable: String) = variable match {
    case s"{{$name}}" if System.getenv(name) != null => System.getenv(name)
    case _ => variable
  }

  /**
   * 根据提交请求中的字段构造Driver描述对象
   * 会构造启动Driver所需的命令行，包含内存、Java选项、类路径等配置
   * 当前暂不支持Python应用，因为Standalone集群模式尚未支持Python
   *
   * @param request 提交Driver的请求对象
   * @param masterUrl Master地址
   * @param masterRestPort Master REST服务端口
   * @return 构造完成的Driver描述对象
   */
  private[rest] def buildDriverDescription(
      request: CreateSubmissionRequest,
      masterUrl: String,
      masterRestPort: Int): DriverDescription = {
    // 必填字段检查，Python暂不支持因此mainClass必填
    val appResource = Option(request.appResource).getOrElse {
      throw new SubmitRestMissingFieldException("Application jar is missing.")
    }
    val mainClass = Option(request.mainClass).getOrElse {
      throw new SubmitRestMissingFieldException("Main class is missing.")
    }

    // 处理可选字段，替换环境变量占位符
    val sparkProperties = request.sparkProperties
      .map(x => (x._1, replacePlaceHolder(x._2)))
    val driverMemory = sparkProperties.get(config.DRIVER_MEMORY.key)
    val driverCores = sparkProperties.get(config.DRIVER_CORES.key)
    val driverDefaultJavaOptions = sparkProperties.get(SparkLauncher.DRIVER_DEFAULT_JAVA_OPTIONS)
    val driverExtraJavaOptions = sparkProperties.get(config.DRIVER_JAVA_OPTIONS.key)
    val driverExtraClassPath = sparkProperties.get(config.DRIVER_CLASS_PATH.key)
    val driverExtraLibraryPath = sparkProperties.get(config.DRIVER_LIBRARY_PATH.key)
    val superviseDriver = sparkProperties.get(config.DRIVER_SUPERVISE.key)
    // spark.master属性和masterUrl语义不同：spark.master可能包含所有注册的Master，而masterUrl仅包含当前活跃Master
    // 为了支持多Master场景下Driver恢复，使用用户提交的spark.master，仅修正端口为Master RPC端口而非REST端口
    val masters = sparkProperties.get("spark.master")
    val (_, masterPort) = Utils.extractHostPortFromSparkUrl(masterUrl)
    val updatedMasters = masters.map(
      _.replace(s":$masterRestPort", s":$masterPort")).getOrElse(masterUrl)
    val appArgs = Option(request.appArgs).getOrElse(Array[String]())
    // 过滤掉本地IP/HOSTNAME环境变量，避免污染远程Driver进程，同时替换环境变量占位符
    val environmentVariables =
      Option(request.environmentVariables).getOrElse(Map.empty[String, String])
        .filterNot(x => x._1.matches("SPARK_LOCAL_(IP|HOSTNAME)"))
        .map(x => (x._1, replacePlaceHolder(x._2)))

    // 构造Driver配置
    val conf = new SparkConf(false)
      .setAll(sparkProperties)
      .set("spark.master", updatedMasters)
    // 处理额外类路径和库路径
    val extraClassPath = driverExtraClassPath.toSeq.flatMap(_.split(File.pathSeparator))
    val extraLibraryPath = driverExtraLibraryPath.toSeq.flatMap(_.split(File.pathSeparator))
    // 拼接Java选项：默认Java选项 + Spark通用Java选项 + 用户额外Java选项
    val defaultJavaOpts = driverDefaultJavaOptions.map(Utils.splitCommandString)
      .getOrElse(Seq.empty)
    val extraJavaOpts = driverExtraJavaOptions.map(Utils.splitCommandString).getOrElse(Seq.empty)
    val sparkJavaOpts = Utils.sparkJavaOpts(conf)
    val javaModuleOptions = JavaModuleOptions.defaultModuleOptionArray().toImmutableArraySeq
    val javaOpts = javaModuleOptions ++ sparkJavaOpts ++ defaultJavaOpts ++ extraJavaOpts
    // 如果使用SparkSubmit作为入口，添加应用名称参数
    val sparkSubmitOpts = if (mainClass.equals(classOf[SparkSubmit].getName)) {
      sparkProperties.get("spark.app.name")
        .map { v => Seq("-c", s"spark.app.name=$v") }
        .getOrElse(Seq.empty[String])
    } else {
      Seq.empty[String]
    }
    // 构造Driver启动命令
    val command = new Command(
      "org.apache.spark.deploy.worker.DriverWrapper",
      Seq("{{WORKER_URL}}", "{{USER_JAR}}", mainClass) ++ sparkSubmitOpts ++ appArgs,
      environmentVariables, extraClassPath, extraLibraryPath, javaOpts)
    // 填充默认值：内存、核心数、supervise模式
    val actualDriverMemory = driverMemory.map(Utils.memoryStringToMb).getOrElse(DEFAULT_MEMORY)
    val actualDriverCores = driverCores.map(_.toInt).getOrElse(DEFAULT_CORES)
    val actualSuperviseDriver = superviseDriver.map(_.toBoolean).getOrElse(DEFAULT_SUPERVISE)
    // 解析资源需求
    val driverResourceReqs = ResourceUtils.parseResourceRequirements(conf,
      config.SPARK_DRIVER_PREFIX)
    new DriverDescription(
      appResource, actualDriverMemory, actualDriverCores, actualSuperviseDriver, command,
      driverResourceReqs)
  }

  /**
   * 处理提交请求，验证通过后构造Driver描述转发给Master，返回响应给客户端
   * 如果请求消息类型不匹配，返回错误给客户端
   *
   * @param requestMessageJson 请求原始JSON字符串
   * @param requestMessage 解析后的请求消息对象
   * @param responseServlet HTTP响应对象
   * @return 提交操作响应结果
   */
  protected override def handleSubmit(
      requestMessageJson: String,
      requestMessage: SubmitRestProtocolMessage,
      responseServlet: HttpServletResponse): SubmitRestProtocolResponse = {
    requestMessage match {
      case submitRequest: CreateSubmissionRequest =>
        // 构造Driver描述并提交给Master
        val driverDescription = buildDriverDescription(
          submitRequest, masterUrl, conf.get(config.MASTER_REST_SERVER_PORT))
        val response = masterEndpoint.askSync[DeployMessages.SubmitDriverResponse](
          DeployMessages.RequestSubmitDriver(driverDescription))
        // 构造响应对象
        val submitResponse = new CreateSubmissionResponse
        submitResponse.serverSparkVersion = sparkVersion
        submitResponse.message = response.message
        submitResponse.success = response.success
        submitResponse.submissionId = response.driverId.orNull
        // 检查是否有服务器不认识的未知字段，返回警告给客户端
        val unknownFields = findUnknownFields(requestMessageJson, requestMessage)
        if (unknownFields.nonEmpty) {
          submitResponse.unknownFields = unknownFields
        }
        submitResponse
      case unexpected =>
        // 消息类型不匹配，返回400错误
        responseServlet.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        handleError(s"Received message of unexpected type ${unexpected.messageType}.")
    }
  }
}