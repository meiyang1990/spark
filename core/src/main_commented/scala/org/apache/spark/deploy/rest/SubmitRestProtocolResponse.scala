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

import java.lang.Boolean

/**
 * 文件级注释：REST应用提交协议响应定义，定义了Spark Standalone集群REST提交接口所有服务端响应消息结构
 */

/**
 * 抽象基类，代表REST应用提交协议中服务端返回的响应基类，定义所有响应共有的基础字段和验证逻辑
 */
private[rest] abstract class SubmitRestProtocolResponse extends SubmitRestProtocolMessage {
  var serverSparkVersion: String = null
  var success: Boolean = null
  var unknownFields: Array[String] = null
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(serverSparkVersion, "serverSparkVersion")
  }
}

/**
 * 响应类，对应创建应用提交请求(CreateSubmissionRequest)的服务端响应，返回提交结果
 */
private[spark] class CreateSubmissionResponse extends SubmitRestProtocolResponse {
  var submissionId: String = null
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(success, "success")
  }
}

/**
 * 响应类，对应终止单个应用提交请求的服务端响应，返回终止操作结果
 */
private[spark] class KillSubmissionResponse extends SubmitRestProtocolResponse {
  var submissionId: String = null
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(submissionId, "submissionId")
    assertFieldIsSet(success, "success")
  }
}

/**
 * 响应类，对应终止所有应用提交请求的服务端响应，返回批量终止操作结果
 */
private[spark] class KillAllSubmissionResponse extends SubmitRestProtocolResponse {
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(success, "success")
  }
}

/**
 * 响应类，对应清理已完成应用请求的服务端响应，返回清理操作结果
 */
private[spark] class ClearResponse extends SubmitRestProtocolResponse {
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(success, "success")
  }
}

/**
 * 响应类，对应服务端健康检查(readyz)请求的服务端响应，返回服务就绪状态
 */
private[spark] class ReadyzResponse extends SubmitRestProtocolResponse {
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(success, "success")
  }
}

/**
 * 响应类，对应查询应用提交状态请求的服务端响应，返回驱动程序当前状态信息
 */
private[spark] class SubmissionStatusResponse extends SubmitRestProtocolResponse {
  var submissionId: String = null
  var driverState: String = null
  var workerId: String = null
  var workerHostPort: String = null

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(submissionId, "submissionId")
    assertFieldIsSet(success, "success")
  }
}

/**
 * 错误响应类，代表REST应用提交协议中请求处理失败时返回的错误信息
 */
private[rest] class ErrorResponse extends SubmitRestProtocolResponse {
  // 服务端支持的最高协议版本，当客户端使用未知版本时返回
  var highestProtocolVersion: String = null
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(message, "message")
  }
}