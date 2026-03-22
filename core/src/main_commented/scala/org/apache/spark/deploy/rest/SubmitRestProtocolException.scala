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

/**
 * REST应用提交协议异常基类，用于封装提交协议处理过程中出现的所有异常
 * @param message 异常描述信息
 * @param cause 原始异常cause，默认为null
 */
private[rest] class SubmitRestProtocolException(message: String, cause: Throwable = null)
  extends Exception(message, cause)

/**
 * REST提交协议消息缺少必填字段时抛出的异常
 * @param message 异常描述信息
 */
private[rest] class SubmitRestMissingFieldException(message: String)
  extends SubmitRestProtocolException(message)

/**
 * REST客户端无法连接到REST服务器时抛出的异常
 * @param message 异常描述信息
 * @param cause 原始异常cause
 */
private[deploy] class SubmitRestConnectionException(message: String, cause: Throwable)
  extends SubmitRestProtocolException(message, cause)