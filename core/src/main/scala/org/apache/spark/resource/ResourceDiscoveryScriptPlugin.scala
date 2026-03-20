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

package org.apache.spark.resource

import java.io.File
import java.util.Optional

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.resource.ResourceDiscoveryPlugin
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys
import org.apache.spark.util.Utils.executeAndGetOutput

/**
 * ResourceDiscoveryScriptPlugin - 默认的资源发现插件
 * 
 * Spark 应用中加载的默认插件，用于控制如何发现自定义资源（如 GPU、FPGA）。
 * 
 * 工作流程：
 * 1. 执行用户指定的发现脚本
 * 2. 获取脚本的 JSON 输出
 * 3. 将 JSON 解析为 ResourceInformation 对象
 * 
 * 设计要点：
 * - 如果用户指定了自定义插件，此插件会作为最后一个执行
 * - 如果资源未被发现，则抛出异常
 * - 验证脚本返回的资源名称与请求的资源名称一致
 *
 * @since 3.0.0
 */
@DeveloperApi
class ResourceDiscoveryScriptPlugin extends ResourceDiscoveryPlugin with Logging {
  override def discoverResource(
      request: ResourceRequest,
      sparkConf: SparkConf): Optional[ResourceInformation] = {
    val script = request.discoveryScript
    val resourceName = request.id.resourceName
    val result = if (script.isPresent) {
      val scriptFile = new File(script.get)
      logInfo(log"Discovering resources for ${MDC(LogKeys.RESOURCE_NAME, resourceName)}" +
        log" with script: ${MDC(LogKeys.PATH, scriptFile)}")
      // check that script exists and try to execute
      if (scriptFile.exists()) {
        val output = executeAndGetOutput(Seq(script.get), new File("."))
        ResourceInformation.parseJson(output)
      } else {
        throw new SparkException(s"Resource script: $scriptFile to discover $resourceName " +
          "doesn't exist!")
      }
    } else {
      throw new SparkException(s"User is expecting to use resource: $resourceName, but " +
        "didn't specify a discovery script!")
    }
    if (!result.name.equals(resourceName)) {
      throw new SparkException(s"Error running the resource discovery script ${script.get}: " +
        s"script returned resource name ${result.name} and we were expecting $resourceName.")
    }
    Optional.of(result)
  }
}
