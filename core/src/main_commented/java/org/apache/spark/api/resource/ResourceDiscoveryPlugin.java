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

package org.apache.spark.api.resource;

import java.util.Optional;

import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.SparkConf;
import org.apache.spark.resource.ResourceInformation;
import org.apache.spark.resource.ResourceRequest;

/**
 * 文件级注释：自定义资源发现插件接口，属于Spark核心模块的资源调度扩展点，允许第三方扩展自定义资源发现逻辑
 * :: DeveloperApi ::
 * 可以动态加载到Spark应用中控制自定义资源发现的插件接口，支持链式调用，不同插件处理不同资源类型
 * <p>
 * 插件必须实现discoveryResource方法来完成资源发现逻辑
 *
 * @since 3.0.0
 */
@DeveloperApi
public interface ResourceDiscoveryPlugin {
  /**
   * 发现请求资源的地址信息
   * <p>
   * 该方法在Spark Driver/Executor/Worker初始化的早期阶段被调用，负责发现资源地址，后续Spark会使用这些地址进行调度并提供给用户
   * 根据部署模式和自定义资源配置，该方法可能被Driver、Executor、或者Standalone模式下的Worker调用
   * 请求中包含ResourceID，可以用于区分调用来源和需要发现的资源类型
   * 该方法会对每个请求的资源类型调用一次，需要根据请求量返回足够的资源地址，如果返回地址数量不满足请求要求，Spark会启动失败
   * 如果当前插件不处理该资源类型，应返回空Optional，Spark会尝试下一个插件，最后回退到默认的发现脚本插件
   *
   * @param request 待发现资源的请求信息
   * @param sparkConf Spark配置对象
   * @return 包含资源名称和地址的ResourceInformation对象的Optional，如果返回空则会尝试其他插件
   */
  Optional<ResourceInformation> discoverResource(ResourceRequest request, SparkConf sparkConf);
}