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

package org.apache.spark.launcher

/**
 * 文件说明：Spark Submit 启动参数解析器的对外访问桥接类
 * 
 * 由于Java不支持Scala的包私有访问特性，需要通过这个抽象类将包内的SparkSubmitOptionParser
 * 暴露给Spark launcher包外的代码使用，同时保持原类不公开为公共API。
 */
/**
 * Spark Submit 参数解析器抽象桥接类
 * 
 * 用于解决Java访问限制问题，将`launcher`包内的`SparkSubmitOptionParser`能力
 * 暴露给Spark其他包的代码使用，同时保持原有实现类不对外公开。
 */
private[spark] abstract class SparkSubmitArgumentsParser extends SparkSubmitOptionParser