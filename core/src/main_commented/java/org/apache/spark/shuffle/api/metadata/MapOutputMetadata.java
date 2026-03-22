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

package org.apache.spark.shuffle.api.metadata;

import java.io.Serializable;

/**
 * 文件级注释：Shuffle阶段Map任务输出元数据接口，定义了Map任务输出提交结果的标记规范
 *
 * :: Private ::
 * 不透明的元数据标记，用于注册Shuffle Map任务输出提交完成的结果
 * <p>
 * 所有实现类都必须支持序列化，因为该元数据需要从执行器发送到驱动器节点
 */
public interface MapOutputMetadata extends Serializable {}