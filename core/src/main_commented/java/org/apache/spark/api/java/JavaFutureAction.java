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

package org.apache.spark.api.java;


import java.util.List;
import java.util.concurrent.Future;

/**
 * Java API封装的异步Spark操作Future接口，扩展JDK Future，支持获取底层异步任务相关信息。
 * 用于Java API中异步执行Spark动作操作，提供对异步作业的跟踪能力。
 * @param <T> 异步操作返回的结果类型
 */
public interface JavaFutureAction<T> extends Future<T> {

  /**
   * 获取当前异步操作底层运行的所有Spark作业ID列表。
   * 返回的是调用时刻作业列表的快照，部分操作可能会分多批次启动多个作业，
   * 因此多次调用此方法可能返回不同的结果列表。
   * 
   * @return 当前异步操作关联的所有Spark作业ID组成的列表
   */
  List<Integer> jobIds();
}