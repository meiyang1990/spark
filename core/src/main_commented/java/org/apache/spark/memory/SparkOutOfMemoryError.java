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
package org.apache.spark.memory;

import org.apache.spark.SparkThrowable;
import org.apache.spark.SparkThrowableHelper;
import org.apache.spark.annotation.Private;

import java.util.Map;

/**
 * 文件概要：Spark内存管理模块自定义内存不足异常类，用于区分任务级内存不足和整个进程内存不足，
 * 仅杀死当前出错任务而非终止整个Executor进程，提高集群资源利用率和稳定性。
 * <p>
 * 类功能说明：当任务无法从内存管理器申请到足够内存时抛出此异常，替代JDK原生的OutOfMemoryError，
 * 实现错误隔离，避免单个任务内存不足导致整个Executor退出，仅影响当前出错任务。
 * 实现了SparkThrowable接口，支持Spark统一的错误分类和错误消息格式化。
 */
@Private
public final class SparkOutOfMemoryError extends OutOfMemoryError implements SparkThrowable {
    String errorClass;
    Map<String, String> messageParameters;
    String sqlState;

    /**
     * 构造SparkOutOfMemoryError实例，使用默认SQL状态。
     *
     * @param errorClass 错误分类码，用于错误标识和消息格式化
     * @param messageParameters 错误消息占位符参数映射
     */
    public SparkOutOfMemoryError(String errorClass, Map<String, String> messageParameters) {
        this(errorClass, messageParameters, null);
    }

    /**
     * 构造SparkOutOfMemoryError实例，指定错误分类、参数和SQL状态。
     *
     * @param errorClass 错误分类码，用于错误标识和消息格式化
     * @param messageParameters 错误消息占位符参数映射
     * @param sqlState SQL状态码，兼容SQL异常场景
     */
    public SparkOutOfMemoryError(String errorClass, Map<String, String> messageParameters,
        String sqlState) {
        // 通过Spark错误工具类格式化错误消息
        super(SparkThrowableHelper.getMessage(errorClass, messageParameters));
        this.errorClass = errorClass;
        this.messageParameters = messageParameters;
        this.sqlState = sqlState;
    }

    /**
     * 获取错误消息参数映射，实现SparkThrowable接口方法。
     *
     * @return 错误消息占位符参数映射
     */
    @Override
    public Map<String, String> getMessageParameters() {
        return messageParameters;
    }

    /**
     * 获取错误分类码，实现SparkThrowable接口方法。
     *
     * @return 错误分类码字符串
     */
    @Override
    public String getCondition() {
        return errorClass;
    }

    /**
     * 获取SQL状态码，实现SparkThrowable接口方法，未指定时使用默认值。
     *
     * @return SQL状态码
     */
    @Override
    public String getSqlState() {
        return sqlState != null ? sqlState : SparkThrowable.super.getSqlState();
    }
}