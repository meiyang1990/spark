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

package org.apache.spark;

import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.scheduler.*;

/**
 * 文件级注释：Spark全量事件监听器基类，统一接收所有SparkListener事件，供开发者扩展实现
 *
 * Class that allows users to receive all SparkListener events.
 * Users should override the onEvent method.
 *
 * This is a concrete Java class in order to ensure that we don't forget to update it when adding
 * new methods to SparkListener: forgetting to add a method will result in a compilation error (if
 * this was a concrete Scala class, default implementations of new event handlers would be inherited
 * from the SparkListener trait).
 *
 * Please note until Spark 3.1.0 this was missing the DevelopApi annotation, this needs to be
 * taken into account if changing this API before a major release.
 *
 * 设计目的：作为Java开发接口，保证当SparkListenerInterface新增事件方法时，此类必须同步实现，
 * 避免遗漏事件转发，保证全量事件接收的完整性
 */
@DeveloperApi
public class SparkFirehoseListener implements SparkListenerInterface {

  /**
   * 统一事件处理入口方法，用户需要重写此方法处理所有接收到的事件
   * @param event Spark调度系统产生的任意事件对象
   */
  public void onEvent(SparkListenerEvent event) { }

  @Override
  public final void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
    // 转发阶段完成事件到统一入口
    onEvent(stageCompleted);
  }

  @Override
  public final void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
    // 转发阶段提交事件到统一入口
    onEvent(stageSubmitted);
  }

  @Override
  public final void onTaskStart(SparkListenerTaskStart taskStart) {
    // 转发任务启动事件到统一入口
    onEvent(taskStart);
  }

  @Override
  public final void onTaskGettingResult(SparkListenerTaskGettingResult taskGettingResult) {
    // 转发任务获取结果事件到统一入口
    onEvent(taskGettingResult);
  }

  @Override
  public final void onTaskEnd(SparkListenerTaskEnd taskEnd) {
    // 转发任务结束事件到统一入口
    onEvent(taskEnd);
  }

  @Override
  public final void onJobStart(SparkListenerJobStart jobStart) {
    // 转发作业启动事件到统一入口
    onEvent(jobStart);
  }

  @Override
  public final void onJobEnd(SparkListenerJobEnd jobEnd) {
    // 转发作业结束事件到统一入口
    onEvent(jobEnd);
  }

  @Override
  public final void onEnvironmentUpdate(SparkListenerEnvironmentUpdate environmentUpdate) {
    // 转发环境更新事件到统一入口
    onEvent(environmentUpdate);
  }

  @Override
  public final void onBlockManagerAdded(SparkListenerBlockManagerAdded blockManagerAdded) {
    // 转发块管理器添加事件到统一入口
    onEvent(blockManagerAdded);
  }

  @Override
  public final void onBlockManagerRemoved(SparkListenerBlockManagerRemoved blockManagerRemoved) {
    // 转发块管理器移除事件到统一入口
    onEvent(blockManagerRemoved);
  }

  @Override
  public final void onUnpersistRDD(SparkListenerUnpersistRDD unpersistRDD) {
    // 转发RDD解除持久化事件到统一入口
    onEvent(unpersistRDD);
  }

  @Override
  public final void onApplicationStart(SparkListenerApplicationStart applicationStart) {
    // 转发应用启动事件到统一入口
    onEvent(applicationStart);
  }

  @Override
  public final void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
    // 转发应用结束事件到统一入口
    onEvent(applicationEnd);
  }

  @Override
  public final void onExecutorMetricsUpdate(
      SparkListenerExecutorMetricsUpdate executorMetricsUpdate) {
    // 转发Executor指标更新事件到统一入口
    onEvent(executorMetricsUpdate);
  }

  @Override
  public final void onStageExecutorMetrics(
      SparkListenerStageExecutorMetrics executorMetrics) {
    // 转发阶段Executor指标事件到统一入口
    onEvent(executorMetrics);
  }

  @Override
  public final void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
    // 转发Executor添加事件到统一入口
    onEvent(executorAdded);
  }

  @Override
  public final void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {
    // 转发Executor移除事件到统一入口
    onEvent(executorRemoved);
  }

  @Override
  public final void onExecutorBlacklisted(SparkListenerExecutorBlacklisted executorBlacklisted) {
    // 转发Executor拉黑事件到统一入口
    onEvent(executorBlacklisted);
  }

  @Override
  public final void onExecutorExcluded(SparkListenerExecutorExcluded executorExcluded) {
    // 转发Executor排除事件到统一入口
    onEvent(executorExcluded);
  }

  @Override
  public void onExecutorBlacklistedForStage(
      SparkListenerExecutorBlacklistedForStage executorBlacklistedForStage) {
    // 转发阶段级Executor拉黑事件到统一入口
    onEvent(executorBlacklistedForStage);
  }

  @Override
  public void onExecutorExcludedForStage(
      SparkListenerExecutorExcludedForStage executorExcludedForStage) {
    // 转发阶段级Executor排除事件到统一入口
    onEvent(executorExcludedForStage);
  }

  @Override
  public void onNodeBlacklistedForStage(
      SparkListenerNodeBlacklistedForStage nodeBlacklistedForStage) {
    // 转发阶段级节点拉黑事件到统一入口
    onEvent(nodeBlacklistedForStage);
  }

  @Override
  public void onNodeExcludedForStage(
      SparkListenerNodeExcludedForStage nodeExcludedForStage) {
    // 转发阶段级节点排除事件到统一入口
    onEvent(nodeExcludedForStage);
  }

  @Override
  public final void onExecutorUnblacklisted(
      SparkListenerExecutorUnblacklisted executorUnblacklisted) {
    // 转发Executor取消拉黑事件到统一入口
    onEvent(executorUnblacklisted);
  }

  @Override
  public final void onExecutorUnexcluded(
      SparkListenerExecutorUnexcluded executorUnexcluded) {
    // 转发Executor取消排除事件到统一入口
    onEvent(executorUnexcluded);
  }

  @Override
  public final void onNodeBlacklisted(SparkListenerNodeBlacklisted nodeBlacklisted) {
    // 转发节点拉黑事件到统一入口
    onEvent(nodeBlacklisted);
  }

  @Override
  public final void onNodeExcluded(SparkListenerNodeExcluded nodeExcluded) {
    // 转发节点排除事件到统一入口
    onEvent(nodeExcluded);
  }

  @Override
  public final void onNodeUnblacklisted(SparkListenerNodeUnblacklisted nodeUnblacklisted) {
    // 转发节点取消拉黑事件到统一入口
    onEvent(nodeUnblacklisted);
  }

  @Override
  public final void onNodeUnexcluded(SparkListenerNodeUnexcluded nodeUnexcluded) {
    // 转发节点取消排除事件到统一入口
    onEvent(nodeUnexcluded);
  }

  @Override
  public void onBlockUpdated(SparkListenerBlockUpdated blockUpdated) {
    // 转发块更新事件到统一入口
    onEvent(blockUpdated);
  }

  @Override
  public void onSpeculativeTaskSubmitted(SparkListenerSpeculativeTaskSubmitted speculativeTask) {
    // 转发推测任务提交事件到统一入口
    onEvent(speculativeTask);
  }

  @Override
  public void onUnschedulableTaskSetAdded(
      SparkListenerUnschedulableTaskSetAdded unschedulableTaskSetAdded) {
    // 转发不可调度任务集添加事件到统一入口
    onEvent(unschedulableTaskSetAdded);
  }

  @Override
  public void onUnschedulableTaskSetRemoved(
      SparkListenerUnschedulableTaskSetRemoved unschedulableTaskSetRemoved) {
    // 转发不可调度任务集移除事件到统一入口
    onEvent(unschedulableTaskSetRemoved);
  }

  @Override
  public void onResourceProfileAdded(SparkListenerResourceProfileAdded event) {
    // 转发资源配置文件添加事件到统一入口
    onEvent(event);
  }

  @Override
  public void onOtherEvent(SparkListenerEvent event) {
    // 转发其他未分类事件到统一入口
    onEvent(event);
  }
}