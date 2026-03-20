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

package org.apache.spark

import java.io.Serializable
import java.util.Objects

/**
 * Job artifact state. For example, Spark Connect client sets the state specifically
 * for the current client.
 *
 * @param uuid UUID to use in the current context of jab artifact set. Usually this is from
 *             a Spark Connect client.
 * @param replClassDirUri The URI for the directory that stores REPL classes.
 */
private[spark] case class JobArtifactState(uuid: String, replClassDirUri: Option[String])

/**
 * Artifact set for a job.
 * This class is used to store session (i.e `SparkSession`) specific resources/artifacts.
 *
 * When Spark Connect is used, this job-set points towards session-specific jars and class files.
 * Note that Spark Connect is not a requirement for using this class.
 *
 * @param state Job artifact state.
 * @param jars Jars belonging to this session.
 * @param files Files belonging to this session.
 * @param archives Archives belonging to this session.
 */
private[spark] class JobArtifactSet(
    val state: Option[JobArtifactState],
    val jars: Map[String, Long],
    val files: Map[String, Long],
    val archives: Map[String, Long]) extends Serializable {
  override def hashCode(): Int = {
    Objects.hash(state, jars.toSeq, files.toSeq, archives.toSeq)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: JobArtifactSet =>
        this.getClass == that.getClass && this.state == that.state &&
          this.jars.toSeq == that.jars.toSeq &&
          this.files.toSeq == that.files.toSeq && this.archives.toSeq == that.archives.toSeq
    }
  }
}


/** JobArtifactSet 伴生对象，管理线程级别的会话 Artifact 状态 */
private[spark] object JobArtifactSet {
  // 空集合，用于测试
  val emptyJobArtifactSet: JobArtifactSet = new JobArtifactSet(
    None, Map.empty, Map.empty, Map.empty)
  // 获取默认 Artifact 集合，用于测试
  def defaultJobArtifactSet: JobArtifactSet = SparkContext.getActive.map(
    getActiveOrDefault).getOrElse(emptyJobArtifactSet)
  // 最近一次设置的状态，用于测试
  var lastSeenState: Option[JobArtifactState] = None

  // 线程本地变量，保存当前线程的 Spark Connect 客户端会话状态
  private[this] val currentClientSessionState: ThreadLocal[Option[JobArtifactState]] =
    new ThreadLocal[Option[JobArtifactState]] {
      override def initialValue(): Option[JobArtifactState] = None
    }

  /** 获取当前线程的 JobArtifactState */
  def getCurrentJobArtifactState: Option[JobArtifactState] = currentClientSessionState.get()

  /**
   * 在指定的 Artifact 状态下执行代码块。
   * 通过 ThreadLocal 设置 Spark Connect 客户端的会话信息，执行完毕后恢复原状态。
   */
  def withActiveJobArtifactState[T](state: JobArtifactState)(block: => T): T = {
    val oldState = currentClientSessionState.get()
    currentClientSessionState.set(Option(state))
    lastSeenState = Option(state)
    try block finally {
      currentClientSessionState.set(oldState)
    }
  }

  /**
   * 获取当前活跃的或默认的 JobArtifactSet。
   * 如果有活跃的 Spark Connect 客户端，使用其会话级别的 jars/files/archives；
   * 否则回退到 SparkContext 的共享资源。
   */
  def getActiveOrDefault(sc: SparkContext): JobArtifactSet = {
    val maybeState = currentClientSessionState.get().map(s => s.copy(
        replClassDirUri = s.replClassDirUri.orElse(sc.conf.getOption("spark.repl.class.uri"))))
    new JobArtifactSet(
      state = maybeState,
      jars = maybeState
        .map(s => sc.addedJars.getOrElse(s.uuid, Map.empty[String, Long]))
        .getOrElse(sc.allAddedJars).toMap,
      files = maybeState
        .map(s => sc.addedFiles.getOrElse(s.uuid, Map.empty[String, Long]))
        .getOrElse(sc.allAddedFiles).toMap,
      archives = maybeState
        .map(s => sc.addedArchives.getOrElse(s.uuid, Map.empty[String, Long]))
        .getOrElse(sc.allAddedArchives).toMap)
  }
}
