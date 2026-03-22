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

package org.apache.spark.deploy.master

import scala.reflect.ClassTag

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rpc.RpcEnv

/**
 * 文件说明: Spark Standalone 集群 Master 节点持久化引擎抽象接口，负责持久化集群状态，支持 Master 故障恢复
 *
 * 持久化语义要求:
 *   - addApplication 和 addWorker 在新应用/Worker 注册完成前调用，确保注册信息已持久化
 *   - removeApplication 和 removeWorker 可在任意时间调用，删除对应持久化信息
 * 满足上述要求后，恢复时所有应用和Worker都会被持久化，但可能存在已经结束但尚未删除的条目，因此恢复时需要额外验证存活状态
 */
@DeveloperApi
/**
 * 持久化引擎抽象基类，定义Master状态持久化的统一接口，用于故障恢复场景
 * 负责存储和恢复应用、Driver、Worker的元数据信息，不同实现可对接不同存储介质
 */
abstract class PersistenceEngine {

  /**
   * 持久化指定对象到存储介质
   * @param name 对象名称，作为持久化键
   * @param obj 要持久化的对象
   */
  def persist(name: String, obj: Object): Unit

  /**
   * 从存储介质中删除指定名称的持久化对象
   * @param name 要删除的对象名称
   */
  def unpersist(name: String): Unit

  /**
   * 读取所有名称前缀匹配的对象，反序列化为指定类型返回
   * @param prefix 对象名称前缀
   * @tparam T 返回对象的类型
   * @return 匹配的对象序列
   */
  def read[T: ClassTag](prefix: String): Seq[T]

  /**
   * 添加应用信息到持久化存储
   * @param app 应用信息对象
   */
  final def addApplication(app: ApplicationInfo): Unit = {
    persist("app_" + app.id, app)
  }

  /**
   * 从持久化存储删除应用信息
   * @param app 应用信息对象
   */
  final def removeApplication(app: ApplicationInfo): Unit = {
    unpersist("app_" + app.id)
  }

  /**
   * 添加Worker信息到持久化存储
   * @param worker Worker信息对象
   */
  final def addWorker(worker: WorkerInfo): Unit = {
    persist("worker_" + worker.id, worker)
  }

  /**
   * 从持久化存储删除Worker信息
   * @param worker Worker信息对象
   */
  final def removeWorker(worker: WorkerInfo): Unit = {
    unpersist("worker_" + worker.id)
  }

  /**
   * 添加Driver信息到持久化存储
   * @param driver Driver信息对象
   */
  final def addDriver(driver: DriverInfo): Unit = {
    persist("driver_" + driver.id, driver)
  }

  /**
   * 从持久化存储删除Driver信息
   * @param driver Driver信息对象
   */
  final def removeDriver(driver: DriverInfo): Unit = {
    unpersist("driver_" + driver.id)
  }

  /**
   * 读取所有持久化的集群状态数据，按创建时间排序（通过ID排序实现）
   * @param rpcEnv RPC环境，用于反序列化持久化对象
   * @return 持久化的应用、Driver、Worker信息三元组
   */
  final def readPersistedData(
      rpcEnv: RpcEnv): (Seq[ApplicationInfo], Seq[DriverInfo], Seq[WorkerInfo]) = {
    // 在RPC环境中执行反序列化，恢复对象
    rpcEnv.deserialize { () =>
      (read[ApplicationInfo]("app_"), read[DriverInfo]("driver_"), read[WorkerInfo]("worker_"))
    }
  }

  /**
   * 关闭持久化引擎，释放资源，默认空实现
   */
  def close(): Unit = {}
}

/**
 * 空实现持久化引擎，不做任何实际持久化操作，用于不需要故障恢复的场景
 * 所有持久化操作都是空实现，读取始终返回空列表
 */
private[master] class BlackHolePersistenceEngine extends PersistenceEngine {

  override def persist(name: String, obj: Object): Unit = {}

  override def unpersist(name: String): Unit = {}

  override def read[T: ClassTag](name: String): Seq[T] = Nil

}