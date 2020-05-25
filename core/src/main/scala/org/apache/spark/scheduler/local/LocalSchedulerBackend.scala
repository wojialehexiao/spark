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

package org.apache.spark.scheduler.local

import java.io.File
import java.net.URL
import java.nio.ByteBuffer

import org.apache.spark.TaskState.TaskState
import org.apache.spark.executor.{Executor, ExecutorBackend}
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext, SparkEnv, TaskState}

private case class ReviveOffers()

private case class StatusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer)

private case class KillTask(taskId: Long, interruptThread: Boolean, reason: String)

private case class StopExecutor()

/**
  * Calls to [[LocalSchedulerBackend]] are all serialized through LocalEndpoint. Using an
  * RpcEndpoint makes the calls on [[LocalSchedulerBackend]] asynchronous, which is necessary
  * to prevent deadlock between [[LocalSchedulerBackend]] and the [[TaskSchedulerImpl]].
  * LocalSchedulerBackend与其他组件的通信都依赖与LocalEndpoint
  */
private[spark] class LocalEndpoint(

                                    override val rpcEnv: RpcEnv,

                                  //用户指定的classpath
                                    userClassPath: Seq[URL],

                                  //Driver中的TaskSchedulerImpl
                                    scheduler: TaskSchedulerImpl,

                                  //LocalEndpoint相关联的LocalSchedulerBackend
                                    executorBackend: LocalSchedulerBackend,

                                  //用于执行任务的CPU核数
                                    private val totalCores: Int)  extends ThreadSafeRpcEndpoint with Logging {


  /**
    * 空闲的CPU内核数
    */
  private var freeCores = totalCores

  /**
    * local部署模式下，与Driver处于同一JVM进程的Executor的身份标识。
    * 由于LocalEndPoint只在local模式中使用， 一次localExecutorId固定为Driver
    */
  val localExecutorId = SparkContext.DRIVER_IDENTIFIER

  /**
    * 与Driver处于同一JVM进程的Executor所在的host
    * 由于LocalEndPoint只在local模式中使用， 一次localExecutorHostname固定为localhost
    */
  val localExecutorHostname = Utils.localCanonicalHostName()

  /**
    * 与Driver处于同一JVM进程的Executor。由于totalCorses等于1，因此应用本地有且只有一个Executor
    *
    */
  private val executor = new Executor(
    localExecutorId, localExecutorHostname, SparkEnv.get, userClassPath, isLocal = true)


  /**
    * 在处理ReviveOffers、StatusUpdate是都会调用reviveOffers方法给Task分配资源
    * @return
    */
  override def receive: PartialFunction[Any, Unit] = {
    case ReviveOffers =>
      reviveOffers()

    case StatusUpdate(taskId, state, serializedData) =>
      scheduler.statusUpdate(taskId, state, serializedData)
      if (TaskState.isFinished(state)) {
        freeCores += scheduler.CPUS_PER_TASK
        reviveOffers()
      }

    case KillTask(taskId, interruptThread, reason) =>
      executor.killTask(taskId, interruptThread, reason)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case StopExecutor =>
      executor.stop()
      context.reply(true)
  }

  /**
    *
    */
  def reviveOffers(): Unit = {
    // local mode doesn't support extra resources like GPUs right now
    //创建一个只包含一个WorkerOffer的序列
    val offers = IndexedSeq(new WorkerOffer(localExecutorId, localExecutorHostname, freeCores,
      Some(rpcEnv.address.hostPort)))


    //给Task分配资源  scheduler.resourceOffers重要
    for (task <- scheduler.resourceOffers(offers).flatten) {

      //将空闲CPU核心数减1
      freeCores -= scheduler.CPUS_PER_TASK

      //运行Task
      executor.launchTask(executorBackend, task)
    }
  }
}

/**
  * Used when running a local version of Spark where the executor, backend, and master all run in
  * the same JVM. It sits behind a [[TaskSchedulerImpl]] and handles launching tasks on a single
  * Executor (created by the [[LocalSchedulerBackend]]) running locally.
  */
private[spark] class LocalSchedulerBackend(
                                            conf: SparkConf,
                                            scheduler: TaskSchedulerImpl,
                                          //固定为1
                                            val totalCores: Int)  extends SchedulerBackend with ExecutorBackend with Logging {


  private val appId = "local-" + System.currentTimeMillis

  /**
    * 即LocalEndpoint的NettyRpcEndpoint
    */
  private var localEndpoint: RpcEndpointRef = null

  // 用于指定的ClassPath
  private val userClassPath = getUserClasspath(conf)

  /**
    *
    */
  private val listenerBus = scheduler.sc.listenerBus


  /**
    * LauncherBackend的匿名实现类的实例。
    * 此匿名实现类实现了LauncherBackend的onStopRequest方法，用于停止Executor、将
    * launcherBackend的状态标记为KILLED，关闭LauncherServer和LauncherBackend之间的Socket连接
    */
  private val launcherBackend = new LauncherBackend() {
    override def conf: SparkConf = LocalSchedulerBackend.this.conf

    override def onStopRequest(): Unit = stop(SparkAppHandle.State.KILLED)
  }

  /**
    * Returns a list of URLs representing the user classpath.
    *
    * @param conf Spark configuration.
    */
  def getUserClasspath(conf: SparkConf): Seq[URL] = {
    val userClassPathStr = conf.get(config.EXECUTOR_CLASS_PATH)
    userClassPathStr.map(_.split(File.pathSeparator)).toSeq.flatten.map(new File(_).toURI.toURL)
  }


  launcherBackend.connect()

  override def start(): Unit = {
    val rpcEnv = SparkEnv.get.rpcEnv
    val executorEndpoint = new LocalEndpoint(rpcEnv, userClassPath, scheduler, this, totalCores)
    localEndpoint = rpcEnv.setupEndpoint("LocalSchedulerBackendEndpoint", executorEndpoint)
    listenerBus.post(SparkListenerExecutorAdded(
      System.currentTimeMillis,
      executorEndpoint.localExecutorId,
      new ExecutorInfo(executorEndpoint.localExecutorHostname, totalCores, Map.empty,
        Map.empty)))

    //发送SetAppId消息
    launcherBackend.setAppId(appId)
    //发送setState消息
    launcherBackend.setState(SparkAppHandle.State.RUNNING)
  }

  override def stop(): Unit = {
    stop(SparkAppHandle.State.FINISHED)
  }

  /**
    * LocalEndpoint接收到ReviveOffers消息后，会调用
    * reviveOffers方法给下一个要调度的Task分配资源并运行Task
    */
  override def reviveOffers(): Unit = {
    localEndpoint.send(ReviveOffers)
  }

  override def defaultParallelism(): Int =
    scheduler.conf.getInt("spark.default.parallelism", totalCores)

  override def killTask(
                         taskId: Long, executorId: String, interruptThread: Boolean, reason: String): Unit = {
    localEndpoint.send(KillTask(taskId, interruptThread, reason))
  }

  /**
    * Task状态的更新。LocalEndpoint接收到StatusUpdate消息后，会首先调用TaskSchedulerImpl的statusUpdate方法更新Task状态
    * @param taskId
    * @param state
    * @param serializedData
    */
  override def statusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer): Unit = {
    localEndpoint.send(StatusUpdate(taskId, state, serializedData))
  }

  override def applicationId(): String = appId

  override def maxNumConcurrentTasks(): Int = totalCores / scheduler.CPUS_PER_TASK

  private def stop(finalState: SparkAppHandle.State): Unit = {
    localEndpoint.ask(StopExecutor)
    try {
      launcherBackend.setState(finalState)
    } finally {
      launcherBackend.close()
    }
  }

}
