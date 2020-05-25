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

package org.apache.spark.scheduler

/**
  * A backend interface for scheduling systems that allows plugging in different ones under
  * TaskSchedulerImpl. We assume a Mesos-like model where the application gets resource offers as
  * machines become available and can launch tasks on them.
  * SchedulerBackend是TaskScheduler的调度后端接口。
  * TaskScheduler给Task分配资源实际是通过SchedulerBackend来完成的，
  * SchedulerBackend给Task分配完资源后将与分配给Task的Executor通信，并要求后者运行Task
  */
private[spark] trait SchedulerBackend {

  /**
    * 当前Job相关的应用程序的身份标识
    */
  private val appId = "spark-application-" + System.currentTimeMillis

  /**
    * 启动SchedulerBackend
    */
  def start(): Unit

  /**
    * 停止SchedulerBackend
    */
  def stop(): Unit

  /**
    * 给调度池中的所有Task分配资源
    */
  def reviveOffers(): Unit

  /**
    * 获取Job的默认并行度
    * @return
    */
  def defaultParallelism(): Int

  /**
    * Requests that an executor kills a running task.
    * 杀死指定的任务。 可以设置interruptThread为true来中断任务执行线程
    *
    * @param taskId          Id of the task.
    * @param executorId      Id of the executor the task is running on.
    * @param interruptThread Whether the executor should interrupt the task thread.
    * @param reason          The reason for the task kill.
    */
  def killTask(
                taskId: Long,
                executorId: String,
                interruptThread: Boolean,
                reason: String): Unit =
    throw new UnsupportedOperationException

  /**
    * SchedulerBackend是否准备就绪
    * @return
    */
  def isReady(): Boolean = true

  /**
    * Get an application ID associated with the job.
    * 获取appId
    *
    * @return An application ID
    */
  def applicationId(): String = appId

  /**
    * Get the attempt ID for this run, if the cluster manager supports multiple
    * attempts. Applications run in client mode will not have attempt IDs.
    * 当应用在cluster模式运行且集群管理器支持应用进行对此尝试时，此方法可以获取应用程序尝试的标识。
    * 当前应用程序在client模式运行时，将不支持多次尝试，因此此方法不会获取到应用程序尝试的标识
    *
    * @return The application attempt id, if available.
    */
  def applicationAttemptId(): Option[String] = None

  /**
    * Get the URLs for the driver logs. These URLs are used to display the links in the UI
    * Executors tab for the driver.
    * 获取Driver日志的URL， 这些将被应用于在SparkUI的Executors标签页中展示
    *
    * @return Map containing the log names and their respective URLs
    */
  def getDriverLogUrls: Option[Map[String, String]] = None

  /**
    * Get the attributes on driver. These attributes are used to replace log URLs when
    * custom log url pattern is specified.
    *
    * @return Map containing attributes on driver.
    */
  def getDriverAttributes: Option[Map[String, String]] = None

  /**
    * Get the max number of tasks that can be concurrent launched currently.
    * Note that please don't cache the value returned by this method, because the number can change
    * due to add/remove executors.
    *
    * @return The max number of tasks that can be concurrent launched currently.
    */
  def maxNumConcurrentTasks(): Int

}
