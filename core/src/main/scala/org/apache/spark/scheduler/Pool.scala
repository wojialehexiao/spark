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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * A Schedulable entity that represents collection of Pools or TaskSetManagers
  */
private[spark] class Pool(
                           val poolName: String,

                           //调度模式 FAIR, FIFO, NONE
                           val schedulingMode: SchedulingMode,
                         //minShare的初始值
                           initMinShare: Int,
                         //weigh的初始值
                           initWeight: Int) extends Schedulable with Logging {

  //用于储存子Schedulable
  val schedulableQueue = new ConcurrentLinkedQueue[Schedulable]
  //调度名称与Schedulable对应关系
  val schedulableNameToSchedulable = new ConcurrentHashMap[String, Schedulable]

  val weight = initWeight
  val minShare = initMinShare

  //正在运行的任务数
  var runningTasks = 0

  //进行调度的优先级
  val priority = 0

  // A pool's stage id is used to break the tie in scheduling.
  // 调度池或TaskSetManager所属Stage的身份标识
  var stageId = -1


  val name = poolName
  /**
    * 父Pool
    */
  var parent: Pool = null

  //调度算法默认FIFO
  private val taskSetSchedulingAlgorithm: SchedulingAlgorithm = {
    schedulingMode match {
      case SchedulingMode.FAIR =>
        new FairSchedulingAlgorithm()
      case SchedulingMode.FIFO =>
        new FIFOSchedulingAlgorithm()
      case _ =>
        val msg = s"Unsupported scheduling mode: $schedulingMode. Use FAIR or FIFO instead."
        throw new IllegalArgumentException(msg)
    }
  }


  override def addSchedulable(schedulable: Schedulable): Unit = {
    require(schedulable != null)
    schedulableQueue.add(schedulable)
    schedulableNameToSchedulable.put(schedulable.name, schedulable)
    schedulable.parent = this
  }

  override def removeSchedulable(schedulable: Schedulable): Unit = {
    schedulableQueue.remove(schedulable)
    schedulableNameToSchedulable.remove(schedulable.name)
  }

  //获取指定名称的Schedulable
  override def getSchedulableByName(schedulableName: String): Schedulable = {
    if (schedulableNameToSchedulable.containsKey(schedulableName)) {
      return schedulableNameToSchedulable.get(schedulableName)
    }

    //从子Schedulable查找
    for (schedulable <- schedulableQueue.asScala) {
      val sched = schedulable.getSchedulableByName(schedulableName)
      if (sched != null) {
        return sched
      }
    }
    null
  }


  /**
    * 当某个Executor丢失后， 将Executor标记为失败，并且重试
    * @param executorId
    * @param host
    * @param reason
    */
  override def executorLost(executorId: String, host: String, reason: ExecutorLossReason): Unit = {
    schedulableQueue.asScala.foreach(_.executorLost(executorId, host, reason))
  }


  /**
    * 检查当前Pool中是否有需要推断执行的任务，深度优先
    * @param minTimeToSpeculation
    * @return
    */
  override def checkSpeculatableTasks(minTimeToSpeculation: Int): Boolean = {
    var shouldRevive = false
    for (schedulable <- schedulableQueue.asScala) {
      shouldRevive |= schedulable.checkSpeculatableTasks(minTimeToSpeculation)
    }
    shouldRevive
  }

  /**
    * 对当前Pool中的所有TaskSetManager按照调度算法进行排序
    * @return
    */
  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
    val sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
    val sortedSchedulableQueue =
      schedulableQueue.asScala.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
    for (schedulable <- sortedSchedulableQueue) {
      sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue
    }
    sortedTaskSetQueue
  }

  /**
    * 增加当前正在运行任务数
    * @param taskNum
    */
  def increaseRunningTasks(taskNum: Int): Unit = {
    runningTasks += taskNum
    if (parent != null) {
      parent.increaseRunningTasks(taskNum)
    }
  }

  /**
    * 减少当前正在运行任务数
    * @param taskNum
    */
  def decreaseRunningTasks(taskNum: Int): Unit = {
    runningTasks -= taskNum
    if (parent != null) {
      parent.decreaseRunningTasks(taskNum)
    }
  }
}
