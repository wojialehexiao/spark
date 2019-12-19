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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.RDDInfo

import scala.collection.mutable.HashMap

/**
  * :: DeveloperApi ::
  * Stores information about a stage to pass from the scheduler to SparkListeners.
  *
  * 用于描述Stage信息，并可以传给SparkListener
  */
@DeveloperApi
class StageInfo(
               //Stage的id
                 val stageId: Int,
               //当前尝试的ID
                 private val attemptId: Int,
               //当前Stage的名称
                 val name: String,
               //当前Stage的Task数量
                 val numTasks: Int,
               //RDD信息（即RDDInfo）的序列
                 val rddInfos: Seq[RDDInfo],
               //当前Stage的父Stage的身份表示序列
                 val parentIds: Seq[Int],
               //详细的线程信息
                 val details: String,
               //Task的度量信息
                 val taskMetrics: TaskMetrics = null,
               //用于存储任务的本地性偏好
                 private[spark] val taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty,

                 private[spark] val shuffleDepId: Option[Int] = None) {
  /**
    * When this stage was submitted from the DAGScheduler to a TaskScheduler.
    * DAGScheduler将当前Stage提交给TaskScheduler的时间
    * */
  var submissionTime: Option[Long] = None

  /**
    * Time when all tasks in the stage completed or when the stage was cancelled.
    * 当前Stage中的所有Task完成的时间或被取消的时间
    * */
  var completionTime: Option[Long] = None
  /**
    * If the stage failed, the reason why.
    * 如果失败了  记录失败原因
    * */
  var failureReason: Option[String] = None

  /**
    * Terminal values of accumulables updated during this stage, including all the user-defined
    * accumulators.
    * 存储了所有聚合器计算的最终值
    */
  val accumulables = HashMap[Long, AccumulableInfo]()

  def stageFailed(reason: String): Unit = {
    failureReason = Some(reason)
    completionTime = Some(System.currentTimeMillis)
  }

  // This would just be the second constructor arg, except we need to maintain this method
  // with parentheses for compatibility
  def attemptNumber(): Int = attemptId

  private[spark] def getStatusString: String = {
    if (completionTime.isDefined) {
      if (failureReason.isDefined) {
        "failed"
      } else {
        "succeeded"
      }
    } else {
      "running"
    }
  }
}

private[spark] object StageInfo {
  /**
    * Construct a StageInfo from a Stage.
    *
    * Each Stage is associated with one or many RDDs, with the boundary of a Stage marked by
    * shuffle dependencies. Therefore, all ancestor RDDs related to this Stage's RDD through a
    * sequence of narrow dependencies should also be associated with this Stage.
    */
  def fromStage(
                 stage: Stage,
                 attemptId: Int,
                 numTasks: Option[Int] = None,
                 taskMetrics: TaskMetrics = null,
                 taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty
               ): StageInfo = {

    //获取RDD的祖先依赖中属于窄依赖的RDD序列
    //对上一步中获得的RDD序列每一个RDD，调用RDDInfo伴生对象的fromRdd方法创建RDDInfo对象
    val ancestorRddInfos = stage.rdd.getNarrowAncestors.map(RDDInfo.fromRdd)
    //给当前Stage的RDD创建对应的RDDInfo对象，将上一步中创建的所有RDDInfo对象与此RDDInfo对象放入序列RDDInfo中
    val rddInfos = Seq(RDDInfo.fromRdd(stage.rdd)) ++ ancestorRddInfos


    val shuffleDepId = stage match {
      case sms: ShuffleMapStage => Option(sms.shuffleDep).map(_.shuffleId)
      case _ => None
    }

    //创建StageInfo
    new StageInfo(
      stage.id,
      attemptId,
      stage.name,
      numTasks.getOrElse(stage.numTasks),
      rddInfos,
      stage.parents.map(_.id),
      stage.details,
      taskMetrics,
      taskLocalityPreferences,
      shuffleDepId)
  }
}
