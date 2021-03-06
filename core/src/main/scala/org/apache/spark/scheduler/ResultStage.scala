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

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

/**
  * ResultStages apply a function on some partitions of an RDD to compute the result of an action.
  * The ResultStage object captures the function to execute, `func`, which will be applied to each
  * partition, and the set of partition IDs, `partitions`. Some stages may not run on all partitions
  * of the RDD, for actions like first() and lookup().
  *
  * 使用指定的函数对RDD中的分区进行计算并得到最终结果。
  * ResultStage是最后执行的Stage，此阶段主要进行作业的收尾工作（例如，对各个分区的数量收拢、打印到控制台或写入到HDFS）
  *
  *
  */
private[spark] class ResultStage(
                                  id: Int,
                                  rdd: RDD[_],
                                //即对RDD的分区进行计算的函数。
                                  val func: (TaskContext, Iterator[_]) => _,
                                //由RDD的各个分区的索引组成的数组
                                  val partitions: Array[Int],
                                  parents: List[Stage],
                                  firstJobId: Int,
                                  callSite: CallSite)
  extends Stage(id, rdd, partitions.length, parents, firstJobId, callSite) {

  /**
    * The active job for this result stage. Will be empty if the job has already finished
    * (e.g., because the job was cancelled).
    * ResultStage处理的 ActiveJob
    */
  private[this] var _activeJob: Option[ActiveJob] = None

  def activeJob: Option[ActiveJob] = _activeJob

  def setActiveJob(job: ActiveJob): Unit = {
    _activeJob = Option(job)
  }

  def removeActiveJob(): Unit = {
    _activeJob = None
  }

  /**
    * Returns the sequence of partition ids that are missing (i.e. needs to be computed).
    *
    * This can only be called when there is an active job.
    *
    * 用于找出当前JOb的所有分区中还没有完成的分区的索引。
    * ResultStage判断一个分区是否完成，是通过ActiveJob的Boolean类型少数组finished，
    * 因为finished记录了每个分区是否完成
    */
  override def findMissingPartitions(): Seq[Int] = {
    val job = activeJob.get
    (0 until job.numPartitions).filter(id => !job.finished(id))
  }

  override def toString: String = "ResultStage " + id
}
