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

import java.util.Properties

import org.apache.spark.util.CallSite

/**
  * A running job in the DAGScheduler. Jobs can be of two types: a result job, which computes a
  * ResultStage to execute an action, or a map-stage job, which computes the map outputs for a
  * ShuffleMapStage before any downstream stages are submitted. The latter is used for adaptive
  * query planning, to look at map output statistics before submitting later stages. We distinguish
  * between these two types of jobs using the finalStage field of this class.
  *
  * Jobs are only tracked for "leaf" stages that clients directly submitted, through DAGScheduler's
  * submitJob or submitMapStage methods. However, either type of job may cause the execution of
  * other earlier stages (for RDDs in the DAG it depends on), and multiple jobs may share some of
  * these previous stages. These dependencies are managed inside DAGScheduler.
  *
  * ActiveJob用来表示已经激活的Job，即被DAGScheduler
  *
  * @param jobId      A unique ID for this job.
  * @param finalStage The stage that this job computes (either a ResultStage for an action or a
  *                   ShuffleMapStage for submitMapStage).
  * @param callSite   Where this job was initiated in the user's program (shown on UI).
  * @param listener   A listener to notify if tasks in this job finish or the job fails.
  * @param properties Scheduling properties attached to the job, such as fair scheduler pool name.
  */
private[spark] class ActiveJob(
                              //Job的身份标识
                                val jobId: Int,
                              //Job的最下游Stage
                                val finalStage: Stage,
                              //应用程序调用栈
                                val callSite: CallSite,
                              //监听当前的Job的JobListener
                                val listener: JobListener,
                              //包含了当前Job的调度、Job group、描述等属性的Properties
                                val properties: Properties) {

  /**
    * Number of partitions we need to compute for this job. Note that result stages may not need
    * to compute all partitions in their target RDD, for actions like first() and lookup().
    *
    * 当前Job的分区数量。如果finalStage为ResultStage， 那么此属性等于ResultStage的partitions属性的长度
    * 如果finalStage为ShuffleMapStage， 那么此属性等于ShuffleMapStage的RDD的partitions属性的长度
    *
    */
  val numPartitions = finalStage match {
    case r: ResultStage => r.partitions.length
    case m: ShuffleMapStage => m.rdd.partitions.length
  }

  /**
    * Which partitions of the stage have finished
    * 每个数组索引代表一个分区的任务是否执行完成
    * */
  val finished = Array.fill[Boolean](numPartitions)(false)

  /**
    * 当前Job的所有任务中已经完成的任务数量
    */
  var numFinished = 0

  /** Resets the status of all partitions in this stage so they are marked as not finished. */
  def resetAllPartitions(): Unit = {
    (0 until numPartitions).foreach(finished.update(_, false))
    numFinished = 0
  }
}
