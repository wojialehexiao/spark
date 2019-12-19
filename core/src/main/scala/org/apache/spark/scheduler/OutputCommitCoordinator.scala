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

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.util.{RpcUtils, ThreadUtils}

import scala.collection.mutable

private sealed trait OutputCommitCoordinationMessage extends Serializable

private case object StopCoordinator extends OutputCommitCoordinationMessage

private case class AskPermissionToCommitOutput(
                                                stage: Int,
                                                stageAttempt: Int,
                                                partition: Int,
                                                attemptNumber: Int)

/**
  * Authority that decides whether tasks can commit output to HDFS. Uses a "first committer wins"
  * policy.
  *
  * OutputCommitCoordinator is instantiated in both the drivers and executors. On executors, it is
  * configured with a reference to the driver's OutputCommitCoordinatorEndpoint, so requests to
  * commit output will be forwarded to the driver's OutputCommitCoordinator.
  *
  * This class was introduced in SPARK-4879; see that JIRA issue (and the associated pull requests)
  * for an extensive design discussion.
  *
  * 用于判断各个stage的分区任务是否有权限将输出提交到HDFS， 并对同一分区任务的多次任务尝试进行协调
  */
private[spark] class OutputCommitCoordinator(conf: SparkConf, isDriver: Boolean) extends Logging {

  // Initialized by SparkEnv
  //OutputCommitCoordinatorEndpoint的NettyRpcEndPointRef引用
  var coordinatorRef: Option[RpcEndpointRef] = None

  // Class used to identify a committer. The task ID for a committer is implicitly defined by
  // the partition being processed, but the coordinator needs to keep track of both the stage
  // attempt and the task attempt, because in some situations the same task may be running
  // concurrently in two different attempts of the same stage.
  private case class TaskIdentifier(stageAttempt: Int, taskAttempt: Int)

  private case class StageState(numPartitions: Int) {
    val authorizedCommitters = Array.fill[TaskIdentifier](numPartitions)(null)
    val failures = mutable.Map[Int, mutable.Set[TaskIdentifier]]()
  }

  /**
    * Map from active stages's id => authorized task attempts for each partition id, which hold an
    * exclusive lock on committing task output for that partition, as well as any known failed
    * attempts in the stage.
    *
    * Entries are added to the top-level map when stages start and are removed they finish
    * (either successfully or unsuccessfully).
    *
    * Access to this map should be guarded by synchronizing on the OutputCommitCoordinator instance.
    *
    */
  private val stageStates = mutable.Map[Int, StageState]()

  /**
    * Returns whether the OutputCommitCoordinator's internal data structures are all empty.
    */
  def isEmpty: Boolean = {
    stageStates.isEmpty
  }

  /**
    * Called by tasks to ask whether they can commit their output to HDFS.
    *
    * If a task attempt has been authorized to commit, then all other attempts to commit the same
    * task will be denied.  If the authorized task attempt fails (e.g. due to its executor being
    * lost), then a subsequent task attempt may be authorized to commit its output.
    *
    * 用于向OutputCommitCoordinatorEndpoint发送AskPermissionToCommitOutput，
    * 并根据响应确认是否有权限将stage的指定分区的输出提交到HDFS
    *
    * @param stage         the stage number
    * @param partition     the partition number
    * @param attemptNumber how many times this task has been attempted
    *                      (see [[TaskContext.attemptNumber()]])
    * @return true if this task is authorized to commit, false otherwise
    */
  def canCommit(
                 stage: Int,
                 stageAttempt: Int,
                 partition: Int,
                 attemptNumber: Int): Boolean = {
    val msg = AskPermissionToCommitOutput(stage, stageAttempt, partition, attemptNumber)
    coordinatorRef match {

      //询问是否有权限将stage的指定分区的输出提交到HDFS上
      case Some(endpointRef) =>
        ThreadUtils.awaitResult(endpointRef.ask[Boolean](msg),
          RpcUtils.askRpcTimeout(conf).duration)


      case None =>
        logError(
          "canCommit called after coordinator was stopped (is SparkEnv shutdown in progress)?")
        false
    }
  }

  /**
    * Called by the DAGScheduler when a stage starts. Initializes the stage's state if it hasn't
    * yet been initialized.
    *
    * 用于启动给定的输出提交到HDFS的协调机制，
    * 其实质为创建给定Stage对应的StageState --与书上不一致
    *
    * @param stage          the stage id.
    * @param maxPartitionId the maximum partition id that could appear in this stage's tasks (i.e.
    *                       the maximum possible value of `context.partitionId`).
    */
  private[scheduler] def stageStart(stage: Int, maxPartitionId: Int): Unit = synchronized {
    stageStates.get(stage) match {
      case Some(state) =>
        require(state.authorizedCommitters.length == maxPartitionId + 1)
        logInfo(s"Reusing state from previous attempt of stage $stage.")

      case _ =>
        stageStates(stage) = new StageState(maxPartitionId + 1)
    }
  }

  // Called by DAGScheduler
  /**
    * 停止给定的stage的输出提交到HFDS的协调机制
    * 实质为删除stageStates对应的 StageState
    * @param stage
    */
  private[scheduler] def stageEnd(stage: Int): Unit = synchronized {
    stageStates.remove(stage)
  }

  // Called by DAGScheduler
  /**
    * 给定stage的给定分区完成后将调用
    *
    * @param stage
    * @param stageAttempt
    * @param partition
    * @param attemptNumber
    * @param reason
    */
  private[scheduler] def taskCompleted(
                                        stage: Int,
                                        stageAttempt: Int,
                                        partition: Int,
                                        attemptNumber: Int,
                                        reason: TaskEndReason): Unit = synchronized {
    val stageState = stageStates.getOrElse(stage, {
      logDebug(s"Ignoring task completion for completed stage")
      return
    })
    reason match {

        //任务执行成功
      case Success =>
      // The task output has been committed successfully



        //任务提交被拒绝
      case _: TaskCommitDenied =>
        logInfo(s"Task was denied committing, stage: $stage.$stageAttempt, " +
          s"partition: $partition, attempt: $attemptNumber")


        //其他原因
      case _ =>
        // Mark the attempt as failed to blacklist from future commit protocol
        val taskId = TaskIdentifier(stageAttempt, attemptNumber)
        stageState.failures.getOrElseUpdate(partition, mutable.Set()) += taskId
        if (stageState.authorizedCommitters(partition) == taskId) {
          logDebug(s"Authorized committer (attemptNumber=$attemptNumber, stage=$stage, " +
            s"partition=$partition) failed; clearing lock")


          stageState.authorizedCommitters(partition) = null
        }
    }
  }

  /**
    * 通过向OutputCommitCoordinatorEndpoint发送StopCoordinator消息以停止OutputCommitCoordinatorEndpoint，
    * 然后清空stageStates
    */
  def stop(): Unit = synchronized {
    if (isDriver) {
      coordinatorRef.foreach(_ send StopCoordinator)
      coordinatorRef = None
      stageStates.clear()
    }
  }

  // Marked private[scheduler] instead of private so this can be mocked in tests
  /**
    * 判断给定的任务尝试是否有权限将给定Stage的指定分区的数据提交到HDFS
    *
    * @param stage
    * @param stageAttempt
    * @param partition
    * @param attemptNumber
    * @return
    */
  private[scheduler] def handleAskPermissionToCommit(
                                                      stage: Int,
                                                      stageAttempt: Int,
                                                      partition: Int,
                                                      attemptNumber: Int): Boolean = synchronized {

    //从stageStates找到指定stage指定分区的StageState
    stageStates.get(stage) match {

      case Some(state) if attemptFailed(state, stageAttempt, partition, attemptNumber) =>
        logInfo(s"Commit denied for stage=$stage.$stageAttempt, partition=$partition: " +
          s"task attempt $attemptNumber already marked as failed.")
        false


      case Some(state) =>

        val existing = state.authorizedCommitters(partition)
        if (existing == null) {
          logDebug(s"Commit allowed for stage=$stage.$stageAttempt, partition=$partition, " +
            s"task attempt $attemptNumber")
          state.authorizedCommitters(partition) = TaskIdentifier(stageAttempt, attemptNumber)
          true
        } else {
          logDebug(s"Commit denied for stage=$stage.$stageAttempt, partition=$partition: " +
            s"already committed by $existing")
          false
        }

      case None =>
        logDebug(s"Commit denied for stage=$stage.$stageAttempt, partition=$partition: " +
          "stage already marked as completed.")
        false
    }
  }

  private def attemptFailed(
                             stageState: StageState,
                             stageAttempt: Int,
                             partition: Int,
                             attempt: Int): Boolean = synchronized {
    val failInfo = TaskIdentifier(stageAttempt, attempt)
    stageState.failures.get(partition).exists(_.contains(failInfo))
  }
}

private[spark] object OutputCommitCoordinator {

  // This endpoint is used only for RPC
  private[spark] class OutputCommitCoordinatorEndpoint(
                                                        override val rpcEnv: RpcEnv, outputCommitCoordinator: OutputCommitCoordinator)
    extends RpcEndpoint with Logging {

    logDebug("init") // force eager creation of logger

    override def receive: PartialFunction[Any, Unit] = {
      case StopCoordinator =>
        logInfo("OutputCommitCoordinator stopped!")
        stop()
    }


    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

      //确认客户端时候有权限将输出提交到HDFS
      case AskPermissionToCommitOutput(stage, stageAttempt, partition, attemptNumber) =>
        context.reply(
          outputCommitCoordinator.handleAskPermissionToCommit(stage, stageAttempt, partition,
            attemptNumber))
    }
  }

}
