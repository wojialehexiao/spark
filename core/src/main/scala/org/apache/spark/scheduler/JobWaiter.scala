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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.internal.Logging

import scala.concurrent.{Future, Promise}

/**
  * An object that waits for a DAGScheduler job to complete. As tasks finish, it passes their
  * results to the given handler function.
  */
private[spark] class JobWaiter[T](
                                 // DAGScheduler, 当前JobWaiter等待执行完成的Job调度者。
                                   dagScheduler: DAGScheduler,
                                 //当前JobWaiter等待执行完成的Job的身份标识
                                   val jobId: Int,
                                 //等待完成的Job包括的Task数量
                                   totalTasks: Int,
                                 //执行结果的处理器
                                   resultHandler: (Int, T) => Unit)  extends JobListener with Logging {

  /**
    * 等待完成的Job中已经完成的Task数量
    */
  private val finishedTasks = new AtomicInteger(0)

  // If the job is finished, this will be its result. In the case of 0 task jobs (e.g. zero
  // partition RDDs), we set the jobResult directly to JobSucceeded.
  /**
    * 用来代表Job完成后的结果。如果totalTasks为零，说明没有Task需要执行，此时jobPromise将被直接设置为Success
    */
  private val jobPromise: Promise[Unit] =
  if (totalTasks == 0) Promise.successful(()) else Promise()

  /**
    * Job是否已经完成
    * @return
    */
  def jobFinished: Boolean = jobPromise.isCompleted

  /**
    *
    * @return
    */
  def completionFuture: Future[Unit] = jobPromise.future


  /**
    * Sends a signal to the DAGScheduler to cancel the job. The cancellation itself is handled
    * asynchronously. After the low level scheduler cancels all the tasks belonging to this job, it
    * will fail this job with a SparkException.
    * 取消对Job的执行。
    */
  def cancel(): Unit = {
    dagScheduler.cancelJob(jobId, None)
  }

  /**
    * JobWaiter重写的在特质JobListener中定义的方法
    * @param index
    * @param result
    */
  override def taskSucceeded(index: Int, result: Any): Unit = {
    // resultHandler call must be synchronized in case resultHandler itself is not thread safe.
    synchronized {
      //调用resultHandler函数来处理Job中每个Task的执行结果
      resultHandler(index, result.asInstanceOf[T])
    }

    //增加已完成的Task数量
    if (finishedTasks.incrementAndGet() == totalTasks) {

      //如果已经完成，那么jobPromise设置为Success
      jobPromise.success(())
    }
  }

  /**
    * JobWaiter重写的在特质JobListener中定义的方法
    * @param exception
    */
  override def jobFailed(exception: Exception): Unit = {
    //将jobPromise设置为Failure
    if (!jobPromise.tryFailure(exception)) {
      logWarning("Ignore failure", exception)
    }
  }

}
