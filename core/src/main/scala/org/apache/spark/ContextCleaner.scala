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

import java.lang.ref.{ReferenceQueue, WeakReference}
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, ScheduledExecutorService, TimeUnit}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.rdd.{RDD, ReliableRDDCheckpointData}
import org.apache.spark.util.{AccumulatorContext, AccumulatorV2, ThreadUtils, Utils}

import scala.collection.JavaConverters._

/**
  * Classes that represent cleaning tasks.
  */
private sealed trait CleanupTask

private case class CleanRDD(rddId: Int) extends CleanupTask

private case class CleanShuffle(shuffleId: Int) extends CleanupTask

private case class CleanBroadcast(broadcastId: Long) extends CleanupTask

private case class CleanAccum(accId: Long) extends CleanupTask

private case class CleanCheckpoint(rddId: Int) extends CleanupTask

/**
  * A WeakReference associated with a CleanupTask.
  *
  * When the referent object becomes only weakly reachable, the corresponding
  * CleanupTaskWeakReference is automatically added to the given reference queue.
  */
private class CleanupTaskWeakReference(
                                        val task: CleanupTask,
                                        referent: AnyRef,
                                        referenceQueue: ReferenceQueue[AnyRef])
  extends WeakReference(referent, referenceQueue)

/**
  * 用于RDD，随机播放和广播状态的异步清除器。
  *
  * 当相关对象超出应用程序范围时，这将为要处理的每个RDD，ShuffleDependency和所关注的广播维护弱引用。
  * 实际清理是在单独的守护程序线程中执行的。
  */
private[spark] class ContextCleaner(sc: SparkContext) extends Logging {

  /**
    * 一个确保`CleanupTaskWeakReference`不被垃圾回收的缓冲区，只要它们没有被参考队列处理。
    *
    * 缓存AnyRef的虚引用
    */
  private val referenceBuffer =
    Collections.newSetFromMap[CleanupTaskWeakReference](new ConcurrentHashMap)

  /**
    * 缓存顶级的AnyRef
    */
  private val referenceQueue = new ReferenceQueue[AnyRef]

  private val listeners = new ConcurrentLinkedQueue[CleanerListener]()

  /**
    * 用于具体清理工作的线程
    */
  private val cleaningThread = new Thread() {
    override def run(): Unit = keepCleaning()
  }


  /**
    * 用于执行gc的调度线程池，只包含一个线程
    */
  private val periodicGCService: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("context-cleaner-periodic-gc")

  /**
    * 在此JVM中触发垃圾回收的时间间隔默认30分钟。
    *
    * 仅当弱引用被垃圾回收时，此上下文清除器才触发清除。
    * 在具有大型驱动程序JVM的长时间运行的应用程序中，驱动程序上的内存压力很小，这可能很少发生或根本没有发生。
    * 根本不清理可能会导致执行程序过一会儿耗尽磁盘空间。
    */
  private val periodicGCInterval = sc.conf.get(CLEANER_PERIODIC_GC_INTERVAL)

  /**
    * 清理非shuffle数据是否是阻塞的（除了shuffle以外，后者由spark.cleaner.referenceTracking.blocking.shuffle参数控制）。
    *
    * 由于SPARK-3015，默认情况下将其设置为true。
    * 这仅是该问题的临时解决方法，这最终是由BlockManager端点以高频率相互发布相互依赖的阻塞RPC消息的方式引起的。
    * 例如，当驱动程序执行GC并清除不再属于范围的所有广播块时，就会发生这种情况。
    */
  private val blockOnCleanupTasks = sc.conf.get(CLEANER_REFERENCE_TRACKING_BLOCKING)

  /**
    * 清理shuffle数据是否是阻塞的
    *
    * When context cleaner is configured to block on every delete request, it can throw timeout
    * exceptions on cleanup of shuffle blocks, as reported in SPARK-3139. To avoid that, this
    * parameter by default disables blocking on shuffle cleanups. Note that this does not affect
    * the cleanup of RDDs and broadcasts. This is intended to be a temporary workaround,
    * until the real RPC issue (referred to in the comment above `blockOnCleanupTasks`) is
    * resolved.
    */
  private val blockOnShuffleCleanupTasks =
    sc.conf.get(CLEANER_REFERENCE_TRACKING_BLOCKING_SHUFFLE)

  @volatile private var stopped = false

  /** Attach a listener object to get information of when objects are cleaned. */
  def attachListener(listener: CleanerListener): Unit = {
    listeners.add(listener)
  }

  /** Start the cleaner. */
  def start(): Unit = {
    cleaningThread.setDaemon(true)
    cleaningThread.setName("Spark Context Cleaner")
    cleaningThread.start()
    periodicGCService.scheduleAtFixedRate(() => System.gc(),
      periodicGCInterval, periodicGCInterval, TimeUnit.SECONDS)
  }

  /**
    * Stop the cleaning thread and wait until the thread has finished running its current task.
    */
  def stop(): Unit = {
    stopped = true
    // Interrupt the cleaning thread, but wait until the current task has finished before
    // doing so. This guards against the race condition where a cleaning thread may
    // potentially clean similarly named variables created by a different SparkContext,
    // resulting in otherwise inexplicable block-not-found exceptions (SPARK-6132).
    synchronized {
      cleaningThread.interrupt()
    }
    cleaningThread.join()
    periodicGCService.shutdown()
  }

  /** Register an RDD for cleanup when it is garbage collected. */
  def registerRDDForCleanup(rdd: RDD[_]): Unit = {
    registerForCleanup(rdd, CleanRDD(rdd.id))
  }

  def registerAccumulatorForCleanup(a: AccumulatorV2[_, _]): Unit = {
    registerForCleanup(a, CleanAccum(a.id))
  }

  /** Register a ShuffleDependency for cleanup when it is garbage collected. */
  def registerShuffleForCleanup(shuffleDependency: ShuffleDependency[_, _, _]): Unit = {
    registerForCleanup(shuffleDependency, CleanShuffle(shuffleDependency.shuffleId))
  }

  /** Register a Broadcast for cleanup when it is garbage collected. */
  def registerBroadcastForCleanup[T](broadcast: Broadcast[T]): Unit = {
    registerForCleanup(broadcast, CleanBroadcast(broadcast.id))
  }

  /** Register a RDDCheckpointData for cleanup when it is garbage collected. */
  def registerRDDCheckpointDataForCleanup[T](rdd: RDD[_], parentId: Int): Unit = {
    registerForCleanup(rdd, CleanCheckpoint(parentId))
  }

  /** Register an object for cleanup. */
  private def registerForCleanup(objectForCleanup: AnyRef, task: CleanupTask): Unit = {
    referenceBuffer.add(new CleanupTaskWeakReference(task, objectForCleanup, referenceQueue))
  }

  /** Keep cleaning RDD, shuffle, and broadcast state. */
  private def keepCleaning(): Unit = Utils.tryOrStopSparkContext(sc) {
    while (!stopped) {
      try {
        val reference = Option(referenceQueue.remove(ContextCleaner.REF_QUEUE_POLL_TIMEOUT))
          .map(_.asInstanceOf[CleanupTaskWeakReference])
        // Synchronize here to avoid being interrupted on stop()
        synchronized {
          reference.foreach { ref =>
            logDebug("Got cleaning task " + ref.task)
            referenceBuffer.remove(ref)
            ref.task match {
              case CleanRDD(rddId) =>
                doCleanupRDD(rddId, blocking = blockOnCleanupTasks)
              case CleanShuffle(shuffleId) =>
                doCleanupShuffle(shuffleId, blocking = blockOnShuffleCleanupTasks)
              case CleanBroadcast(broadcastId) =>
                doCleanupBroadcast(broadcastId, blocking = blockOnCleanupTasks)
              case CleanAccum(accId) =>
                doCleanupAccum(accId, blocking = blockOnCleanupTasks)
              case CleanCheckpoint(rddId) =>
                doCleanCheckpoint(rddId)
            }
          }
        }
      } catch {
        case ie: InterruptedException if stopped => // ignore
        case e: Exception => logError("Error in cleaning thread", e)
      }
    }
  }

  /** Perform RDD cleanup. */
  def doCleanupRDD(rddId: Int, blocking: Boolean): Unit = {
    try {
      logDebug("Cleaning RDD " + rddId)
      sc.unpersistRDD(rddId, blocking)
      listeners.asScala.foreach(_.rddCleaned(rddId))
      logDebug("Cleaned RDD " + rddId)
    } catch {
      case e: Exception => logError("Error cleaning RDD " + rddId, e)
    }
  }

  /** Perform shuffle cleanup. */
  def doCleanupShuffle(shuffleId: Int, blocking: Boolean): Unit = {
    try {
      logDebug("Cleaning shuffle " + shuffleId)
      mapOutputTrackerMaster.unregisterShuffle(shuffleId)
      blockManagerMaster.removeShuffle(shuffleId, blocking)
      listeners.asScala.foreach(_.shuffleCleaned(shuffleId))
      logDebug("Cleaned shuffle " + shuffleId)
    } catch {
      case e: Exception => logError("Error cleaning shuffle " + shuffleId, e)
    }
  }

  /** Perform broadcast cleanup. */
  def doCleanupBroadcast(broadcastId: Long, blocking: Boolean): Unit = {
    try {
      logDebug(s"Cleaning broadcast $broadcastId")
      broadcastManager.unbroadcast(broadcastId, true, blocking)
      listeners.asScala.foreach(_.broadcastCleaned(broadcastId))
      logDebug(s"Cleaned broadcast $broadcastId")
    } catch {
      case e: Exception => logError("Error cleaning broadcast " + broadcastId, e)
    }
  }

  /** Perform accumulator cleanup. */
  def doCleanupAccum(accId: Long, blocking: Boolean): Unit = {
    try {
      logDebug("Cleaning accumulator " + accId)
      AccumulatorContext.remove(accId)
      listeners.asScala.foreach(_.accumCleaned(accId))
      logDebug("Cleaned accumulator " + accId)
    } catch {
      case e: Exception => logError("Error cleaning accumulator " + accId, e)
    }
  }

  /**
    * Clean up checkpoint files written to a reliable storage.
    * Locally checkpointed files are cleaned up separately through RDD cleanups.
    */
  def doCleanCheckpoint(rddId: Int): Unit = {
    try {
      logDebug("Cleaning rdd checkpoint data " + rddId)
      ReliableRDDCheckpointData.cleanCheckpoint(sc, rddId)
      listeners.asScala.foreach(_.checkpointCleaned(rddId))
      logDebug("Cleaned rdd checkpoint data " + rddId)
    }
    catch {
      case e: Exception => logError("Error cleaning rdd checkpoint data " + rddId, e)
    }
  }

  private def blockManagerMaster = sc.env.blockManager.master

  private def broadcastManager = sc.env.broadcastManager

  private def mapOutputTrackerMaster = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
}

private object ContextCleaner {
  private val REF_QUEUE_POLL_TIMEOUT = 100
}

/**
  * Listener class used for testing when any item has been cleaned by the Cleaner class.
  */
private[spark] trait CleanerListener {
  def rddCleaned(rddId: Int): Unit

  def shuffleCleaned(shuffleId: Int): Unit

  def broadcastCleaned(broadcastId: Long): Unit

  def accumCleaned(accId: Long): Unit

  def checkpointCleaned(rddId: Long): Unit
}
