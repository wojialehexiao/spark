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

package org.apache.spark.util.collection

import scala.collection.mutable

import org.apache.spark.util.SizeEstimator

/**
 * A general interface for collections to keep track of their estimated sizes in bytes.
 * We sample with a slow exponential back-off using the SizeEstimator to amortize the time,
 * as each call to SizeEstimator is somewhat expensive (order of a few milliseconds).
 *
 * Spark在Shuffle阶段， 给map任务的输出增加了缓存、聚合的数据结构。
 * 这些数据结构使用各种执行内存， 为了对这些数据结构大小进行计算， 以便扩充大小或在没有内存是溢出到磁盘，
 * SizeTracker定义了对集合进行采样和估算的规范
 */
private[spark] trait SizeTracker {

  import SizeTracker._

  /**
   * Controls the base of the exponential which governs the rate of sampling.
   * E.g., a value of 2 would mean we sample at 1, 2, 4, 8, ... elements.
   * 采样增长速率
   */
  private val SAMPLE_GROWTH_RATE = 1.1

  /**
   * Samples taken since last resetSamples(). Only the last two are kept for extrapolation.
   * 样本队列，。 最后两个样本将被用于估算
   * */
  private val samples = new mutable.Queue[Sample]

  /**
   * The average number of bytes per update between our last two samples.
   * 评价每次更新的字节数
   * */
  private var bytesPerUpdate: Double = _

  /**
   * Total number of insertions and updates into the map since the last resetSamples().
   * 更新操作（包括插入和更新）的总次数
   * */
  private var numUpdates: Long = _

  /**
   * The value of 'numUpdates' at which we will take our next sample.
   * 下次采样值
   * numUpdates的值增长到与nextSampleNum相同是才会再次采样
   * */
  private var nextSampleNum: Long = _

  resetSamples()

  /**
   * Reset samples collected so far.
   * This should be called after the collection undergoes a dramatic change in size.
   */
  protected def resetSamples(): Unit = {
    numUpdates = 1
    nextSampleNum = 1
    samples.clear()
    takeSample()
  }

  /**
   * Callback to be invoked after every update.
   * 用于向集合中更新了元素之后进行回调， 以触发对集合的采样
   */
  protected def afterUpdate(): Unit = {
    numUpdates += 1
    if (nextSampleNum == numUpdates) {
      takeSample()
    }
  }

  /**
   * Take a new sample of the current collection's size.
   * 采集样本
   */
  private def takeSample(): Unit = {

    //估算集合的大小并作为样本
    samples.enqueue(Sample(SizeEstimator.estimate(this), numUpdates))
    // Only use the last two samples to extrapolate
    //保留队列的最后两个样本
    if (samples.size > 2) {
      samples.dequeue()
    }

    val bytesDelta = samples.toList.reverse match {
      case latest :: previous :: tail =>
        (latest.size - previous.size).toDouble / (latest.numUpdates - previous.numUpdates)
      // If fewer than 2 samples, assume no change
      case _ => 0
    }

    // 计算每次更新的字节数
    bytesPerUpdate = math.max(0, bytesDelta)

    //机选下次的采样号
    nextSampleNum = math.ceil(numUpdates * SAMPLE_GROWTH_RATE).toLong
  }

  /**
   * Estimate the current size of the collection in bytes. O(1) time.
   *
   * 估算集合大小
   */
  def estimateSize(): Long = {
    assert(samples.nonEmpty)
    val extrapolatedDelta = bytesPerUpdate * (numUpdates - samples.last.numUpdates)
    (samples.last.size + extrapolatedDelta).toLong
  }
}

private object SizeTracker {
  case class Sample(size: Long, numUpdates: Long)
}
