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

package org.apache.spark.storage

import org.apache.spark.SparkEnv
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.config._
import org.apache.spark.rdd.{RDD, RDDOperationScope}
import org.apache.spark.util.Utils

/**
  * 用于描述RDD的信息
  *
  * @param id
  * @param name
  * @param numPartitions
  * @param storageLevel
  * @param isBarrier
  * @param parentIds
  * @param callSite
  * @param scope
  */
@DeveloperApi
class RDDInfo(
             //RDD的id
               val id: Int,
             //RDD的名称
               var name: String,
             //RDD的分区数量
               val numPartitions: Int,
             //RDD的存储级别
               var storageLevel: StorageLevel,
             //
               val isBarrier: Boolean,
             //RDD的父RDD的id序列。说明RDD会有零到多个父RDD
               val parentIds: Seq[Int],
             //RDD的用于调用栈信息
               val callSite: String = "",
             //RDD的操作范围，scope的类型为RDDOperationScope， 每个RDD都有一个RDDOperationScope
             //RDDOperationScope与stage或者job之间并无特殊关系，一个RDDOperationScope可以存在于一个State内，也可以跨越多个Job
               val scope: Option[RDDOperationScope] = None)
  extends Ordered[RDDInfo] {

  /**
    * 缓存的分区数量
    */
  var numCachedPartitions = 0

  /**
    * 使用内存的大小
    */
  var memSize = 0L

  /**
    * 使用磁盘的大小
    */
  var diskSize = 0L

  /**
    * Block存储在外部的大小
    */
  var externalBlockStoreSize = 0L

  def isCached: Boolean = (memSize + diskSize > 0) && numCachedPartitions > 0

  override def toString: String = {
    import Utils.bytesToString
    ("RDD \"%s\" (%d) StorageLevel: %s; CachedPartitions: %d; TotalPartitions: %d; " +
      "MemorySize: %s; DiskSize: %s").format(
      name, id, storageLevel.toString, numCachedPartitions, numPartitions,
      bytesToString(memSize), bytesToString(diskSize))
  }

  /**
    * 用于compare排序
    * @param that
    * @return
    */
  override def compare(that: RDDInfo): Int = {
    this.id - that.id
  }
}

private[spark] object RDDInfo {

  /**
    * 从RDD构建出对应的RDDInfo
    * @param rdd
    * @return
    */
  def fromRdd(rdd: RDD[_]): RDDInfo = {

    //获取名称或类名
    val rddName = Option(rdd.name).getOrElse(Utils.getFormattedClassName(rdd))

    //获取所有父RDD的身份标识作为RDDInfo的parentIds
    val parentIds = rdd.dependencies.map(_.rdd.id)

    val callsiteLongForm = Option(SparkEnv.get)
      .map(_.conf.get(EVENT_LOG_CALLSITE_LONG_FORM))
      .getOrElse(false)

    val callSite = if (callsiteLongForm) {
      rdd.creationSite.longForm
    } else {
      rdd.creationSite.shortForm
    }

    new RDDInfo(rdd.id, rddName, rdd.partitions.length,
      rdd.getStorageLevel, rdd.isBarrier(), parentIds, callSite, rdd.scope)
  }
}
