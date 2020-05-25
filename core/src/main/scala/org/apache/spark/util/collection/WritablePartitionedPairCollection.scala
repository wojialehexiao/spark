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

import java.util.Comparator


/**
 * A common interface for size-tracking collections of key-value pairs that
 *
 *  - Have an associated partition for each key-value pair.
 *  - Support a memory-efficient sorted iterator
 *  - Support a WritablePartitionedIterator for writing the contents directly as bytes.
 *
 *  对键值对构成的集合进行大小跟踪的通用接口。
 *  这里每个键值对都有相关联的分区， 例如， key为（0， #）， value为1的键值对， 真正键实际是#， 而0则是键#的分区id。
 *  WritablePartitionedPairCollection基于内存进行有效排序， 并可以创建集合内容按照字节自恶如磁盘的WritablePartitionedIterator
 *
 */
private[spark] trait WritablePartitionedPairCollection[K, V] {
  /**
   * Insert a key-value pair with a partition into the collection
   *
   *
   */
  def insert(partition: Int, key: K, value: V): Unit

  /**
   * Iterate through the data in order of partition ID and then the given comparator. This may
   * destroy the underlying collection.
   * 根据给定的对key进行比较的比较器， 返回对集合中的数据按照分区ID的顺序进行迭代的迭代器。
   */
  def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
  : Iterator[((Int, K), V)]

  /**
   * Iterate through the data and write out the elements instead of returning them. Records are
   * returned in order of their partition ID and then the given comparator.
   * This may destroy the underlying collection.
   */
  def destructiveSortedWritablePartitionedIterator(keyComparator: Option[Comparator[K]]): WritablePartitionedIterator = {

    // 获取对集合中数据按照分区ID的顺序进行迭代的迭代器
    val it = partitionedDestructiveSortedIterator(keyComparator)

    // 创建并返回WritablePartitionedIterator的匿名实现类的实例
    new WritablePartitionedIterator {
      private[this] var cur = if (it.hasNext) it.next() else null

      def writeNext(writer: PairsWriter): Unit = {
        writer.write(cur._1._2, cur._2)
        cur = if (it.hasNext) it.next() else null
      }

      def hasNext(): Boolean = cur != null

      // 用于获取下一个元素的分区ID
      def nextPartition(): Int = cur._1._1
    }
  }
}

/**
 * WritablePartitionedPairCollection伴生对象自定义生成两种比较器的方法
 */
private[spark] object WritablePartitionedPairCollection {
  /**
   * A comparator for (Int, K) pairs that orders them by only their partition ID.
   * 对有partition id和key构成的两个对偶对象按照parttionid 进行排序
   */
  def partitionComparator[K]: Comparator[(Int, K)] = (a: (Int, K), b: (Int, K)) => a._1 - b._1

  /**
   * A comparator for (Int, K) pairs that orders them both by their partition ID and a key ordering.
   *
   */
  def partitionKeyComparator[K](keyComparator: Comparator[K]): Comparator[(Int, K)] =
    (a: (Int, K), b: (Int, K)) => {

      //对partition id和key构成的两个对偶对象按照partitionid进行比较
      val partitionDiff = a._1 - b._1
      if (partitionDiff != 0) {
        partitionDiff
      } else {

        //根据key进行第二级比较
        keyComparator.compare(a._2, b._2)
      }
    }
}

/**
 * Iterator that writes elements to a DiskBlockObjectWriter instead of returning them. Each element
 * has an associated partition.
 */
private[spark] trait WritablePartitionedIterator {
  def writeNext(writer: PairsWriter): Unit

  def hasNext(): Boolean

  def nextPartition(): Int
}
