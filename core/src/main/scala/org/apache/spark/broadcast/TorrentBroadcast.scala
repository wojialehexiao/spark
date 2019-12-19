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

package org.apache.spark.broadcast

import java.io._
import java.lang.ref.SoftReference
import java.nio.ByteBuffer
import java.util.zip.Adler32

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage._
import org.apache.spark.util.{KeyLock, Utils}
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

/**
 * A BitTorrent-like implementation of [[org.apache.spark.broadcast.Broadcast]].
 *
 * The mechanism is as follows:
 *
 * The driver divides the serialized object into small chunks and
 * stores those chunks in the BlockManager of the driver.
 *
 * On each executor, the executor first attempts to fetch the object from its BlockManager. If
 * it does not exist, it then uses remote fetches to fetch the small chunks from the driver and/or
 * other executors if available. Once it gets the chunks, it puts the chunks in its own
 * BlockManager, ready for other executors to fetch from.
 *
 * This prevents the driver from being the bottleneck in sending out multiple copies of the
 * broadcast data (one per executor).
 *
 * When initialized, TorrentBroadcast objects read SparkEnv.get.conf.
 *
 * @param obj object to broadcast
 * @param id A unique identifier for the broadcast variable.
 */
private[spark] class TorrentBroadcast[T: ClassTag](obj: T, id: Long)
  extends Broadcast[T](id) with Logging with Serializable {

  /**
   * Value of the broadcast object on executors. This is reconstructed by [[readBroadcastBlock]],
   * which builds this value by reading blocks from the driver and/or other executors.
   *
   * On the driver, if the value is required, it is read lazily from the block manager. We hold
   * a soft reference so that it can be garbage collected if required, as we can always reconstruct
   * in the future.
    *
    * 从Executor或者Driver上读取的广播块的值。_value是通过调用readBroadcastBlock方法获取的广播对象
    * 由于
   */
  @transient private var _value: SoftReference[T] = _

  /**
    * The compression codec to use, or None if compression is disabled
    * 用于广播对象的压缩解码器。可以设置 spark.broadcast.compress true启用， 默认启用
    *
    * */
  @transient private var compressionCodec: Option[CompressionCodec] = _
  /**
    * Size of each block. Default value is 4MB.  This value is only read by the broadcaster.
    * 每块大大小。只读属性。可以通过spark.braodcast.blockSize属性设置。默认为4MB
    * */
  @transient private var blockSize: Int = _

  private def setConf(conf: SparkConf): Unit = {
    compressionCodec = if (conf.get(config.BROADCAST_COMPRESS)) {
      Some(CompressionCodec.createCodec(conf))
    } else {
      None
    }
    // Note: use getSizeAsKb (not bytes) to maintain compatibility if no units are provided
    blockSize = conf.get(config.BROADCAST_BLOCKSIZE).toInt * 1024
    checksumEnabled = conf.get(config.BROADCAST_CHECKSUM)
  }
  setConf(SparkEnv.get.conf)

  /**
    * 广播Id。
    */
  private val broadcastId = BroadcastBlockId(id)

  /**
    * Total number of blocks this broadcast variable contains.
    * 广播变量包含的块的数量。常量不可改变
    * */
  private val numBlocks: Int = writeBlocks(obj)

  /**
    * Whether to generate checksum for blocks or not.
    * 是否给广播变量生成校验和。可以通过 spark.braodcast.checksum属性设置。默认为true
    * */
  private var checksumEnabled: Boolean = false

  /**
    * The checksum for all the blocks.
    * 用于储存每个广播快的校验和的数组
    *
    * */
  private var checksums: Array[Int] = _

  override protected def getValue() = synchronized {
    val memoized: T = if (_value == null) null.asInstanceOf[T] else _value.get
    if (memoized != null) {
      memoized
    } else {
      val newlyRead = readBroadcastBlock()
      _value = new SoftReference[T](newlyRead)
      newlyRead
    }
  }

  private def calcChecksum(block: ByteBuffer): Int = {
    val adler = new Adler32()
    if (block.hasArray) {
      adler.update(block.array, block.arrayOffset + block.position(), block.limit()
        - block.position())
    } else {
      val bytes = new Array[Byte](block.remaining())
      block.duplicate.get(bytes)
      adler.update(bytes)
    }
    adler.getValue.toInt
  }

  /**
   * Divide the object into multiple blocks and put those blocks in the block manager.
   *
   * @param value the object to divide
   * @return number of blocks this broadcast variable is divided into
   */
  private def writeBlocks(value: T): Int = {
    import StorageLevel._
    // Store a copy of the broadcast variable in the driver so that tasks run on the driver
    // do not create a duplicate copy of the broadcast variable's value.

    //获取BlockManager
    val blockManager = SparkEnv.get.blockManager

    //调用BlockManager的pushSingle方法将广播对象写入到本地的储存体系。
    if (!blockManager.putSingle(broadcastId, value, MEMORY_AND_DISK, tellMaster = false)) {
      throw new SparkException(s"Failed to store $broadcastId in BlockManager")
    }

    //调用 TorrentBroadcast.blockifyObject方法将对象转化成一系列块
    //每个块的大小由blockSize决定，使用当前SparkEnvironment中的序列化组件进行序列化，使用TorrentBroadcast自身的
    //compressionCodec进行压缩
    val blocks =
      TorrentBroadcast.blockifyObject(value, blockSize, SparkEnv.get.serializer, compressionCodec)


    if (checksumEnabled) {
      checksums = new Array[Int](blocks.length)
    }
    blocks.zipWithIndex.foreach { case (block, i) =>

      //如果需要校验和，生成校验和
      if (checksumEnabled) {
        checksums(i) = calcChecksum(block)
      }

      //生成广播分片Id， 并调用putBytes将广播块写入本地的存储体系
      val pieceId = BroadcastBlockId(id, "piece" + i)
      val bytes = new ChunkedByteBuffer(block.duplicate())

      if (!blockManager.putBytes(pieceId, bytes, MEMORY_AND_DISK_SER, tellMaster = true)) {
        throw new SparkException(s"Failed to store $pieceId of $broadcastId in local BlockManager")
      }
    }
    blocks.length
  }

  /**
    * Fetch torrent blocks from the driver and/or other executors.
    * 从Driver、Executor的存储体系中获取块。
    *
    * */
  private def readBlocks(): Array[BlockData] = {
    // Fetch chunks of data. Note that all these chunks are stored in the BlockManager and reported
    // to the driver, so other executors can pull these chunks from this executor as well.
    //新建用于存储每个分片广播块的数组。
    val blocks = new Array[BlockData](numBlocks)
    //获取当前SparkEnv的BlockManager
    val bm = SparkEnv.get.blockManager

    //对各个广播分片进行随机洗牌，避免对广播块的获取出现"热点"，提升性能。
    for (pid <- Random.shuffle(Seq.range(0, numBlocks))) {
      val pieceId = BroadcastBlockId(id, "piece" + pid)
      logDebug(s"Reading piece $pieceId of $broadcastId")
      // First try getLocalBytes because there is a chance that previous attempts to fetch the
      // broadcast blocks have already fetched some of the blocks. In that case, some blocks
      // would be available locally (on this executor).
      //从本地存储体系中获取序列化的分片广播块
      bm.getLocalBytes(pieceId) match {

          //如果本地可以获取到，则将分片广播块放入到blocks，并且释放此广播块的锁
        case Some(block) =>
          blocks(pid) = block
          releaseBlockManagerLock(pieceId)

          //如果本地没有，则调用BlockManager的getRemoteBytes方法从远程的存储体系中获取分片广播块。
          //对于获取的分片广播块调用calcChecksum方法计算校验和，并将此校验块调用writeBlocks方法时存入checksums的校验和进行比较
          //如果校验和不一致，说明块的数据有损坏，抛出异常
        case None =>
          bm.getRemoteBytes(pieceId) match {
            case Some(b) =>
              if (checksumEnabled) {
                val sum = calcChecksum(b.chunks(0))
                if (sum != checksums(pid)) {
                  throw new SparkException(s"corrupt remote block $pieceId of $broadcastId:" +
                    s" $sum != ${checksums(pid)}")
                }
              }
              // We found the block from remote executors/driver's BlockManager, so put the block
              // in this executor's BlockManager.
              //如果校验和相同，调用BlockManager的pushBytes方法，将分片广播块写入到本地存储体系，以便当前Executor的
              //其他任务不再获取分片广播块。
              if (!bm.putBytes(pieceId, b, StorageLevel.MEMORY_AND_DISK_SER, tellMaster = true)) {
                throw new SparkException(
                  s"Failed to store $pieceId of $broadcastId in local BlockManager")
              }

              //最后将分片广播块放入blocks
              blocks(pid) = new ByteBufferBlockData(b, true)
            case None =>
              throw new SparkException(s"Failed to get $pieceId of $broadcastId")
          }
      }
    }
    blocks
  }

  /**
   * Remove all persisted state associated with this Torrent broadcast on the executors.
   */
  override protected def doUnpersist(blocking: Boolean): Unit = {
    TorrentBroadcast.unpersist(id, removeFromDriver = false, blocking)
  }

  /**
   * Remove all persisted state associated with this Torrent broadcast on the executors
   * and driver.
   */
  override protected def doDestroy(blocking: Boolean): Unit = {
    TorrentBroadcast.unpersist(id, removeFromDriver = true, blocking)
  }

  /** Used by the JVM when serializing this object. */
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    assertValid()
    out.defaultWriteObject()
  }


  /**
    *
    * @return
    */
  private def readBroadcastBlock(): T = Utils.tryOrIOException {
    TorrentBroadcast.torrentBroadcastLock.withLock(broadcastId) {
      // As we only lock based on `broadcastId`, whenever using `broadcastCache`, we should only
      // touch `broadcastId`.

      //获取缓存
      val broadcastCache = SparkEnv.get.broadcastManager.cachedValues

      Option(broadcastCache.get(broadcastId)).map(_.asInstanceOf[T]).getOrElse {


        setConf(SparkEnv.get.conf)

        //获取BlockManager 组件
        val blockManager = SparkEnv.get.blockManager

        //从本地获取pushSingle保存的对象
        blockManager.getLocalValues(broadcastId) match {
          case Some(blockResult) =>
            if (blockResult.data.hasNext) {

              //获取到对象
              val x = blockResult.data.next().asInstanceOf[T]

              //释放BlockManager的锁
              releaseBlockManagerLock(broadcastId)

              //放入缓存
              if (x != null) {
                broadcastCache.put(broadcastId, x)
              }

              x
            } else {
              throw new SparkException(s"Failed to get locally stored broadcast data: $broadcastId")
            }

            //如果没有获取到,说明数据是通过pushBytes方法序列化写入存储体系的。
          case None =>
            val estimatedTotalSize = Utils.bytesToString(numBlocks * blockSize)
            logInfo(s"Started reading broadcast variable $id with $numBlocks pieces " +
              s"(estimated total size $estimatedTotalSize)")
            val startTimeNs = System.nanoTime()

            //调用
            val blocks = readBlocks()
            logInfo(s"Reading broadcast variable $id took ${Utils.getUsedTimeNs(startTimeNs)}")

            try {

              //将数据库解密拼接反序列化为对象
              val obj = TorrentBroadcast.unBlockifyObject[T](
                blocks.map(_.toInputStream()), SparkEnv.get.serializer, compressionCodec)
              // Store the merged copy in BlockManager so other tasks on this executor don't
              // need to re-fetch it.
              //保存到本地存储
              val storageLevel = StorageLevel.MEMORY_AND_DISK
              if (!blockManager.putSingle(broadcastId, obj, storageLevel, tellMaster = false)) {
                throw new SparkException(s"Failed to store $broadcastId in BlockManager")
              }

              if (obj != null) {
                //放入缓存
                broadcastCache.put(broadcastId, obj)
              }

              obj
            } finally {
              blocks.foreach(_.dispose())
            }
        }
      }
    }
  }

  /**
   * If running in a task, register the given block's locks for release upon task completion.
   * Otherwise, if not running in a task then immediately release the lock.
   */
  private def releaseBlockManagerLock(blockId: BlockId): Unit = {
    val blockManager = SparkEnv.get.blockManager
    Option(TaskContext.get()) match {
      case Some(taskContext) =>
        taskContext.addTaskCompletionListener[Unit](_ => blockManager.releaseLock(blockId))
      case None =>
        // This should only happen on the driver, where broadcast variables may be accessed
        // outside of running tasks (e.g. when computing rdd.partitions()). In order to allow
        // broadcast variables to be garbage collected we need to free the reference here
        // which is slightly unsafe but is technically okay because broadcast variables aren't
        // stored off-heap.
        blockManager.releaseLock(blockId)
    }
  }

}


private object TorrentBroadcast extends Logging {

  /**
   * A [[KeyLock]] whose key is [[BroadcastBlockId]] to ensure there is only one thread fetching
   * the same [[TorrentBroadcast]] block.
   */
  private val torrentBroadcastLock = new KeyLock[BroadcastBlockId]

  def blockifyObject[T: ClassTag](
      obj: T,
      blockSize: Int,
      serializer: Serializer,
      compressionCodec: Option[CompressionCodec]): Array[ByteBuffer] = {
    val cbbos = new ChunkedByteBufferOutputStream(blockSize, ByteBuffer.allocate)
    val out = compressionCodec.map(c => c.compressedOutputStream(cbbos)).getOrElse(cbbos)
    val ser = serializer.newInstance()
    val serOut = ser.serializeStream(out)
    Utils.tryWithSafeFinally {
      serOut.writeObject[T](obj)
    } {
      serOut.close()
    }
    cbbos.toChunkedByteBuffer.getChunks()
  }

  def unBlockifyObject[T: ClassTag](
      blocks: Array[InputStream],
      serializer: Serializer,
      compressionCodec: Option[CompressionCodec]): T = {
    require(blocks.nonEmpty, "Cannot unblockify an empty array of blocks")
    val is = new SequenceInputStream(blocks.iterator.asJavaEnumeration)
    val in: InputStream = compressionCodec.map(c => c.compressedInputStream(is)).getOrElse(is)
    val ser = serializer.newInstance()
    val serIn = ser.deserializeStream(in)
    val obj = Utils.tryWithSafeFinally {
      serIn.readObject[T]()
    } {
      serIn.close()
    }
    obj
  }

  /**
   * Remove all persisted blocks associated with this torrent broadcast on the executors.
   * If removeFromDriver is true, also remove these persisted blocks on the driver.
    * 广播块去持久化
   */
  def unpersist(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = {
    logDebug(s"Unpersisting TorrentBroadcast $id")
    SparkEnv.get.blockManager.master.removeBroadcast(id, removeFromDriver, blocking)
  }
}
