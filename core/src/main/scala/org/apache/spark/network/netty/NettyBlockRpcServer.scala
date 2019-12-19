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

package org.apache.spark.network.netty

import java.nio.ByteBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, StreamCallbackWithID, TransportClient}
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}
import org.apache.spark.network.shuffle.protocol._
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockId, ShuffleBlockId, StorageLevel}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
  * Serves requests to open blocks by simply registering one chunk per block requested.
  * Handles opening and uploading arbitrary BlockManager blocks.
  *
  * Opened blocks are registered with the "one-for-one" strategy, meaning each Transport-layer Chunk
  * is equivalent to one Spark-level shuffle block.
  *
  *
  *
  */
class NettyBlockRpcServer(
                           appId: String,
                           serializer: Serializer,
                           blockManager: BlockDataManager)
  extends RpcHandler with Logging {

  /**
    * 一对一的流服务
    */
  private val streamManager = new OneForOneStreamManager()

  override def receive(
                        client: TransportClient,
                        rpcMessage: ByteBuffer,
                        responseContext: RpcResponseCallback): Unit = {


    //反序列化消息
    val message = BlockTransferMessage.Decoder.fromByteBuffer(rpcMessage)
    logTrace(s"Received request: $message")

    message match {

        //打开（读取）Block
      case openBlocks: OpenBlocks =>
        val blocksNum = openBlocks.blockIds.length

        //取出OpenBlocks消息携带的数组，调用BlockManager的getBlockData
        //获取数组中每一个BlockId对应的Block
        val blocks = for (i <- (0 until blocksNum).view)
          yield blockManager.getBlockData(BlockId.apply(openBlocks.blockIds(i)))

        //将ManagedBuffer序列注册到OneForOneStreamManager的stream缓存
        val streamId = streamManager.registerStream(appId, blocks.iterator.asJava,
          client.getChannel)

        //响应客户端
        logTrace(s"Registered streamId $streamId with $blocksNum buffers")
        responseContext.onSuccess(new StreamHandle(streamId, blocksNum).toByteBuffer)



      //获取shuffleBlock -- 待定
      case fetchShuffleBlocks: FetchShuffleBlocks =>
        val blocks = fetchShuffleBlocks.mapIds.zipWithIndex.flatMap { case (mapId, index) =>
          fetchShuffleBlocks.reduceIds.apply(index).map { reduceId =>
            blockManager.getBlockData(
              ShuffleBlockId(fetchShuffleBlocks.shuffleId, mapId, reduceId))
          }
        }
        val numBlockIds = fetchShuffleBlocks.reduceIds.map(_.length).sum


        val streamId = streamManager.registerStream(appId, blocks.iterator.asJava,
          client.getChannel)

        logTrace(s"Registered streamId $streamId with $numBlockIds buffers")
        responseContext.onSuccess(
          new StreamHandle(streamId, numBlockIds).toByteBuffer)


      //上传Block
      case uploadBlock: UploadBlock =>
        // StorageLevel and ClassTag are serialized as bytes using our JavaSerializer.
        // 对元数据进行反序列化，得到存储级别和类型标记
        val (level, classTag) = deserializeMetadata(uploadBlock.metadata)

        //将Block的数据封装为NioManagedBuffer
        val data = new NioManagedBuffer(ByteBuffer.wrap(uploadBlock.blockData))

        //获取BlockId
        val blockId = BlockId(uploadBlock.blockId)
        logDebug(s"Receiving replicated block $blockId with level ${level} " +
          s"from ${client.getSocketAddress}")

        //放入本地存储体系
        blockManager.putBlockData(blockId, data, level, classTag)

        //响应
        responseContext.onSuccess(ByteBuffer.allocate(0))
    }
  }

  override def receiveStream(
                              client: TransportClient,
                              messageHeader: ByteBuffer,
                              responseContext: RpcResponseCallback): StreamCallbackWithID = {
    val message =
      BlockTransferMessage.Decoder.fromByteBuffer(messageHeader).asInstanceOf[UploadBlockStream]
    val (level, classTag) = deserializeMetadata(message.metadata)
    val blockId = BlockId(message.blockId)
    logDebug(s"Receiving replicated block $blockId with level ${level} as stream " +
      s"from ${client.getSocketAddress}")
    // This will return immediately, but will setup a callback on streamData which will still
    // do all the processing in the netty thread.
    blockManager.putBlockDataAsStream(blockId, level, classTag)
  }

  private def deserializeMetadata[T](metadata: Array[Byte]): (StorageLevel, ClassTag[T]) = {
    serializer
      .newInstance()
      .deserialize(ByteBuffer.wrap(metadata))
      .asInstanceOf[(StorageLevel, ClassTag[T])]
  }

  override def getStreamManager(): StreamManager = streamManager
}
