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

import java.io.IOException
import java.nio.ByteBuffer
import java.util.{HashMap => JHashMap, Map => JMap}

import com.codahale.metrics.{Metric, MetricSet}
import org.apache.spark.internal.config
import org.apache.spark.network._
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.client.{RpcResponseCallback, TransportClientBootstrap, TransportClientFactory}
import org.apache.spark.network.crypto.{AuthClientBootstrap, AuthServerBootstrap}
import org.apache.spark.network.server._
import org.apache.spark.network.shuffle.protocol.{UploadBlock, UploadBlockStream}
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager, OneForOneBlockFetcher, RetryingBlockFetcher}
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage.BlockManagerMessages.IsExecutorAlive
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.util.Utils
import org.apache.spark.{ExecutorDeadException, SecurityManager, SparkConf}

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.{Success, Try}

/**
  * A BlockTransferService that uses Netty to fetch a set of blocks at time.
  * 与NettyRpcEnv不同在于 NettyRpcEnv采用了NettyRpcHandler，而NettyBlockTransferService采用了NettyBlockRpcServer
  */
private[spark] class NettyBlockTransferService(
                                                conf: SparkConf,
                                                securityManager: SecurityManager,
                                                bindAddress: String,
                                                override val hostName: String,
                                                _port: Int,
                                                numCores: Int,
                                                driverEndPointRef: RpcEndpointRef = null)
  extends BlockTransferService {

  // TODO: Don't use Java serialization, use a more cross-version compatible serialization format.
  private val serializer = new JavaSerializer(conf)
  private val authEnabled = securityManager.isAuthenticationEnabled()
  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numCores)

  private[this] var transportContext: TransportContext = _
  private[this] var server: TransportServer = _
  private[this] var clientFactory: TransportClientFactory = _
  private[this] var appId: String = _

  override def init(blockDataManager: BlockDataManager): Unit = {

    //创建NettyBlockRpcServer。
    val rpcHandler = new NettyBlockRpcServer(conf.getAppId, serializer, blockDataManager)

    //准备服务端引导程序
    var serverBootstrap: Option[TransportServerBootstrap] = None
    //准备客户端引导程序
    var clientBootstrap: Option[TransportClientBootstrap] = None

    if (authEnabled) {
      serverBootstrap = Some(new AuthServerBootstrap(transportConf, securityManager))
      clientBootstrap = Some(new AuthClientBootstrap(transportConf, conf.getAppId, securityManager))
    }

    //创建 TransportContext
    transportContext = new TransportContext(transportConf, rpcHandler)

    //创建传输客户端工厂
    clientFactory = transportContext.createClientFactory(clientBootstrap.toSeq.asJava)

    //创建TransportServer
    server = createServer(serverBootstrap.toList)

    //获取当前应用的ID
    appId = conf.getAppId
    logInfo(s"Server created on ${hostName}:${server.getPort}")
  }

  /** Creates and binds the TransportServer, possibly trying multiple ports. */
  private def createServer(bootstraps: List[TransportServerBootstrap]): TransportServer = {
    def startService(port: Int): (TransportServer, Int) = {
      val server = transportContext.createServer(bindAddress, port, bootstraps.asJava)
      (server, server.getPort)
    }

    Utils.startServiceOnPort(_port, startService, conf, getClass.getName)._1
  }

  override def shuffleMetrics(): MetricSet = {
    require(server != null && clientFactory != null, "NettyBlockTransferServer is not initialized")

    new MetricSet {
      val allMetrics = new JHashMap[String, Metric]()

      override def getMetrics: JMap[String, Metric] = {
        allMetrics.putAll(clientFactory.getAllMetrics.getMetrics)
        allMetrics.putAll(server.getAllMetrics.getMetrics)
        allMetrics
      }
    }
  }

  /**
    * 从远程下载Block
    *
    * @param host the host of the remote node.
    * @param port the port of the remote node.
    * @param execId the executor id.
    * @param blockIds block ids to fetch.
    * @param listener the listener to receive block fetching status.
    * @param tempFileManager
    */
  override def fetchBlocks(
                            host: String,
                            port: Int,
                            execId: String,
                            blockIds: Array[String],
                            listener: BlockFetchingListener,
                            tempFileManager: DownloadFileManager): Unit = {


    logTrace(s"Fetch blocks from $host:$port (executor id $execId)")
    try {
      //创建 RetryingBlockFetcher.BlockFetchStarter ，此匿名类实现了 createAndStart反复
      val blockFetchStarter = new RetryingBlockFetcher.BlockFetchStarter {
        override def createAndStart(blockIds: Array[String],
                                    listener: BlockFetchingListener): Unit = {
          try {
            val client = clientFactory.createClient(host, port)
            new OneForOneBlockFetcher(client, appId, execId, blockIds, listener,
              transportConf, tempFileManager).start()
          } catch {
            case e: IOException =>
              Try {
                driverEndPointRef.askSync[Boolean](IsExecutorAlive(execId))
              } match {
                case Success(v) if v == false =>
                  throw new ExecutorDeadException(s"The relative remote executor(Id: $execId)," +
                    " which maintains the block data to fetch is dead.")
                case _ => throw e
              }
          }
        }
      }


      //获取 spark.$module.io.maxRetries获取最大重试次数
      val maxRetries = transportConf.maxIORetries()
      //创建Block的重试线程
      if (maxRetries > 0) {
        // Note this Fetcher will correctly handle maxRetries == 0; we avoid it just in case there's
        // a bug in this code. We should remove the if statement once we're sure of the stability.
        new RetryingBlockFetcher(transportConf, blockFetchStarter, blockIds, listener).start()
      } else {
        blockFetchStarter.createAndStart(blockIds, listener)
      }
    } catch {
      case e: Exception =>
        logError("Exception while beginning fetchBlocks", e)
        blockIds.foreach(listener.onBlockFetchFailure(_, e))
    }
  }

  override def port: Int = server.getPort

  override def uploadBlock(
                            hostname: String,
                            port: Int,
                            execId: String,
                            blockId: BlockId,
                            blockData: ManagedBuffer,
                            level: StorageLevel,
                            classTag: ClassTag[_]): Future[Unit] = {

    //创建空的 Promise
    val result = Promise[Unit]()

    //创建TransportClient
    val client = clientFactory.createClient(hostname, port)

    // StorageLevel and ClassTag are serialized as bytes using our JavaSerializer.
    // Everything else is encoded using our binary protocol.
    //将储存基本和标记初始化
    val metadata = JavaUtils.bufferToArray(serializer.newInstance().serialize((level, classTag)))

    //判断是否以流的方式上传
    val asStream = blockData.size() > conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM)


    val callback = new RpcResponseCallback {
      override def onSuccess(response: ByteBuffer): Unit = {
        logTrace(s"Successfully uploaded block $blockId${if (asStream) " as stream" else ""}")
        result.success((): Unit)
      }

      override def onFailure(e: Throwable): Unit = {
        logError(s"Error while uploading $blockId${if (asStream) " as stream" else ""}", e)
        result.failure(e)
      }
    }


    if (asStream) {
      val streamHeader = new UploadBlockStream(blockId.name, metadata).toByteBuffer
      client.uploadStream(new NioManagedBuffer(streamHeader), blockData, callback)
    } else {
      // Convert or copy nio buffer into array in order to serialize it.
      //调用ManagedBuffer的nioByteBuffer方法将Block的数据转换或者复制为Nio的ByteBuffer
      val array = JavaUtils.bufferToArray(blockData.nioByteBuffer())

      //使用sendRpc发送UploadBlock
      client.sendRpc(new UploadBlock(appId, execId, blockId.name, metadata, array).toByteBuffer,
        callback)
    }

    result.future
  }

  override def close(): Unit = {
    if (server != null) {
      server.close()
    }
    if (clientFactory != null) {
      clientFactory.close()
    }
    if (transportContext != null) {
      transportContext.close()
    }
  }
}
