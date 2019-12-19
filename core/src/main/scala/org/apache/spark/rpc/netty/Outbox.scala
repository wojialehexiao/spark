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

package org.apache.spark.rpc.netty

import java.nio.ByteBuffer
import java.util.concurrent.Callable
import javax.annotation.concurrent.GuardedBy

import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.rpc.{RpcAddress, RpcEnvStoppedException}

private[netty] sealed trait OutboxMessage {

  def sendWith(client: TransportClient): Unit

  def onFailure(e: Throwable): Unit

}

private[netty] case class OneWayOutboxMessage(content: ByteBuffer) extends OutboxMessage
  with Logging {

  override def sendWith(client: TransportClient): Unit = {
    client.send(content)
  }

  override def onFailure(e: Throwable): Unit = {
    e match {
      case e1: RpcEnvStoppedException => logDebug(e1.getMessage)
      case e1: Throwable => logWarning(s"Failed to send one-way RPC.", e1)
    }
  }

}

private[netty] case class RpcOutboxMessage(
    content: ByteBuffer,
    _onFailure: (Throwable) => Unit,
    _onSuccess: (TransportClient, ByteBuffer) => Unit)
  extends OutboxMessage with RpcResponseCallback with Logging {

  private var client: TransportClient = _
  private var requestId: Long = _

  override def sendWith(client: TransportClient): Unit = {
    this.client = client
    //RpcOutboxMessage 本身实现了 RpcResponseCallback， 所以在调用client.senRpc时传入this，完成后实现回调
    this.requestId = client.sendRpc(content, this)
  }

  private[netty] def removeRpcRequest(): Unit = {
    if (client != null) {
      client.removeRpcRequest(requestId)
    } else {
      logError("Ask terminated before connecting successfully")
    }
  }

  def onTimeout(): Unit = {
    removeRpcRequest()
  }

  def onAbort(): Unit = {
    removeRpcRequest()
  }

  override def onFailure(e: Throwable): Unit = {
    _onFailure(e)
  }

  override def onSuccess(response: ByteBuffer): Unit = {
    _onSuccess(client, response)
  }

}


private[netty] class Outbox(
                             //当前OutboxNettyRpcEnv所在的
                             nettyEnv: NettyRpcEnv,
                           //Outbox对应的NettyRpcEnv的地址
                             val address: RpcAddress) {

  outbox => // Give this an alias so we can use it more clearly in closures.

  /**
    * 向其他远端NettyRpcEnv上的所有RpcEndpoint发送消息的列表
    */
  @GuardedBy("this")
  private val messages = new java.util.LinkedList[OutboxMessage]

  /**
    * 当前Outbox内的TransportClient。消息的发送都依赖于此传送客户端
    */
  @GuardedBy("this")
  private var client: TransportClient = null

  /**
   * connectFuture points to the connect task. If there is no connect task, connectFuture will be
   * null.
    * 当前Outbox内连接的任务的 Future
   */
  @GuardedBy("this")
  private var connectFuture: java.util.concurrent.Future[Unit] = null

  /**
    * 当前Outbox是否停止的状态
    */
  @GuardedBy("this")
  private var stopped = false

  /**
   * If there is any thread draining the message queue
    * 当前Outbox内正有线程处理message列表中消息的状态
   */
  @GuardedBy("this")
  private var draining = false

  /**
   * Send a message. If there is no active connection, cache it and launch a new connection. If
   * [[Outbox]] is stopped, the sender will be notified with a [[SparkException]].
    * 发送消息
   */
  def send(message: OutboxMessage): Unit = {

    /**
      * 如果停止，则抛异常， 如果没有，将message放到messages列表中， 并调用drainOutbox()
      */
    val dropped = synchronized {
      if (stopped) {
        true
      } else {
        messages.add(message)
        false
      }
    }
    if (dropped) {
      message.onFailure(new SparkException("Message is dropped because Outbox is stopped"))
    } else {
      drainOutbox()
    }
  }

  /**
   * Drain the message queue. If there is other draining thread, just exit. If the connection has
   * not been established, launch a task in the `nettyEnv.clientConnectionExecutor` to setup the
   * connection.
   */
  private def drainOutbox(): Unit = {
    var message: OutboxMessage = null
    synchronized {

      //如果Outbox已经停止
      if (stopped) {
        return
      }

      //如果正在连接远端服务
      if (connectFuture != null) {
        // We are connecting to the remote address, so just exit
        return
      }

      //如果client为null，说明还未连接。此时调用launchConnect连接远端的任务。
      if (client == null) {
        // There is no connect task but client is null, so we need to launch the connect task.
        launchConnectTask()
        return
      }

      //如果正有线程咋处理发送message中的消息
      if (draining) {
        // There is some thread draining, so just exit
        return
      }

      //如果没有消息
      message = messages.poll()
      if (message == null) {
        return
      }
      draining = true
    }

    //循环处理message列表中的消息。
    while (true) {
      try {
        val _client = synchronized { client }
        if (_client != null) {
          message.sendWith(_client)
        } else {
          assert(stopped)
        }
      } catch {
        case NonFatal(e) =>
          handleNetworkFailure(e)
          return
      }
      synchronized {
        if (stopped) {
          return
        }
        message = messages.poll()
        if (message == null) {
          draining = false
          return
        }
      }
    }
  }

  private def launchConnectTask(): Unit = {

    /**
      * 异步连接
      */
    connectFuture = nettyEnv.clientConnectionExecutor.submit(new Callable[Unit] {

      override def call(): Unit = {
        try {
          val _client = nettyEnv.createClient(address)
          outbox.synchronized {
            client = _client
            if (stopped) {
              closeClient()
            }
          }
        } catch {
          case ie: InterruptedException =>
            // exit
            return
          case NonFatal(e) =>
            outbox.synchronized { connectFuture = null }
            handleNetworkFailure(e)
            return
        }
        outbox.synchronized { connectFuture = null }
        // It's possible that no thread is draining now. If we don't drain here, we cannot send the
        // messages until the next message arrives.
        //处理消息
        drainOutbox()
      }
    })
  }

  /**
   * Stop [[Inbox]] and notify the waiting messages with the cause.
   */
  private def handleNetworkFailure(e: Throwable): Unit = {
    synchronized {
      assert(connectFuture == null)
      if (stopped) {
        return
      }
      stopped = true
      closeClient()
    }
    // Remove this Outbox from nettyEnv so that the further messages will create a new Outbox along
    // with a new connection
    nettyEnv.removeOutbox(address)

    // Notify the connection failure for the remaining messages
    //
    // We always check `stopped` before updating messages, so here we can make sure no thread will
    // update messages and it's safe to just drain the queue.
    var message = messages.poll()
    while (message != null) {
      message.onFailure(e)
      message = messages.poll()
    }
    assert(messages.isEmpty)
  }

  private def closeClient(): Unit = synchronized {
    // Just set client to null. Don't close it in order to reuse the connection.
    client = null
  }

  /**
   * Stop [[Outbox]]. The remaining messages in the [[Outbox]] will be notified with a
   * [[SparkException]].
    * 停止Outbox：
    *   将Outbox停止状态改为true
    *   关闭Outbox中的TransportClient
    *   清空消息列表
   */
  def stop(): Unit = {
    synchronized {
      if (stopped) {
        return
      }
      stopped = true
      if (connectFuture != null) {
        connectFuture.cancel(true)
      }
      closeClient()
    }

    // We always check `stopped` before updating messages, so here we can make sure no thread will
    // update messages and it's safe to just drain the queue.
    var message = messages.poll()
    while (message != null) {
      message.onFailure(new SparkException("Message is dropped because Outbox is stopped"))
      message = messages.poll()
    }
  }
}
