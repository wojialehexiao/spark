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

package org.apache.spark.serializer

import java.io.{BufferedInputStream, BufferedOutputStream, InputStream, OutputStream}
import java.nio.ByteBuffer

import org.apache.spark.SparkConf
import org.apache.spark.internal.config
import org.apache.spark.io.CompressionCodec
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.storage._
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

import scala.reflect.ClassTag

/**
  * Component which configures serialization, compression and encryption for various Spark
  * components, including automatic selection of which [[Serializer]] to use for shuffles.
  * 给各种spark自检提供序列化、压缩及加密的服务、
  */
private[spark] class SerializerManager(
                                        //默认的序列化器
                                        defaultSerializer: Serializer,
                                        //SparkConf
                                        conf: SparkConf,
                                        //加密使用的密钥
                                        encryptionKey: Option[Array[Byte]]) {

  def this(defaultSerializer: Serializer, conf: SparkConf) = this(defaultSerializer, conf, None)

  /**
    * KryoSerializer序列化器。采用google的Kryo序列化库的实现
    */
  private[this] val kryoSerializer = new KryoSerializer(conf)

  def setDefaultClassLoader(classLoader: ClassLoader): Unit = {
    kryoSerializer.setDefaultClassLoader(classLoader)
  }

   //字符串类型标记， 即ClassTag[String]
  private[this] val stringClassTag: ClassTag[String] = implicitly[ClassTag[String]]

  /**
    * 原生类型及原生类型数组的类型标记的集合
    *
    */
  private[this] val primitiveAndPrimitiveArrayClassTags: Set[ClassTag[_]] = {
    val primitiveClassTags = Set[ClassTag[_]](
      ClassTag.Boolean,
      ClassTag.Byte,
      ClassTag.Char,
      ClassTag.Double,
      ClassTag.Float,
      ClassTag.Int,
      ClassTag.Long,
      ClassTag.Null,
      ClassTag.Short
    )
    val arrayClassTags = primitiveClassTags.map(_.wrap)
    primitiveClassTags ++ arrayClassTags
  }

  // Whether to compress broadcast variables that are stored
  /**
    * 是否对广播变量进行压缩
    */
  private[this] val compressBroadcast = conf.get(config.BROADCAST_COMPRESS)

  // Whether to compress shuffle output that are stored
  /**
    * 是否对shuffle输出数据进行压缩
    */
  private[this] val compressShuffle = conf.get(config.SHUFFLE_COMPRESS)


  // Whether to compress RDD partitions that are stored serialized
  /**
    * 是否对RDD压缩
    */
  private[this] val compressRdds = conf.get(config.RDD_COMPRESS)


  // Whether to compress shuffle output temporarily spilled to disk
  /**
    * 是否对溢出到磁盘的shuffle数据进行压缩
    */
  private[this] val compressShuffleSpill = conf.get(config.SHUFFLE_SPILL_COMPRESS)

  /* The compression codec to use. Note that the "lazy" val is necessary because we want to delay
   * the initialization of the compression codec until it is first used. The reason is that a Spark
   * program could be using a user-defined codec in a third party jar, which is loaded in
   * Executor.updateDependencies. When the BlockManager is initialized, user level jars hasn't been
   * loaded yet. */
  /**
    * SerializerManager使用的压缩解码器。
    */
  private lazy val compressionCodec: CompressionCodec = CompressionCodec.createCodec(conf)

  /**
    * 当前SerialzerManager是否支持加密。如果想要支持加密，必须在构造是传入encryptionKey
    *
    * @return
    */
  def encryptionEnabled: Boolean = encryptionKey.isDefined

  /**
    * 对于标记的ct，是否使用kryoSerializer进行序列化
    * 当ct属于primitiveAndPrimitiveArrayClassTags，或者stringClassTag
    * @param ct
    * @return
    */
  def canUseKryo(ct: ClassTag[_]): Boolean = {
    primitiveAndPrimitiveArrayClassTags.contains(ct) || ct == stringClassTag
  }

  // SPARK-18617: As feature in SPARK-13990 can not be applied to Spark Streaming now. The worst
  // result is streaming job based on `Receiver` mode can not run on Spark 2.x properly. It may be
  // a rational choice to close `kryo auto pick` feature for streaming in the first step.
  /**
    * 获取序列化器
    * @param ct
    * @param autoPick
    * @return
    */
  def getSerializer(ct: ClassTag[_], autoPick: Boolean): Serializer = {
    if (autoPick && canUseKryo(ct)) {
      kryoSerializer
    } else {
      defaultSerializer
    }
  }

  /**
    * Pick the best serializer for shuffling an RDD of key-value pairs.
    */
  def getSerializer(keyClassTag: ClassTag[_], valueClassTag: ClassTag[_]): Serializer = {
    if (canUseKryo(keyClassTag) && canUseKryo(valueClassTag)) {
      kryoSerializer
    } else {
      defaultSerializer
    }
  }

  private def shouldCompress(blockId: BlockId): Boolean = {
    blockId match {
      case _: ShuffleBlockId => compressShuffle
      case _: BroadcastBlockId => compressBroadcast
      case _: RDDBlockId => compressRdds
      case _: TempLocalBlockId => compressShuffleSpill
      case _: TempShuffleBlockId => compressShuffle
      case _ => false
    }
  }

  /**
    * Wrap an input stream for encryption and compression
    * 对Block的输入流进行压缩与加密
    */
  def wrapStream(blockId: BlockId, s: InputStream): InputStream = {
    wrapForCompression(blockId, wrapForEncryption(s))
  }

  /**
    * Wrap an output stream for encryption and compression
    * 对Block的输出流进行压缩与加密
    */
  def wrapStream(blockId: BlockId, s: OutputStream): OutputStream = {
    wrapForCompression(blockId, wrapForEncryption(s))
  }

  /**
    * Wrap an input stream for encryption if shuffle encryption is enabled
    * 对输入流进行加密
    */
  def wrapForEncryption(s: InputStream): InputStream = {
    encryptionKey
      .map { key => CryptoStreamUtils.createCryptoInputStream(s, conf, key) }
      .getOrElse(s)
  }

  /**
    * Wrap an output stream for encryption if shuffle encryption is enabled
    * 对输出流进行加密
    */
  def wrapForEncryption(s: OutputStream): OutputStream = {
    encryptionKey
      .map { key => CryptoStreamUtils.createCryptoOutputStream(s, conf, key) }
      .getOrElse(s)
  }

  /**
    * Wrap an output stream for compression if block compression is enabled for its block type
    */
  def wrapForCompression(blockId: BlockId, s: OutputStream): OutputStream = {
    if (shouldCompress(blockId)) compressionCodec.compressedOutputStream(s) else s
  }

  /**
    * Wrap an input stream for compression if block compression is enabled for its block type
    *
    */
  def wrapForCompression(blockId: BlockId, s: InputStream): InputStream = {
    if (shouldCompress(blockId)) compressionCodec.compressedInputStream(s) else s
  }

  /** Serializes into a stream.
    * 对block的输出序列化*/
  def dataSerializeStream[T: ClassTag](
                                        blockId: BlockId,
                                        outputStream: OutputStream,
                                        values: Iterator[T]): Unit = {
    val byteStream = new BufferedOutputStream(outputStream)
    val autoPick = !blockId.isInstanceOf[StreamBlockId]
    val ser = getSerializer(implicitly[ClassTag[T]], autoPick).newInstance()
    ser.serializeStream(wrapForCompression(blockId, byteStream)).writeAll(values).close()
  }

  /** Serializes into a chunked byte buffer.
    * 序列化成分块字节缓冲区
    * */
  def dataSerialize[T: ClassTag](
                                  blockId: BlockId,
                                  values: Iterator[T]): ChunkedByteBuffer = {
    dataSerializeWithExplicitClassTag(blockId, values, implicitly[ClassTag[T]])
  }

  /**
    * Serializes into a chunked byte buffer.
    * 使用明确的类型标记， 序列化成分块字节缓冲区
    * */
  def dataSerializeWithExplicitClassTag(
                                         blockId: BlockId,
                                         values: Iterator[_],
                                         classTag: ClassTag[_]): ChunkedByteBuffer = {
    val bbos = new ChunkedByteBufferOutputStream(1024 * 1024 * 4, ByteBuffer.allocate)
    val byteStream = new BufferedOutputStream(bbos)
    val autoPick = !blockId.isInstanceOf[StreamBlockId]
    val ser = getSerializer(classTag, autoPick).newInstance()
    ser.serializeStream(wrapForCompression(blockId, byteStream)).writeAll(values).close()
    bbos.toChunkedByteBuffer
  }

  /**
    * Deserializes an InputStream into an iterator of values and disposes of it when the end of
    * the iterator is reached.
    * 将输入流反序列化为值的迭代器
    */
  def dataDeserializeStream[T](
                                blockId: BlockId,
                                inputStream: InputStream)
                              (classTag: ClassTag[T]): Iterator[T] = {
    val stream = new BufferedInputStream(inputStream)
    val autoPick = !blockId.isInstanceOf[StreamBlockId]
    getSerializer(classTag, autoPick)
      .newInstance()
      .deserializeStream(wrapForCompression(blockId, stream))
      .asIterator.asInstanceOf[Iterator[T]]
  }
}
