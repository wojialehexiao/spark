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

import java.io.File
import java.net.Socket
import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Properties

import com.google.common.collect.MapMaker
import org.apache.hadoop.conf.Configuration

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.python.PythonWorkerFactory
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.config._
import org.apache.spark.memory.{MemoryManager, UnifiedMemoryManager}
import org.apache.spark.metrics.{MetricsSystem, MetricsSystemInstances}
import org.apache.spark.network.netty.{NettyBlockTransferService, SparkTransportConf}
import org.apache.spark.network.shuffle.ExternalBlockStoreClient
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.{LiveListenerBus, OutputCommitCoordinator}
import org.apache.spark.scheduler.OutputCommitCoordinator.OutputCommitCoordinatorEndpoint
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.serializer.{JavaSerializer, Serializer, SerializerManager}
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.storage._
import org.apache.spark.util.{RpcUtils, Utils}

/**
 * :: DeveloperApi ::
 * 保存正在运行的Spark实例（master或worker实例）的所有运行时环境对象，
 * 包括序列化程序，RpcEnv，块管理器，映射输出跟踪器等。
  * 当前，Spark代码通过全局变量查找SparkEnv，因此所有线程都可以访问相同的SparkEnv。
  * 可以通过SparkEnv.get访问它（例如，在创建SparkContext之后）。
 */
@DeveloperApi
class SparkEnv (
    val executorId: String,
    private[spark] val rpcEnv: RpcEnv,
    val serializer: Serializer,
    val closureSerializer: Serializer,
    val serializerManager: SerializerManager,
    val mapOutputTracker: MapOutputTracker,
    val shuffleManager: ShuffleManager,
    val broadcastManager: BroadcastManager,
    val blockManager: BlockManager,
    val securityManager: SecurityManager,
    val metricsSystem: MetricsSystem,
    val memoryManager: MemoryManager,
    val outputCommitCoordinator: OutputCommitCoordinator,
    val conf: SparkConf) extends Logging {

  /**
    *
    */
  @volatile private[spark] var isStopped = false

  /**
    * 所有Python实现的worker缓存
    */
  private val pythonWorkers = mutable.HashMap[(String, Map[String, String]), PythonWorkerFactory]()

  // A general, soft-reference map for metadata needed during HadoopRDD split computation
  // (e.g., HadoopFileRDD uses this to cache JobConfs and InputFormats).
  /**
    * HadoopRDD进行任务切分时所需的元数据软引用。
    * 例如，HadoopFileRDD将使用hadoopJobMetadata缓存JobConf和InputFormat
    */
  private[spark] val hadoopJobMetadata = new MapMaker().softValues().makeMap[String, Any]()


  /**
    * driver的临时目录。
    */
  private[spark] var driverTmpDir: Option[String] = None

  private[spark] def stop(): Unit = {

    if (!isStopped) {
      isStopped = true
      pythonWorkers.values.foreach(_.stop())
      mapOutputTracker.stop()
      shuffleManager.stop()
      broadcastManager.stop()
      blockManager.stop()
      blockManager.master.stop()
      metricsSystem.stop()
      outputCommitCoordinator.stop()
      rpcEnv.shutdown()
      rpcEnv.awaitTermination()

      // If we only stop sc, but the driver process still run as a services then we need to delete
      // the tmp dir, if not, it will create too many tmp dirs.
      // We only need to delete the tmp dir create by driver
      driverTmpDir match {
        case Some(path) =>
          try {
            Utils.deleteRecursively(new File(path))
          } catch {
            case e: Exception =>
              logWarning(s"Exception while deleting Spark temp dir: $path", e)
          }
        case None => // We just need to delete tmp dir created by driver, so do nothing on executor
      }
    }
  }

  private[spark]
  def createPythonWorker(pythonExec: String, envVars: Map[String, String]): java.net.Socket = {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.getOrElseUpdate(key, new PythonWorkerFactory(pythonExec, envVars)).create()
    }
  }

  private[spark]
  def destroyPythonWorker(pythonExec: String,
      envVars: Map[String, String], worker: Socket): Unit = {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.get(key).foreach(_.stopWorker(worker))
    }
  }

  private[spark]
  def releasePythonWorker(pythonExec: String,
      envVars: Map[String, String], worker: Socket): Unit = {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.get(key).foreach(_.releaseWorker(worker))
    }
  }
}

object SparkEnv extends Logging {
  @volatile private var env: SparkEnv = _

  private[spark] val driverSystemName = "sparkDriver"
  private[spark] val executorSystemName = "sparkExecutor"

  def set(e: SparkEnv): Unit = {
    env = e
  }

  /**
   * Returns the SparkEnv.
   */
  def get: SparkEnv = {
    env
  }

  /**
   * Create a SparkEnv for the driver.
   */
  private[spark] def createDriverEnv(
      conf: SparkConf,
      isLocal: Boolean,
      listenerBus: LiveListenerBus,
      numCores: Int,
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {
    assert(conf.contains(DRIVER_HOST_ADDRESS),
      s"${DRIVER_HOST_ADDRESS.key} is not set on the driver!")
    assert(conf.contains(DRIVER_PORT), s"${DRIVER_PORT.key} is not set on the driver!")
    val bindAddress = conf.get(DRIVER_BIND_ADDRESS)
    val advertiseAddress = conf.get(DRIVER_HOST_ADDRESS)
    val port = conf.get(DRIVER_PORT)
    val ioEncryptionKey = if (conf.get(IO_ENCRYPTION_ENABLED)) {
      Some(CryptoStreamUtils.createKey(conf))
    } else {
      None
    }
    create(
      conf,
      SparkContext.DRIVER_IDENTIFIER,
      bindAddress,
      advertiseAddress,
      Option(port),
      isLocal,
      numCores,
      ioEncryptionKey,
      listenerBus = listenerBus,
      mockOutputCommitCoordinator = mockOutputCommitCoordinator
    )
  }

  /**
   * Create a SparkEnv for an executor.
   * In coarse-grained mode, the executor provides an RpcEnv that is already instantiated.
   */
  private[spark] def createExecutorEnv(
      conf: SparkConf,
      executorId: String,
      hostname: String,
      numCores: Int,
      ioEncryptionKey: Option[Array[Byte]],
      isLocal: Boolean): SparkEnv = {
    val env = create(
      conf,
      executorId,
      hostname,
      hostname,
      None,
      isLocal,
      numCores,
      ioEncryptionKey
    )
    SparkEnv.set(env)
    env
  }

  /**
   * Helper method to create a SparkEnv for a driver or an executor.
   */
  private def create(
      conf: SparkConf,
      executorId: String,
      bindAddress: String,
      advertiseAddress: String,
      port: Option[Int],
      isLocal: Boolean,
      numUsableCores: Int,
      ioEncryptionKey: Option[Array[Byte]],
      listenerBus: LiveListenerBus = null,
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {

    val isDriver = executorId == SparkContext.DRIVER_IDENTIFIER

    // Listener bus is only used on the driver
    if (isDriver) {
      assert(listenerBus != null, "Attempted to create driver SparkEnv with null listener bus!")
    }

    /**
      * 安全管理器
      */
    val authSecretFileConf = if (isDriver) AUTH_SECRET_FILE_DRIVER else AUTH_SECRET_FILE_EXECUTOR
    val securityManager = new SecurityManager(conf, ioEncryptionKey, authSecretFileConf)
    if (isDriver) {
      securityManager.initializeAuth()
    }

    ioEncryptionKey.foreach { _ =>
      if (!securityManager.isEncryptionEnabled()) {
        logWarning("I/O encryption enabled without RPC encryption: keys will be visible on the " +
          "wire.")
      }
    }

    /**
      * RPC环境
      * 如果为Drivre名称为 sparkDriver， 否则为sparkExecutor
      */
    val systemName = if (isDriver) driverSystemName else executorSystemName
    val rpcEnv = RpcEnv.create(systemName, bindAddress, advertiseAddress, port.getOrElse(-1), conf,
      securityManager, numUsableCores, !isDriver)


    // Figure out which port RpcEnv actually bound to in case the original port is 0 or occupied.
    if (isDriver) {
      conf.set(DRIVER_PORT, rpcEnv.address.port)
    }

    // Create an instance of the class with the given name, possibly initializing it with our conf
    def instantiateClass[T](className: String): T = {
      val cls = Utils.classForName(className)
      // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
      // SparkConf, then one taking no arguments
      try {
        cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
          .newInstance(conf, java.lang.Boolean.valueOf(isDriver))
          .asInstanceOf[T]
      } catch {
        case _: NoSuchMethodException =>
          try {
            cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
          } catch {
            case _: NoSuchMethodException =>
              cls.getConstructor().newInstance().asInstanceOf[T]
          }
      }
    }

    // Create an instance of the class named by the given SparkConf property
    // if the property is not set, possibly initializing it with our conf
    def instantiateClassFromConf[T](propertyName: ConfigEntry[String]): T = {
      instantiateClass[T](conf.get(propertyName))
    }

    /**
      * 序列化管理器
      */
    val serializer = instantiateClassFromConf[Serializer](SERIALIZER)
    logDebug(s"Using serializer: ${serializer.getClass}")

    val serializerManager = new SerializerManager(serializer, conf, ioEncryptionKey)

    val closureSerializer = new JavaSerializer(conf)

    /**
      * 用于注册RpcEndpoint或者查找RpcEndpoint的方法
      * @param name
      * @param endpointCreator
      * @return
      */
    def registerOrLookupEndpoint(
        name: String, endpointCreator: => RpcEndpoint):
      RpcEndpointRef = {
      //如果当前实例是Driver，则调用代码setupEndpoint方法向Dispatcher注册Endpoint
      if (isDriver) {
        logInfo("Registering " + name)
        rpcEnv.setupEndpoint(name, endpointCreator)

        //如果是Executor，则调用工具类RpcUtils的makeDriverRef方法向远端的NettyRpcEnv询问获取相关的RpcEndpointRef
      } else {
        RpcUtils.makeDriverRef(name, conf, rpcEnv)
      }
    }

    /**
      * 广播管理器
      */
    val broadcastManager = new BroadcastManager(isDriver, conf, securityManager)


    /**
      * map任务输出跟踪器
      * 如果当前实例是Driver，则创建MapOutputTrackerMaster，然后创建MapOutputTrackerMasterEndpoint，
      * 并注册到Dispatcher中， 名为MapOutputTracker
      *
      * 如果当前实例是Executor，则创建MapOutputTrackerWorker，
      * 并从远端Driver实例的NettryRpcEnv的Dispatcher中查找MapOutputTrackerMasterEndpoint的引用
      */
    val mapOutputTracker = if (isDriver) {
      new MapOutputTrackerMaster(conf, broadcastManager, isLocal)
    } else {
      new MapOutputTrackerWorker(conf)
    }

    // Have to assign trackerEndpoint after initialization as MapOutputTrackerEndpoint
    // requires the MapOutputTracker itself
    mapOutputTracker.trackerEndpoint = registerOrLookupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(
        rpcEnv, mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf))


    /**
      * 构建存储体系
      */
    // Let the user specify short names for shuffle managers
    val shortShuffleMgrNames = Map(
      "sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName,
      "tungsten-sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName)
    val shuffleMgrName = conf.get(config.SHUFFLE_MANAGER)
    val shuffleMgrClass =
      shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase(Locale.ROOT), shuffleMgrName)
    /**
      * 根据配置获取ShuffleManager，其实总为 SortShuffleManager
      */
    val shuffleManager = instantiateClass[ShuffleManager](shuffleMgrClass)


    /**
      *
      */
    val memoryManager: MemoryManager = UnifiedMemoryManager(conf, numUsableCores)

    /**
      * 获取当前SparkEnv的块传输服务BlockTransferService对外提供的端口号。
      * 如果当前为Driver，则从SparkConf中获取DRIVER_BLOCK_MANAGER_PORT指定的端口
      * 如果当前为Master，则从SparkConf中获取BLOCK_MANAGER_PORT指定的端口
      */
    val blockManagerPort = if (isDriver) {
      conf.get(DRIVER_BLOCK_MANAGER_PORT)
    } else {
      conf.get(BLOCK_MANAGER_PORT)
    }

    val externalShuffleClient = if (conf.get(config.SHUFFLE_SERVICE_ENABLED)) {
      val transConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numUsableCores)
      Some(new ExternalBlockStoreClient(transConf, securityManager,
        securityManager.isAuthenticationEnabled(), conf.get(config.SHUFFLE_REGISTRATION_TIMEOUT)))
    } else {
      None
    }

    val blockManagerMaster = new BlockManagerMaster(registerOrLookupEndpoint(
      BlockManagerMaster.DRIVER_ENDPOINT_NAME,
      new BlockManagerMasterEndpoint(
        rpcEnv,
        isLocal,
        conf,
        listenerBus,
        if (conf.get(config.SHUFFLE_SERVICE_FETCH_RDD_ENABLED)) {
          externalShuffleClient
        } else {
          None
        })),
      conf, isDriver)

    val blockTransferService =
      new NettyBlockTransferService(conf, securityManager, bindAddress, advertiseAddress,
        blockManagerPort, numUsableCores, blockManagerMaster.driverEndpoint)

    // NB: blockManager is not valid until initialize() is called later.
    val blockManager = new BlockManager(
      executorId,
      rpcEnv,
      blockManagerMaster,
      serializerManager,
      conf,
      memoryManager,
      mapOutputTracker,
      shuffleManager,
      blockTransferService,
      securityManager,
      externalShuffleClient)


    /**
      * 创建度量系统
      */
    val metricsSystem = if (isDriver) {
      // Don't start metrics system right now for Driver.
      // We need to wait for the task scheduler to give us an app ID.
      // Then we can start the metrics system.
      //当前实例为Driver，创建度量系统，并指定度量系统的名为driver。此时虽然创建了，但并未启动，
      // 目的是等待SparkContext中的任务调度器TaskScheduler告诉度量系统应用程序的ID后启动
      MetricsSystem.createMetricsSystem(MetricsSystemInstances.DRIVER, conf, securityManager)
    } else {
      // We need to set the executor ID before the MetricsSystem is created because sources and
      // sinks specified in the metrics configuration file will want to incorporate this executor's
      // ID into the metrics they report.
      //当前实例为Executor， 设置spark.executor.id属性为Executor的ID，然后启动度量系统
      conf.set(EXECUTOR_ID, executorId)
      val ms = MetricsSystem.createMetricsSystem(MetricsSystemInstances.EXECUTOR, conf,
        securityManager)
      ms.start()
      ms
    }


    /**
      * 输出提交协调器
      * 当Spark应用程序使用了Spark SQL（包括Hive）或者需要将任务输出保存到HDFS时，
      * 就会用到输出提交协调器OutputCommitCoordinator， OutputCommitCoordinator将决定任务是否可以提交输出到HDFS
      * 无论是Driver还是Executor，在SparkEnv中都包含了自组件OutputCommitCoordinator。
      * 在Driver上注册了OutputCommitCoordinatorEndpoint，所有Executor上的OutputCommitCoordinator都是通过
      * OutputCommitCoordinatorEndPoint的RpcEndpointRef来询问Driver上的OutputCommitCoordinator，是否能够输出提交到HDFS
      */
    /**
      * 新建OutputCommitCoordinator实例
      */
    val outputCommitCoordinator = mockOutputCommitCoordinator.getOrElse {
      new OutputCommitCoordinator(conf, isDriver)
    }

    /**
      * 如果当前实例是Driver，则创建OutputCommitCoordinatorEndpoint，并注册到Dispatcher中，名为OutputCommitCoordinator
      * 如果当前应用程序是Executor，则从远端Driver实例的NettyRpcEnv的Dispatcher中查找OutputCommitCoordinatorEndpoint的引用
      */
    val outputCommitCoordinatorRef = registerOrLookupEndpoint("OutputCommitCoordinator",
      new OutputCommitCoordinatorEndpoint(rpcEnv, outputCommitCoordinator))

    /**
      * 无论是Driver还是Executor，最后都有coordinatorRef持有OutputCommitCoordinatorEndpoint的引用
      */
    outputCommitCoordinator.coordinatorRef = Some(outputCommitCoordinatorRef)



    /**
      * 构造SparkEnv
      */
    val envInstance = new SparkEnv(
      executorId,
      rpcEnv,
      serializer,
      closureSerializer,
      serializerManager,
      mapOutputTracker,
      shuffleManager,
      broadcastManager,
      blockManager,
      securityManager,
      metricsSystem,
      memoryManager,
      outputCommitCoordinator,
      conf)

    // Add a reference to tmp dir created by driver, we will delete this tmp dir when stop() is
    // called, and we only need to do it for driver. Because driver may run as a service, and if we
    // don't delete this tmp dir when sc is stopped, then will create too many tmp dirs.

    if (isDriver) {
      val sparkFilesDir = Utils.createTempDir(Utils.getLocalDir(conf), "userFiles").getAbsolutePath
      envInstance.driverTmpDir = Some(sparkFilesDir)
    }

    envInstance
  }

  /**
   * Return a map representation of jvm information, Spark properties, system properties, and
   * class paths. Map keys define the category, and map values represent the corresponding
   * attributes as a sequence of KV pairs. This is used mainly for SparkListenerEnvironmentUpdate.
   */
  private[spark]
  def environmentDetails(
      conf: SparkConf,
      hadoopConf: Configuration,
      schedulingMode: String,
      addedJars: Seq[String],
      addedFiles: Seq[String]): Map[String, Seq[(String, String)]] = {

    import Properties._
    val jvmInformation = Seq(
      ("Java Version", s"$javaVersion ($javaVendor)"),
      ("Java Home", javaHome),
      ("Scala Version", versionString)
    ).sorted

    // Spark properties
    // This includes the scheduling mode whether or not it is configured (used by SparkUI)
    val schedulerMode =
      if (!conf.contains(SCHEDULER_MODE)) {
        Seq((SCHEDULER_MODE.key, schedulingMode))
      } else {
        Seq.empty[(String, String)]
      }
    val sparkProperties = (conf.getAll ++ schedulerMode).sorted

    // System properties that are not java classpaths
    val systemProperties = Utils.getSystemProperties.toSeq
    val otherProperties = systemProperties.filter { case (k, _) =>
      k != "java.class.path" && !k.startsWith("spark.")
    }.sorted

    // Class paths including all added jars and files
    val classPathEntries = javaClassPath
      .split(File.pathSeparator)
      .filterNot(_.isEmpty)
      .map((_, "System Classpath"))
    val addedJarsAndFiles = (addedJars ++ addedFiles).map((_, "Added By User"))
    val classPaths = (addedJarsAndFiles ++ classPathEntries).sorted

    // Add Hadoop properties, it will not ignore configs including in Spark. Some spark
    // conf starting with "spark.hadoop" may overwrite it.
    val hadoopProperties = hadoopConf.asScala
      .map(entry => (entry.getKey, entry.getValue)).toSeq.sorted
    Map[String, Seq[(String, String)]](
      "JVM Information" -> jvmInformation,
      "Spark Properties" -> sparkProperties,
      "Hadoop Properties" -> hadoopProperties,
      "System Properties" -> otherProperties,
      "Classpath Entries" -> classPaths)
  }
}
