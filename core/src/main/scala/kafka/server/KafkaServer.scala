/**
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

package kafka.server

import java.io.{File, IOException}
import java.net.{InetAddress, SocketTimeoutException}
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import kafka.api.{KAFKA_0_9_0, KAFKA_2_2_IV0, KAFKA_2_4_IV1}
import kafka.cluster.Broker
import kafka.common.{GenerateBrokerIdException, InconsistentBrokerIdException, InconsistentBrokerMetadataException, InconsistentClusterIdException}
import kafka.controller.KafkaController
import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.log.{LogConfig, LogManager}
import kafka.metrics.{KafkaMetricsGroup, KafkaMetricsReporter, KafkaYammerMetrics}
import kafka.network.SocketServer
import kafka.security.CredentialProvider
import kafka.utils._
import kafka.zk.{BrokerInfo, KafkaZkClient}
import org.apache.kafka.clients.{ApiVersions, ClientDnsLookup, ManualMetadataUpdater, NetworkClient, NetworkClientUtils, CommonClientConfigs}
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.message.ControlledShutdownRequestData
import org.apache.kafka.common.metrics.{JmxReporter, Metrics, MetricsReporter, _}
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ControlledShutdownRequest, ControlledShutdownResponse}
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.security.{JaasContext, JaasUtils}
import org.apache.kafka.common.utils.{AppInfoParser, LogContext, Time}
import org.apache.kafka.common.{ClusterResource, Endpoint, Node}
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.zookeeper.client.ZKClientConfig

import scala.jdk.CollectionConverters._
import scala.collection.{Map, Seq, mutable}

object KafkaServer {
  // Copy the subset of properties that are relevant to Logs
  // I'm listing out individual properties here since the names are slightly different in each Config class...
  private[kafka] def copyKafkaConfigToLog(kafkaConfig: KafkaConfig): java.util.Map[String, Object] = {
    val logProps = new util.HashMap[String, Object]()
    logProps.put(LogConfig.SegmentBytesProp, kafkaConfig.logSegmentBytes)
    logProps.put(LogConfig.SegmentMsProp, kafkaConfig.logRollTimeMillis)
    logProps.put(LogConfig.SegmentJitterMsProp, kafkaConfig.logRollTimeJitterMillis)
    logProps.put(LogConfig.SegmentIndexBytesProp, kafkaConfig.logIndexSizeMaxBytes)
    logProps.put(LogConfig.FlushMessagesProp, kafkaConfig.logFlushIntervalMessages)
    logProps.put(LogConfig.FlushMsProp, kafkaConfig.logFlushIntervalMs)
    logProps.put(LogConfig.RetentionBytesProp, kafkaConfig.logRetentionBytes)
    logProps.put(LogConfig.RetentionMsProp, kafkaConfig.logRetentionTimeMillis: java.lang.Long)
    logProps.put(LogConfig.MaxMessageBytesProp, kafkaConfig.messageMaxBytes)
    logProps.put(LogConfig.IndexIntervalBytesProp, kafkaConfig.logIndexIntervalBytes)
    logProps.put(LogConfig.DeleteRetentionMsProp, kafkaConfig.logCleanerDeleteRetentionMs)
    logProps.put(LogConfig.MinCompactionLagMsProp, kafkaConfig.logCleanerMinCompactionLagMs)
    logProps.put(LogConfig.MaxCompactionLagMsProp, kafkaConfig.logCleanerMaxCompactionLagMs)
    logProps.put(LogConfig.FileDeleteDelayMsProp, kafkaConfig.logDeleteDelayMs)
    logProps.put(LogConfig.MinCleanableDirtyRatioProp, kafkaConfig.logCleanerMinCleanRatio)
    logProps.put(LogConfig.CleanupPolicyProp, kafkaConfig.logCleanupPolicy)
    logProps.put(LogConfig.MinInSyncReplicasProp, kafkaConfig.minInSyncReplicas)
    logProps.put(LogConfig.CompressionTypeProp, kafkaConfig.compressionType)
    logProps.put(LogConfig.UncleanLeaderElectionEnableProp, kafkaConfig.uncleanLeaderElectionEnable)
    logProps.put(LogConfig.PreAllocateEnableProp, kafkaConfig.logPreAllocateEnable)
    logProps.put(LogConfig.MessageFormatVersionProp, kafkaConfig.logMessageFormatVersion.version)
    logProps.put(LogConfig.MessageTimestampTypeProp, kafkaConfig.logMessageTimestampType.name)
    logProps.put(LogConfig.MessageTimestampDifferenceMaxMsProp, kafkaConfig.logMessageTimestampDifferenceMaxMs: java.lang.Long)
    logProps.put(LogConfig.MessageDownConversionEnableProp, kafkaConfig.logMessageDownConversionEnable: java.lang.Boolean)
    logProps
  }

  private[server] def metricConfig(kafkaConfig: KafkaConfig): MetricConfig = {
    new MetricConfig()
      .samples(kafkaConfig.metricNumSamples) // todo 默认 2
      .recordLevel(Sensor.RecordingLevel.forName(kafkaConfig.metricRecordingLevel)) // todo 默认info
      .timeWindow(kafkaConfig.metricSampleWindowMs, TimeUnit.MILLISECONDS) // todo 默认为30s
  }

  def zkClientConfigFromKafkaConfig(config: KafkaConfig, forceZkSslClientEnable: Boolean = false) =
    if (!config.zkSslClientEnable && !forceZkSslClientEnable)
      None
    else {
      val clientConfig = new ZKClientConfig()
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslClientEnableProp, "true")
      config.zkClientCnxnSocketClassName.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkClientCnxnSocketProp, _))
      config.zkSslKeyStoreLocation.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslKeyStoreLocationProp, _))
      config.zkSslKeyStorePassword.foreach(x => KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslKeyStorePasswordProp, x.value))
      config.zkSslKeyStoreType.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslKeyStoreTypeProp, _))
      config.zkSslTrustStoreLocation.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslTrustStoreLocationProp, _))
      config.zkSslTrustStorePassword.foreach(x => KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslTrustStorePasswordProp, x.value))
      config.zkSslTrustStoreType.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslTrustStoreTypeProp, _))
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslProtocolProp, config.ZkSslProtocol)
      config.ZkSslEnabledProtocols.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslEnabledProtocolsProp, _))
      config.ZkSslCipherSuites.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslCipherSuitesProp, _))
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp, config.ZkSslEndpointIdentificationAlgorithm)
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslCrlEnableProp, config.ZkSslCrlEnable.toString)
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslOcspEnableProp, config.ZkSslOcspEnable.toString)
      Some(clientConfig)
    }

  val MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS: Long = 120000
}

/**
 * Represents the lifecycle of a single Kafka broker. Handles all functionality required
 * to start up and shutdown a single Kafka node.
 */
// TODO:  KafkaServer 构造器， time=默认为系统时间
class KafkaServer(val config: KafkaConfig, time: Time = Time.SYSTEM, threadNamePrefix: Option[String] = None,
                  kafkaMetricsReporters: Seq[KafkaMetricsReporter] = List()) extends Logging with KafkaMetricsGroup {
  // TODO: 是否启动完成
  private val startupComplete = new AtomicBoolean(false)
  // TODO: 是否停止标志
  private val isShuttingDown = new AtomicBoolean(false)
  // TODO: 是否启动标志
  private val isStartingUp = new AtomicBoolean(false)
  // TODO: 阻塞主线程等待 KafkaServer 的关闭
  private var shutdownLatch = new CountDownLatch(1)

  //properties for MetricsContext
  private val metricsPrefix: String = "kafka.server"
  private val KAFKA_CLUSTER_ID: String = "kafka.cluster.id"
  private val KAFKA_BROKER_ID: String = "kafka.broker.id"

  // TODO: 日志对象
  private var logContext: LogContext = null

  // TODO: metrics 实例
  var kafkaYammerMetrics: KafkaYammerMetrics = null
  var metrics: Metrics = null

  // TODO: broker server的状态实例
  val brokerState: BrokerState = new BrokerState

  // TODO: API接口类，用于处理数据类请求
  var dataPlaneRequestProcessor: KafkaApis = null
  // TODO: API接口，用于处理控制类请求 
  var controlPlaneRequestProcessor: KafkaApis = null

  // TODO: 权限管理 
  var authorizer: Option[Authorizer] = None
  // TODO: 启动socket，监听9092端口，等待接收客户端请求
  var socketServer: SocketServer = null
  // TODO:  数据类请求处理线程池
  var dataPlaneRequestHandlerPool: KafkaRequestHandlerPool = null
  // TODO: 命令类处理线程池
  var controlPlaneRequestHandlerPool: KafkaRequestHandlerPool = null

  // TODO: 日志管理器
  var logDirFailureChannel: LogDirFailureChannel = null
  // TODO: 用于管理记录在本地的日志数据 
  var logManager: LogManager = null

  // TODO: 副本管理器
  var replicaManager: ReplicaManager = null
  // TODO: topic增删管理器
  var adminManager: AdminManager = null
  // TODO: token管理器
  var tokenManager: DelegationTokenManager = null

  // TODO: 动态配置
  var dynamicConfigHandlers: Map[String, ConfigHandler] = null
  var dynamicConfigManager: DynamicConfigManager = null
  var credentialProvider: CredentialProvider = null
  var tokenCache: DelegationTokenCache = null

  // TODO: 消费协调者
  var groupCoordinator: GroupCoordinator = null

  // TODO: 事务协调者
  var transactionCoordinator: TransactionCoordinator = null

  // TODO: controller
  var kafkaController: KafkaController = null

  var brokerToControllerChannelManager: BrokerToControllerChannelManager = null

  // TODO: 定时任务调度器
  var kafkaScheduler: KafkaScheduler = null

  // TODO: 集群分区状态信息缓存
  var metadataCache: MetadataCache = null
  // TODO: quota配额管理器
  var quotaManagers: QuotaFactory.QuotaManagers = null

  // TODO: zookeeper配置和客户端 , server.properties中 zookeeper.connect， zookeeper.connection.timeout.ms 配置信息
  val zkClientConfig: ZKClientConfig = KafkaServer.zkClientConfigFromKafkaConfig(config).getOrElse(new ZKClientConfig())
  private var _zkClient: KafkaZkClient = null

  val correlationId: AtomicInteger = new AtomicInteger(0)
  // TODO: broker的元数据信息， 每个磁盘都需要有这样的信息
  // cat meta.properties
  //  #
  //  #Sat Sep 23 13:04:14 SGT 2023
  //  broker.id=1009
  //  version=0
  //  cluster.id=BIbPq3azQnSIjx2QURxtBA
  //
  val brokerMetaPropsFile = "meta.properties"
  // TODO: config.logDirs. e.g. log.dirs=/mnt/ssd/0/kafka/,/mnt/ssd/1/kafka/,/mnt/ssd/2/kafka/ 
  // TODO: <"/mnt/ssd/0/kafka/", BrokerMetadataCheckpoint(new File("/mnt/ssd/0/kafka/meta.properties"))>
  // TODO: <"/mnt/ssd/1/kafka/", BrokerMetadataCheckpoint(new File("/mnt/ssd/1/kafka/meta.properties"))>
  // TODO: <"/mnt/ssd/2/kafka/", BrokerMetadataCheckpoint(new File("/mnt/ssd/2/kafka/meta.properties"))>
  val brokerMetadataCheckpoints = config.logDirs.map(logDir => (logDir, new BrokerMetadataCheckpoint(new File(logDir + File.separator + brokerMetaPropsFile)))).toMap

  // TODO: cluster.id=BIbPq3azQnSIjx2QURxtBA
  private var _clusterId: String = null
  private var _brokerTopicStats: BrokerTopicStats = null

  // TODO: 监听器
  private var _featureChangeListener: FinalizedFeatureChangeListener = null

  val brokerFeatures: BrokerFeatures = BrokerFeatures.createDefault()

  val featureCache: FinalizedFeatureCache = new FinalizedFeatureCache(brokerFeatures)

  def clusterId: String = _clusterId

  // Visible for testing
  private[kafka] def zkClient = _zkClient

  private[kafka] def brokerTopicStats = _brokerTopicStats

  private[kafka] def featureChangeListener = _featureChangeListener

  // TODO: broker的状态
  newGauge("BrokerState", () => brokerState.currentState)
  newGauge("ClusterId", () => clusterId)
  newGauge("yammer-metrics-count", () =>  KafkaYammerMetrics.defaultRegistry.allMetrics.size)

  val linuxIoMetricsCollector = new LinuxIoMetricsCollector("/proc", time, logger.underlying)

  if (linuxIoMetricsCollector.usable()) {
    newGauge("linux-disk-read-bytes", () => linuxIoMetricsCollector.readBytes())
    newGauge("linux-disk-write-bytes", () => linuxIoMetricsCollector.writeBytes())
  }

  // TODO: broker启动，初始化各个组件
  /**
   * Start up API for bringing up a single instance of the Kafka server.
   * Instantiates the LogManager, the SocketServer and the request handlers - KafkaRequestHandlers
   */
  def startup(): Unit = {
    try {
      info("starting")

      if (isShuttingDown.get)
        throw new IllegalStateException("Kafka server is still shutting down, cannot re-start!")

      if (startupComplete.get)
        return

      // TODO: 设置broker状态为 Starting ，表明broker开始启动
      val canStartup = isStartingUp.compareAndSet(false, true)
      if (canStartup) {
        brokerState.newState(Starting)

        // TODO: 初始化zk客户端 time=系统默认时间， 创建zk客户端，并且创建 broker在zk上面从存放元数据的目录
        /* setup zookeeper */
        initZkClient(time)

        /* initialize features */
        _featureChangeListener = new FinalizedFeatureChangeListener(featureCache, _zkClient)
        if (config.isFeatureVersioningSupported) {
          _featureChangeListener.initOrThrow(config.zkConnectionTimeoutMs)
        }

        // TODO: 获取或生成 clusterId
        /* Get or create cluster_id */
        _clusterId = getOrGenerateClusterId(zkClient)
        // TODO:  cluster.id=BIbPq3azQnSIjx2QURxtBA
        info(s"Cluster ID = $clusterId")

        /* load metadata */
        // TODO:  (BrokerMetadata(brokerId, clusterId), offlineDirs)
        val (preloadedBrokerMetadataCheckpoint, initialOfflineDirs) = getBrokerMetadataAndOfflineDirs

        // TODO: 这里之前有遇到过相同的异常：先创建一个kafka集群，然后把broker停掉，修改 meta.properties 文件里面的clusterId，就会出现这个异常
        /* check cluster id */
        if (preloadedBrokerMetadataCheckpoint.clusterId.isDefined && preloadedBrokerMetadataCheckpoint.clusterId.get != clusterId)
          throw new InconsistentClusterIdException(
            s"The Cluster ID ${clusterId} doesn't match stored clusterId ${preloadedBrokerMetadataCheckpoint.clusterId} in meta.properties. " +
            s"The broker is trying to join the wrong cluster. Configured zookeeper.connect may be wrong.")

        /* generate brokerId */
        // TODO: brokerId获取或生成的地方： config.brokerId, metadata.brokerId, zk generate brokerId
        config.brokerId = getOrGenerateBrokerId(preloadedBrokerMetadataCheckpoint)
        // TODO: 创建  LogContext 实例
        logContext = new LogContext(s"[KafkaServer id=${config.brokerId}] ")
        // TODO: KafkaServer继承了 Logging， 在Logging中有 logIdent
        this.logIdent = logContext.logPrefix

        // TODO: 动态配置初始化，DynamicConfigManager启动之后任何的动态修改，都会生效， 参考  dynamicConfigManager.startup()
        // initialize dynamic broker configs from ZooKeeper. Any updates made after this will be
        // applied after DynamicConfigManager starts.
        config.dynamicConfig.initialize(zkClient)

        // TODO: 配置： background.threads。 BackgroundThreads = 10 默认值 ，创建 KafkaScheduler 实例
        /* start scheduler */
        kafkaScheduler = new KafkaScheduler(config.backgroundThreads)
        // TODO: 启动  kafkaScheduler
        kafkaScheduler.startup()

        // TODO: 实例化 kafkaYammerMetrics
        /* create and configure metrics */
        kafkaYammerMetrics = KafkaYammerMetrics.INSTANCE
        kafkaYammerMetrics.configure(config.originals)

        // TODO: 创建JmxReporter实例 ，prefix=""
        val jmxReporter = new JmxReporter()
        jmxReporter.configure(config.originals)

        val reporters = new util.ArrayList[MetricsReporter]
        reporters.add(jmxReporter)

        val metricConfig = KafkaServer.metricConfig(config)
        val metricsContext = createKafkaMetricsContext()
        // TODO:  创建Metrics 实例
        metrics = new Metrics(metricConfig, reporters, time, true, metricsContext)

        // TODO: 创建  BrokerTopicStats 实例
        /* register broker metrics */
        _brokerTopicStats = new BrokerTopicStats

        // TODO: 限流管理器，创建 QuotaManagers 实例
        quotaManagers = QuotaFactory.instantiate(config, metrics, time, threadNamePrefix.getOrElse(""))
        notifyClusterListeners(kafkaMetricsReporters ++ metrics.reporters.asScala)

        // TODO: offline 的磁盘路径 通道
        logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size)

        /* start log manager */
        // TODO: initialOfflineDirs - 有IO异常的路径
        logManager = LogManager(config, initialOfflineDirs, zkClient, brokerState, kafkaScheduler, time, brokerTopicStats, logDirFailureChannel)
        // TODO: 启动 6 个线程，
        logManager.startup()

        // TODO: 创建 MetadataCache 实例
        metadataCache = new MetadataCache(config.brokerId)
        // Enable delegation token cache for all SCRAM mechanisms to simplify dynamic update.
        // This keeps the cache up-to-date if new SCRAM mechanisms are enabled dynamically.
        tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
        credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache)

        // TODO: 创建  SocketServer 对象- 启动socket，监听9092端口，等待接收客户端请求
        // Create and start the socket server acceptor threads so that the bound port is known.
        // Delay starting processors until the end of the initialization sequence to ensure
        // that credentials have been loaded before processing authentications.
        socketServer = new SocketServer(config, metrics, time, credentialProvider)
        // TODO: 调用startup方法
        socketServer.startup(startProcessingRequests = false)

        // TODO: 副本同步manager启动
        /* start replica manager */
        // TODO: 创建  BrokerToControllerChannelManagerImpl 实例，实现broker和controller之间的连接通信，进行metadata的同步
        brokerToControllerChannelManager = new BrokerToControllerChannelManagerImpl(metadataCache, time, metrics, config, threadNamePrefix)
        // TODO: 创建 副本同步manager
        replicaManager = createReplicaManager(isShuttingDown)
        // TODO: 启动  replicaManager
        replicaManager.startup()
        // TODO: 启动 brokerToControllerChannelManager，该方法里面会创建BrokerToControllerRequestThread实例，然后启动该线程
        brokerToControllerChannelManager.start()

        // TODO: 创建broker info 
        val brokerInfo = createBrokerInfo
        // TODO: 15460, 在zk中注册broker信息，并返回broker的epoch
        val brokerEpoch = zkClient.registerBroker(brokerInfo)

        // TODO: 创建BrokerMetadata(1001, BIbPq3azQnSIjx2QURxtBA)
        // Now that the broker is successfully registered, checkpoint its metadata
        // TODO: 遍历broker的所有的live dir，往每个dir里面写入
        // cat meta.properties
        //  #
        //  #Sat Sep 23 13:04:14 SGT 2023
        //  broker.id=1001
        //  version=0
        //  cluster.id=BIbPq3azQnSIjx2QURxtBA
        checkpointBrokerMetadata(BrokerMetadata(config.brokerId, Some(clusterId)))

        // TODO: 创建 DelegationTokenManager 实例
        /* start token manager */
        tokenManager = new DelegationTokenManager(config, tokenCache, time , zkClient)
        // TODO: 由于我们没有配置，所以这里不会做什么事情 
        tokenManager.startup()

        // TODO: 构建KafkaController对象实例
        /* start kafka controller */
        kafkaController = new KafkaController(config, zkClient, time, metrics, brokerInfo, brokerEpoch, tokenManager, brokerFeatures, featureCache, threadNamePrefix)
        // TODO: KafkaController 启动
        kafkaController.startup()

        // TODO: 创建 AdminManager 实例, 提供一些kafka内部使用的接口，e.g. 创建topic， 删除topic等操作
        adminManager = new AdminManager(config, metrics, metadataCache, zkClient)

        // TODO: 创建GroupCoordinator实例
        /* start group coordinator */
        // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
        groupCoordinator = GroupCoordinator(config, zkClient, replicaManager, Time.SYSTEM, metrics)
        groupCoordinator.startup()

        /* start transaction coordinator, with a separate background thread scheduler for transaction expiration and log loading */
        // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
        // TODO: 创建事务coordinator实例 ，可以看到和groupCoordinator类似
        transactionCoordinator = TransactionCoordinator(config, replicaManager, new KafkaScheduler(threads = 1, threadNamePrefix = "transaction-log-manager-"), zkClient, metrics, metadataCache, Time.SYSTEM)
        // TODO: 1. 启动scheduler线程
        //       2. 往scheduler线程里面添加 transaction-abort 任务
        //       3. 往scheduler线程里面添加 transactionalId-expiration 任务
        //       4. 启动 txnMarkerChannelManager线程
        //       5. 修改 isActive=true，表示启动完成
        transactionCoordinator.startup()

        // TODO: 认证相关组件，默认是没有配置认证组件的
        /* Get the authorizer and initialize it if one is specified.*/
        authorizer = config.authorizer
        authorizer.foreach(_.configure(config.originals))
        val authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = authorizer match {
          case Some(authZ) =>
            authZ.start(brokerInfo.broker.toServerInfo(clusterId, config)).asScala.map { case (ep, cs) =>
              ep -> cs.toCompletableFuture
            }
          case None =>
            brokerInfo.broker.endPoints.map { ep =>
              ep.toJava -> CompletableFuture.completedFuture[Void](null)
            }.toMap
        }

        // TODO:  创建FetchManager实例
        val fetchManager = new FetchManager(Time.SYSTEM,
          new FetchSessionCache(config.maxIncrementalFetchSessionCacheSlots, // TODO: 1000
            KafkaServer.MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS)) // TODO: 120s

        // TODO: API接口类，用于处理数据类请求 ,创建KafkaApis实例,即所有的数据请求都会通过这个API类进行处理
        /* start processing requests */
        dataPlaneRequestProcessor = new KafkaApis(socketServer.dataPlaneRequestChannel, replicaManager, adminManager, groupCoordinator, transactionCoordinator,
          kafkaController, zkClient, config.brokerId, config, metadataCache, metrics, authorizer, quotaManagers,
          fetchManager, brokerTopicStats, clusterId, time, tokenManager, brokerFeatures, featureCache)

        // TODO: 创建KafkaRequestHandlerPool实例 ， config.numIoThreads=8， SocketServer.DataPlaneThreadPrefix=data-plane
        dataPlaneRequestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.dataPlaneRequestChannel, dataPlaneRequestProcessor, time,
          config.numIoThreads, s"${SocketServer.DataPlaneMetricPrefix}RequestHandlerAvgIdlePercent", SocketServer.DataPlaneThreadPrefix)

        // TODO: 由于这里的RequestChannel大小为1，所以遍历队列元素，进行processor和handlerPool创建，所以KafkaApis和 KafkaRequestHandlerPool 只是创建一次
        socketServer.controlPlaneRequestChannelOpt.foreach { controlPlaneRequestChannel =>
          controlPlaneRequestProcessor = new KafkaApis(controlPlaneRequestChannel, replicaManager, adminManager, groupCoordinator, transactionCoordinator,
            kafkaController, zkClient, config.brokerId, config, metadataCache, metrics, authorizer, quotaManagers,
            fetchManager, brokerTopicStats, clusterId, time, tokenManager, brokerFeatures, featureCache)

          // TODO: 这里的线程池大小为1 ， ControlPlaneRequestHandlerAvgIdlePercent， ControlPlaneThreadPrefix=control-plane
          controlPlaneRequestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.controlPlaneRequestChannelOpt.get, controlPlaneRequestProcessor, time,
            1, s"${SocketServer.ControlPlaneMetricPrefix}RequestHandlerAvgIdlePercent", SocketServer.ControlPlaneThreadPrefix)
        }

        Mx4jLoader.maybeLoad()

        // TODO: 动态配置
        /* Add all reconfigurables for config change notification before starting config handlers */
        config.dynamicConfig.addReconfigurables(this)

        // TODO: 动态配置支持4种类型 topic, client, user, broker, 并且每一种类型对应单独的处理器器
        /* start dynamic config manager */
        dynamicConfigHandlers = Map[String, ConfigHandler](ConfigType.Topic -> new TopicConfigHandler(logManager, config, quotaManagers, kafkaController),
                                                           ConfigType.Client -> new ClientIdConfigHandler(quotaManagers),
                                                           ConfigType.User -> new UserConfigHandler(quotaManagers, credentialProvider),
                                                           ConfigType.Broker -> new BrokerConfigHandler(config, quotaManagers))

        // TODO: 创建  DynamicConfigManager 实例
        // Create the config manager. start listening to notifications
        dynamicConfigManager = new DynamicConfigManager(zkClient, dynamicConfigHandlers)
        // TODO: 调用startup方法，启动动态配置manager
        dynamicConfigManager.startup()

        // TODO: 开始启动控制流和数据流的 Processor， Acceptor ，这里开启以后，broker就可以开始处理各种请求了
        socketServer.startProcessingRequests(authorizerFutures)

        brokerState.newState(RunningAsBroker)
        shutdownLatch = new CountDownLatch(1)
        startupComplete.set(true)
        isStartingUp.set(false)
        AppInfoParser.registerAppInfo(metricsPrefix, config.brokerId.toString, metrics, time.milliseconds())
        info("started")
      }
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServer startup. Prepare to shutdown", e)
        isStartingUp.set(false)
        shutdown()
        throw e
    }
  }

  private[server] def notifyClusterListeners(clusterListeners: Seq[AnyRef]): Unit = {
    // TODO: 创建  ClusterResourceListeners 实例
    val clusterResourceListeners = new ClusterResourceListeners
    clusterResourceListeners.maybeAddAll(clusterListeners.asJava)
    clusterResourceListeners.onUpdate(new ClusterResource(clusterId))
  }

  private[server] def notifyMetricsReporters(metricsReporters: Seq[AnyRef]): Unit = {
    val metricsContext = createKafkaMetricsContext()
    metricsReporters.foreach {
      case x: MetricsReporter => x.contextChange(metricsContext)
      case _ => //do nothing
    }
  }

  private[server] def createKafkaMetricsContext() : KafkaMetricsContext = {
    val contextLabels = new util.HashMap[String, Object]
    contextLabels.put(KAFKA_CLUSTER_ID, clusterId)
    contextLabels.put(KAFKA_BROKER_ID, config.brokerId.toString)
    // TODO: 配置文件中 以 metrics.context. 开头的配置 
    contextLabels.putAll(config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX))
    // TODO: 创建  KafkaMetricsContext实例， metricsPrefix=kafka.server 
    val metricsContext = new KafkaMetricsContext(metricsPrefix, contextLabels)
    metricsContext
  }

  // TODO: 1. 创建  AlterIsrManagerImpl 实例
  //  2. 创建 ReplicaManager实例
  protected def createReplicaManager(isShuttingDown: AtomicBoolean): ReplicaManager = {
    // TODO: 创建  AlterIsrManagerImpl 实例
    val alterIsrManager = new AlterIsrManagerImpl(brokerToControllerChannelManager, kafkaScheduler,
      time, config.brokerId, () => kafkaController.brokerEpoch)
    // TODO: 创建 ReplicaManager实例
    new ReplicaManager(config, metrics, time, zkClient, kafkaScheduler, logManager, isShuttingDown, quotaManagers,
      brokerTopicStats, metadataCache, logDirFailureChannel, alterIsrManager)
  }

  private def initZkClient(time: Time): Unit = {
    // TODO: zookeeper.connect=zk-node:2131,zk-node1:2131,zk-node2:2131/kafka01
    info(s"Connecting to zookeeper on ${config.zkConnect}")

    def createZkClient(zkConnect: String, isSecure: Boolean) = {
      // TODO: 创建 KafkaZkClient 实例
      //  zkSessionTimeoutMs=18000
      //  zkConnectionTimeoutMs=18000
      //  zkMaxInFlightRequests=10
      KafkaZkClient(zkConnect, isSecure, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs,
        config.zkMaxInFlightRequests, time, name = Some("Kafka server"), zkClientConfig = Some(zkClientConfig))
    }

    // TODO: 判断zk信息中是否有 '/'
    val chrootIndex = config.zkConnect.indexOf("/")
    val chrootOption = {
      // TODO: 配置了 '/', /kafka01
      if (chrootIndex > 0) Some(config.zkConnect.substring(chrootIndex))
      // TODO: 如果是配置 zk-node:2131,zk-node1:2131,zk-node2:2131，没有 '/' 就会以zk的根目录进行创建kafka的zk节点信息
      else None
    }

    // TODO: 是否配置了zk认证 
    val secureAclsEnabled = config.zkEnableSecureAcls
    val isZkSecurityEnabled = JaasUtils.isZkSaslEnabled() || KafkaConfig.zkTlsClientAuthEnabled(zkClientConfig)

    if (secureAclsEnabled && !isZkSecurityEnabled)
      throw new java.lang.SecurityException(s"${KafkaConfig.ZkEnableSecureAclsProp} is true, but ZooKeeper client TLS configuration identifying at least $KafkaConfig.ZkSslClientEnableProp, $KafkaConfig.ZkClientCnxnSocketProp, and $KafkaConfig.ZkSslKeyStoreLocationProp was not present and the " +
        s"verification of the JAAS login file failed ${JaasUtils.zkSecuritySysConfigString}")

    // TODO: chrootOption=zk-node:2131,zk-node1:2131,zk-node2:2131 
    // make sure chroot path exists
    chrootOption.foreach { chroot => //     chroot=/kafka01
      // TODO: zk-node:2131,zk-node1:2131,zk-node2:2131 
      val zkConnForChrootCreation = config.zkConnect.substring(0, chrootIndex)
      // TODO: 创建zk客户端 
      val zkClient = createZkClient(zkConnForChrootCreation, secureAclsEnabled)
      // TODO: 创建 /kafka01 目录 
      zkClient.makeSurePersistentPathExists(chroot)
      info(s"Created zookeeper path $chroot")
      // TODO: 关闭zk客户端，这里的主要功能是检查1. 能够连接到zk，2. 检查chroot 路径是否存在
      zkClient.close()
    }

    // TODO: 创建真正的zk客户端
    _zkClient = createZkClient(config.zkConnect, secureAclsEnabled)
    // TODO: 递归创建zk上面的存元数据的目录
    _zkClient.createTopLevelPaths()
  }

  private def getOrGenerateClusterId(zkClient: KafkaZkClient): String = {
    // TODO:  3bjYkEdwQ7WLZZue8vBorw
    zkClient.getClusterId.getOrElse(zkClient.createOrGetClusterId(CoreUtils.generateUuidAsBase64()))
  }

  def createBrokerInfo: BrokerInfo = {
    // TODO: SASL_PLAINTEXT://10.123.123.123:9092
    val endPoints = config.advertisedListeners.map(e => s"${e.host}:${e.port}")
    // TODO: 从zk中获取已经注册的broker信息，然后检查是否和该broker有重复的记录，预期结果是没有重复记录。如果有，则抛出IllegalArgumentException
    zkClient.getAllBrokersInCluster.filter(_.id != config.brokerId).foreach { broker =>
      val commonEndPoints = broker.endPoints.map(e => s"${e.host}:${e.port}").intersect(endPoints)
      require(commonEndPoints.isEmpty, s"Configured end points ${commonEndPoints.mkString(",")} in" +
        s" advertised listeners are already registered by broker ${broker.id}")
    }

    // TODO: 配置的是 port=9092
    val listeners = config.advertisedListeners.map { endpoint =>
      if (endpoint.port == 0)
        endpoint.copy(port = socketServer.boundPort(endpoint.listenerName))
      else
      // TODO: SASL_PLAINTEXT://10.123.123.123:9092
        endpoint
    }

    val updatedEndpoints = listeners.map(endpoint =>
      if (endpoint.host == null || endpoint.host.trim.isEmpty)
        endpoint.copy(host = InetAddress.getLocalHost.getCanonicalHostName)
      else
      // TODO: SASL_PLAINTEXT://10.123.123.123:9092
        endpoint
    )

    // TODO: com.sun.management.jmxremote.port=9010 by default
    val jmxPort = System.getProperty("com.sun.management.jmxremote.port", "-1").toInt
    BrokerInfo(
      // TODO: rack=null
      Broker(config.brokerId, updatedEndpoints, config.rack, brokerFeatures.supportedFeatures),
      config.interBrokerProtocolVersion, // TODO: 2.7-IV2
      jmxPort)
  }

  /**
   * Performs controlled shutdown
   */
  private def controlledShutdown(): Unit = {

    def node(broker: Broker): Node = broker.node(config.interBrokerListenerName)

    val socketTimeoutMs = config.controllerSocketTimeoutMs

    def doControlledShutdown(retries: Int): Boolean = {
      val metadataUpdater = new ManualMetadataUpdater()
      val networkClient = {
        val channelBuilder = ChannelBuilders.clientChannelBuilder(
          config.interBrokerSecurityProtocol,
          JaasContext.Type.SERVER,
          config,
          config.interBrokerListenerName,
          config.saslMechanismInterBrokerProtocol,
          time,
          config.saslInterBrokerHandshakeRequestEnable,
          logContext)
        val selector = new Selector(
          NetworkReceive.UNLIMITED,
          config.connectionsMaxIdleMs,
          metrics,
          time,
          "kafka-server-controlled-shutdown",
          Map.empty.asJava,
          false,
          channelBuilder,
          logContext
        )
        new NetworkClient(
          selector,
          metadataUpdater,
          config.brokerId.toString,
          1,
          0,
          0,
          Selectable.USE_DEFAULT_BUFFER_SIZE,
          Selectable.USE_DEFAULT_BUFFER_SIZE,
          config.requestTimeoutMs,
          config.connectionSetupTimeoutMs,
          config.connectionSetupTimeoutMaxMs,
          ClientDnsLookup.USE_ALL_DNS_IPS,
          time,
          false,
          new ApiVersions,
          logContext)
      }

      var shutdownSucceeded: Boolean = false

      try {

        var remainingRetries = retries
        var prevController: Broker = null
        var ioException = false

        while (!shutdownSucceeded && remainingRetries > 0) {
          remainingRetries = remainingRetries - 1

          // 1. Find the controller and establish a connection to it.

          // Get the current controller info. This is to ensure we use the most recent info to issue the
          // controlled shutdown request.
          // If the controller id or the broker registration are missing, we sleep and retry (if there are remaining retries)
          zkClient.getControllerId match {
            case Some(controllerId) =>
              zkClient.getBroker(controllerId) match {
                case Some(broker) =>
                  // if this is the first attempt, if the controller has changed or if an exception was thrown in a previous
                  // attempt, connect to the most recent controller
                  if (ioException || broker != prevController) {

                    ioException = false

                    if (prevController != null)
                      networkClient.close(node(prevController).idString)

                    prevController = broker
                    metadataUpdater.setNodes(Seq(node(prevController)).asJava)
                  }
                case None =>
                  info(s"Broker registration for controller $controllerId is not available (i.e. the Controller's ZK session expired)")
              }
            case None =>
              info("No controller registered in ZooKeeper")
          }

          // 2. issue a controlled shutdown to the controller
          if (prevController != null) {
            try {

              if (!NetworkClientUtils.awaitReady(networkClient, node(prevController), time, socketTimeoutMs))
                throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")

              // send the controlled shutdown request
              val controlledShutdownApiVersion: Short =
                if (config.interBrokerProtocolVersion < KAFKA_0_9_0) 0
                else if (config.interBrokerProtocolVersion < KAFKA_2_2_IV0) 1
                else if (config.interBrokerProtocolVersion < KAFKA_2_4_IV1) 2
                else 3

              val controlledShutdownRequest = new ControlledShutdownRequest.Builder(
                  new ControlledShutdownRequestData()
                    .setBrokerId(config.brokerId)
                    .setBrokerEpoch(kafkaController.brokerEpoch),
                    controlledShutdownApiVersion)
              val request = networkClient.newClientRequest(node(prevController).idString, controlledShutdownRequest,
                time.milliseconds(), true)
              val clientResponse = NetworkClientUtils.sendAndReceive(networkClient, request, time)

              val shutdownResponse = clientResponse.responseBody.asInstanceOf[ControlledShutdownResponse]
              if (shutdownResponse.error == Errors.NONE && shutdownResponse.data.remainingPartitions.isEmpty) {
                shutdownSucceeded = true
                info("Controlled shutdown succeeded")
              }
              else {
                info(s"Remaining partitions to move: ${shutdownResponse.data.remainingPartitions}")
                info(s"Error from controller: ${shutdownResponse.error}")
              }
            }
            catch {
              case ioe: IOException =>
                ioException = true
                warn("Error during controlled shutdown, possibly because leader movement took longer than the " +
                  s"configured controller.socket.timeout.ms and/or request.timeout.ms: ${ioe.getMessage}")
                // ignore and try again
            }
          }
          if (!shutdownSucceeded) {
            Thread.sleep(config.controlledShutdownRetryBackoffMs)
            warn("Retrying controlled shutdown after the previous attempt failed...")
          }
        }
      }
      finally
        networkClient.close()

      shutdownSucceeded
    }

    if (startupComplete.get() && config.controlledShutdownEnable) {
      // We request the controller to do a controlled shutdown. On failure, we backoff for a configured period
      // of time and try again for a configured number of retries. If all the attempt fails, we simply force
      // the shutdown.
      info("Starting controlled shutdown")

      brokerState.newState(PendingControlledShutdown)

      val shutdownSucceeded = doControlledShutdown(config.controlledShutdownMaxRetries.intValue)

      if (!shutdownSucceeded)
        warn("Proceeding to do an unclean shutdown as all the controlled shutdown attempts failed")
    }
  }

  /**
   * Shutdown API for shutting down a single instance of the Kafka server.
   * Shuts down the LogManager, the SocketServer and the log cleaner scheduler thread
   */
  def shutdown(): Unit = {
    try {
      info("shutting down")

      // TODO:  broker在调用 startUp()方法时，会设置 isStartingUp.compareAndSet(false, true)，即在broker没有起来就去关闭，是非法的
      if (isStartingUp.get)
        throw new IllegalStateException("Kafka server is still starting up, cannot shut down!")

      // To ensure correct behavior under concurrent calls, we need to check `shutdownLatch` first since it gets updated
      // last in the `if` block. If the order is reversed, we could shutdown twice or leave `isShuttingDown` set to
      // `true` at the end of this method.
      if (shutdownLatch.getCount > 0 && isShuttingDown.compareAndSet(false, true)) {
        // TODO: 对所有没有关闭的组件进行关闭
        CoreUtils.swallow(controlledShutdown(), this)
        brokerState.newState(BrokerShuttingDown)

        if (dynamicConfigManager != null)
          CoreUtils.swallow(dynamicConfigManager.shutdown(), this)

        // Stop socket server to stop accepting any more connections and requests.
        // Socket server will be shutdown towards the end of the sequence.
        if (socketServer != null)
          CoreUtils.swallow(socketServer.stopProcessingRequests(), this)
        if (dataPlaneRequestHandlerPool != null)
          CoreUtils.swallow(dataPlaneRequestHandlerPool.shutdown(), this)
        if (controlPlaneRequestHandlerPool != null)
          CoreUtils.swallow(controlPlaneRequestHandlerPool.shutdown(), this)
        if (kafkaScheduler != null)
          CoreUtils.swallow(kafkaScheduler.shutdown(), this)

        if (dataPlaneRequestProcessor != null)
          CoreUtils.swallow(dataPlaneRequestProcessor.close(), this)
        if (controlPlaneRequestProcessor != null)
          CoreUtils.swallow(controlPlaneRequestProcessor.close(), this)
        CoreUtils.swallow(authorizer.foreach(_.close()), this)
        if (adminManager != null)
          CoreUtils.swallow(adminManager.shutdown(), this)

        if (transactionCoordinator != null)
          CoreUtils.swallow(transactionCoordinator.shutdown(), this)
        if (groupCoordinator != null)
          CoreUtils.swallow(groupCoordinator.shutdown(), this)

        if (tokenManager != null)
          CoreUtils.swallow(tokenManager.shutdown(), this)

        if (replicaManager != null)
          CoreUtils.swallow(replicaManager.shutdown(), this)

        if (brokerToControllerChannelManager != null)
          CoreUtils.swallow(brokerToControllerChannelManager.shutdown(), this)

        if (logManager != null)
          CoreUtils.swallow(logManager.shutdown(), this)

        if (kafkaController != null)
          CoreUtils.swallow(kafkaController.shutdown(), this)

        if (featureChangeListener != null)
          CoreUtils.swallow(featureChangeListener.close(), this)

        if (zkClient != null)
          CoreUtils.swallow(zkClient.close(), this)

        if (quotaManagers != null)
          CoreUtils.swallow(quotaManagers.shutdown(), this)

        // Even though socket server is stopped much earlier, controller can generate
        // response for controlled shutdown request. Shutdown server at the end to
        // avoid any failures (e.g. when metrics are recorded)
        if (socketServer != null)
          CoreUtils.swallow(socketServer.shutdown(), this)
        if (metrics != null)
          CoreUtils.swallow(metrics.close(), this)
        if (brokerTopicStats != null)
          CoreUtils.swallow(brokerTopicStats.close(), this)

        // Clear all reconfigurable instances stored in DynamicBrokerConfig
        config.dynamicConfig.clear()

        brokerState.newState(NotRunning)

        // TODO: 恢复到为启动状态
        startupComplete.set(false)
        isShuttingDown.set(false)
        CoreUtils.swallow(AppInfoParser.unregisterAppInfo(metricsPrefix, config.brokerId.toString, metrics), this)
        shutdownLatch.countDown()
        info("shut down completed")
      }
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServer shutdown.", e)
        isShuttingDown.set(false)
        throw e
    }
  }

  /**
   * After calling shutdown(), use this API to wait until the shutdown is complete
   */
  def awaitShutdown(): Unit = shutdownLatch.await()

  def getLogManager: LogManager = logManager

  def boundPort(listenerName: ListenerName): Int = socketServer.boundPort(listenerName)

  /**
   * Reads the BrokerMetadata. If the BrokerMetadata doesn't match in all the log.dirs, InconsistentBrokerMetadataException is
   * thrown.
   *
   * The log directories whose meta.properties can not be accessed due to IOException will be returned to the caller
   *
   * @return A 2-tuple containing the brokerMetadata and a sequence of offline log directories.
   */
  private def getBrokerMetadataAndOfflineDirs: (BrokerMetadata, Seq[String]) = {
    // TODO: broker metadata map 
    val brokerMetadataMap = mutable.HashMap[String, BrokerMetadata]()
    val brokerMetadataSet = mutable.HashSet[BrokerMetadata]()
    // TODO: log dirs 
    val offlineDirs = mutable.ArrayBuffer.empty[String]

    // TODO: 配置文件中的logDirs e.g. log.dirs=/mnt/ssd/0/kafka/,/mnt/ssd/1/kafka/,/mnt/ssd/2/kafka/
    for (logDir <- config.logDirs) {
      try {
        // TODO: 读取 /mnt/ssd/0/kafka/meta.properties 文件
        val brokerMetadataOpt = brokerMetadataCheckpoints(logDir).read()
        brokerMetadataOpt.foreach { brokerMetadata =>
          brokerMetadataMap += (logDir -> brokerMetadata)
          // TODO: Set的功能是去重，这里的brokerMetadata=BrokerMetadata(brokerId, clusterId)，每个路径下面都是一样的，
          //  所以，这个集合里面只能有一个值
          brokerMetadataSet += brokerMetadata
        }
      } catch {
        case e: IOException =>
          // TODO: 出现IO异常的时候，把对应的log路径加入 ，表明该路径为offlineDir
          offlineDirs += logDir
          error(s"Fail to read $brokerMetaPropsFile under log directory $logDir", e)
      }
    }

    if (brokerMetadataSet.size > 1) {
      // TODO: 当集合里面出现了两个或两个以上的值，说明clusterId，brokerId就不一致了。预期结果：同一个broker，clusterId和brokerId都应相同
      val builder = new StringBuilder

      for ((logDir, brokerMetadata) <- brokerMetadataMap)
        builder ++= s"- $logDir -> $brokerMetadata\n"

      throw new InconsistentBrokerMetadataException(
        s"BrokerMetadata is not consistent across log.dirs. This could happen if multiple brokers shared a log directory (log.dirs) " +
        s"or partial data was manually copied from another broker. Found:\n${builder.toString()}"
      )
    } else if (brokerMetadataSet.size == 1)
    // TODO: 预期结果 (BrokerMetadata(brokerId, clusterId), offlineDirs)
      (brokerMetadataSet.last, offlineDirs)
    else
      (BrokerMetadata(-1, None), offlineDirs)
  }


  /**
   * Checkpoint the BrokerMetadata to all the online log.dirs
   *
   * @param brokerMetadata
   */
  private def checkpointBrokerMetadata(brokerMetadata: BrokerMetadata) = {
    // TODO: 变量所有live的logDir
    for (logDir <- config.logDirs if logManager.isLogDirOnline(new File(logDir).getAbsolutePath)) {
      // TODO: BrokerMetadataCheckpoint(new File("/mnt/ssd/0/kafka/meta.properties"))
      val checkpoint = brokerMetadataCheckpoints(logDir)
      checkpoint.write(brokerMetadata)
    }
  }

  /**
   * Generates new brokerId if enabled or reads from meta.properties based on following conditions
   * <ol>
   * <li> config has no broker.id provided and broker id generation is enabled, generates a broker.id based on Zookeeper's sequence
   * <li> config has broker.id and meta.properties contains broker.id if they don't match throws InconsistentBrokerIdException
   * <li> config has broker.id and there is no meta.properties file, creates new meta.properties and stores broker.id
   * <ol>
   *
   * @return The brokerId.
   */
  private def getOrGenerateBrokerId(brokerMetadata: BrokerMetadata): Int = {
    // TODO: 从配置中获取brokerId
    val brokerId = config.brokerId

    // TODO: 如果配置的brokerId和brokerMetadata中的brokerId不一致，抛出  InconsistentBrokerIdException 异常
    if (brokerId >= 0 && brokerMetadata.brokerId >= 0 && brokerMetadata.brokerId != brokerId)
      throw new InconsistentBrokerIdException(
        s"Configured broker.id $brokerId doesn't match stored broker.id ${brokerMetadata.brokerId} in meta.properties. " +
        s"If you moved your data, make sure your configured broker.id matches. " +
        s"If you intend to create a new broker, you should remove all data in your data directories (log.dirs).")
    else if (brokerMetadata.brokerId < 0 && brokerId < 0 && config.brokerIdGenerationEnable) // generate a new brokerId from Zookeeper
    // TODO: 自动生成brokerId
      generateBrokerId
    else if (brokerMetadata.brokerId >= 0) // pick broker.id from meta.properties
    // TODO: 如果我们没有在配置中配置，并且metadata中又存在，则使用metadata中的brokerId
      brokerMetadata.brokerId
    else
      brokerId
  }

  /**
    * Return a sequence id generated by updating the broker sequence id path in ZK.
    * Users can provide brokerId in the config. To avoid conflicts between ZK generated
    * sequence id and configured brokerId, we increment the generated sequence id by KafkaConfig.MaxReservedBrokerId.
    */
  private def generateBrokerId: Int = {
    try {
      // TODO: 从zk的路径 /brokers/seqid 获取version， 再加上1000，组成broker id。
      //  reserved.broker.max.id=1000 默认值
      zkClient.generateBrokerSequenceId() + config.maxReservedBrokerId
    } catch {
      case e: Exception =>
        error("Failed to generate broker.id due to ", e)
        throw new GenerateBrokerIdException("Failed to generate broker.id", e)
    }
  }
}
