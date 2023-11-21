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

package kafka.log

import java.io._
import java.nio.file.Files
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger

import kafka.metrics.KafkaMetricsGroup
import kafka.server.checkpoints.OffsetCheckpointFile
import kafka.server.{BrokerState, RecoveringFromUncleanShutdown, _}
import kafka.utils._
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.errors.{KafkaStorageException, LogDirNotFoundException}

import scala.jdk.CollectionConverters._
import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

/**
 * The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
 * All read and write operations are delegated to the individual log instances.
 *
 * The log manager maintains logs in one or more directories. New logs are created in the data directory
 * with the fewest logs. No attempt is made to move partitions after the fact or balance based on
 * size or I/O rate.
 *
 * A background thread handles log retention by periodically truncating excess log segments.
 */
@threadsafe
class LogManager(logDirs: Seq[File],
                 initialOfflineDirs: Seq[File],
                 val topicConfigs: Map[String, LogConfig], // note that this doesn't get updated after creation
                 val initialDefaultConfig: LogConfig,
                 val cleanerConfig: CleanerConfig,
                 recoveryThreadsPerDataDir: Int,
                 val flushCheckMs: Long,
                 val flushRecoveryOffsetCheckpointMs: Long,
                 val flushStartOffsetCheckpointMs: Long,
                 val retentionCheckMs: Long,
                 val maxPidExpirationMs: Int,
                 scheduler: Scheduler, // TODO: kafka的10个线程的线程池，后台运行
                 val brokerState: BrokerState,
                 brokerTopicStats: BrokerTopicStats,
                 logDirFailureChannel: LogDirFailureChannel,
                 time: Time) extends Logging with KafkaMetricsGroup {

  import LogManager._

  val LockFile = ".lock"
  val InitialTaskDelayMs = 30 * 1000

  // TODO: log 创建或删除锁
  private val logCreationOrDeletionLock = new Object
  // TODO: 当前的log ，放入到 Pool对象中
  private val currentLogs = new Pool[TopicPartition, Log]()
  // Future logs are put in the directory with "-future" suffix. Future log is created when user wants to move replica
  // from one log directory to another log directory on the same broker. The directory of the future log will be renamed
  // to replace the current log of the partition after the future log catches up with the current log
  // TODO: 同一个broker，不同磁盘间的复制，就产生-future后缀的日志
  private val futureLogs = new Pool[TopicPartition, Log]()
  // Each element in the queue contains the log object to be deleted and the time it is scheduled for deletion.
  // TODO: 等待删除的log
  private val logsToBeDeleted = new LinkedBlockingQueue[(Log, Long)]()

  // TODO: /mnt/ssd/0/kafka/,/mnt/ssd/1/kafka/,/mnt/ssd/2/kafka/
  private val _liveLogDirs: ConcurrentLinkedQueue[File] = createAndValidateLogDirs(logDirs, initialOfflineDirs)
  // TODO: 使用了volatile 关键字，对其他线程可见，  _currentDefaultConfig为默认的配置，该配置是从配置文件中读取，
  //  用户自定义的配置会覆盖该配置
  @volatile private var _currentDefaultConfig = initialDefaultConfig
  // TODO: 每个数据盘 恢复线程数，主要是kafka启动和关闭的时候，默认为1
  @volatile private var numRecoveryThreadsPerDataDir = recoveryThreadsPerDataDir

  // This map contains all partitions whose logs are getting loaded and initialized. If log configuration
  // of these partitions get updated at the same time, the corresponding entry in this map is set to "true",
  // which triggers a config reload after initialization is finished (to get the latest config value).
  // See KAFKA-8813 for more detail on the race condition
  // Visible for testing
  private[log] val partitionsInitializing = new ConcurrentHashMap[TopicPartition, Boolean]().asScala

  // TODO: 更新默认配置
  def reconfigureDefaultLogConfig(logConfig: LogConfig): Unit = {
    this._currentDefaultConfig = logConfig
  }

  // TODO: 获取默认配置
  def currentDefaultConfig: LogConfig = _currentDefaultConfig

  def liveLogDirs: Seq[File] = {
    if (_liveLogDirs.size == logDirs.size)
      logDirs
    else
      _liveLogDirs.asScala.toBuffer
  }

  // TODO: 锁
  private val dirLocks = lockLogDirs(liveLogDirs)
  // TODO: recovery-point-offset-checkpoint 文件
  // TODO: <File("/mnt/ssd/0/kafka/"), OffsetCheckpointFile("/mnt/ssd/0/kafka/recovery-point-offset-checkpoint")>
  // TODO: <File("/mnt/ssd/1/kafka/"), OffsetCheckpointFile("/mnt/ssd/1/kafka/recovery-point-offset-checkpoint")>
  @volatile private var recoveryPointCheckpoints = liveLogDirs.map(dir =>
    (dir, new OffsetCheckpointFile(new File(dir, RecoveryPointCheckpointFile), logDirFailureChannel))).toMap
  // TODO: log-start-offset-checkpoint 文件
  // TODO: <File("/mnt/ssd/0/kafka/"), OffsetCheckpointFile("/mnt/ssd/0/kafka/log-start-offset-checkpoint")>
  // TODO: <File("/mnt/ssd/1/kafka/"), OffsetCheckpointFile("/mnt/ssd/1/kafka/log-start-offset-checkpoint")>
  @volatile private var logStartOffsetCheckpoints = liveLogDirs.map(dir =>
    (dir, new OffsetCheckpointFile(new File(dir, LogStartOffsetCheckpointFile), logDirFailureChannel))).toMap

  // TODO: preferred log dirs 
  private val preferredLogDirs = new ConcurrentHashMap[TopicPartition, String]()

  private def offlineLogDirs: Iterable[File] = {
    // TODO: 把logDirs里面的File全部放入到Set集合里面
    val logDirsSet = mutable.Set[File]() ++= logDirs
    // TODO: 遍历 _liveLogDirs，把包含在logDirsSet里面的dir File移除
    _liveLogDirs.forEach(dir => logDirsSet -= dir)
    // TODO: 剩下的就是Offline的log dir File了
    logDirsSet
  }

  // TODO: 加载磁盘上面的log
  loadLogs()

  private[kafka] val cleaner: LogCleaner =
    if (cleanerConfig.enableCleaner)
      new LogCleaner(cleanerConfig, liveLogDirs, currentLogs, logDirFailureChannel, time = time)
    else
      null

  newGauge("OfflineLogDirectoryCount", () => offlineLogDirs.size)

  for (dir <- logDirs) {
    newGauge("LogDirectoryOffline",
      () => if (_liveLogDirs.contains(dir)) 0 else 1,
      Map("logDirectory" -> dir.getAbsolutePath))
  }

  /**
   * Create and check validity of the given directories that are not in the given offline directories, specifically:
   * <ol>
   * <li> Ensure that there are no duplicates in the directory list
   * <li> Create each directory if it doesn't exist
   * <li> Check that each path is a readable directory
   * </ol>
   */
  // TODO: dirs=/mnt/ssd/0/kafka/,/mnt/ssd/1/kafka/,/mnt/ssd/2/kafka/
  // TODO: initialOfflineDirs= 有IO异常的路径
  private def createAndValidateLogDirs(dirs: Seq[File], initialOfflineDirs: Seq[File]): ConcurrentLinkedQueue[File] = {
    val liveLogDirs = new ConcurrentLinkedQueue[File]()
    val canonicalPaths = mutable.HashSet.empty[String]

    // TODO: dir=/mnt/ssd/0/kafka/
    for (dir <- dirs) {
      try {
        // TODO: 如果一个路径有IO异常，则抛 IOException
        if (initialOfflineDirs.contains(dir))
          throw new IOException(s"Failed to load ${dir.getAbsolutePath} during broker startup")

        // TODO: 如果路径不存在
        if (!dir.exists) {
          info(s"Log directory ${dir.getAbsolutePath} not found, creating it.")
          // TODO: 创建路径
          val created = dir.mkdirs()
          if (!created)
          // TODO: 创建不了，抛IOException
            throw new IOException(s"Failed to create data directory ${dir.getAbsolutePath}")
        }
        // TODO: dir=/mnt/ssd/0/kafka/  应该是一个文件夹，并且可读
        if (!dir.isDirectory || !dir.canRead)
          throw new IOException(s"${dir.getAbsolutePath} is not a readable log directory.")

        // getCanonicalPath() throws IOException if a file system query fails or if the path is invalid (e.g. contains
        // the Nul character). Since there's no easy way to distinguish between the two cases, we treat them the same
        // and mark the log directory as offline.
        if (!canonicalPaths.add(dir.getCanonicalPath))
          throw new KafkaException(s"Duplicate log directory found: ${dirs.mkString(", ")}")

        // TODO: 排除了Exception的dir，剩下的就是 liveLogDir了 
        liveLogDirs.add(dir)
      } catch {
        case e: IOException =>
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Failed to create or validate data directory ${dir.getAbsolutePath}", e)
      }
    }
    // TODO: 如果LiveLogDirs为空，说明没有一个可用磁盘存放log，退出Kafka 服务 
    if (liveLogDirs.isEmpty) {
      fatal(s"Shutdown broker because none of the specified log dirs from ${dirs.mkString(", ")} can be created or validated")
      Exit.halt(1)
    }

    liveLogDirs
  }

  def resizeRecoveryThreadPool(newSize: Int): Unit = {
    info(s"Resizing recovery thread pool size for each data dir from $numRecoveryThreadsPerDataDir to $newSize")
    numRecoveryThreadsPerDataDir = newSize
  }

  /**
   * The log directory failure handler. It will stop log cleaning in that directory.
   *
   * @param dir        the absolute path of the log directory
   */
  def handleLogDirFailure(dir: String): Unit = {
    warn(s"Stopping serving logs in dir $dir")
    logCreationOrDeletionLock synchronized {
      _liveLogDirs.remove(new File(dir))
      if (_liveLogDirs.isEmpty) {
        fatal(s"Shutdown broker because all log dirs in ${logDirs.mkString(", ")} have failed")
        Exit.halt(1)
      }

      recoveryPointCheckpoints = recoveryPointCheckpoints.filter { case (file, _) => file.getAbsolutePath != dir }
      logStartOffsetCheckpoints = logStartOffsetCheckpoints.filter { case (file, _) => file.getAbsolutePath != dir }
      if (cleaner != null)
        cleaner.handleLogDirFailure(dir)

      def removeOfflineLogs(logs: Pool[TopicPartition, Log]): Iterable[TopicPartition] = {
        val offlineTopicPartitions: Iterable[TopicPartition] = logs.collect {
          case (tp, log) if log.parentDir == dir => tp
        }
        offlineTopicPartitions.foreach { topicPartition => {
          val removedLog = removeLogAndMetrics(logs, topicPartition)
          removedLog.foreach {
            log => log.closeHandlers()
          }
        }}

        offlineTopicPartitions
      }

      val offlineCurrentTopicPartitions = removeOfflineLogs(currentLogs)
      val offlineFutureTopicPartitions = removeOfflineLogs(futureLogs)

      warn(s"Logs for partitions ${offlineCurrentTopicPartitions.mkString(",")} are offline and " +
           s"logs for future partitions ${offlineFutureTopicPartitions.mkString(",")} are offline due to failure on log directory $dir")
      dirLocks.filter(_.file.getParent == dir).foreach(dir => CoreUtils.swallow(dir.destroy(), this))
    }
  }

  /**
   * Lock all the given directories
   */
  private def lockLogDirs(dirs: Seq[File]): Seq[FileLock] = {
    dirs.flatMap { dir =>
      try {
        val lock = new FileLock(new File(dir, LockFile))
        if (!lock.tryLock())
          throw new KafkaException("Failed to acquire lock on file .lock in " + lock.file.getParent +
            ". A Kafka instance in another process or thread is using this directory.")
        Some(lock)
      } catch {
        case e: IOException =>
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Disk error while locking directory $dir", e)
          None
      }
    }
  }

  private def addLogToBeDeleted(log: Log): Unit = {
    // TODO: File(/mnt/ssd/1/kafka/test08075-0-delete)
    this.logsToBeDeleted.add((log, time.milliseconds()))
  }

  // Only for testing
  private[log] def hasLogsToBeDeleted: Boolean = !logsToBeDeleted.isEmpty

  // TODO: 加载log文件
  // TODO:  logDir=File(/mnt/ssd/1/kafka/test08075-0)
  // TODO:  recoveryPoints = <TopicPartition(test08075, 0), 0>
  // TODO:  logStartOffsets = <TopicPartition(test08075, 0), 10>
  private def loadLog(logDir: File,
                      recoveryPoints: Map[TopicPartition, Long],
                      logStartOffsets: Map[TopicPartition, Long]): Log = {
    // TODO: TopicPartition("test08075", 0)
    val topicPartition = Log.parseTopicPartitionName(logDir)
    // TODO: test08075 所对应的配置信息
    val config = topicConfigs.getOrElse(topicPartition.topic, currentDefaultConfig)
    // TODO: 0 
    val logRecoveryPoint = recoveryPoints.getOrElse(topicPartition, 0L)
    // TODO: 10 
    val logStartOffset = logStartOffsets.getOrElse(topicPartition, 0L)

    // TODO: 构建 Log 实例
    val log = Log(
      dir = logDir, // todo File(/mnt/ssd/1/kafka/test08075-0)
      config = config,
      logStartOffset = logStartOffset, // todo 10
      recoveryPoint = logRecoveryPoint, // todo 0
      maxProducerIdExpirationMs = maxPidExpirationMs, // todo 7-days
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs, // todo 10 mins
      scheduler = scheduler, // TODO: kafka的10个线程的线程池，后台运行 
      time = time,
      brokerTopicStats = brokerTopicStats, // TODO: broker topic 状态信息 
      logDirFailureChannel = logDirFailureChannel)

    // TODO: 判断是否标记为 -delete 的文件  
    if (logDir.getName.endsWith(Log.DeleteDirSuffix)) {
      // TODO: 加入到 即将被删除的队列 
      addLogToBeDeleted(log)
    } else {
      // TODO: File(/mnt/ssd/1/kafka/test08075-0) 
      val previous = {
        // TODO: 是否标记为 -future 的文件 ，这里不是这样的文件
        if (log.isFuture)
          this.futureLogs.put(topicPartition, log)
        else
        // TODO: 那么，就会 放入到 currentLogs里面， <TopicPartition(test08075, 0), log> , 然后返回 log对象给 previous，
        //  previous一定不为null
          this.currentLogs.put(topicPartition, log)
      }

      // TODO: previous 变量 即为 log，一定不为空，那么这里就一定会抛出异常 IllegalStateException， why？
      if (previous != null) {
        if (log.isFuture)
          throw new IllegalStateException(s"Duplicate log directories found: ${log.dir.getAbsolutePath}, ${previous.dir.getAbsolutePath}")
        else
          throw new IllegalStateException(s"Duplicate log directories for $topicPartition are found in both ${log.dir.getAbsolutePath} " +
            s"and ${previous.dir.getAbsolutePath}. It is likely because log directory failure happened while broker was " +
            s"replacing current replica with future replica. Recover broker from this failure by manually deleting one of the two directories " +
            s"for this partition. It is recommended to delete the partition in the log directory that is known to have failed recently.")
      }
    }

    // TODO: 返回log对象
    log
  }

  /**
   * Recover and load all logs in the given data directories
   */
  private def loadLogs(): Unit = {
    // TODO: 开始加载log数据
    info(s"Loading logs from log dirs $liveLogDirs")
    // TODO: 记录开始时间
    val startMs = time.hiResClockMs()
    // TODO: 线程池
    val threadPools = ArrayBuffer.empty[ExecutorService]
    // TODO: 记录offline的路径
    val offlineDirs = mutable.Set.empty[(String, IOException)]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]
    // TODO: log数量记录
    var numTotalLogs = 0

    // TODO: 遍历全部的live log日志目录 /mnt/ssd/0/kafka/,/mnt/ssd/1/kafka/,/mnt/ssd/2/kafka/
    for (dir <- liveLogDirs) {
      // TODO: dir 绝对路径 /mnt/ssd/0/kafka/
      val logDirAbsolutePath = dir.getAbsolutePath
      try {
        // TODO: 创建 newFixedThreadPool 实例， numRecoveryThreadsPerDataDir 默认为1
        val pool = Executors.newFixedThreadPool(numRecoveryThreadsPerDataDir)
        threadPools.append(pool)

        // TODO: /mnt/ssd/0/kafka/.kafka_cleanshutdown
        val cleanShutdownFile = new File(dir, Log.CleanShutdownFile)
        // TODO: 如果 .kafka_cleanshutdown  文件存在
        if (cleanShutdownFile.exists) {
          info(s"Skipping recovery for all logs in $logDirAbsolutePath since clean shutdown file was found")
        } else {
          // log recovery itself is being performed by `Log` class during initialization
          info(s"Attempting recovery for all logs in $logDirAbsolutePath since no clean shutdown file was found")
          // TODO: 标记broker状态为 RecoveringFromUncleanShutdown
          brokerState.newState(RecoveringFromUncleanShutdown)
        }

        var recoveryPoints = Map[TopicPartition, Long]()
        try {
          // TODO: <File("/mnt/ssd/0/kafka/"), OffsetCheckpointFile("/mnt/ssd/0/kafka/recovery-point-offset-checkpoint")>
          //  tp1  par1  1     <- the format is: TOPIC  PARTITION  OFFSET
          //  tp1  par2  2
          //  <TopicPartition, Offset>
          recoveryPoints = this.recoveryPointCheckpoints(dir).read()
        } catch {
          case e: Exception =>
            // TODO: 如果出现exception，那么这里就会把offset设置为0
            warn(s"Error occurred while reading recovery-point-offset-checkpoint file of directory " +
              s"$logDirAbsolutePath, resetting the recovery checkpoint to 0", e)
        }

        var logStartOffsets = Map[TopicPartition, Long]()
        try {
          // TODO: <File("/mnt/ssd/1/kafka/"), OffsetCheckpointFile("/mnt/ssd/1/kafka/log-start-offset-checkpoint")>
          //  tp1  par1  1     <- the format is: TOPIC  PARTITION  OFFSET
          //  tp1  par2  2
          //  <TopicPartition, Offset>
          logStartOffsets = this.logStartOffsetCheckpoints(dir).read()
        } catch {
          case e: Exception =>
            warn(s"Error occurred while reading log-start-offset-checkpoint file of directory " +
              s"$logDirAbsolutePath, resetting to the base offset of the first segment", e)
        }

        // TODO: /mnt/ssd/1/kafka/
        /**
         *
         *  下面是 Directory，格式为： topic_name-partitionNum
         *  __consumer_offsets-16
            __consumer_offsets-31
            __consumer_offsets-4
            __transaction_state-35
            __transaction_state-8
            test08075-0
            test08075-2

            下面是File
            cleaner-offset-checkpoint
            log-start-offset-checkpoint
            meta.properties
            recovery-point-offset-checkpoint
            replication-offset-checkpoint
         */
        // TODO:  加载 /mnt/ssd/1/kafka/ 目录下面所有的 目录夹文件，该文件夹文件是 topic_name-partitionNum
        val logsToLoad = Option(dir.listFiles).getOrElse(Array.empty).filter(_.isDirectory)
        val numLogsLoaded = new AtomicInteger(0)
        // TODO: log数量 ，以上面为例： 7
        numTotalLogs += logsToLoad.length

        // TODO: logDir=/mnt/ssd/1/kafka/test08075-0
        val jobsForDir = logsToLoad.map { logDir =>
          // TODO: 创建 Runnable 对象
          val runnable: Runnable = () => {
            try {
              debug(s"Loading log $logDir")

              // TODO: 记录加载log开始时间
              val logLoadStartMs = time.hiResClockMs()
              // TODO: logDir=/mnt/ssd/1/kafka/test08075-0
              // TODO:  recoveryPoints = <TopicPartition(test08075, 0), 0>
              // TODO:  logStartOffsets = <TopicPartition(test08075, 0), 10>
              val log = loadLog(logDir, recoveryPoints, logStartOffsets)
              // TODO: log加载时间
              val logLoadDurationMs = time.hiResClockMs() - logLoadStartMs
              val currentNumLoaded = numLogsLoaded.incrementAndGet()

              info(s"Completed load of $log with ${log.numberOfSegments} segments in ${logLoadDurationMs}ms " +
                s"($currentNumLoaded/${logsToLoad.length} loaded in $logDirAbsolutePath)")
            } catch {
              case e: IOException =>
                offlineDirs.add((logDirAbsolutePath, e))
                error(s"Error while loading log dir $logDirAbsolutePath", e)
            }
          }
          // TODO: 返回Runnable 对象实例
          runnable
        }

        jobs(cleanShutdownFile) = jobsForDir.map(pool.submit)
      } catch {
        case e: IOException =>
          offlineDirs.add((logDirAbsolutePath, e))
          error(s"Error while loading log dir $logDirAbsolutePath", e)
      }
    }

    try {
      for ((cleanShutdownFile, dirJobs) <- jobs) {
        dirJobs.foreach(_.get)
        try {
          cleanShutdownFile.delete()
        } catch {
          case e: IOException =>
            offlineDirs.add((cleanShutdownFile.getParent, e))
            error(s"Error while deleting the clean shutdown file $cleanShutdownFile", e)
        }
      }

      offlineDirs.foreach { case (dir, e) =>
        logDirFailureChannel.maybeAddOfflineLogDir(dir, s"Error while deleting the clean shutdown file in dir $dir", e)
      }
    } catch {
      case e: ExecutionException =>
        error(s"There was an error in one of the threads during logs loading: ${e.getCause}")
        throw e.getCause
    } finally {
      threadPools.foreach(_.shutdown())
    }

    info(s"Loaded $numTotalLogs logs in ${time.hiResClockMs() - startMs}ms.")
  }

  /**
   *  Start the background threads to flush logs and do log cleanup
   */
  def startup(): Unit = {
    // TODO: kafka的10个线程的线程池，后台运行  
    /* Schedule the cleanup task to delete old logs */
    if (scheduler != null) {
      info("Starting log cleanup with a period of %d ms.".format(retentionCheckMs))
      scheduler.schedule("kafka-log-retention",
                         cleanupLogs _, // TODO: 标记为 .deleted 文件 
                         delay = InitialTaskDelayMs,
                         period = retentionCheckMs, // TODO: 默认5mins
                         TimeUnit.MILLISECONDS)
      info("Starting log flusher with a default period of %d ms.".format(flushCheckMs))
      scheduler.schedule("kafka-log-flusher",
                         flushDirtyLogs _, // TODO: 定时刷新还没有写到磁盘上日志。 一般来说，我们建议不要设置此设置并使用复制来实现持久性，并允许操作系统的后台刷新功能，因为它更高效。
                         delay = InitialTaskDelayMs,
                         period = flushCheckMs, // TODO: Long.MaxValue
                         TimeUnit.MILLISECONDS)
      // TODO: 定时将所有数据目录所有日志的检查点写到检查点文件中
      scheduler.schedule("kafka-recovery-point-checkpoint",
                         checkpointLogRecoveryOffsets _,
                         delay = InitialTaskDelayMs,
                         period = flushRecoveryOffsetCheckpointMs, // TODO: 60s
                         TimeUnit.MILLISECONDS)
      scheduler.schedule("kafka-log-start-offset-checkpoint",
                         checkpointLogStartOffsets _,
                         delay = InitialTaskDelayMs,
                         period = flushStartOffsetCheckpointMs, // TODO: 60s
                         TimeUnit.MILLISECONDS)
      // TODO: 定时删除标记为 delete 的日志文件 
      scheduler.schedule("kafka-delete-logs", // will be rescheduled after each delete logs with a dynamic period
                         deleteLogs _,
                         delay = InitialTaskDelayMs, // TODO: 30s
                         unit = TimeUnit.MILLISECONDS)
    }
    if (cleanerConfig.enableCleaner)
    // TODO:  创建 CleanerThread 实例
      cleaner.startup()
  }

  /**
   * Close all the logs
   */
  def shutdown(): Unit = {
    info("Shutting down.")

    removeMetric("OfflineLogDirectoryCount")
    for (dir <- logDirs) {
      removeMetric("LogDirectoryOffline", Map("logDirectory" -> dir.getAbsolutePath))
    }

    val threadPools = ArrayBuffer.empty[ExecutorService]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]

    // stop the cleaner first
    if (cleaner != null) {
      CoreUtils.swallow(cleaner.shutdown(), this)
    }

    val localLogsByDir = logsByDir

    // close logs in each dir
    for (dir <- liveLogDirs) {
      debug(s"Flushing and closing logs at $dir")

      val pool = Executors.newFixedThreadPool(numRecoveryThreadsPerDataDir)
      threadPools.append(pool)

      val logs = logsInDir(localLogsByDir, dir).values

      val jobsForDir = logs.map { log =>
        val runnable: Runnable = () => {
          // flush the log to ensure latest possible recovery point
          log.flush()
          log.close()
        }
        runnable
      }

      jobs(dir) = jobsForDir.map(pool.submit).toSeq
    }

    try {
      for ((dir, dirJobs) <- jobs) {
        dirJobs.foreach(_.get)

        val logs = logsInDir(localLogsByDir, dir)

        // update the last flush point
        debug(s"Updating recovery points at $dir")
        checkpointRecoveryOffsetsAndCleanSnapshotsInDir(dir, logs, logs.values.toSeq)

        debug(s"Updating log start offsets at $dir")
        checkpointLogStartOffsetsInDir(dir, logs)

        // mark that the shutdown was clean by creating marker file
        debug(s"Writing clean shutdown marker at $dir")
        CoreUtils.swallow(Files.createFile(new File(dir, Log.CleanShutdownFile).toPath), this)
      }
    } catch {
      case e: ExecutionException =>
        error(s"There was an error in one of the threads during LogManager shutdown: ${e.getCause}")
        throw e.getCause
    } finally {
      threadPools.foreach(_.shutdown())
      // regardless of whether the close succeeded, we need to unlock the data directories
      dirLocks.foreach(_.destroy())
    }

    info("Shutdown complete.")
  }

  /**
   * Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
   *
   * @param partitionOffsets Partition logs that need to be truncated
   * @param isFuture True iff the truncation should be performed on the future log of the specified partitions
   */
  def truncateTo(partitionOffsets: Map[TopicPartition, Long], isFuture: Boolean): Unit = {
    val affectedLogs = ArrayBuffer.empty[Log]
    for ((topicPartition, truncateOffset) <- partitionOffsets) {
      val log = {
        if (isFuture)
          futureLogs.get(topicPartition)
        else
          currentLogs.get(topicPartition)
      }
      // If the log does not exist, skip it
      if (log != null) {
        // May need to abort and pause the cleaning of the log, and resume after truncation is done.
        val needToStopCleaner = truncateOffset < log.activeSegment.baseOffset
        if (needToStopCleaner && !isFuture)
          abortAndPauseCleaning(topicPartition)
        try {
          if (log.truncateTo(truncateOffset))
            affectedLogs += log
          if (needToStopCleaner && !isFuture)
            maybeTruncateCleanerCheckpointToActiveSegmentBaseOffset(log, topicPartition)
        } finally {
          if (needToStopCleaner && !isFuture)
            resumeCleaning(topicPartition)
        }
      }
    }

    for ((dir, logs) <- affectedLogs.groupBy(_.parentDirFile)) {
      checkpointRecoveryOffsetsAndCleanSnapshotsInDir(dir, logs)
    }
  }

  /**
   * Delete all data in a partition and start the log at the new offset
   *
   * @param topicPartition The partition whose log needs to be truncated
   * @param newOffset The new offset to start the log with
   * @param isFuture True iff the truncation should be performed on the future log of the specified partition
   */
  def truncateFullyAndStartAt(topicPartition: TopicPartition, newOffset: Long, isFuture: Boolean): Unit = {
    val log = {
      if (isFuture)
        futureLogs.get(topicPartition)
      else
        currentLogs.get(topicPartition)
    }
    // If the log does not exist, skip it
    if (log != null) {
      // Abort and pause the cleaning of the log, and resume after truncation is done.
      if (!isFuture)
        abortAndPauseCleaning(topicPartition)
      try {
        log.truncateFullyAndStartAt(newOffset)
        if (!isFuture)
          maybeTruncateCleanerCheckpointToActiveSegmentBaseOffset(log, topicPartition)
      } finally {
        if (!isFuture)
          resumeCleaning(topicPartition)
      }
      checkpointRecoveryOffsetsAndCleanSnapshotsInDir(log.parentDirFile, Seq(log))
    }
  }

  /**
   * Write out the current recovery point for all logs to a text file in the log directory
   * to avoid recovering the whole log on startup.
   */
  def checkpointLogRecoveryOffsets(): Unit = {
    val logsByDirCached = logsByDir
    liveLogDirs.foreach { logDir =>
      val logsToCheckpoint = logsInDir(logsByDirCached, logDir)
      checkpointRecoveryOffsetsAndCleanSnapshotsInDir(logDir, logsToCheckpoint, logsToCheckpoint.values.toSeq)
    }
  }

  /**
   * Write out the current log start offset for all logs to a text file in the log directory
   * to avoid exposing data that have been deleted by DeleteRecordsRequest
   */
  def checkpointLogStartOffsets(): Unit = {
    val logsByDirCached = logsByDir
    liveLogDirs.foreach { logDir =>
      checkpointLogStartOffsetsInDir(logDir, logsInDir(logsByDirCached, logDir))
    }
  }

  /**
   * Checkpoint recovery offsets for all the logs in logDir and clean the snapshots of all the
   * provided logs.
   *
   * @param logDir the directory in which the logs to be checkpointed are
   * @param logsToCleanSnapshot the logs whose snapshots will be cleaned
   */
  // Only for testing
  private[log] def checkpointRecoveryOffsetsAndCleanSnapshotsInDir(logDir: File, logsToCleanSnapshot: Seq[Log]): Unit = {
    checkpointRecoveryOffsetsAndCleanSnapshotsInDir(logDir, logsInDir(logDir), logsToCleanSnapshot)
  }

  /**
   * Checkpoint recovery offsets for all the provided logs and clean the snapshots of all the
   * provided logs.
   *
   * @param logDir the directory in which the logs are
   * @param logsToCheckpoint the logs to be checkpointed
   * @param logsToCleanSnapshot the logs whose snapshots will be cleaned
   */
  private def checkpointRecoveryOffsetsAndCleanSnapshotsInDir(logDir: File, logsToCheckpoint: Map[TopicPartition, Log],
                                                              logsToCleanSnapshot: Seq[Log]): Unit = {
    try {
      recoveryPointCheckpoints.get(logDir).foreach { checkpoint =>
        val recoveryOffsets = logsToCheckpoint.map { case (tp, log) => tp -> log.recoveryPoint }
        checkpoint.write(recoveryOffsets)
      }
      logsToCleanSnapshot.foreach(_.deleteSnapshotsAfterRecoveryPointCheckpoint())
    } catch {
      case e: KafkaStorageException =>
        error(s"Disk error while writing recovery offsets checkpoint in directory $logDir: ${e.getMessage}")
      case e: IOException =>
        logDirFailureChannel.maybeAddOfflineLogDir(logDir.getAbsolutePath,
          s"Disk error while writing recovery offsets checkpoint in directory $logDir: ${e.getMessage}", e)
    }
  }

  /**
   * Checkpoint log start offsets for all the provided logs in the provided directory.
   *
   * @param logDir the directory in which logs are checkpointed
   * @param logsToCheckpoint the logs to be checkpointed
   */
  private def checkpointLogStartOffsetsInDir(logDir: File, logsToCheckpoint: Map[TopicPartition, Log]): Unit = {
    try {
      logStartOffsetCheckpoints.get(logDir).foreach { checkpoint =>
        val logStartOffsets = logsToCheckpoint.collect {
          case (tp, log) if log.logStartOffset > log.logSegments.head.baseOffset => tp -> log.logStartOffset
        }
        checkpoint.write(logStartOffsets)
      }
    } catch {
      case e: KafkaStorageException =>
        error(s"Disk error while writing log start offsets checkpoint in directory $logDir: ${e.getMessage}")
    }
  }

  // The logDir should be an absolute path
  def maybeUpdatePreferredLogDir(topicPartition: TopicPartition, logDir: String): Unit = {
    // Do not cache the preferred log directory if either the current log or the future log for this partition exists in the specified logDir
    if (!getLog(topicPartition).exists(_.parentDir == logDir) &&
        !getLog(topicPartition, isFuture = true).exists(_.parentDir == logDir))
      preferredLogDirs.put(topicPartition, logDir)
  }

  /**
   * Abort and pause cleaning of the provided partition and log a message about it.
   */
  def abortAndPauseCleaning(topicPartition: TopicPartition): Unit = {
    if (cleaner != null) {
      cleaner.abortAndPauseCleaning(topicPartition)
      info(s"The cleaning for partition $topicPartition is aborted and paused")
    }
  }

  /**
   * Resume cleaning of the provided partition and log a message about it.
   */
  private def resumeCleaning(topicPartition: TopicPartition): Unit = {
    if (cleaner != null) {
      cleaner.resumeCleaning(Seq(topicPartition))
      info(s"Cleaning for partition $topicPartition is resumed")
    }
  }

  /**
   * Truncate the cleaner's checkpoint to the based offset of the active segment of
   * the provided log.
   */
  private def maybeTruncateCleanerCheckpointToActiveSegmentBaseOffset(log: Log, topicPartition: TopicPartition): Unit = {
    if (cleaner != null) {
      cleaner.maybeTruncateCheckpoint(log.parentDirFile, topicPartition, log.activeSegment.baseOffset)
    }
  }

  /**
   * Get the log if it exists, otherwise return None
   *
   * @param topicPartition the partition of the log
   * @param isFuture True iff the future log of the specified partition should be returned
   */
  def getLog(topicPartition: TopicPartition, isFuture: Boolean = false): Option[Log] = {
    if (isFuture)
      Option(futureLogs.get(topicPartition))
    else
      Option(currentLogs.get(topicPartition))
  }

  /**
   * Method to indicate that logs are getting initialized for the partition passed in as argument.
   * This method should always be followed by [[kafka.log.LogManager#finishedInitializingLog]] to indicate that log
   * initialization is done.
   */
  def initializingLog(topicPartition: TopicPartition): Unit = {
    partitionsInitializing(topicPartition) = false
  }

  /**
   * Mark the partition configuration for all partitions that are getting initialized for topic
   * as dirty. That will result in reloading of configuration once initialization is done.
   */
  def topicConfigUpdated(topic: String): Unit = {
    partitionsInitializing.keys.filter(_.topic() == topic).foreach {
      topicPartition => partitionsInitializing.replace(topicPartition, false, true)
    }
  }

  /**
   * Mark all in progress partitions having dirty configuration if broker configuration is updated.
   */
  def brokerConfigUpdated(): Unit = {
    partitionsInitializing.keys.foreach {
      topicPartition => partitionsInitializing.replace(topicPartition, false, true)
    }
  }

  /**
   * Method to indicate that the log initialization for the partition passed in as argument is
   * finished. This method should follow a call to [[kafka.log.LogManager#initializingLog]].
   *
   * It will retrieve the topic configs a second time if they were updated while the
   * relevant log was being loaded.
   */
  def finishedInitializingLog(topicPartition: TopicPartition,
                              maybeLog: Option[Log],
                              fetchLogConfig: () => LogConfig): Unit = {
    val removedValue = partitionsInitializing.remove(topicPartition)
    if (removedValue.contains(true))
      maybeLog.foreach(_.updateConfig(fetchLogConfig()))
  }

  /**
   * If the log already exists, just return a copy of the existing log
   * Otherwise if isNew=true or if there is no offline log directory, create a log for the given topic and the given partition
   * Otherwise throw KafkaStorageException
   *
   * @param topicPartition The partition whose log needs to be returned or created
   * @param loadConfig A function to retrieve the log config, this is only called if the log is created
   * @param isNew Whether the replica should have existed on the broker or not
   * @param isFuture True if the future log of the specified partition should be returned or created
   * @throws KafkaStorageException if isNew=false, log is not found in the cache and there is offline log directory on the broker
   */
  def getOrCreateLog(topicPartition: TopicPartition, loadConfig: () => LogConfig, isNew: Boolean = false, isFuture: Boolean = false): Log = {
    logCreationOrDeletionLock synchronized {
      getLog(topicPartition, isFuture).getOrElse {
        // create the log if it has not already been created in another thread
        if (!isNew && offlineLogDirs.nonEmpty)
          throw new KafkaStorageException(s"Can not create log for $topicPartition because log directories ${offlineLogDirs.mkString(",")} are offline")

        val logDirs: List[File] = {
          val preferredLogDir = preferredLogDirs.get(topicPartition)

          if (isFuture) {
            if (preferredLogDir == null)
              throw new IllegalStateException(s"Can not create the future log for $topicPartition without having a preferred log directory")
            else if (getLog(topicPartition).get.parentDir == preferredLogDir)
              throw new IllegalStateException(s"Can not create the future log for $topicPartition in the current log directory of this partition")
          }

          if (preferredLogDir != null)
            List(new File(preferredLogDir))
          else
            nextLogDirs()
        }

        val logDirName = {
          if (isFuture)
            Log.logFutureDirName(topicPartition)
          else
            Log.logDirName(topicPartition)
        }

        val logDir = logDirs
          .iterator // to prevent actually mapping the whole list, lazy map
          .map(createLogDirectory(_, logDirName))
          .find(_.isSuccess)
          .getOrElse(Failure(new KafkaStorageException("No log directories available. Tried " + logDirs.map(_.getAbsolutePath).mkString(", "))))
          .get // If Failure, will throw

        val config = loadConfig()
        val log = Log(
          dir = logDir,
          config = config,
          logStartOffset = 0L,
          recoveryPoint = 0L,
          maxProducerIdExpirationMs = maxPidExpirationMs,
          producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
          scheduler = scheduler,
          time = time,
          brokerTopicStats = brokerTopicStats,
          logDirFailureChannel = logDirFailureChannel)

        if (isFuture)
          futureLogs.put(topicPartition, log)
        else
          currentLogs.put(topicPartition, log)

        info(s"Created log for partition $topicPartition in $logDir with properties " + s"{${config.originals.asScala.mkString(", ")}}.")
        // Remove the preferred log dir since it has already been satisfied
        preferredLogDirs.remove(topicPartition)

        log
      }
    }
  }

  private[log] def createLogDirectory(logDir: File, logDirName: String): Try[File] = {
    val logDirPath = logDir.getAbsolutePath
    if (isLogDirOnline(logDirPath)) {
      val dir = new File(logDirPath, logDirName)
      try {
        Files.createDirectories(dir.toPath)
        Success(dir)
      } catch {
        case e: IOException =>
          val msg = s"Error while creating log for $logDirName in dir $logDirPath"
          logDirFailureChannel.maybeAddOfflineLogDir(logDirPath, msg, e)
          warn(msg, e)
          Failure(new KafkaStorageException(msg, e))
      }
    } else {
      Failure(new KafkaStorageException(s"Can not create log $logDirName because log directory $logDirPath is offline"))
    }
  }

  /**
   *  Delete logs marked for deletion. Delete all logs for which `currentDefaultConfig.fileDeleteDelayMs`
   *  has elapsed after the delete was scheduled. Logs for which this interval has not yet elapsed will be
   *  considered for deletion in the next iteration of `deleteLogs`. The next iteration will be executed
   *  after the remaining time for the first log that is not deleted. If there are no more `logsToBeDeleted`,
   *  `deleteLogs` will be executed after `currentDefaultConfig.fileDeleteDelayMs`.
   */
  private def deleteLogs(): Unit = {
    var nextDelayMs = 0L
    try {
      def nextDeleteDelayMs: Long = {
        if (!logsToBeDeleted.isEmpty) {
          val (_, scheduleTimeMs) = logsToBeDeleted.peek()
          scheduleTimeMs + currentDefaultConfig.fileDeleteDelayMs - time.milliseconds()
        } else
          currentDefaultConfig.fileDeleteDelayMs
      }

      while ({nextDelayMs = nextDeleteDelayMs; nextDelayMs <= 0}) {
        val (removedLog, _) = logsToBeDeleted.take()
        if (removedLog != null) {
          try {
            removedLog.delete()
            info(s"Deleted log for partition ${removedLog.topicPartition} in ${removedLog.dir.getAbsolutePath}.")
          } catch {
            case e: KafkaStorageException =>
              error(s"Exception while deleting $removedLog in dir ${removedLog.parentDir}.", e)
          }
        }
      }
    } catch {
      case e: Throwable =>
        error(s"Exception in kafka-delete-logs thread.", e)
    } finally {
      try {
        scheduler.schedule("kafka-delete-logs",
          deleteLogs _,
          delay = nextDelayMs,
          unit = TimeUnit.MILLISECONDS)
      } catch {
        case e: Throwable =>
          if (scheduler.isStarted) {
            // No errors should occur unless scheduler has been shutdown
            error(s"Failed to schedule next delete in kafka-delete-logs thread", e)
          }
      }
    }
  }

  /**
    * Mark the partition directory in the source log directory for deletion and
    * rename the future log of this partition in the destination log directory to be the current log
    *
    * @param topicPartition TopicPartition that needs to be swapped
    */
  def replaceCurrentWithFutureLog(topicPartition: TopicPartition): Unit = {
    logCreationOrDeletionLock synchronized {
      val sourceLog = currentLogs.get(topicPartition)
      val destLog = futureLogs.get(topicPartition)

      info(s"Attempting to replace current log $sourceLog with $destLog for $topicPartition")
      if (sourceLog == null)
        throw new KafkaStorageException(s"The current replica for $topicPartition is offline")
      if (destLog == null)
        throw new KafkaStorageException(s"The future replica for $topicPartition is offline")

      destLog.renameDir(Log.logDirName(topicPartition))
      destLog.updateHighWatermark(sourceLog.highWatermark)

      // Now that future replica has been successfully renamed to be the current replica
      // Update the cached map and log cleaner as appropriate.
      futureLogs.remove(topicPartition)
      currentLogs.put(topicPartition, destLog)
      if (cleaner != null) {
        cleaner.alterCheckpointDir(topicPartition, sourceLog.parentDirFile, destLog.parentDirFile)
        resumeCleaning(topicPartition)
      }

      try {
        sourceLog.renameDir(Log.logDeleteDirName(topicPartition))
        // Now that replica in source log directory has been successfully renamed for deletion.
        // Close the log, update checkpoint files, and enqueue this log to be deleted.
        sourceLog.close()
        val logDir = sourceLog.parentDirFile
        val logsToCheckpoint = logsInDir(logDir)
        checkpointRecoveryOffsetsAndCleanSnapshotsInDir(logDir, logsToCheckpoint, ArrayBuffer.empty)
        checkpointLogStartOffsetsInDir(logDir, logsToCheckpoint)
        sourceLog.removeLogMetrics()
        addLogToBeDeleted(sourceLog)
      } catch {
        case e: KafkaStorageException =>
          // If sourceLog's log directory is offline, we need close its handlers here.
          // handleLogDirFailure() will not close handlers of sourceLog because it has been removed from currentLogs map
          sourceLog.closeHandlers()
          sourceLog.removeLogMetrics()
          throw e
      }

      info(s"The current replica is successfully replaced with the future replica for $topicPartition")
    }
  }

  /**
    * Rename the directory of the given topic-partition "logdir" as "logdir.uuid.delete" and
    * add it in the queue for deletion.
    *
    * @param topicPartition TopicPartition that needs to be deleted
    * @param isFuture True iff the future log of the specified partition should be deleted
    * @param checkpoint True if checkpoints must be written
    * @return the removed log
    */
  def asyncDelete(topicPartition: TopicPartition,
                  isFuture: Boolean = false,
                  checkpoint: Boolean = true): Option[Log] = {
    val removedLog: Option[Log] = logCreationOrDeletionLock synchronized {
      removeLogAndMetrics(if (isFuture) futureLogs else currentLogs, topicPartition)
    }
    removedLog match {
      case Some(removedLog) =>
        // We need to wait until there is no more cleaning task on the log to be deleted before actually deleting it.
        if (cleaner != null && !isFuture) {
          cleaner.abortCleaning(topicPartition)
          if (checkpoint) {
            cleaner.updateCheckpoints(removedLog.parentDirFile, partitionToRemove = Option(topicPartition))
          }
        }
        removedLog.renameDir(Log.logDeleteDirName(topicPartition))
        if (checkpoint) {
          val logDir = removedLog.parentDirFile
          val logsToCheckpoint = logsInDir(logDir)
          checkpointRecoveryOffsetsAndCleanSnapshotsInDir(logDir, logsToCheckpoint, ArrayBuffer.empty)
          checkpointLogStartOffsetsInDir(logDir, logsToCheckpoint)
        }
        addLogToBeDeleted(removedLog)
        info(s"Log for partition ${removedLog.topicPartition} is renamed to ${removedLog.dir.getAbsolutePath} and is scheduled for deletion")

      case None =>
        if (offlineLogDirs.nonEmpty) {
          throw new KafkaStorageException(s"Failed to delete log for ${if (isFuture) "future" else ""} $topicPartition because it may be in one of the offline directories ${offlineLogDirs.mkString(",")}")
        }
    }

    removedLog
  }

  /**
   * Rename the directories of the given topic-partitions and add them in the queue for
   * deletion. Checkpoints are updated once all the directories have been renamed.
   *
   * @param topicPartitions The set of topic-partitions to delete asynchronously
   * @param errorHandler The error handler that will be called when a exception for a particular
   *                     topic-partition is raised
   */
  def asyncDelete(topicPartitions: Set[TopicPartition],
                  errorHandler: (TopicPartition, Throwable) => Unit): Unit = {
    val logDirs = mutable.Set.empty[File]

    topicPartitions.foreach { topicPartition =>
      try {
        getLog(topicPartition).foreach { log =>
          logDirs += log.parentDirFile
          asyncDelete(topicPartition, checkpoint = false)
        }
        getLog(topicPartition, isFuture = true).foreach { log =>
          logDirs += log.parentDirFile
          asyncDelete(topicPartition, isFuture = true, checkpoint = false)
        }
      } catch {
        case e: Throwable => errorHandler(topicPartition, e)
      }
    }

    val logsByDirCached = logsByDir
    logDirs.foreach { logDir =>
      if (cleaner != null) cleaner.updateCheckpoints(logDir)
      val logsToCheckpoint = logsInDir(logsByDirCached, logDir)
      checkpointRecoveryOffsetsAndCleanSnapshotsInDir(logDir, logsToCheckpoint, ArrayBuffer.empty)
      checkpointLogStartOffsetsInDir(logDir, logsToCheckpoint)
    }
  }

  /**
   * Provides the full ordered list of suggested directories for the next partition.
   * Currently this is done by calculating the number of partitions in each directory and then sorting the
   * data directories by fewest partitions.
   */
  private def nextLogDirs(): List[File] = {
    if(_liveLogDirs.size == 1) {
      List(_liveLogDirs.peek())
    } else {
      // count the number of logs in each parent directory (including 0 for empty directories
      val logCounts = allLogs.groupBy(_.parentDir).map { case (parent, logs) => parent -> logs.size }
      val zeros = _liveLogDirs.asScala.map(dir => (dir.getPath, 0)).toMap
      val dirCounts = (zeros ++ logCounts).toBuffer

      // choose the directory with the least logs in it
      dirCounts.sortBy(_._2).map {
        case (path: String, _: Int) => new File(path)
      }.toList
    }
  }

  /**
   * Delete any eligible logs. Return the number of segments deleted.
   * Only consider logs that are not compacted.
   */
  def cleanupLogs(): Unit = {
    debug("Beginning log cleanup...")
    var total = 0
    val startMs = time.milliseconds

    // clean current logs.
    val deletableLogs = {
      if (cleaner != null) {
        // prevent cleaner from working on same partitions when changing cleanup policy
        cleaner.pauseCleaningForNonCompactedPartitions()
      } else {
        currentLogs.filter {
          case (_, log) => !log.config.compact
        }
      }
    }

    try {
      deletableLogs.foreach {
        case (topicPartition, log) =>
          debug(s"Garbage collecting '${log.name}'")
          // TODO: 会把segment标记为 xx.deleted  
          total += log.deleteOldSegments()

          // TODO: 在一个broker的不同磁盘件移动  partition的 segments 
          val futureLog = futureLogs.get(topicPartition)
          if (futureLog != null) {
            // clean future logs
            debug(s"Garbage collecting future log '${futureLog.name}'")
            total += futureLog.deleteOldSegments()
          }
      }
    } finally {
      if (cleaner != null) {
        cleaner.resumeCleaning(deletableLogs.map(_._1))
      }
    }

    debug(s"Log cleanup completed. $total files deleted in " +
                  (time.milliseconds - startMs) / 1000 + " seconds")
  }

  /**
   * Get all the partition logs
   */
  def allLogs: Iterable[Log] = currentLogs.values ++ futureLogs.values

  def logsByTopic(topic: String): Seq[Log] = {
    (currentLogs.toList ++ futureLogs.toList).collect {
      case (topicPartition, log) if topicPartition.topic == topic => log
    }
  }

  /**
   * Map of log dir to logs by topic and partitions in that dir
   */
  private def logsByDir: Map[String, Map[TopicPartition, Log]] = {
    // This code is called often by checkpoint processes and is written in a way that reduces
    // allocations and CPU with many topic partitions.
    // When changing this code please measure the changes with org.apache.kafka.jmh.server.CheckpointBench
    val byDir = new mutable.AnyRefMap[String, mutable.AnyRefMap[TopicPartition, Log]]()
    def addToDir(tp: TopicPartition, log: Log): Unit = {
      byDir.getOrElseUpdate(log.parentDir, new mutable.AnyRefMap[TopicPartition, Log]()).put(tp, log)
    }
    currentLogs.foreachEntry(addToDir)
    futureLogs.foreachEntry(addToDir)
    byDir
  }

  private def logsInDir(dir: File): Map[TopicPartition, Log] = {
    logsByDir.getOrElse(dir.getAbsolutePath, Map.empty)
  }

  private def logsInDir(cachedLogsByDir: Map[String, Map[TopicPartition, Log]],
                        dir: File): Map[TopicPartition, Log] = {
    cachedLogsByDir.getOrElse(dir.getAbsolutePath, Map.empty)
  }

  // logDir should be an absolute path
  def isLogDirOnline(logDir: String): Boolean = {
    // The logDir should be an absolute path
    if (!logDirs.exists(_.getAbsolutePath == logDir))
      throw new LogDirNotFoundException(s"Log dir $logDir is not found in the config.")

    _liveLogDirs.contains(new File(logDir))
  }

  /**
   * Flush any log which has exceeded its flush interval and has unwritten messages.
   */
  private def flushDirtyLogs(): Unit = {
    debug("Checking for dirty logs to flush...")

    for ((topicPartition, log) <- currentLogs.toList ++ futureLogs.toList) {
      try {
        val timeSinceLastFlush = time.milliseconds - log.lastFlushTime
        debug(s"Checking if flush is needed on ${topicPartition.topic} flush interval ${log.config.flushMs}" +
              s" last flushed ${log.lastFlushTime} time since last flush: $timeSinceLastFlush")
        // TODO:  log.config.flushMs = Long.Max_value, 一般来说，我们建议不要设置此设置并使用复制来实现持久性，
        //  并允许操作系统的后台刷新功能，因为它更高效。如果这里没有设置，那么这里一直不会被执行
        if(timeSinceLastFlush >= log.config.flushMs)
          log.flush()
      } catch {
        case e: Throwable =>
          error(s"Error flushing topic ${topicPartition.topic}", e)
      }
    }
  }

  private def removeLogAndMetrics(logs: Pool[TopicPartition, Log], tp: TopicPartition): Option[Log] = {
    val removedLog = logs.remove(tp)
    if (removedLog != null) {
      removedLog.removeLogMetrics()
      Some(removedLog)
    } else {
      None
    }
  }
}

object LogManager {

  val RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint"
  val LogStartOffsetCheckpointFile = "log-start-offset-checkpoint"
  val ProducerIdExpirationCheckIntervalMs = 10 * 60 * 1000

  def apply(config: KafkaConfig,
            initialOfflineDirs: Seq[String],
            zkClient: KafkaZkClient,
            brokerState: BrokerState,
            kafkaScheduler: KafkaScheduler,
            time: Time,
            brokerTopicStats: BrokerTopicStats,
            logDirFailureChannel: LogDirFailureChannel): LogManager = {
    // TODO: 拷贝kafka配置为默认配置 
    val defaultProps = KafkaServer.copyKafkaConfigToLog(config)

    // TODO: 参数校验
    LogConfig.validateValues(defaultProps)
    // TODO: 构建 LogConfig对象 
    val defaultLogConfig = LogConfig(defaultProps)

    // TODO: (<topic_name, logConfig>, <topic_name, Exception>)
    // read the log configurations from zookeeper
    val (topicConfigs, failed) = zkClient.getLogConfigs(
      // TODO: 获取zk中的所有topic 
      zkClient.getAllTopicsInCluster(),
      defaultProps
    )
    // TODO: 如果 获取topic配置信息有异常的时候，会在这里抛出来
    if (!failed.isEmpty) throw failed.head._2

    // TODO: 创建 CleanerConfig实例
    val cleanerConfig = LogCleaner.cleanerConfig(config)

    // TODO: 创建  LogManager 实例
    new LogManager(logDirs = config.logDirs.map(new File(_).getAbsoluteFile), // TODO: log.dirs=/mnt/ssd/0/kafka/,/mnt/ssd/1/kafka/,/mnt/ssd/2/kafka/ 
      initialOfflineDirs = initialOfflineDirs.map(new File(_).getAbsoluteFile), // TODO: 有IO异常的路径
      topicConfigs = topicConfigs, // TODO: topic的配置信息，配置信息从zk中获取
      initialDefaultConfig = defaultLogConfig,
      cleanerConfig = cleanerConfig,
      recoveryThreadsPerDataDir = config.numRecoveryThreadsPerDataDir, // TODO: 1 
      flushCheckMs = config.logFlushSchedulerIntervalMs, // todo Long.MaxValue
      flushRecoveryOffsetCheckpointMs = config.logFlushOffsetCheckpointIntervalMs, // todo 60000
      flushStartOffsetCheckpointMs = config.logFlushStartOffsetCheckpointIntervalMs, // todo 60000
      retentionCheckMs = config.logCleanupIntervalMs, // todo 5mins
      maxPidExpirationMs = config.transactionalIdExpirationMs, // todo 7-days
      scheduler = kafkaScheduler, // todo 默认为10个线程的线程池，后台运行
      brokerState = brokerState,
      brokerTopicStats = brokerTopicStats,
      logDirFailureChannel = logDirFailureChannel, // TODO: logDIR channel 
      time = time)
  }
}
