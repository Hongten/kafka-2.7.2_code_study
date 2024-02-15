/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.common

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

import kafka.utils.{Logging, ShutdownableThread}
import kafka.zk.{KafkaZkClient, StateChangeHandlers}
import kafka.zookeeper.{StateChangeHandler, ZNodeChildChangeHandler}
import org.apache.kafka.common.utils.Time

import scala.collection.Seq
import scala.util.{Failure, Try}

/**
 * Handle the notificationMessage.
 */
trait NotificationHandler {
  def processNotification(notificationMessage: Array[Byte]): Unit
}

/**
 * A listener that subscribes to seqNodeRoot for any child changes where all children are assumed to be sequence node
 * with seqNodePrefix. When a child is added under seqNodeRoot this class gets notified, it looks at lastExecutedChange
 * number to avoid duplicate processing and if it finds an unprocessed child, it reads its data and calls supplied
 * notificationHandler's processNotification() method with the child's data as argument. As part of processing these changes it also
 * purges any children with currentTime - createTime > changeExpirationMs.
 *
 * @param zkClient
 * @param seqNodeRoot
 * @param seqNodePrefix
 * @param notificationHandler
 * @param changeExpirationMs
 * @param time
 */
class ZkNodeChangeNotificationListener(private val zkClient: KafkaZkClient,
                                       private val seqNodeRoot: String,
                                       private val seqNodePrefix: String,
                                       private val notificationHandler: NotificationHandler,
                                       private val changeExpirationMs: Long = 15 * 60 * 1000,
                                       private val time: Time = Time.SYSTEM) extends Logging {
  private var lastExecutedChange = -1L
  // TODO: 存放变更通知的队列
  private val queue = new LinkedBlockingQueue[ChangeNotification]
  // TODO: 创建ChangeEventProcessThread 实例
  private val thread = new ChangeEventProcessThread(s"$seqNodeRoot-event-process-thread")
  private val isClosed = new AtomicBoolean(false)

  def init(): Unit = {
    zkClient.registerStateChangeHandler(ZkStateChangeHandler)
    zkClient.registerZNodeChildChangeHandler(ChangeNotificationHandler)
    addChangeNotification()
    // TODO: 启动 ChangeEventProcessThread
    thread.start()
  }

  def close() = {
    isClosed.set(true)
    zkClient.unregisterStateChangeHandler(ZkStateChangeHandler.name)
    zkClient.unregisterZNodeChildChangeHandler(ChangeNotificationHandler.path)
    queue.clear()
    thread.shutdown()
  }

  /**
   * Process notifications
   */
  private def processNotifications(): Unit = {
    try {
      val notifications = zkClient.getChildren(seqNodeRoot).sorted
      if (notifications.nonEmpty) {
        info(s"Processing notification(s) to $seqNodeRoot")
        val now = time.milliseconds
        for (notification <- notifications) {
          val changeId = changeNumber(notification)
          if (changeId > lastExecutedChange) {
            // TODO: 处理变更
            processNotification(notification)
            lastExecutedChange = changeId
          }
        }
        purgeObsoleteNotifications(now, notifications)
      }
    } catch {
      case e: InterruptedException => if (!isClosed.get) error(s"Error while processing notification change for path = $seqNodeRoot", e)
      case e: Exception => error(s"Error while processing notification change for path = $seqNodeRoot", e)
    }
  }

  private def processNotification(notification: String): Unit = {
    val changeZnode = seqNodeRoot + "/" + notification
    val (data, _) = zkClient.getDataAndStat(changeZnode)
    data match {
      case Some(d) => Try(notificationHandler.processNotification(d)) match {
        case Failure(e) => error(s"error processing change notification ${new String(d, UTF_8)} from $changeZnode", e)
        case _ =>
      }
      case None => warn(s"read null data from $changeZnode")
    }
  }

  private def addChangeNotification(): Unit = {
    if (!isClosed.get && queue.peek() == null)
      queue.put(new ChangeNotification)
  }

  class ChangeNotification {
    def process(): Unit = processNotifications()
  }

  /**
   * Purges expired notifications.
   *
   * @param now
   * @param notifications
   */
  // TODO: 清理过期zk节点
  //  作用：
  //     1. 大量无用的seqNode进行传输，会增加网络带宽负担
  //     2. 占用zk服务端内存已经存储资源
  //     3. kafka会做大量无效的判断和计算
  //   清理方法：
  //     1. 当broker监听到notification变化回调时，记录系统时间
  //     2. 获取/config/changes下面所有的子节点，读取每个seqNode的创建时间
  //     3. 系统时间减去节点创建时间如果超过了 changeExpirationMs=15mins（默认）即过期，就会被删除
  private def purgeObsoleteNotifications(now: Long, notifications: Seq[String]): Unit = {
    for (notification <- notifications.sorted) {
      val notificationNode = seqNodeRoot + "/" + notification
      val (data, stat) = zkClient.getDataAndStat(notificationNode)
      if (data.isDefined) {
        // TODO: 如果超过了过期时间 15mins（默认），就会删除对应的zk节点
        if (now - stat.getCtime > changeExpirationMs) {
          debug(s"Purging change notification $notificationNode")
          zkClient.deletePath(notificationNode)
        }
      }
    }
  }

  /* get the change number from a change notification znode */
  private def changeNumber(name: String): Long = name.substring(seqNodePrefix.length).toLong

  class ChangeEventProcessThread(name: String) extends ShutdownableThread(name = name) {
    // TODO: 从队列中获取到变更通知进行处理
    override def doWork(): Unit = queue.take().process()
  }

  object ChangeNotificationHandler extends ZNodeChildChangeHandler {
    override val path: String = seqNodeRoot
    override def handleChildChange(): Unit = addChangeNotification()
  }

  object ZkStateChangeHandler extends  StateChangeHandler {
    override val name: String = StateChangeHandlers.zkNodeChangeListenerHandler(seqNodeRoot)
    override def afterInitializingSession(): Unit = addChangeNotification()
  }
}

