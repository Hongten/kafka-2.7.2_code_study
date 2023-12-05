/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.util.concurrent.{LinkedBlockingDeque, TimeUnit}

import kafka.common.{InterBrokerSendThread, RequestAndCompletionHandler}
import kafka.utils.Logging
import org.apache.kafka.clients._
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.Node
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.JaasContext

import scala.collection.mutable
import scala.jdk.CollectionConverters._


trait BrokerToControllerChannelManager {
  def sendRequest(request: AbstractRequest.Builder[_ <: AbstractRequest],
                  callback: RequestCompletionHandler): Unit

  def start(): Unit

  def shutdown(): Unit
}


/**
 * This class manages the connection between a broker and the controller. It runs a single
 * {@link BrokerToControllerRequestThread} which uses the broker's metadata cache as its own metadata to find
 * and connect to the controller. The channel is async and runs the network connection in the background.
 * The maximum number of in-flight requests are set to one to ensure orderly response from the controller, therefore
 * care must be taken to not block on outstanding requests for too long.
 */
class BrokerToControllerChannelManagerImpl(metadataCache: kafka.server.MetadataCache, // TODO: metadata缓存数据
                                           time: Time,
                                           metrics: Metrics,
                                           config: KafkaConfig,
                                           threadNamePrefix: Option[String] = None) extends BrokerToControllerChannelManager with Logging {
  // TODO: 请求队列
  private val requestQueue = new LinkedBlockingDeque[BrokerToControllerQueueItem]
  // TODO: [broker-10010-to-controller]
  private val logContext = new LogContext(s"[broker-${config.brokerId}-to-controller] ")
  private val manualMetadataUpdater = new ManualMetadataUpdater()
  // TODO: 请问线程创建
  private val requestThread = newRequestThread

  // TODO: 线程启动
  override def start(): Unit = {
    requestThread.start()
  }

  // TODO: 线程关闭
  override def shutdown(): Unit = {
    requestThread.shutdown()
    requestThread.awaitShutdown()
  }

  private[server] def newRequestThread = {
    // TODO:  ListenerName("PLAINTEXT")
    val brokerToControllerListenerName = config.controlPlaneListenerName.getOrElse(config.interBrokerListenerName)
    // TODO: PLAINTEXT(0, "PLAINTEXT")
    val brokerToControllerSecurityProtocol = config.controlPlaneSecurityProtocol.getOrElse(config.interBrokerSecurityProtocol)

    val networkClient = {
      // TODO: 创建  channelBuilder 实例
      val channelBuilder = ChannelBuilders.clientChannelBuilder(
        brokerToControllerSecurityProtocol,
        JaasContext.Type.SERVER,
        config,
        brokerToControllerListenerName,
        config.saslMechanismInterBrokerProtocol,
        time,
        config.saslInterBrokerHandshakeRequestEnable,
        logContext
      )
      // TODO: 创建 selector 
      val selector = new Selector(
        NetworkReceive.UNLIMITED,
        Selector.NO_IDLE_TIMEOUT_MS,
        metrics,
        time,
        "BrokerToControllerChannel",
        Map("BrokerId" -> config.brokerId.toString).asJava,
        false,
        channelBuilder,
        logContext
      )
      // TODO: 创建NetworkClient实例 
      new NetworkClient(
        selector,
        manualMetadataUpdater,
        config.brokerId.toString,           //todo 1001
        1,
        50,
        50,
        Selectable.USE_DEFAULT_BUFFER_SIZE, //todo -1
        Selectable.USE_DEFAULT_BUFFER_SIZE, //todo -1
        config.requestTimeoutMs,            //todo 40s
        config.connectionSetupTimeoutMs,    //todo 10s
        config.connectionSetupTimeoutMaxMs, //todo 127s
        ClientDnsLookup.USE_ALL_DNS_IPS,    //todo use_all_dns_ips
        time,
        false,
        new ApiVersions,
        logContext
      )
    }
    // TODO: threadNamePrefix=None , 返回 broker-1001-to-controller-send-thread
    val threadName = threadNamePrefix match {
      case None => s"broker-${config.brokerId}-to-controller-send-thread"
      case Some(name) => s"$name:broker-${config.brokerId}-to-controller-send-thread"
    }

    // TODO: 创建  BrokerToControllerRequestThread 线程实例
    new BrokerToControllerRequestThread(networkClient, manualMetadataUpdater, requestQueue, metadataCache, config,
      brokerToControllerListenerName, time, threadName)
  }

  override def sendRequest(request: AbstractRequest.Builder[_ <: AbstractRequest],
                           callback: RequestCompletionHandler): Unit = {
    requestQueue.put(BrokerToControllerQueueItem(request, callback))
    requestThread.wakeup()
  }
}

case class BrokerToControllerQueueItem(request: AbstractRequest.Builder[_ <: AbstractRequest],
                                       callback: RequestCompletionHandler)

class BrokerToControllerRequestThread(networkClient: KafkaClient,
                                      metadataUpdater: ManualMetadataUpdater,
                                      requestQueue: LinkedBlockingDeque[BrokerToControllerQueueItem],
                                      metadataCache: kafka.server.MetadataCache,
                                      config: KafkaConfig,
                                      listenerName: ListenerName,
                                      time: Time,
                                      threadName: String)
  extends InterBrokerSendThread(threadName, networkClient, time, isInterruptible = false) {

  // TODO: 记录controller节点 
  private var activeController: Option[Node] = None

  // TODO: 默认30s 
  override def requestTimeoutMs: Int = config.controllerSocketTimeoutMs

  override def generateRequests(): Iterable[RequestAndCompletionHandler] = {
    val requestsToSend = new mutable.Queue[RequestAndCompletionHandler]
    val topRequest = requestQueue.poll()
    if (topRequest != null) {
      val request = RequestAndCompletionHandler(
        activeController.get,
        topRequest.request,
        handleResponse(topRequest),
        )
      requestsToSend.enqueue(request)
    }
    requestsToSend
  }

  private[server] def handleResponse(request: BrokerToControllerQueueItem)(response: ClientResponse): Unit = {
    if (response.wasDisconnected()) {
      activeController = None
      requestQueue.putFirst(request)
    } else if (response.responseBody().errorCounts().containsKey(Errors.NOT_CONTROLLER)) {
      // just close the controller connection and wait for metadata cache update in doWork
      networkClient.close(activeController.get.idString)
      activeController = None
      requestQueue.putFirst(request)
    } else {
      request.callback.onComplete(response)
    }
  }

  private[server] def backoff(): Unit = pause(100, TimeUnit.MILLISECONDS)

  override def doWork(): Unit = {
    if (activeController.isDefined) {
      super.doWork()
    } else {
      debug("Controller isn't cached, looking for local metadata changes")
      val controllerOpt = metadataCache.getControllerId.flatMap(metadataCache.getAliveBroker)
      if (controllerOpt.isDefined) {
        if (activeController.isEmpty || activeController.exists(_.id != controllerOpt.get.id))
          info(s"Recorded new controller, from now on will use broker ${controllerOpt.get.id}")
        activeController = Option(controllerOpt.get.node(listenerName))
        metadataUpdater.setNodes(metadataCache.getAliveBrokers.map(_.node(listenerName)).asJava)
      } else {
        // need to backoff to avoid tight loops
        debug("No controller defined in metadata cache, retrying after backoff")
        backoff()
      }
    }
  }
}
