/**
 *
 *
 *
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

package kafka.metrics

import kafka.utils.{CoreUtils, VerifiableProperties}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.Seq
import scala.collection.mutable.ArrayBuffer


/**
 * Base trait for reporter MBeans. If a client wants to expose these JMX
 * operations on a custom reporter (that implements
 * [[kafka.metrics.KafkaMetricsReporter]]), the custom reporter needs to
 * additionally implement an MBean trait that extends this trait so that the
 * registered MBean is compliant with the standard MBean convention.
 */
// TODO: KafkaMetricsReporterMBean是基本trait 
trait KafkaMetricsReporterMBean {
  // TODO: 调用yammer的CsvReporter的start方法开启reporter
  def startReporter(pollingPeriodInSeconds: Long): Unit

  // TODO: 调用yammer的CsvReporter的shutdown方法关闭reporter
  def stopReporter(): Unit
  /**
   *
   * @return The name with which the MBean will be registered.
   */
  // TODO: 获取MBean的名称，格式为：kafka:type=kafka.metrics.KafkaCSVMetricsReporter
  def getMBeanName: String
}

/**
  * Implement {@link org.apache.kafka.common.ClusterResourceListener} to receive cluster metadata once it's available. Please see the class documentation for ClusterResourceListener for more information.
  */
trait KafkaMetricsReporter {
  // TODO: 在KafkaCSVMetricsReporter.scala中实现了该init方法
  def init(props: VerifiableProperties): Unit
}

object KafkaMetricsReporter {
  // TODO: 标识该reporter是否已经启动，并在启动reporter的过程中充当锁的作用
  val ReporterStarted: AtomicBoolean = new AtomicBoolean(false)
  private var reporters: ArrayBuffer[KafkaMetricsReporter] = null

  // TODO: startReporters就是启动MetricConfig中定义的所有reporter
  def startReporters (verifiableProps: VerifiableProperties): Seq[KafkaMetricsReporter] = {
    ReporterStarted synchronized {
      if (!ReporterStarted.get()) {
        reporters = ArrayBuffer[KafkaMetricsReporter]()
        val metricsConfig = new KafkaMetricsConfig(verifiableProps)
        // TODO: kafka.metrics.reporters
        if(metricsConfig.reporters.nonEmpty) {
          metricsConfig.reporters.foreach(reporterType => {
            // TODO: 具体方法是调用Utils.createObject方法通过反射机制创建所有reporter，并初始化每个reporter，
            //  最后将reporter注册到MBean中
            val reporter = CoreUtils.createObject[KafkaMetricsReporter](reporterType)
            reporter.init(verifiableProps)
            reporters += reporter
            reporter match {
              case bean: KafkaMetricsReporterMBean => CoreUtils.registerMBean(reporter, bean.getMBeanName)
              case _ =>
            }
          })
          ReporterStarted.set(true)
        }
      }
    }
    reporters
  }
}

