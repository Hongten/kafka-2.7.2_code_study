package kafka.metrics

import kafka.coordinator.transaction.TransactionStateManager
import kafka.server.KafkaConfig
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.metrics.stats.Max
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.utils.Time
import org.junit.Test

import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

class MetricsTest {

  var metrics: Metrics = null
  val serverProps: Properties = new Properties
  // Note: need to start the local zk.
  serverProps.setProperty("zookeeper.connect", "mylocalip:2186/aaabbb")

  private val metricsPrefix: String = "kafka.server"
  private val KAFKA_CLUSTER_ID: String = "kafka.cluster.id"
  private val KAFKA_BROKER_ID: String = "kafka.broker.id"

  val time: Time = Time.SYSTEM

  // Please refer to study_notes/images/Kafka_2.7.2-Metrics.drawio.png
  @Test
  def test(): Unit = {
    val config = KafkaConfig.fromProps(serverProps, false)

    // jmxReporter creation
    val jmxReporter = new JmxReporter()
    jmxReporter.configure(config.originals)

    val reporters = new util.ArrayList[MetricsReporter]
    reporters.add(jmxReporter)

    val metricConfig = metricConfig1(config)
    val metricsContext = createKafkaMetricsContext()

    metrics = new Metrics(metricConfig, reporters, time, true, metricsContext)


    // create sensor
    val partitionLoadSensor = metrics.sensor(TransactionStateManager.LoadTimeSensor)

    // add metrics
    partitionLoadSensor.add(metrics.metricName("partition-load-time-max",
      TransactionStateManager.MetricsGroup,
      "The max time it took to load the partitions in the last 30sec"), new Max())

    val endTimeMs = time.milliseconds()
    // record
    partitionLoadSensor.record(20.2, endTimeMs, false)

    println(partitionLoadSensor)

  }

  def createKafkaMetricsContext(): KafkaMetricsContext = {
    val contextLabels = new util.HashMap[String, Object]
    contextLabels.put(KAFKA_CLUSTER_ID, "BIbPq3azQnSIjx2QURxtBA")
    contextLabels.put(KAFKA_BROKER_ID, "1001")
    // TODO: 配置文件中 以 metrics.context. 开头的配置
    contextLabels.putAll(KafkaConfig.fromProps(serverProps, false).originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX))
    // TODO: 创建  KafkaMetricsContext实例， metricsPrefix=kafka.server
    val metricsContext = new KafkaMetricsContext(metricsPrefix, contextLabels)
    metricsContext
  }

  def metricConfig1(kafkaConfig: KafkaConfig): MetricConfig = {
    new MetricConfig()
      .samples(kafkaConfig.metricNumSamples) // todo 默认 2
      .recordLevel(Sensor.RecordingLevel.forName(kafkaConfig.metricRecordingLevel)) // todo 默认info
      .timeWindow(kafkaConfig.metricSampleWindowMs, TimeUnit.MILLISECONDS) // todo 默认为30s
  }

}
