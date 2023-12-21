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

import java.io._
import java.nio.file.{Files, NoSuchFileException}
import java.util.Properties

import kafka.utils._
import org.apache.kafka.common.utils.Utils

// TODO: brokerId=1001, clusterId=BIbPq3azQnSIjx2QURxtBA
case class BrokerMetadata(brokerId: Int,
                          clusterId: Option[String]) {

  override def toString: String  = {
    s"BrokerMetadata(brokerId=$brokerId, clusterId=${clusterId.map(_.toString).getOrElse("None")})"
  }
}

/**
  * This class saves broker's metadata to a file
  */
class BrokerMetadataCheckpoint(val file: File) extends Logging {
  private val lock = new Object()

  // cat meta.properties
  //  #
  //  #Sat Sep 23 13:04:14 SGT 2023
  //  broker.id=1009
  //  version=0
  //  cluster.id=BIbPq3azQnSIjx2QURxtBA
  //
  def write(brokerMetadata: BrokerMetadata) = {
    // TODO: 同步锁
    lock synchronized {
      try {
        // TODO: 构建  Properties 实例
        val brokerMetaProps = new Properties()
        brokerMetaProps.setProperty("version", 0.toString)
        brokerMetaProps.setProperty("broker.id", brokerMetadata.brokerId.toString)
        brokerMetadata.clusterId.foreach { clusterId =>
          brokerMetaProps.setProperty("cluster.id", clusterId)
        }
        // TODO: /mnt/ssd/0/kafka/meta.properties.tmp
        val temp = new File(file.getAbsolutePath + ".tmp")
        val fileOutputStream = new FileOutputStream(temp)
        try {
          brokerMetaProps.store(fileOutputStream, "")
          fileOutputStream.flush()
          fileOutputStream.getFD().sync()
        } finally {
          Utils.closeQuietly(fileOutputStream, temp.getName)
        }
        // TODO: 先把内容写入到 /mnt/ssd/0/kafka/meta.properties.tmp 文件中，在拷贝到 /mnt/ssd/0/kafka/meta.properties文件中
        // TODO: 当这里出现异常，系统就会遗留 /mnt/ssd/0/kafka/meta.properties.tmp 文件，所以，在read()方法里面，会先把这个.tmp文件
        // TODO: 删除后，再 进行读取
        Utils.atomicMoveWithFallback(temp.toPath, file.toPath)
      } catch {
        case ie: IOException =>
          error("Failed to write meta.properties due to", ie)
          throw ie
      }
    }
  }

  def read(): Option[BrokerMetadata] = {
    // TODO: 如果存在 "/mnt/ssd/0/kafka/meta.properties.tmp" 则删除该文件
    Files.deleteIfExists(new File(file.getPath + ".tmp").toPath()) // try to delete any existing temp files for cleanliness

    // TODO: 同步锁
    lock synchronized {
      try {
        // TODO: 加载 /mnt/ssd/0/kafka/meta.properties
        //  #Sat Sep 23 13:04:14 SGT 2023
        //  broker.id=1009
        //  version=0
        //  cluster.id=BIbPq3azQnSIjx2QURxtBA
        val brokerMetaProps = new VerifiableProperties(Utils.loadProps(file.getAbsolutePath()))
        // TODO: 对version进行校验
        val version = brokerMetaProps.getIntInRange("version", (0, Int.MaxValue))
        version match {
          case 0 =>
            // TODO: 对brokerId， clusterId进行校验
            val brokerId = brokerMetaProps.getIntInRange("broker.id", (0, Int.MaxValue))
            val clusterId = Option(brokerMetaProps.getString("cluster.id", null))
            // TODO: 返回brokerMetadata
            return Some(BrokerMetadata(brokerId, clusterId))
          case _ =>
            throw new IOException("Unrecognized version of the server meta.properties file: " + version)
        }
      } catch {
        case _: NoSuchFileException =>
          warn("No meta.properties file under dir %s".format(file.getAbsolutePath()))
          None
        case e1: Exception =>
          error("Failed to read meta.properties file under dir %s due to %s".format(file.getAbsolutePath(), e1.getMessage))
          throw e1
      }
    }
  }
}
