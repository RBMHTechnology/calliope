/*
 * Copyright 2015 - 2017 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rbmhtechnology.calliope

import java.util.{Map => JMap}

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, ProducerSettings}
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization._
import org.scalatest._

import scala.collection.immutable.Seq

object KafkaSpec {
  case class Message(emitter: String, sequenceNr: Long)

  class MessageSerialization extends Serializer[Message] with Deserializer[Message] {
    override def close(): Unit =
      ()

    override def configure(configs: JMap[String, _], isKey: Boolean): Unit =
      ()

    override def serialize(topic: String, data: Message): Array[Byte] =
      s"${data.emitter}-${data.sequenceNr}".getBytes("UTF-8")

    override def deserialize(topic: String, data: Array[Byte]): Message = {
      val components = new String(data, "UTF-8").split("-")
      Message(components.head, components.last.toLong)
    }
  }

  class LongSerialization extends Serializer[Long] with Deserializer[Long] {
    val jLongSerializer = new LongSerializer
    val jLongDerializer = new LongDeserializer

    override def close(): Unit =
      ()

    override def configure(configs: JMap[String, _], isKey: Boolean): Unit =
      ()

    override def serialize(topic: String, data: Long): Array[Byte] =
      jLongSerializer.serialize(topic, data)

    override def deserialize(topic: String, data: Array[Byte]): Long =
      jLongDerializer.deserialize(topic, data)
  }

  def messageSourceDecoder(cr: ConsumerRecord[Long, Message]): String =
    cr.value.emitter

  def messageSequenceNrDecoder(cr: ConsumerRecord[Long, Message]): Long =
    cr.value.sequenceNr

  def messageSequenceEncoder(spr: SequencedProducerRecord[Long, Message], messageSource: String, messageSequenceNr: Long): (Long, Message) =
    (messageSequenceNr, spr.value.copy(emitter = messageSource, sequenceNr = messageSequenceNr))
}

abstract class KafkaSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with BeforeAndAfterAll {
  import KafkaSpec._

  implicit val materializer = ActorMaterializer()

  val producerSettings: ProducerSettings[Long, Message] = ProducerSettings(system, new LongSerialization, new MessageSerialization)
    .withBootstrapServers(s"localhost:${KafkaServer.kafkaPort}")

  val consumerSettings: ConsumerSettings[Long, Message] = ConsumerSettings(system, new LongSerialization, new MessageSerialization)
    .withBootstrapServers(s"localhost:${KafkaServer.kafkaPort}")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    .withGroupId("test")

  val topic: String = "test"

  val topicPartitions: Seq[TopicPartition] = Seq(
    new TopicPartition(topic, 0),
    new TopicPartition(topic, 1),
    new TopicPartition(topic, 2))

  override def beforeAll(): Unit = {
    KafkaServer.start()
  }

  override def afterAll(): Unit = {
    materializer.shutdown()
    TestKit.shutdownActorSystem(system)
    KafkaServer.stop()
  }
}
