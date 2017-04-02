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
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization._
import org.scalatest._

object KafkaSpec {
  case class ExampleEvent(eventId: String, aggregateId: String, payload: String)

  implicit val event = new Event[ExampleEvent, String] {
    override def eventId(a: ExampleEvent): String = a.eventId
  }

  implicit val eventAggregate = new Aggregate[ExampleEvent, String] {
    override def aggregateId(a: ExampleEvent): String = a.aggregateId
  }

  class EventSerialization extends Serializer[ExampleEvent] with Deserializer[ExampleEvent] {
    override def close(): Unit =
      ()

    override def configure(configs: JMap[String, _], isKey: Boolean): Unit =
      ()

    override def serialize(topic: String, data: ExampleEvent): Array[Byte] =
      s"${data.eventId}-${data.aggregateId}-${data.payload}".getBytes("UTF-8")

    override def deserialize(topic: String, data: Array[Byte]): ExampleEvent = {
      val components = new String(data, "UTF-8").split("-")
      ExampleEvent(components.head, components(1), components(2))
    }
  }
}

abstract class KafkaSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with BeforeAndAfterAll {
  import KafkaSpec._

  implicit val materializer = ActorMaterializer()

  val producerSettings: ProducerSettings[String, ExampleEvent] = ProducerSettings(system, new StringSerializer, new EventSerialization)
    .withBootstrapServers(s"localhost:${KafkaServer.kafkaPort}")

  def consumerSettings(groupId: String): ConsumerSettings[String, ExampleEvent] = ConsumerSettings(system, new StringDeserializer, new EventSerialization)
    .withBootstrapServers(s"localhost:${KafkaServer.kafkaPort}")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    .withGroupId(groupId)

  val group: String = "test"
  val topic: String = "test"

  val tp0: TopicPartition = new TopicPartition(topic, 0)
  val tp1: TopicPartition = new TopicPartition(topic, 1)
  val tp2: TopicPartition = new TopicPartition(topic, 2)

  val topicPartitions: Set[TopicPartition] = Set(tp0, tp1, tp2)

  override def beforeAll(): Unit = {
    KafkaServer.start()
  }

  override def afterAll(): Unit = {
    materializer.shutdown()
    TestKit.shutdownActorSystem(system)
    KafkaServer.stop()
  }
}
