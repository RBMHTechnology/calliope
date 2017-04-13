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

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, ProducerSettings}
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import com.rbmhtechnology.calliope.serializer.kafka.{PayloadFormatDeserializer, PayloadFormatSerializer}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization._
import org.scalatest._

abstract class KafkaSpec extends TestKit(ActorSystem("test")) with WordSpecLike with BeforeAndAfterAll {

  implicit val materializer = ActorMaterializer()

  val bootstrapServers = s"localhost:${KafkaServer.kafkaPort}"

  def producerSettings[A](valueSerializer: Serializer[A]): ProducerSettings[String, A] =
    ProducerSettings(system, new StringSerializer, valueSerializer)
      .withBootstrapServers(bootstrapServers)

  def producerSettings[A <: AnyRef](): ProducerSettings[String, A] =
    producerSettings(PayloadFormatSerializer[A])

  def consumerSettings[A](groupId: String, valueDeserializer: Deserializer[A]): ConsumerSettings[String, A] =
    ConsumerSettings(system, new StringDeserializer, valueDeserializer)
      .withBootstrapServers(bootstrapServers)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withGroupId(groupId)

  def consumerSettings(groupId: String): ConsumerSettings[String, AnyRef] =
    consumerSettings(groupId, PayloadFormatDeserializer.apply)

  def consumerSettings[A](groupId: String, converter: AnyRef => A): ConsumerSettings[String, A] =
    consumerSettings(groupId, PayloadFormatDeserializer(converter))

  override def beforeAll(): Unit = {
    KafkaServer.start()
  }

  override def afterAll(): Unit = {
    materializer.shutdown()
    TestKit.shutdownActorSystem(system)
    KafkaServer.stop()
  }
}
