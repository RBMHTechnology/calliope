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

import akka.NotUsed
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.scalatest.BeforeAndAfterEach

import scala.collection.immutable.Seq

class KafkaEventHubSpec extends KafkaSpec with BeforeAndAfterEach {
  import KafkaSpec._

  val e1 = ExampleEvent("e1", "a1", "u") // offset 0
  val e2 = ExampleEvent("e2", "a2", "v") // offset 1
  val e3 = ExampleEvent("e3", "a1", "w") // offset 2
  val e4 = ExampleEvent("e4", "a2", "x") // offset 3
  val e5 = ExampleEvent("e5", "a1", "y") // offset 4

  val index: KafkaInmemIndex[String, ExampleEvent] = KafkaIndex.inmem[String, ExampleEvent](aggregate, system)
  var producer: KafkaProducer[String, ExampleEvent] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    index.connect(consumerSettings(group), topicPartitions)
    producer = producerSettings.createKafkaProducer()
    Seq(e1, e2, e3, e4).foreach(send)
  }

  def send(event: ExampleEvent): Unit =
    producer.send(new ProducerRecord[String, ExampleEvent](tp0.topic, tp0.partition, null, event))

  override def afterAll(): Unit = {
    producer.close()
    index.shutdown()
    super.afterAll()
  }

  def eventHub(topicPartitions: Set[TopicPartition]): KafkaEventHub[String, ExampleEvent] =
    KafkaEvents.hub(consumerSettings(group), producerSettings, topicPartitions, index)

  def eventSubscriber(source: Source[ConsumerRecord[String, ExampleEvent], NotUsed]): TestSubscriber.Probe[ExampleEvent] =
    source.map(_.value).toMat(TestSink.probe[ExampleEvent])(Keep.right).run()

  "An aggregate event source" must {
    "consume past and live aggregate events" in {
      val hub = eventHub(Set(tp0))
      val sub = eventSubscriber(hub.aggregateEvents("a1"))

      sub.request(3)
      sub.expectNextN(2) should be(Seq(e1, e3))

      send(e5)
      sub.expectNext() should be(e5)
    }
  }
}
