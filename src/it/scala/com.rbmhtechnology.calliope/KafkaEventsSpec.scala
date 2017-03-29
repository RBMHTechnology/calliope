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

import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Keep
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.scalatest.BeforeAndAfterEach

import scala.collection.immutable.Seq

class KafkaEventsSpec extends KafkaSpec with BeforeAndAfterEach {
  import KafkaSpec._

  val e1 = ExampleEvent("e1", "a1", "x")
  val e2 = ExampleEvent("e2", "a2", "y")
  val e3 = ExampleEvent("e3", "a3", "z")

  var producer: KafkaProducer[String, ExampleEvent] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    producer = producerSettings.createKafkaProducer()
    producer.send(new ProducerRecord[String, ExampleEvent](tp1.topic, tp1.partition, null, e1))
    producer.send(new ProducerRecord[String, ExampleEvent](tp2.topic, tp2.partition, null, e2))
    producer.send(new ProducerRecord[String, ExampleEvent](tp2.topic, tp2.partition, null, e3)).get
  }

  override def afterAll(): Unit = {
    producer.close()
    super.afterAll()
  }

  private def endSubscriber: TestSubscriber.Probe[ExampleEvent] =
    KafkaMetadata.endOffsets(consumerSettings(group), topic).take(1).flatMapConcat(KafkaEvents.until(consumerSettings(group), _)).map(_.value)
      .toMat(TestSink.probe[ExampleEvent])(Keep.right).run()

  private def toSubscriber(offsets: Map[TopicPartition, Long]): TestSubscriber.Probe[ExampleEvent] =
    KafkaEvents.to(consumerSettings(group), offsets).map(_.value)
      .toMat(TestSink.probe[ExampleEvent])(Keep.right).run()

  private def untilSubscriber(offsets: Map[TopicPartition, Long]): TestSubscriber.Probe[ExampleEvent] =
    KafkaEvents.until(consumerSettings(group), offsets).map(_.value)
      .toMat(TestSink.probe[ExampleEvent])(Keep.right).run()

  private def offsetsTracker(topic: String): (KafkaOffsetsTracker, TestSubscriber.Probe[ExampleEvent]) =
    Consumer.plainSource(consumerSettings(group), Subscriptions.topics(topic))
      .viaMat(KafkaOffsetsTracker.flow(Map.empty))(Keep.right).map(_.value)
      .toMat(TestSink.probe[ExampleEvent])(Keep.both).run()

  "An until event source" must {
    "complete immediately if given offsets are not greater than begin offsets" in {
      val sub = untilSubscriber(Map(tp0 -> 0L, tp1 -> 0L, tp2 -> 0L))
      sub.request(1)
      sub.expectComplete()
    }
    "emit all events if given offsets are equal to end offsets " in {
      val sub = endSubscriber
      sub.request(3)
      sub.expectNextN(3) should be(Seq(e1, e2, e3))
      sub.expectComplete()
    }
    "emit a subset of events if given offsets are less than or equal to end offsets" in {
      val sub = untilSubscriber(Map(tp0 -> 0L, tp1 -> 1L, tp2 -> 1L))
      sub.request(2)
      sub.expectNextN(2) should be(Seq(e1, e2))
      sub.expectComplete()
    }
  }

  "A to event source" must {
    "emit a events with offsets less than or equal to given offsets" in {
      val sub = toSubscriber(Map(tp1 -> 0L, tp2 -> 0L))
      sub.request(2)
      sub.expectNextN(2) should be(Seq(e1, e2))
      sub.expectComplete()
    }
  }

  "An offsets tracker" must {
    "provide consumed offsets at runtime" in {
      val (co, sub) = offsetsTracker(topic)

      sub.request(3)
      sub.expectNextN(3)
      co.untilOffsets should be(Map(tp1 -> 1L, tp2 -> 2L))
    }
  }
}
