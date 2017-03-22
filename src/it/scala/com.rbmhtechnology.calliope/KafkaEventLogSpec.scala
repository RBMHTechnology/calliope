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
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

class KafkaEventLogSpec extends KafkaSpec {
  import KafkaSpec._

  val e1 = ExampleEvent("1", "A", "P1")
  val e2 = ExampleEvent("2", "A", "P2")

  def record(event: ExampleEvent): ProducerRecord[String, ExampleEvent] =
    new ProducerRecord(topic, event.aggregateId, event)

  def probes(groupId: String): (TestPublisher.Probe[ProducerRecord[String, ExampleEvent]], TestSubscriber.Probe[ConsumerRecord[String, ExampleEvent]]) = {
    val log = KafkaEventLog(producerSettings, consumerSettings(groupId), Subscriptions.topics(topic))
    TestSource.probe[ProducerRecord[String, ExampleEvent]].via(log).toMat(TestSink.probe[ConsumerRecord[String, ExampleEvent]])(Keep.both).run()
  }

  "An event log flow" must {
    "produce and consume events" in {
      val (pub, sub) = probes("test")

      pub.sendNext(record(e1))
      pub.sendNext(record(e2))

      sub.request(2)
      sub.expectNextN(2).map(_.value) should be(Seq(e1, e2)) // hashed to same partition
    }
    "collaborate with other event log flows" in {
      val (pub1, sub1) = probes("test-1")
      val (pub2, sub2) = probes("test-2")

      pub1.sendNext(record(e1))
      pub2.sendNext(record(e2))

      sub1.request(2)
      sub2.request(2)

      sub1.expectNextN(2).map(_.value) should be(Seq(e1, e2))
      sub2.expectNextN(2).map(_.value) should be(Seq(e1, e2))
    }
  }
}
