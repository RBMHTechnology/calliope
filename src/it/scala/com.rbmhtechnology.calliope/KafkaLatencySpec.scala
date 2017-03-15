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

import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.immutable.Map
import scala.concurrent.duration._

class KafkaLatencySpec extends KafkaSpec {
  import KafkaSpec._

  def messagePublisher: TestPublisher.Probe[Message] =
    TestSource.probe[Message]
      .map(m => new ProducerRecord[Long, Message](topicPartitions.head.topic, topicPartitions.head.partition, m.sequenceNr, m))
      .toMat(Producer.plainSink[Long, Message](producerSettings))(Keep.left)
      .run()

  def messageSubscriber: TestSubscriber.Probe[Message] =
    Consume.from(consumerSettings, Map(topicPartitions.head -> -1L)).map(_.value)
      .toMat(TestSink.probe[Message])(Keep.right)
      .run()

  "An publisher-subscriber pair" should {
    "round-trip a message with sufficiently low latency" in {
      val pub = messagePublisher
      val sub = messageSubscriber

      sub.request(10)

      0 until 10 foreach { i =>
        // warmup
        pub.sendNext(Message("e", i))
        sub.expectNext(Message("e", i))
      }

      sub.request(1)

      val duration = measure {
        pub.sendNext(Message("e", 10))
        sub.expectNext(Message("e", 10))
      }

      println(s"single round-trip took ${duration.toMillis} ms")
    }
  }

  private def measure(block: => Unit): FiniteDuration = {
    val t0 = System.nanoTime()
    block
    (System.nanoTime() - t0).nanos
  }
}
