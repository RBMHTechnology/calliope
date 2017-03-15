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
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.immutable.Seq
import scala.util.Random

class KafkaIndexSpec extends KafkaSpec {
  import IndexStore._
  import KafkaSpec._

  def messagePublisher: TestPublisher.Probe[Message] =
    TestSource.probe[Message]
      .map { m =>
        val tp = topicPartitions(Random.nextInt(topicPartitions.size))
        new ProducerRecord[Long, Message](tp.topic, tp.partition, m.sequenceNr, m)
      }.toMat(Producer.plainSink[Long, Message](producerSettings))(Keep.left)
      .run()

  def messageSubscriber(source: Source[Message, NotUsed]): TestSubscriber.Probe[Message] =
    source.toMat(TestSink.probe[Message])(Keep.right).run()

  "An index connection" must {
    "allow subscription to index streams" in {
      val messages = Seq(
        Message("e1", 0L),
        Message("e1", 1L),
        Message("e2", 0L),
        Message("e2", 1L),
        Message("e3", 0L),
        Message("e3", 1L))

      val indexStore = inmemStore[String, Long, Message, String] // indexes messages by emitter
      val indexConnection = connect[String, Long, Message, String](consumerSettings, topicPartitions.toSet, indexStore, _.value.emitter, messageSourceDecoder, messageSequenceNrDecoder)

      val sub1 = messageSubscriber(indexConnection.subscribe.map(_.value)) // subscribe to messages of all emitters
      sub1.request(8)

      val pub = messagePublisher
      messages.foreach(pub.sendNext)

      sub1.expectNextN(6).groupBy(_.emitter) should be(messages.groupBy(_.emitter)) // assert ordering per emitter
      val sub2 = messageSubscriber(indexConnection.subscribe("e2").map(_.value)) // subscribe to messages of emitter e2

      sub2.request(3)
      sub2.expectNext(Message("e2", 0L)) // obtained from index
      sub2.expectNext(Message("e2", 1L)) // obtained from index

      pub.sendNext(Message("e1", 2L))
      pub.sendNext(Message("e2", 2L))

      sub1.expectNextUnordered(Message("e1", 2L), Message("e2", 2L)) // live update from all emitters stream
      sub2.expectNext(Message("e2", 2L))                             // live update from emitter e2 stream
    }
  }
}
