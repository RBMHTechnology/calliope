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

import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import akka.{Done, NotUsed}

import scala.collection.immutable.{Map, Seq}
import scala.concurrent.Future

class KafkaProduceSpec extends KafkaSpec {
  import KafkaSpec._

  def subscriber: TestSubscriber.Probe[Message] =
    Resequence.fromInc(consumerSettings, messageSourceDecoder, messageSequenceNrDecoder, Map.empty[String, Long], topicPartitions.map(_ -> -1L).toMap).map(_._1.value)
      .toMat(TestSink.probe[Message])(Keep.right)
      .run()

  def publisher(sink: Sink[SequencedProducerRecord[Long, Message], Future[Done]]): TestPublisher.Probe[Message] =
    TestSource.probe[Message].map(m => new SequencedProducerRecord[Long, Message](-1L, m)).to(sink).run()

  def sequence: Source[Map[String, Source[Long, NotUsed]], NotUsed] =
    Sequence.from(consumerSettings, messageSourceDecoder, messageSequenceNrDecoder, Map.empty[String, Long], topicPartitions.map(_ -> -1L).toMap)

  "A producer" must {
    "produce messages using a recovered sequence" in {
      val pro = Message("", -1L)
      val sub = subscriber

      val seq1 = sequence // emits sequences (from empty topic partitions)
      val pub1a = publisher(Produce.to(producerSettings, "e1", messageSequenceEncoder, seq1, topicPartitions.toSet))
      val pub2a = publisher(Produce.to(producerSettings, "e2", messageSequenceEncoder, seq1, topicPartitions.toSet))

      pub1a.sendNext(pro)
      pub1a.sendNext(pro)
      pub1a.sendNext(pro)
      pub2a.sendNext(pro)
      pub2a.sendNext(pro)

      sub.request(5)
      val messages = sub.expectNextN(5).groupBy(_.emitter)

      messages("e1") should be(Seq(
        Message("e1", 0L),
        Message("e1", 1L),
        Message("e1", 2L)))

      messages("e2") should be(Seq(
        Message("e2", 0L),
        Message("e2", 1L)))

      val seq2 = sequence // emits sequences (from written topic partitions)
      val pub1b = publisher(Produce.to(producerSettings, "e1", messageSequenceEncoder, seq2, topicPartitions.toSet))
      val pub2b = publisher(Produce.to(producerSettings, "e2", messageSequenceEncoder, seq2, topicPartitions.toSet))

      pub1a.sendNext(pro)
      pub2a.sendNext(pro)

      sub.request(2)
      sub.expectNextUnordered(Message("e1", 3L), Message("e2", 2L))
    }
  }
}
