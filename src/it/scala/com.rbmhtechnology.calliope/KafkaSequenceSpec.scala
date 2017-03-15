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
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.scalatest.concurrent.ScalaFutures

import scala.collection.immutable.{Map, Seq}

class KafkaSequenceSpec extends KafkaSpec with ScalaFutures {
  import KafkaSpec._

  val tp0 = topicPartitions(0)
  val tp1 = topicPartitions(1)

  var producer: KafkaProducer[Long, Message] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    producer = producerSettings.createKafkaProducer()
  }

  override def afterAll(): Unit = {
    producer.close()
    super.afterAll()
  }

  def subscriber(committedMessageSequenceNrs: Map[String, Long],
                 committedStorageSequenceNrs: Map[TopicPartition, Long]): TestSubscriber.Probe[Map[String, Source[Long, NotUsed]]] =
    Sequence.from(consumerSettings, messageSourceDecoder, messageSequenceNrDecoder, committedMessageSequenceNrs, committedStorageSequenceNrs)
      .toMat(TestSink.probe[Map[String, Source[Long, NotUsed]]])(Keep.right).run()

  def assertSequence(committedMessageSequenceNrs: Map[String, Long],
                     committedStorageSequenceNrs: Map[TopicPartition, Long],
                     expectedSequences: Seq[Seq[Long]]): Unit = {

    val sub = subscriber(committedMessageSequenceNrs, committedStorageSequenceNrs)
    sub.request(1)

    val sequences1 = sub.expectNext().mapValues(_.take(4).runWith(Sink.seq[Long]).futureValue)
    sub.expectComplete()

    sequences1("e0") should be(expectedSequences(0))
    sequences1("e1") should be(expectedSequences(1))
    sequences1("e2") should be(expectedSequences(2))
    sequences1("e3") should be(expectedSequences(3))
  }

  "A sequence" can {
    "be recovered from empty topic partitions" in {
      val committedMessageSequenceNrs = Map("e0" -> -1L, "e1" -> -1L, "e2" -> -1L, "e3" -> -1L)
      val committedStorageSequenceNrs = Map(tp0 -> 0L, tp1 -> -1L)

      val expectedSequences = Seq(
        Seq(0L, 1L, 2L, 3L),
        Seq(0L, 1L, 2L, 3L),
        Seq(0L, 1L, 2L, 3L),
        Seq(0L, 1L, 2L, 3L))

      assertSequence(committedMessageSequenceNrs, committedStorageSequenceNrs, expectedSequences)
    }
    "be recovered from written topic partitions" in {
      // partition 0
      producer.send(new ProducerRecord[Long, Message](tp0.topic, tp0.partition, 0L, Message("e0", 0L))).get
      producer.send(new ProducerRecord[Long, Message](tp0.topic, tp0.partition, 0L, Message("e0", 1L))).get
      producer.send(new ProducerRecord[Long, Message](tp0.topic, tp0.partition, 0L, Message("e1", 0L))).get
      producer.send(new ProducerRecord[Long, Message](tp0.topic, tp0.partition, 1L, Message("e2", 1L))).get
      producer.send(new ProducerRecord[Long, Message](tp0.topic, tp0.partition, 2L, Message("e2", 2L))).get
      producer.send(new ProducerRecord[Long, Message](tp0.topic, tp0.partition, 0L, Message("e3", 0L))).get

      // partition 1
      producer.send(new ProducerRecord[Long, Message](tp1.topic, tp1.partition, 1L, Message("e1", 1L))).get
      producer.send(new ProducerRecord[Long, Message](tp1.topic, tp1.partition, 0L, Message("e2", 0L))).get
      producer.send(new ProducerRecord[Long, Message](tp1.topic, tp1.partition, 2L, Message("e1", 3L))).get // after message snr gap

      val committedMessageSequenceNrs = Map("e0" -> 1L, "e1" -> 1L, "e2" -> 0L)
      // actual storage sequence numbers taken from resequencer output
      val committedStorageSequenceNrs1 = Map(tp0 -> 2L, tp1 -> 1L)
      // lower storage sequence numbers
      val committedStorageSequenceNrs2 = Map(tp0 -> 0L, tp1 -> -1L)

      val expectedSequences = Seq(
        Seq(2L, 3L, 4L, 5L),
        Seq(2L, 4L, 5L, 6L),
        Seq(3L, 4L, 5L, 6L),
        Seq(1L, 2L, 3L, 4L))

      assertSequence(committedMessageSequenceNrs, committedStorageSequenceNrs1, expectedSequences)
      assertSequence(committedMessageSequenceNrs, committedStorageSequenceNrs2, expectedSequences)
    }
  }
}
