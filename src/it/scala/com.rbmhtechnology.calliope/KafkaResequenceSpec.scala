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
import org.apache.kafka.common.TopicPartition

import scala.collection.immutable.Seq
import scala.util.Random

class KafkaResequenceSpec extends KafkaSpec {
  import KafkaSpec._

  val emitters = Seq("e1", "e2", "e3")
  val numMessagesPerEmitter = 100
  val numMessages: Int = emitters.size * numMessagesPerEmitter
  val messages: Seq[Message] = for {
    i <- 0 until numMessagesPerEmitter
    e <- emitters
  } yield Message(e, i)

  var consumed: Seq[(Message, ResequenceProgressIncrement[TopicPartition])] = Seq.empty

  override def beforeAll(): Unit = {
    super.beforeAll()

    val pub = messagePublisher(topicPartitions)
    messages.foreach(pub.sendNext)

    val sub = messageSubscriber(Map.empty, topicPartitions.map(_ -> -1L).toMap)
    sub.request(numMessages)
    consumed = sub.expectNextN(numMessages)
  }

  def messagePublisher(storagePartitions: Seq[TopicPartition]): TestPublisher.Probe[Message] =
    TestSource.probe[Message]
      .map { m =>
        val tp = storagePartitions(Random.nextInt(storagePartitions.size))
        new ProducerRecord[Long, Message](tp.topic, tp.partition, m.sequenceNr, m)
      }.toMat(Producer.plainSink[Long, Message](producerSettings))(Keep.left)
       .run()

  def messageSubscriber(committedMessageSequenceNrs: Map[String, Long], committedStorageSequenceNrs: Map[TopicPartition, Long]): TestSubscriber.Probe[(Message, ResequenceProgressIncrement[TopicPartition])] =
    Resequence.fromInc(consumerSettings, messageSourceDecoder, messageSequenceNrDecoder, committedMessageSequenceNrs, committedStorageSequenceNrs).map { case (cr, p) => cr.value -> p }
      .toMat(TestSink.probe[(Message, ResequenceProgressIncrement[TopicPartition])])(Keep.right)
      .run()

  def committableMessageSubscriber(progressStore: ProgressStore[String, TopicPartition]): TestSubscriber.Probe[ResequencedConsumerRecord[Long, Message, String] with Committable] =
    Resequence.from(consumerSettings, messageSourceDecoder, messageSequenceNrDecoder)(progressStore, topicPartitions.toSet)
      .toMat(TestSink.probe[ResequencedConsumerRecord[Long, Message, String] with Committable])(Keep.right)
      .run()

  def resequenceFromCustomProgress(messageCutoff: Int, storageCutoffs: Map[TopicPartition, Int]): Unit = {
    val committedMessageSequenceNrs = consumed.take(messageCutoff).map(_._1).foldLeft(Map.empty[String, Long]) {
      case (acc, m) => acc.updated(m.emitter, m.sequenceNr)
    }

    val committedStorageSequenceNrs = storageCutoffs.map {
      case (partition, cutoff) => partition -> consumed.take(cutoff).map(_._2).foldLeft(-1L) {
        case (acc, p) => if (p.storagePartition == partition) p.storageSequenceNr else acc
      }
    }

    val sub = messageSubscriber(committedMessageSequenceNrs, committedStorageSequenceNrs)
    sub.request(numMessages - messageCutoff)

    val actual = sub.expectNextN(numMessages - messageCutoff)
    emitters.foreach(emitter => actual.map(_._1).filter(_.emitter == emitter) should be(
      messages.filter(m => m.emitter == emitter && m.sequenceNr > committedMessageSequenceNrs(emitter))))
  }

  "A Resequence stage" must {
    "resequence a stream per message source consumed from multiple partitions starting from the first stream element" in {
      emitters.foreach(emitter => consumed.map(_._1).filter(_.emitter == emitter) should be(messages.filter(_.emitter == emitter)))
    }
    "resequence a stream per message source consumed from multiple partitions starting from a custom progress" in {
      // this is the case when message sequence number are written together with storage sequence numbers atomically
      // (e.g. message sequence numbers and storage sequence numbers at target)
      val messageCutoff = numMessages / 3 * 2
      resequenceFromCustomProgress(messageCutoff, topicPartitions.map(_ -> messageCutoff).toMap)
    }
    "resequence a stream per message source consumed from multiple partitions starting from a custom progress " +
      "where storage sequence number is taken from an earlier progress than message sequence number" in {
      // this is the case when message sequence number are written before storage sequence numbers are written
      // (e.g. message sequence numbers at target and storage sequence numbers i.e. offsets within Kafka)
      val messageCutoff = numMessages / 3 * 2
      def storageCutoff = messageCutoff - Random.nextInt(20)
      resequenceFromCustomProgress(messageCutoff, topicPartitions.map(_ -> storageCutoff).toMap)
    }
    "resequence a stream per message source consumed from multiple partitions starting from a stored progress" in {
      val progressStore = new InmemProgressStore[String, TopicPartition]
      val messageCutoff = numMessages / 3 * 2

      val sub1 = committableMessageSubscriber(progressStore)
      sub1.request(messageCutoff)
      sub1.expectNextN(messageCutoff).last.commit()

      val sub2 = committableMessageSubscriber(progressStore)
      sub2.request(numMessages - messageCutoff)
      val remaining = sub2.expectNextN(numMessages - messageCutoff)

      val progress1 = progressStore.read("test")
      val progress2 = remaining.last.resequenceProgress

      def actualRemainingSequenceNrs(emitter: String): Seq[Long] =
        remaining.map(_.consumerRecord.value).filter(_.emitter == emitter).map(_.sequenceNr)

      def expectedRemainingSequenceNrs(emitter: String): Seq[Long] =
        (progress1.messageSequenceNrs(emitter) + 1L) until numMessagesPerEmitter

      emitters.foreach { emitter =>
        actualRemainingSequenceNrs(emitter) should be(expectedRemainingSequenceNrs(emitter))
        progress2.messageSequenceNrs(emitter) should be(numMessagesPerEmitter - 1L)
      }
    }
  }
}
