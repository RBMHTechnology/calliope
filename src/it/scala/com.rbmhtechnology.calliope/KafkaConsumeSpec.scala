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

import akka.stream.scaladsl.Keep
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition

import scala.collection.immutable.{Map, Seq}

class KafkaConsumeSpec extends KafkaSpec {
  import KafkaSpec._

  val numMessages = 8
  var producer: KafkaProducer[Long, Message] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    producer = producerSettings.createKafkaProducer()

    0 until numMessages foreach { i =>
      val tp = topicPartitions(i % 3)
      producer.send(new ProducerRecord[Long, Message](tp.topic, tp.partition, i, Message("e", i))).get
    }
  }

  override def afterAll(): Unit = {
    producer.close()
    super.afterAll()
  }

  def fromSnapshotSubscriber(committedStorageSequenceNrs: Map[TopicPartition, Long]): TestSubscriber.Probe[ConsumerRecord[Long, Message]] =
    Consume.fromSnapshot(consumerSettings, committedStorageSequenceNrs).toMat(TestSink.probe[ConsumerRecord[Long, Message]])(Keep.right).run()

  def fromSubscriber(committedStorageSequenceNrs: Map[TopicPartition, Long]): TestSubscriber.Probe[ConsumerRecord[Long, Message]] =
    Consume.from(consumerSettings, committedStorageSequenceNrs).toMat(TestSink.probe[ConsumerRecord[Long, Message]])(Keep.right).run()

  def rangeSubscriber(committedStorageSequenceNrs: Map[TopicPartition, Long], currentStorageSequenceNrs: Map[TopicPartition, Long]): TestSubscriber.Probe[ConsumerRecord[Long, Message]] =
    Consume.range(consumerSettings, committedStorageSequenceNrs, currentStorageSequenceNrs).toMat(TestSink.probe[ConsumerRecord[Long, Message]])(Keep.right).run()

  "A consumer" must {
    "consume" in {
      val sub = fromSubscriber(topicPartitions.map(_ -> -1L).toMap)
      sub.request(numMessages)
      sub.expectNextN(numMessages).map(_.value.sequenceNr).sorted should be(Seq(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L))
    }
    "consume from given offsets" in {
      val sub = fromSubscriber(Map(topicPartitions(0) -> 1L, topicPartitions(1) -> 0L, topicPartitions(2) -> -1L))
      sub.request(5)
      sub.expectNextN(5).map(_.value.sequenceNr).sorted should be(Seq(2L, 4L, 5L, 6L, 7L))
    }
    "consume from given offsets until given offsets and then complete" in {
      val sub = rangeSubscriber(topicPartitions.map(_ -> 0L).toMap, topicPartitions.map(_ -> 2L).toMap)
      sub.request(3)
      sub.expectNextN(3).map(_.value.sequenceNr).sorted should be(Seq(3L, 4L, 5L))
      sub.expectComplete()
    }
    "consume from given offsets until end of snapshot and then complete" in {
      val sub = fromSnapshotSubscriber(Map(topicPartitions(0) -> 1L, topicPartitions(1) -> 0L, topicPartitions(2) -> -1L))
      sub.request(5)
      sub.expectNextN(5).map(_.value.sequenceNr).sorted should be(Seq(2L, 4L, 5L, 6L, 7L))
      sub.expectComplete()
    }
    "complete immediately when consuming from not yet created topic snapshot" in {
      val sub = fromSnapshotSubscriber(Map(new TopicPartition("none", 0) -> -1L, new TopicPartition("none", 0) -> -1L))
      sub.request(1)
      sub.expectComplete()
    }
  }
}
