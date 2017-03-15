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
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition

class KafkaOffsetsSpec extends KafkaSpec {
  import KafkaSpec._

  var producer: KafkaProducer[Long, Message] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    producer = producerSettings.createKafkaProducer()
  }

  override def afterAll(): Unit = {
    producer.close()
    super.afterAll()
  }

  def endOffsetsSubscriber(storagePartitions: Set[TopicPartition]): TestSubscriber.Probe[Map[TopicPartition, Long]] =
    Offsets.endOffsets(consumerSettings, storagePartitions).toMat(TestSink.probe[Map[TopicPartition, Long]])(Keep.right).run()

  "An Offsets source" must {
    "emit the current end offsets of given partitions and then complete" in {
      val tp0 = topicPartitions(0)
      val tp1 = topicPartitions(1)
      val tp2 = topicPartitions(2)

      producer.send(new ProducerRecord[Long, Message](tp1.topic, tp1.partition, 0L, Message("e", 0L))).get
      producer.send(new ProducerRecord[Long, Message](tp2.topic, tp2.partition, 1L, Message("e", 1L))).get
      producer.send(new ProducerRecord[Long, Message](tp2.topic, tp2.partition, 2L, Message("e", 2L))).get

      val sub = endOffsetsSubscriber(topicPartitions.toSet)
      sub.request(1)

      val expectedOffsets = Map(tp0 -> 0L, tp1 -> 1L, tp2 -> 2L)
      val actualOffsets= sub.expectNext()

      actualOffsets should be(expectedOffsets)
      sub.expectComplete()
    }
    "emit the current end offsets of given not yet created partitions and then complete" in {
      val tp0 = new TopicPartition("none", 0)
      val tp1 = new TopicPartition("none", 1)

      val sub = endOffsetsSubscriber(Set(tp0, tp1))
      sub.request(1)

      val expectedOffsets = Map(tp0 -> 0L, tp1 -> 0L)
      val actualOffsets= sub.expectNext()

      actualOffsets should be(expectedOffsets)
      sub.expectComplete()
    }
  }
}
