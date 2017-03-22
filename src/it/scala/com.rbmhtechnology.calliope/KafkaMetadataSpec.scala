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

import scala.collection.JavaConverters._

class KafkaMetadataSpec extends KafkaSpec {
  import KafkaSpec._

  var producer: KafkaProducer[String, ExampleEvent] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    producer = producerSettings.createKafkaProducer()
  }

  override def afterAll(): Unit = {
    producer.close()
    super.afterAll()
  }

  def endOffsetsSubscriber(topic: String): TestSubscriber.Probe[Map[TopicPartition, Long]] =
    KafkaMetadata.endOffsets(consumerSettings(topic), topic).toMat(TestSink.probe[Map[TopicPartition, Long]])(Keep.right).run()

  def committedOffsetsSubscriber(topic: String): TestSubscriber.Probe[Map[TopicPartition, Long]] =
    KafkaMetadata.committedOffsets(consumerSettings(topic), topic).toMat(TestSink.probe[Map[TopicPartition, Long]])(Keep.right).run()

  "A Metadata.endOffsets source" must {
    "emit the current end offsets of given topic and then complete" in {
      val tp0 = topicPartitions(0)
      val tp1 = topicPartitions(1)
      val tp2 = topicPartitions(2)

      producer.send(new ProducerRecord[String, ExampleEvent](tp1.topic, tp1.partition, null, ExampleEvent("e1", "a1", "x")))
      producer.send(new ProducerRecord[String, ExampleEvent](tp2.topic, tp2.partition, null, ExampleEvent("e2", "a2", "y")))
      producer.send(new ProducerRecord[String, ExampleEvent](tp2.topic, tp2.partition, null, ExampleEvent("e3", "a3", "z"))).get

      val sub = endOffsetsSubscriber(topic)
      sub.request(1)

      val expectedOffsets = Map(tp0 -> 0L, tp1 -> 1L, tp2 -> 2L)
      val actualOffsets= sub.expectNext()

      actualOffsets should be(expectedOffsets)
      sub.expectComplete()
    }
    "emit the committed offsets of given topic and then complete" in {
      val tp0 = topicPartitions(0)
      val tp1 = topicPartitions(1)
      val tp2 = topicPartitions(2)

      val consumer = consumerSettings(topic).createKafkaConsumer()
      consumer.subscribe(Seq(topic).asJava)
      consumer.poll(3000).count should be(3)
      consumer.commitSync()
      consumer.close()

      val sub = committedOffsetsSubscriber(topic)
      sub.request(1)

      val expectedOffsets = Map(tp0 -> 0L, tp1 -> 1L, tp2 -> 2L)
      val actualOffsets= sub.expectNext()

      actualOffsets should be(expectedOffsets)
      sub.expectComplete()
    }
    "emit the current end offsets of given not yet created topic and then complete" in {
      val tp0 = new TopicPartition("none", 0)
      val tp1 = new TopicPartition("none", 1)
      val tp2 = new TopicPartition("none", 2)

      val sub = endOffsetsSubscriber("none")
      sub.request(1)

      val expectedOffsets = Map(tp0 -> 0L, tp1 -> 0L, tp2 -> 0L)
      val actualOffsets= sub.expectNext()

      actualOffsets should be(expectedOffsets)
      sub.expectComplete()
    }
    "emit the commit offsets of given not yet created topic and then complete" in {
      val tp0 = new TopicPartition("none", 0)
      val tp1 = new TopicPartition("none", 1)
      val tp2 = new TopicPartition("none", 2)

      val sub = committedOffsetsSubscriber("none")
      sub.request(1)

      val expectedOffsets = Map(tp0 -> 0L, tp1 -> 0L, tp2 -> 0L)
      val actualOffsets= sub.expectNext()

      actualOffsets should be(expectedOffsets)
      sub.expectComplete()
    }
  }
}
