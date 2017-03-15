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

import java.util.concurrent.{Future => JFuture}

import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Keep
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.scalatest.Ignore

import scala.collection.immutable.Map
import scala.concurrent.duration._

@Ignore
class KafkaLoadSpec extends KafkaSpec {
  import KafkaSpec._

  val numMessages = 500000
  var producer: KafkaProducer[Long, Message] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    producer = producerSettings.createKafkaProducer()

    0.until(numMessages - 1).foreach(send)
    send(numMessages - 1).get()
  }

  override def afterAll(): Unit = {
    producer.close()
    super.afterAll()
  }

  def send(i: Int): JFuture[RecordMetadata] = {
    val tp = topicPartitions(i % 3)
    producer.send(new ProducerRecord[Long, Message](tp.topic, tp.partition, i.toLong, Message("e", i)))
  }

  def consumer1: TestSubscriber.Probe[ConsumerRecord[Long, Message]] =
    Consumer.plainSource(consumerSettings, Subscriptions.assignment(topicPartitions.toSet))
      .toMat(TestSink.probe[ConsumerRecord[Long, Message]])(Keep.right).run()

  def consumer2: TestSubscriber.Probe[ConsumerRecord[Long, Message]] =
    Consume.from(consumerSettings, topicPartitions.map(_ -> -1L).toMap)
      .toMat(TestSink.probe[ConsumerRecord[Long, Message]])(Keep.right).run()

  def resequencer1: TestSubscriber.Probe[ConsumerRecord[Long, Message]] =
    Resequence.fromInc(consumerSettings, messageSourceDecoder, messageSequenceNrDecoder, Map.empty[String, Long], topicPartitions.map(_ -> -1L).toMap).map(_._1)
      .toMat(TestSink.probe[ConsumerRecord[Long, Message]])(Keep.right).run()

  def resequencer2: TestSubscriber.Probe[ConsumerRecord[Long, Message]] =
    Resequence.from(consumerSettings, messageSourceDecoder, messageSequenceNrDecoder, Map.empty[String, Long], topicPartitions.map(_ -> -1L).toMap).map(_.consumerRecord)
      .toMat(TestSink.probe[ConsumerRecord[Long, Message]])(Keep.right).run()

  def resequencer3(progressStore: ProgressStore[String, TopicPartition]): TestSubscriber.Probe[ConsumerRecord[Long, Message]] =
    Resequence.from(consumerSettings, messageSourceDecoder, messageSequenceNrDecoder)(progressStore, topicPartitions.toSet).map(_.consumerRecord)
      .toMat(TestSink.probe[ConsumerRecord[Long, Message]])(Keep.right).run()

  def consumeWith(sub: TestSubscriber.Probe[ConsumerRecord[Long, Message]]): Unit = {
    sub.request(numMessages)
    val duration = measure(0.until(numMessages).foreach(_ => sub.expectNext()))
    println(s"consumption of $numMessages messages took ${duration.toMillis} ms")
  }

  "An akka.kafka consumer" must {
    s"consume $numMessages messages" in {
      consumeWith(consumer1)
    }
  }

  "A calliope consumer" must {
    s"consume $numMessages messages" in {
      consumeWith(consumer2)
    }
    s"consume and resequence $numMessages messages (emitting incremental progress data)" in {
      consumeWith(resequencer1)
    }
    s"consume and resequence $numMessages messages (emitting accumulated progress data)" in {
      consumeWith(resequencer2)
    }
    s"consume and resequence $numMessages messages (emitting committable progress data)" in {
      consumeWith(resequencer3(noopStore))
    }
  }

  private val noopStore: ProgressStore[String, TopicPartition] = new ProgressStore[String, TopicPartition] {
    override def read(groupId: String): ResequenceProgress[String, TopicPartition] = ResequenceProgress[String, TopicPartition]()
    override def update(groupId: String, resequenceProgress: ResequenceProgress[String, TopicPartition]): Unit = ()
  }

  private def measure(block: => Unit): FiniteDuration = {
    val t0 = System.nanoTime()
    block
    (System.nanoTime() - t0).nanos
  }
}
