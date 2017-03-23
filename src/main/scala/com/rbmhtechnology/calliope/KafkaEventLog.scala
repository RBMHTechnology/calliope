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

import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscription, Subscriptions}
import akka.stream.scaladsl.{Flow, Keep}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition

object KafkaEventLog {
  def from[K, V](producerSettings: ProducerSettings[K, V],
                 consumerSettings: ConsumerSettings[K, V],
                 from: Map[TopicPartition, Long]): Flow[ProducerRecord[K, V], ConsumerRecord[K, V], Control] =
    apply(producerSettings, consumerSettings, Subscriptions.assignmentWithOffset(from))

  def from[K, V](producer: KafkaProducer[K, V],
                 producerSettings: ProducerSettings[K, V],
                 consumerSettings: ConsumerSettings[K, V],
                 from: Map[TopicPartition, Long]): Flow[ProducerRecord[K, V], ConsumerRecord[K, V], Control] =
    apply(producer, producerSettings, consumerSettings, Subscriptions.assignmentWithOffset(from))

  def apply[K, V](producerSettings: ProducerSettings[K, V],
                  consumerSettings: ConsumerSettings[K, V],
                  subscription: Subscription): Flow[ProducerRecord[K, V], ConsumerRecord[K, V], Control] = {

    val sink = Producer.plainSink(producerSettings)
    val source = Consumer.plainSource(consumerSettings, subscription)

    Flow.fromSinkAndSourceMat(sink, source)(Keep.right)
  }

  def apply[K, V](producer: KafkaProducer[K, V],
                  producerSettings: ProducerSettings[K, V],
                  consumerSettings: ConsumerSettings[K, V],
                  subscription: Subscription): Flow[ProducerRecord[K, V], ConsumerRecord[K, V], Control] = {

    val sink = Producer.plainSink(producerSettings, producer)
    val source = Consumer.plainSource(consumerSettings, subscription)

    Flow.fromSinkAndSourceMat(sink, source)(Keep.right)
  }

  def committable[K, V](producerSettings: ProducerSettings[K, V],
                        consumerSettings: ConsumerSettings[K, V],
                        subscription: Subscription): Flow[ProducerRecord[K, V], CommittableMessage[K, V], Control] = {

    val sink = Producer.plainSink(producerSettings)
    val source = Consumer.committableSource(consumerSettings, subscription)

    Flow.fromSinkAndSourceMat(sink, source)(Keep.right)
  }

  def committable[K, V](producer: KafkaProducer[K, V],
                        producerSettings: ProducerSettings[K, V],
                        consumerSettings: ConsumerSettings[K, V],
                        subscription: Subscription): Flow[ProducerRecord[K, V], CommittableMessage[K, V], Control] = {

    val sink = Producer.plainSink(producerSettings, producer)
    val source = Consumer.committableSource(consumerSettings, subscription)

    Flow.fromSinkAndSourceMat(sink, source)(Keep.right)
  }
}
