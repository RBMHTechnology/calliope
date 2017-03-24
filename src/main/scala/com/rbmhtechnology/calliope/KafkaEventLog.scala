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
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscription}
import akka.stream.scaladsl.{Flow, Keep, Source}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.language.higherKinds

object KafkaEventLog {
  def apply[A[_, _], K, V, M](producerSettings: ProducerSettings[K, V],
                              source: Source[A[K, V], M]): Flow[ProducerRecord[K, V], A[K, V], M] =
    Flow.fromSinkAndSourceMat(Producer.plainSink(producerSettings), source)(Keep.right)

  def apply[A[_, _], K, V, M](producer: KafkaProducer[K, V],
                              producerSettings: ProducerSettings[K, V],
                              source: Source[A[K, V], M]): Flow[ProducerRecord[K, V], A[K, V], M] =
    Flow.fromSinkAndSourceMat(Producer.plainSink(producerSettings), source)(Keep.right)

  def apply[K, V](producerSettings: ProducerSettings[K, V],
                  consumerSettings: ConsumerSettings[K, V],
                  subscription: Subscription): Flow[ProducerRecord[K, V], ConsumerRecord[K, V], Control] =
    apply(producerSettings, Consumer.plainSource(consumerSettings, subscription))

  def apply[K, V](producer: KafkaProducer[K, V],
                  producerSettings: ProducerSettings[K, V],
                  consumerSettings: ConsumerSettings[K, V],
                  subscription: Subscription): Flow[ProducerRecord[K, V], ConsumerRecord[K, V], Control] =
    apply(producer, producerSettings, Consumer.plainSource(consumerSettings, subscription))

  def committable[K, V](producerSettings: ProducerSettings[K, V],
                        consumerSettings: ConsumerSettings[K, V],
                        subscription: Subscription): Flow[ProducerRecord[K, V], CommittableMessage[K, V], Control] =
    apply(producerSettings, Consumer.committableSource(consumerSettings, subscription))

  def committable[K, V](producer: KafkaProducer[K, V],
                        producerSettings: ProducerSettings[K, V],
                        consumerSettings: ConsumerSettings[K, V],
                        subscription: Subscription): Flow[ProducerRecord[K, V], CommittableMessage[K, V], Control] =
    apply(producer, producerSettings, Consumer.committableSource(consumerSettings, subscription))
}
