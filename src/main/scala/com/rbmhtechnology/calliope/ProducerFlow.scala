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
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffset}
import akka.kafka.{ConsumerMessage, ProducerMessage}
import akka.stream.scaladsl.Flow
import org.apache.kafka.clients.producer.ProducerRecord

object ProducerFlow {

  trait Producible[A, K, V] {
    def topic(message: A): String
    def key(message: A): K
    def value(message: A): V
    def timestamp(message: A): Option[Long]
  }

  object Producible {
    implicit def producibleConsumerMessage[KIn, VIn, KOut, VOut](implicit producible: Producible[VIn, KOut, VOut]): Producible[ConsumerMessage.CommittableMessage[KIn, VIn], KOut, VOut] =
      new Producible[ConsumerMessage.CommittableMessage[KIn, VIn], KOut, VOut] {
        override def topic(message: CommittableMessage[KIn, VIn]): String =
          producible.topic(message.record.value())

        override def key(message: CommittableMessage[KIn, VIn]): KOut =
          producible.key(message.record.value())

        override def value(message: CommittableMessage[KIn, VIn]): VOut =
          producible.value(message.record.value())

        override def timestamp(message: CommittableMessage[KIn, VIn]): Option[Long] =
          producible.timestamp(message.record.value())
      }
  }

  trait Committable[A] {
    def offset(message: A): CommittableOffset
  }

  object Committable {
    implicit def committableConsumerMessage[K, V]: Committable[ConsumerMessage.CommittableMessage[K, V]] = new Committable[ConsumerMessage.CommittableMessage[K, V]] {
      override def offset(message: CommittableMessage[K, V]): CommittableOffset = message.committableOffset
    }
  }

  private[this] val instance = new ProducerFlow[Any]{}

  def apply[A]: ProducerFlow[A] =
    instance.asInstanceOf[ProducerFlow[A]]
}

trait ProducerFlow[A] {
  import ProducerFlow._

  def toRecord[K, V](implicit producible: Producible[A, K, V]): Flow[A, ProducerRecord[K, V], NotUsed] =
    Flow[A].map(createRecord(_))

  def toMessage[K, V, PassThrough](p: A => PassThrough)(implicit producible: Producible[A, K, V]): Flow[A, ProducerMessage.Message[K, V, PassThrough], NotUsed] =
    Flow[A].map(a => ProducerMessage.Message[K, V, PassThrough](createRecord(a), p(a)))

  def toMessage[K, V, P](implicit producible: Producible[A, K, V]): Flow[A, ProducerMessage.Message[K, V, Unit], NotUsed] =
    toMessage(_ => Unit)

  def toCommittableMessage[K, V](implicit producible: Producible[A, K, V], committable: Committable[A]): Flow[A, ProducerMessage.Message[K, V, ConsumerMessage.CommittableOffset], NotUsed] =
    toMessage(committable.offset)

  private def createRecord[K, V](a: A)(implicit producible: Producible[A, K, V]): ProducerRecord[K, V] =
    new ProducerRecord[K, V](producible.topic(a), null, producible.timestamp(a).map(long2Long).orNull, producible.key(a), producible.value(a))
}
