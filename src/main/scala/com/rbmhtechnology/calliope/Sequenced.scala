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
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
  * Type class for elements that contain sequence numbers.
  */
trait Sequenced[A] {
  def sequenceNr(event: A): Long
}

object Sequenced {

  implicit def sequencedConsumerRecord[K, V](implicit sequenced: Sequenced[V]): Sequenced[ConsumerRecord[K, V]] = new Sequenced[ConsumerRecord[K, V]] {
    override def sequenceNr(a: ConsumerRecord[K, V]): Long = sequenced.sequenceNr(a.value())
  }

  implicit def sequencedCommittableMessage[K, V](implicit sequenced: Sequenced[V]): Sequenced[CommittableMessage[K, V]] = new Sequenced[CommittableMessage[K, V]] {
    override def sequenceNr(a: CommittableMessage[K, V]): Long = sequenced.sequenceNr(a.record.value())
  }
}
