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

import java.time.Instant

import com.rbmhtechnology.calliope.ProducerFlow.Producible

case class EventRecord[A](payload: A, sourceId: String, sequenceNr: Long, creationTimestamp: Instant, topic: String, aggregateId: String)

object EventRecord {

  implicit def sequenced[A]: Sequenced[EventRecord[A]] =
    (event: EventRecord[A]) => event.sequenceNr

  implicit def timestamped[A]: Timestamped[EventRecord[A]] =
    (event: EventRecord[A]) => event.creationTimestamp

  implicit def producible[A] = new Producible[EventRecord[A], String, SequencedEvent[A]] {
    override def topic(event: EventRecord[A]): String =
      event.topic

    override def key(event: EventRecord[A]): String =
      event.aggregateId

    override def value(event: EventRecord[A]): SequencedEvent[A] =
      SequencedEvent(event.payload, event.sourceId, event.sequenceNr, event.creationTimestamp)

    override def timestamp(event: EventRecord[A]): Option[Long] =
      Some(event.creationTimestamp.toEpochMilli)
  }
}