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

package com.rbmhtechnology.calliope.javadsl

import java.lang.{Iterable => JIterable}

import com.rbmhtechnology.calliope.{StoredEvent, scaladsl}

import scala.collection.immutable

trait EventStore {

  def readEvents(fromSequenceNr: Long, limit: Int): JIterable[StoredEvent]

  def writeEvent(event: Array[Byte], topic: String, aggregateId: String, onCommit: Runnable): Unit

  def deleteEvents(toSequenceNr: Long): Unit
}

object EventStore {
  import scala.collection.JavaConverters._

  implicit class EventStoreConverter(store: EventStore) {
    def asScala: scaladsl.EventStore = new scaladsl.EventStore {
      override def readEvents(fromSequenceNr: Long, limit: Int): immutable.Seq[StoredEvent] =
        store.readEvents(fromSequenceNr, limit).asScala.toVector

      override def writeEvent(event: Array[Byte], topic: String, aggregateId: String, onCommit: => Unit): Unit =
        store.writeEvent(event, topic, aggregateId, () => onCommit)

      override def deleteEvents(toSequenceNr: Long): Unit =
        store.deleteEvents(toSequenceNr)
    }
  }
}