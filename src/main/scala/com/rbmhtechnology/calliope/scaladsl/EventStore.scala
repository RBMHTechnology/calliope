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

package com.rbmhtechnology.calliope.scaladsl

import akka.actor.ActorSystem
import com.rbmhtechnology.calliope._

import scala.collection.immutable.Seq
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

trait EventStore {

  def readEvents(fromSequenceNr: Long, limit: Int): Seq[StoredEvent]

  def writeEvent(event: Array[Byte], topic: String, aggregateId: String, onCommit: => Unit): Unit

  def deleteEvents(toSequenceNr: Long): Unit
}

private[scaladsl] object EventStoreReader {
  def withTimestampGapDetection[A](sourceId: String, eventStore: EventStore, transactionTimeout: FiniteDuration)
                                  (implicit system: ActorSystem, executionContext: ExecutionContext): EventStoreReader[A] =
    new EventStoreReader[A](sourceId, eventStore) with ReaderTimestampGapDetection[A] {
      override def persistenceTimeout: FiniteDuration = transactionTimeout
    }
}

private[scaladsl] class EventStoreReader[A](sourceId: String, eventStore: EventStore)
                                           (implicit system: ActorSystem, val executionContext: ExecutionContext)
  extends EventReader[A] {

  private val serializer = PayloadSerializer()

  override def readEvents(fromSequenceNr: Long, maxItems: Int): Future[Seq[EventRecord[A]]] =
    Future {
      eventStore.readEvents(fromSequenceNr, maxItems)
    } map { events =>
      events.take(maxItems).map { ev =>
        EventRecord[A](serializer.deserialize(ev.payload).asInstanceOf[A], sourceId, ev.sequenceNr, ev.creationTimestamp, ev.topic, ev.aggregateId)
      }
    }
}

private[scaladsl] class EventStoreDeleter(eventStore: EventStore)
                                         (implicit executionContext: ExecutionContext) extends EventDeleter {

  override def deleteEvents(toSequenceNr: Long): Future[Unit] =
    Future {
      eventStore.deleteEvents(toSequenceNr)
    }
}

private[scaladsl] class EventStoreWriter[A](val defaultTopic: String, eventStore: EventStore, onCommit: => Unit)
                                           (implicit system: ActorSystem) extends EventWriter[A] {

  private val serializer = PayloadSerializer()

  override def writeEventToTopic(event: A, aggregateId: String, topic: String): Unit = {
    eventStore.writeEvent(serializer.serialize(event), topic, aggregateId, onCommit)
  }
}
