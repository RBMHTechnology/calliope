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

import java.sql.ResultSet

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink

import scala.collection.immutable

case class Event(sourceId: String, sequenceNr: Long, payload: String)

object Event {
  implicit val sourced = new Sourced[Event] {
    override def sourceId(a: Event): String = a.sourceId
  }

  implicit val sequenced = new Sequenced[Event] {
    override def sequenceNr(event: Event): Long = event.sequenceNr
  }
}

class DeduplicationApiSpecification {

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  def specification(): Unit = {

    val maxPartitions = Int.MaxValue

    val store = new SequenceStore(new TestStorageAdapter())

    val control = Consumer.plainPartitionedSource[String, Event](null, Subscriptions.topics("topic"))
      .via(Deduplication.flow(maxPartitions, store.loadSequences))
      .map { x =>
        // business logic
        store.persist(x)
        x
      }
      .to(Sink.ignore)
      .run()

    val control1 = Consumer.plainPartitionedSource[String, Event](null, Subscriptions.topics("topic"))
      .via(Deduplication.plain(maxPartitions, store.loadSequences))
      .map { x =>
        // business logic
        store.persist(x)
        x
      }
      .to(Sink.ignore)
      .run()

    val control2 = Consumer.committablePartitionedSource[String, Event](null, Subscriptions.topics("topic"))
      .via(Deduplication.committable(maxPartitions, store.loadSequences))
      .map { x =>
        // business logic
        store.persist(x)
        x
      }
      .batch(max = 20, first => CommittableOffsetBatch.empty.updated(first.committableOffset)) { (batch, elem) =>
        batch.updated(elem.committableOffset)
      }
      .to(Sink.ignore)
      .run()
  }

  class TestStorageAdapter extends StorageAdapter[SourceSequenceNr] {
    override def query(sql: String, mapper: (ResultSet) => SourceSequenceNr): immutable.Seq[SourceSequenceNr] = ???
    override def update(sql: String): Int = ???
  }
}

