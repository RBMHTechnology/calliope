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
import akka.stream.scaladsl.{Flow, Source}
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.collection.immutable
import scala.concurrent.Future

case class SourceSequenceNr(sourceId: String, sequenceNr: Long)

trait Sourced[A] {
  def sourceId(a: A): String
}

trait Partitioned[A] {
  def topic(event: A): String
  def partition(event: A): Int
}

object Partitioned {

  implicit def partitionedConsumerRecord[K, V]: Partitioned[ConsumerRecord[K, V]] = new Partitioned[ConsumerRecord[K, V]] {
    override def partition(event: ConsumerRecord[K, V]): Int = event.partition()

    override def topic(event: ConsumerRecord[K, V]): String = event.topic()
  }

  implicit def partitionedCommittableMessage[K, V]: Partitioned[CommittableMessage[K, V]] = new Partitioned[CommittableMessage[K, V]] {
    override def partition(event: CommittableMessage[K, V]): Int = event.record.partition()

    override def topic(event: CommittableMessage[K, V]): String = event.record.topic()
  }
}

trait Valued[A, V] {
  def value(a: A): V
}

object Valued {
  implicit def consumerRecordMessage[K, V]: Valued[ConsumerRecord[K, V], V] = new Valued[ConsumerRecord[K, V], V] {
    override def value(a: ConsumerRecord[K, V]): V = a.value()
  }

  implicit def committableMessageMessage[K, V]: Valued[CommittableMessage[K, V], V] = new Valued[CommittableMessage[K, V], V] {
    override def value(a: CommittableMessage[K, V]): V = a.record.value()
  }
}

object Deduplication {

  type SourceSequences = immutable.Seq[SourceSequenceNr]
  type SourceSequenceRegistry = Map[String, Long]

  import scala.concurrent.ExecutionContext.Implicits.global

  def plain[K, V](maxPartitions: Int, f: TopicPartition => Future[SourceSequences])(implicit src: Sourced[V], seq: Sequenced[V]): Flow[(TopicPartition, Source[ConsumerRecord[K, V], NotUsed]), ConsumerRecord[K, V], NotUsed] =
    flow[K, V, ConsumerRecord[K, V]](maxPartitions, f)

  def committable[K, V](maxPartitions: Int, f: TopicPartition => Future[SourceSequences])(implicit src: Sourced[V], seq: Sequenced[V]): Flow[(TopicPartition, Source[CommittableMessage[K, V], NotUsed]), CommittableMessage[K, V], NotUsed] =
    flow[K, V, CommittableMessage[K, V]](maxPartitions, f)

  def flow[K, V, M](maxPartitions: Int, f: TopicPartition => Future[SourceSequences])(implicit valued: Valued[M, V], src: Sourced[V], seq: Sequenced[V]): Flow[(TopicPartition, Source[M, NotUsed]), M, NotUsed] =
    Flow[(TopicPartition, Source[M, NotUsed])]
      .mapAsync(1) { case (tp, source) =>
        f(tp).map(s => (tp, source, s.map(x => x.sourceId -> x.sequenceNr).toMap))
      }
      .flatMapMerge(maxPartitions, x => {
        x._2.scan[(SourceSequenceRegistry, Option[M])]((x._3, None)) { case ((s, _), m) =>
          val v = valued.value(m)
          val sourceId = src.sourceId(v)
          val snr = seq.sequenceNr(v)

          if (s.get(sourceId).forall(_ < snr))
            (s + (sourceId -> snr), Some(m))
          else
            (s, None)
        }
          .collect {
            case (_, Some(v)) => v
          }
      })
}

class SequenceStore {

  import scala.concurrent.ExecutionContext.Implicits.global

  def loadSequences(tp: TopicPartition): Future[immutable.Seq[SourceSequenceNr]] =
    Future.successful(immutable.Seq.empty)

  def persist[K, V, M](m: M)(implicit mes: Valued[M, V], part: Partitioned[M], src: Sourced[V], seq: Sequenced[V]): Future[Done] = {
    val v = mes.value(m)

    Future.successful((part.topic(m), part.partition(m), src.sourceId(v), seq.sequenceNr(v)))
      .map(_ => Done)
  }
}
