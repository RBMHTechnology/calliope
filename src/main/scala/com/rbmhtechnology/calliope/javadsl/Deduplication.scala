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

import java.sql.ResultSet
import java.util.concurrent.CompletableFuture
import java.util.function.{Function => JFunction}
import java.util.{Collection => JCollection}

import akka.japi.{Pair => JPair}
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.stream.javadsl.Source
import akka.stream.scaladsl.Flow
import akka.{Done, NotUsed}
import com.rbmhtechnology.calliope._
import com.rbmhtechnology.{calliope => scaladsl}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.compat.java8.FunctionConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext.Implicits.global

object Deduplication {

  type SourceSequences = JCollection[SourceSequenceNr]

  def sourced[A](f: JFunction[A, String]): Sourced[A] =
    new Sourced[A] {
      override def sourceId(a: A): String = f.apply(a)
    }

  def sequenced[A](f: JFunction[A, Long]): Sequenced[A] =
    new Sequenced[A] {
      override def sequenceNr(event: A): Long = f.apply(event)
    }

  def plain[K, V](maxPartitions: Int,
                  f: JFunction[TopicPartition, CompletableFuture[SourceSequences]],
                  src: JFunction[V, String],
                  snr: JFunction[V, Long]): akka.stream.javadsl.Flow[JPair[TopicPartition, Source[ConsumerRecord[K, V], NotUsed]], ConsumerRecord[K, V], NotUsed] = {
    Flow[JPair[TopicPartition, Source[ConsumerRecord[K, V], NotUsed]]]
      .map(p => (p.first, p.second.asScala))
      .via(scaladsl.Deduplication.plain[K, V](maxPartitions, f.asScala.andThen(ft => ft.toScala.map(_.asScala.toVector)))(sourced(src), sequenced(snr)))
      .asJava
  }

  def committable[K, V](maxPartitions: Int,
                        f: JFunction[TopicPartition, CompletableFuture[SourceSequences]],
                        src: JFunction[V, String],
                        snr: JFunction[V, Long]): akka.stream.javadsl.Flow[JPair[TopicPartition, Source[CommittableMessage[K, V], NotUsed]], CommittableMessage[K, V], NotUsed] = {
    Flow[JPair[TopicPartition, Source[CommittableMessage[K, V], NotUsed]]]
      .map(p => (p.first, p.second.asScala))
      .via(scaladsl.Deduplication.committable[K, V](maxPartitions, f.asScala.andThen(ft => ft.toScala.map(_.asScala.toVector)))(sourced(src), sequenced(snr)))
      .asJava
  }
}

class SequenceStore(delegate: scaladsl.SequenceStore) {

  private def partitioned[M](f: JFunction[M, TopicPartition]): Partitioned[M] =
    new Partitioned[M] {
      override def partition(event: M): Int = f.apply(event).partition()

      override def topic(event: M): String = f.apply(event).topic()
    }

  private def sourced[A](f: JFunction[A, String]): Sourced[A] =
    new Sourced[A] {
      override def sourceId(a: A): String = f.apply(a)
    }

  private def sequenced[A](f: JFunction[A, Long]): Sequenced[A] =
    new Sequenced[A] {
      override def sequenceNr(event: A): Long = f.apply(event)
    }

  def loadSequences(tp: TopicPartition): CompletableFuture[JCollection[SourceSequenceNr]] =
    delegate.loadSequences(tp).map(_.asJavaCollection).toJava.toCompletableFuture

  def persist[V, M](m: M,
                    valueF: JFunction[M, V],
                    partitionF: JFunction[M, TopicPartition],
                    sourceF: JFunction[V, String],
                    sequenceF: JFunction[V, Long]): CompletableFuture[Done] =
    delegate.persist(m)(partitioned(partitionF), sourced(valueF.andThen(sourceF)), sequenced(valueF.andThen(sequenceF))).toJava.toCompletableFuture

  def persistConsumerRecord[K, V](m: ConsumerRecord[K, V],
                                  sourceF: JFunction[V, String],
                                  sequenceF: JFunction[V, Long]): CompletableFuture[Done] = {
    implicit val src = sourced(sourceF)
    implicit val seq = sequenced(sequenceF)

    delegate.persist(m).toJava.toCompletableFuture
  }

  def persistCommittableMessage[K, V](m: CommittableMessage[K, V],
                                      sourceF: JFunction[V, String],
                                      sequenceF: JFunction[V, Long]): CompletableFuture[Done] = {
    implicit val src = sourced(sourceF)
    implicit val seq = sequenced(sequenceF)

    delegate.persist(m).toJava.toCompletableFuture
  }

  def bind[V](sourceF: JFunction[V, String],
              sequenceF: JFunction[V, Long]): BoundSequenceStore[V] =
    new BoundSequenceStore[V](this, sourceF, sequenceF)
}

class BoundSequenceStore[V](delegate: SequenceStore,
                            sourceF: JFunction[V, String],
                            sequenceF: JFunction[V, Long]) {

  def loadSequences(tp: TopicPartition): CompletableFuture[JCollection[SourceSequenceNr]] =
    delegate.loadSequences(tp)

  def persist[M](m: M,
                 valueF: JFunction[M, V],
                 partitionF: JFunction[M, TopicPartition]): CompletableFuture[Done] =
    delegate.persist[V, M](m, valueF, partitionF, sourceF, sequenceF)

  def persistConsumerRecord[K](m: ConsumerRecord[K, V]): CompletableFuture[Done] =
    delegate.persistConsumerRecord[K, V](m, sourceF, sequenceF)

  def persistCommittableMessage[K](m: CommittableMessage[K, V]): CompletableFuture[Done] =
    delegate.persistCommittableMessage[K, V](m, sourceF, sequenceF)
}

object SequenceStore {

  def create(storageAdapter: StorageAdapter[SourceSequenceNr]): SequenceStore =
    new SequenceStore(new scaladsl.SequenceStore(storageAdapter.asScala))

  def sequencedEvent[E](storageAdapter: StorageAdapter[SourceSequenceNr]): BoundSequenceStore[SequencedEvent[E]] =
    new SequenceStore(new scaladsl.SequenceStore(storageAdapter.asScala))
      .bind(
        new JFunction[SequencedEvent[E], String] {
          override def apply(t: SequencedEvent[E]): String = t.sourceId
        },
        new JFunction[SequencedEvent[E], Long] {
          override def apply(t: SequencedEvent[E]): Long = t.sequenceNr
        }
      )
}

trait StorageAdapter[A] {

  def query(sql: String, mapper: JFunction[ResultSet, A]): JCollection[A]

  def update(sql: String): Int
}

object StorageAdapter {

  implicit class ExtendedStorageAdapter[A](delegate: StorageAdapter[A]) {
    def asScala: scaladsl.StorageAdapter[A] = new scaladsl.StorageAdapter[A] {

      override def query(sql: String, mapper: (ResultSet) => A): immutable.Seq[A] =
        delegate.query(sql, asJavaFunction(mapper)).asScala.toVector

      override def update(sql: String): Int =
        delegate.update(sql)
    }
  }

}


