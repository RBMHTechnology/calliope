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

import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import akka.pattern.after
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.collection.immutable.{Map, Seq}
import scala.concurrent.Future
import scala.concurrent.duration._

object KafkaIndex {
  def inmem[K, V: Aggregate](system: ActorSystem): KafkaInmemIndex[K, V] =
    new KafkaInmemIndex[K, V](system)

  def scanning[K, V: Aggregate](consumerSettings: ConsumerSettings[K, V]): KafkaIndex[K, V] =
    new KafkaScanningIndex[K, V](consumerSettings)
}

trait KafkaIndex[K, V] {
  def aggregateEvents(aggregateId: String, untilOffsets: Map[TopicPartition, Long]): Source[ConsumerRecord[K, V], NotUsed]
}

private class KafkaScanningIndex[K, V: Aggregate](consumerSettings: ConsumerSettings[K, V]) extends KafkaIndex[K, V] {
  def aggregateEvents(aggregateId: String, untilOffsets: Map[TopicPartition, Long]): Source[ConsumerRecord[K, V], NotUsed] =
    KafkaEvents.until(consumerSettings, untilOffsets).filter(cr => implicitly[Aggregate[V]].aggregateId(cr.value) == aggregateId)
}

private object KafkaInmemIndex {
  case class State[K, V: Aggregate](aggregateIndex: Map[String, Seq[ConsumerRecord[K, V]]] =
                                      Map.empty[String, Seq[ConsumerRecord[K, V]]].withDefaultValue(Seq.empty),
                                    committedOffsets: Map[TopicPartition, Long] =
                                      Map.empty.withDefaultValue(-1L)) {

    def append(cr: ConsumerRecord[K, V]): State[K, V] = {
      val aggregateId = implicitly[Aggregate[V]].aggregateId(cr.value)
      copy(
        aggregateIndex.updated(aggregateId, aggregateIndex.getOrElse(aggregateId, Seq.empty) :+ cr),
        committedOffsets.updated(new TopicPartition(cr.topic, cr.partition), cr.offset))
    }
  }
}

class KafkaInmemIndex[K, V: Aggregate](system: ActorSystem) extends KafkaIndex[K, V] {
  import KafkaInmemIndex._

  private implicit val materializer: ActorMaterializer =
    ActorMaterializer()(system)

  private val state: AtomicReference[State[K, V]] =
    new AtomicReference(State())

  override def aggregateEvents(aggregateId: String, untilOffsets: Map[TopicPartition, Long]): Source[ConsumerRecord[K, V], NotUsed] =
    Source.fromFuture(conditionalRead(aggregateId, untilOffsets)).mapConcat(identity)

  def conditionalRead(aggregateId: String, untilOffsets: Map[TopicPartition, Long]): Future[Seq[ConsumerRecord[K, V]]] = withCurrentState { cs =>
    if (offsetsCovered(untilOffsets, cs.committedOffsets)) Future.successful(cs.aggregateIndex(aggregateId).filter { cr =>
      cr.offset < untilOffsets.getOrElse(new TopicPartition(cr.topic, cr.partition), Long.MaxValue)
    }) else after(100.millis, system.scheduler)(conditionalRead(aggregateId, untilOffsets))(system.dispatcher)
  }

  def append(cr: ConsumerRecord[K, V]): Unit =
    state.updateAndGet(_.append(cr))

  def connect(consumerSettings: ConsumerSettings[K, V], topicPartitions: Set[TopicPartition]): Unit =
    KafkaEvents.from(consumerSettings, topicPartitions.map(_ -> 0L).toMap).map(append).runWith(Sink.ignore)

  def shutdown(): Unit =
    materializer.shutdown()

  private def offsetsCovered(untilOffsets: Map[TopicPartition, Long], committedOffsets: Map[TopicPartition, Long]): Boolean =
    untilOffsets.forall { case (tp, offset) => committedOffsets(tp) + 1L >= offset }

  private def withCurrentState[A](body: State[K, V] => A): A =
    body(state.get)
}
