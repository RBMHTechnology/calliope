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
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.Materializer
import akka.stream.scaladsl.{BroadcastHub, Keep, Source}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.collection.immutable.Map

object KafkaEvents {
  def hub[K, V: Aggregate](consumerSettings: ConsumerSettings[K, V],
                           fromOffsets: Map[TopicPartition, Long])
                          (implicit materializer: Materializer): KafkaEventHub[K, V] = {
    val (tracker, source) = from(consumerSettings, fromOffsets)
      .viaMat(KafkaOffsetsTracker.flow(fromOffsets))(Keep.right)
      .toMat(BroadcastHub.sink[ConsumerRecord[K, V]])(Keep.both).run()
    new KafkaEventHubImpl[K, V](source, tracker)
  }

  def from[K, V](consumerSettings: ConsumerSettings[K, V],
                 fromOffsets: Map[TopicPartition, Long]): Source[ConsumerRecord[K, V], NotUsed] =
    Consumer.plainSource(consumerSettings, Subscriptions.assignmentWithOffset(fromOffsets)).mapMaterializedValue(_ => NotUsed)

  def to[K, V](consumerSettings: ConsumerSettings[K, V],
               toOffsets: Map[TopicPartition, Long]): Source[ConsumerRecord[K, V], NotUsed] =
    until(consumerSettings, toOffsets.mapValues(_ + 1L))

  def until[K, V](consumerSettings: ConsumerSettings[K, V],
                  untilOffsets: Map[TopicPartition, Long]): Source[ConsumerRecord[K, V], NotUsed] =
    KafkaMetadata.beginOffsets(consumerSettings, untilOffsets.keySet).flatMapConcat { beginOffsets =>
      val filteredOffsets = untilOffsets.filter { case (k, v) => v > beginOffsets.getOrElse(k, 0L) }
      if (filteredOffsets.isEmpty) Source.empty[ConsumerRecord[K, V]]
      else KafkaEvents.from(consumerSettings, beginOffsets).takeWhile(untilPredicate(filteredOffsets), inclusive = true)
    }

  private def untilPredicate[K, V](untilOffsets: Map[TopicPartition, Long]): ConsumerRecord[K, V] => Boolean = {
    var current = untilOffsets.withDefaultValue(Long.MaxValue)
    cr => {
      val tp = new TopicPartition(cr.topic, cr.partition)
      if (cr.offset == current(tp) - 1L) current = current - tp
      current.nonEmpty
    }
  }
}
