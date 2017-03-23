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
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{Flow, Source}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.collection.immutable.Map

object KafkaEvents {
  trait ConsumedOffsets {
    def get: Map[TopicPartition, Long]
  }

  private class ConsumedOffsetsImpl(ref: AtomicReference[Map[TopicPartition, Long]]) extends ConsumedOffsets {
    def get: Map[TopicPartition, Long] = ref.get
  }

  def consumedOffsets[K, V](initialOffsets: Map[TopicPartition, Long]): Flow[ConsumerRecord[K, V], ConsumerRecord[K, V], ConsumedOffsets] = {
    val currentOffsets = new AtomicReference[Map[TopicPartition, Long]](initialOffsets)
    Flow[ConsumerRecord[K, V]].map {
      cr => currentOffsets.updateAndGet(updateOffsets(_, cr))
      cr
    }.mapMaterializedValue(_ => new ConsumedOffsetsImpl(currentOffsets))
  }

  def until[K, V](consumerSettings: ConsumerSettings[K, V], offsets: Map[TopicPartition, Long]): Source[ConsumerRecord[K, V], NotUsed] =
    KafkaMetadata.beginOffsets(consumerSettings, offsets.keySet).flatMapConcat { beginOffsets =>
      val untilOffsets = offsets.filter {
        case (k, v) => v > beginOffsets.getOrElse(k, 0L)
      }
      if (untilOffsets.isEmpty)
        Source.empty[ConsumerRecord[K, V]]
      else
        Consumer.plainSource(consumerSettings, Subscriptions.assignmentWithOffset(beginOffsets))
          .takeWhile(untilPredicate(untilOffsets), inclusive = true)
    }

  private def untilPredicate[K, V](untilOffsets: Map[TopicPartition, Long]): ConsumerRecord[K, V] => Boolean = {
    var current = untilOffsets.withDefaultValue(Long.MaxValue)
    cr => {
      val tp = new TopicPartition(cr.topic, cr.partition)
      if (cr.offset == current(tp) - 1L) current = current - tp
      current.nonEmpty
    }
  }

  private[calliope] def updateOffsets[K, V](currentOffsets: Map[TopicPartition, Long], cr: ConsumerRecord[K, V]): Map[TopicPartition, Long] =
    currentOffsets.updated(new TopicPartition(cr.topic, cr.partition), cr.offset)
}
