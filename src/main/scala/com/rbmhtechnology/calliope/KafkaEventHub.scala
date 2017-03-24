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
import akka.kafka.ConsumerSettings
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition

import scala.collection.immutable.Map

trait KafkaEventHub[K, V] {
  def aggregateEvents(aggregateId: String): Source[ConsumerRecord[K, V], NotUsed]
  def aggregateEventLog(aggregateId: String, sink: Sink[ProducerRecord[K, V], _]): Flow[ProducerRecord[K, V], ConsumerRecord[K, V], NotUsed]
}

private class KafkaEventHubImpl[K, V: Aggregate](consumerSettings: ConsumerSettings[K, V], tracker: KafkaOffsetsTracker, source: Source[ConsumerRecord[K, V], NotUsed]) extends KafkaEventHub[K, V] {
  def aggregateEvents(aggregateId: String): Source[ConsumerRecord[K, V], NotUsed] =
    live(aggregateId).prepend(Source.lazily(() => past(aggregateId, tracker.consumedOffsets))).via(dedup)

  def aggregateEventLog(aggregateId: String, sink: Sink[ProducerRecord[K, V], _]): Flow[ProducerRecord[K, V], ConsumerRecord[K, V], NotUsed] =
    Flow.fromSinkAndSource(sink, aggregateEvents(aggregateId))

  /** Reads past aggregate events from Kafka. Will be later replaced by a source that reads from an aggregate index. */
  private def past(aggregateId: String, untilOffsets: Map[TopicPartition, Long]): Source[ConsumerRecord[K, V], NotUsed] =
    KafkaEvents.until(consumerSettings, untilOffsets).filter(cr => implicitly[Aggregate[V]].aggregateId(cr.value) == aggregateId)

  private def live(aggregateId: String): Source[ConsumerRecord[K, V], NotUsed] =
    source.filter(cr => implicitly[Aggregate[V]].aggregateId(cr.value) == aggregateId)

  private def dedup: Flow[ConsumerRecord[K, V], ConsumerRecord[K, V], NotUsed] =
    Flow[ConsumerRecord[K, V]].filter {
      var offsets: Map[TopicPartition, Long] = Map.empty
      cr => {
        val topicPartition = new TopicPartition(cr.topic, cr.partition)
        val recordedOffset = offsets.getOrElse(topicPartition, -1L)
        if (cr.offset > recordedOffset) {
          offsets = offsets.updated(topicPartition, cr.offset)
          true
        } else false
      }
    }
}
