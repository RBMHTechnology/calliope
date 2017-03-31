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
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition

import scala.collection.immutable.Map

trait KafkaEventHub[K, V] {
  def aggregateEvents(aggregateId: K, index: KafkaIndex[K, V]): Source[ConsumerRecord[K, V], NotUsed]

  def aggregateEventLog(aggregateId: K, index: KafkaIndex[K, V], sink: Sink[ProducerRecord[K, V], _]): Flow[ProducerRecord[K, V], ConsumerRecord[K, V], NotUsed] =
    Flow.fromSinkAndSource(sink, aggregateEvents(aggregateId, index))
}

private class KafkaEventHubImpl[K, V](eventSource: Source[ConsumerRecord[K, V], NotUsed],
                                      eventSink: Sink[ProducerRecord[K, V], NotUsed],
                                      endOffsetsSource: Source[Map[TopicPartition, Long], NotUsed])
                                     (implicit aggregate: Aggregate[V, K]) extends KafkaEventHub[K, V] {
  def aggregateEvents(aggregateId: K, index: KafkaIndex[K, V]): Source[ConsumerRecord[K, V], NotUsed] =
    live(aggregateId).prepend(recovery(aggregateId, index)).via(dedup)

  def aggregateLog(aggregateId: K, index: KafkaIndex[K, V]): Flow[ProducerRecord[K, V], ConsumerRecord[K, V], NotUsed] =
    Flow.fromSinkAndSource(eventSink, aggregateEvents(aggregateId, index))

  private def live(aggregateId: K): Source[ConsumerRecord[K, V], NotUsed] =
    eventSource.filter(cr => aggregate.aggregateId(cr.value) == aggregateId)

  private def recovery(aggregateId: K, index: KafkaIndex[K, V]): Source[ConsumerRecord[K, V], NotUsed] =
    endOffsetsSource.flatMapConcat(index.aggregateEvents(aggregateId, _))

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
