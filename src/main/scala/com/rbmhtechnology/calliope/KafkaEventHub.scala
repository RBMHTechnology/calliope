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
  def aggregateEvents(aggregateId: K): Source[ConsumerRecord[K, V], NotUsed]
  def aggregateEventLog(aggregateId: K): Flow[ProducerRecord[K, V], ConsumerRecord[K, V], NotUsed]
}

private class KafkaEventHubImpl[K, V](eventIndex: KafkaIndex[K, V],
                                      eventSource: Source[ConsumerRecord[K, V], NotUsed],
                                      eventSink: Sink[ProducerRecord[K, V], NotUsed],
                                      endOffsetsSource: Source[Map[TopicPartition, Long], NotUsed])
                                     (implicit aggregate: Aggregate[V, K]) extends KafkaEventHub[K, V] {
  import Processor._

  def aggregateEvents(aggregateId: K): Source[ConsumerRecord[K, V], NotUsed] =
    live(aggregateId).prepend(recovery(aggregateId, eventIndex)).via(dedupR(dedupPredicate))

  def aggregateEventLog(aggregateId: K): Flow[ProducerRecord[K, V], ConsumerRecord[K, V], NotUsed] =
    Flow.fromSinkAndSource(eventSink, aggregateEvents(aggregateId))

  private def processorEvents(aggregateId: K): Source[Control[ConsumerRecord[K, V]], NotUsed] =
    live(aggregateId).map(Delivery(_)).prepend(recovery(aggregateId, eventIndex).map(Delivery(_)).concat(Source.single(Recovered))).via(dedupC(dedupPredicate))

  private def processorLog(aggregateId: K, index: KafkaIndex[K, V]): Flow[ProducerRecord[K, V], Control[ConsumerRecord[K, V]], NotUsed] =
    Flow.fromSinkAndSource(eventSink, processorEvents(aggregateId))

  private def live(aggregateId: K): Source[ConsumerRecord[K, V], NotUsed] =
    eventSource.filter(cr => aggregate.aggregateId(cr.value) == aggregateId)

  private def recovery(aggregateId: K, index: KafkaIndex[K, V]): Source[ConsumerRecord[K, V], NotUsed] =
    endOffsetsSource.flatMapConcat(index.aggregateEvents(aggregateId, _))

  private def dedupR(predicate: ConsumerRecord[K, V] => Boolean): Flow[ConsumerRecord[K, V], ConsumerRecord[K, V], NotUsed] =
    Flow[ConsumerRecord[K, V]].filter(predicate)

  private def dedupC(predicate: ConsumerRecord[K, V] => Boolean): Flow[Control[ConsumerRecord[K, V]], Control[ConsumerRecord[K, V]], NotUsed] =
    Flow[Control[ConsumerRecord[K, V]]].filter {
      case Delivery(cr) => predicate(cr)
      case Recovered    => true
    }

  private def dedupPredicate: ConsumerRecord[K, V] => Boolean = {
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
