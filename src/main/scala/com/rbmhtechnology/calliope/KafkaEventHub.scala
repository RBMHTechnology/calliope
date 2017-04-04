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
import akka.stream.scaladsl.{BidiFlow, Flow, Sink, Source}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition

import scala.collection.immutable.Map

trait KafkaEventHub[K, V] {
  def topic: String
  def index: KafkaIndex[K, V]

  def events: Source[ConsumerRecord[K, V], NotUsed]
  def aggregateEvents(aggregateId: K): Source[ConsumerRecord[K, V], NotUsed]
  def aggregateEventLog(aggregateId: K): Flow[ProducerRecord[K, V], ConsumerRecord[K, V], NotUsed]

  def requestProcessorN[ID, REQ, RES](maxProcessors: Int, logic: K => ProcessorLogic[V, REQ, RES])
                                     (implicit event: Event[V, ID], aggregate: Aggregate[REQ, K]): Flow[REQ, RES, NotUsed]
  def requestProcessor1[ID, REQ, RES](aggregateId: K, logic: ProcessorLogic[V, REQ, RES])
                                     (implicit event: Event[V, ID]): Flow[REQ, RES, NotUsed]

}

private class KafkaEventHubImpl[K, V](override val topic: String,
                                      override val index: KafkaIndex[K, V],
                                      eventSource: Source[ConsumerRecord[K, V], NotUsed],
                                      eventSink: Sink[ProducerRecord[K, V], NotUsed],
                                      endOffsetsSource: Source[Map[TopicPartition, Long], NotUsed])
                                     (implicit aggregate: Aggregate[V, K]) extends KafkaEventHub[K, V] {
  import Processor._

  override def events: Source[ConsumerRecord[K, V], NotUsed] =
    eventSource

  override def aggregateEvents(aggregateId: K): Source[ConsumerRecord[K, V], NotUsed] =
    live(aggregateId).prepend(recovery(aggregateId, index)).via(dedupR(dedupPredicate))

  override def aggregateEventLog(aggregateId: K): Flow[ProducerRecord[K, V], ConsumerRecord[K, V], NotUsed] =
    Flow.fromSinkAndSource(eventSink, aggregateEvents(aggregateId))

  override def requestProcessorN[ID, REQ, RES](maxProcessors: Int, logic: K => ProcessorLogic[V, REQ, RES])(implicit event: Event[V, ID], aggregate: Aggregate[REQ, K]): Flow[REQ, RES, NotUsed] =
    Sequenced[REQ, RES].join(Flows.groupBy[Sequenced[REQ], Sequenced[RES], K](maxProcessors, sequenced => aggregate.aggregateId(sequenced.message), aggregateId => requestProcessor(aggregateId, logic(aggregateId))))

  override def requestProcessor1[ID, REQ, RES](aggregateId: K, logic: ProcessorLogic[V, REQ, RES])(implicit event: Event[V, ID]): Flow[REQ, RES, NotUsed] =
    requestProcessor(aggregateId, BidiFlow.fromGraph(Processor[ID, V, REQ, RES](logic)))

  private def requestProcessor[ID, REQ, RES](aggregateId: K, logic: ProcessorLogic[V, REQ, RES])(implicit event: Event[V, ID]): Flow[Sequenced[REQ], Sequenced[RES], NotUsed] =
    requestProcessor(aggregateId, BidiFlow.fromGraph(new Processor[ID, V, REQ, RES](logic)))

  private def requestProcessor[REQ, RES](aggregateId: K, processor: BidiFlow[REQ, V, Recovery[V], RES, NotUsed]): Flow[REQ, RES, NotUsed] =
    BidiFlow.fromGraph(processor).atop(processorLogAdapter).join(processorLog(aggregateId, index))

  private def processorEvents(aggregateId: K): Source[Recovery[ConsumerRecord[K, V]], NotUsed] =
    live(aggregateId).map(Received(_)).prepend(recovery(aggregateId, index).map(Received(_)).concat(Source.single(Recovered))).via(dedupC(dedupPredicate))

  private def processorLog(aggregateId: K, index: KafkaIndex[K, V]): Flow[ProducerRecord[K, V], Recovery[ConsumerRecord[K, V]], NotUsed] =
    Flow.fromSinkAndSource(eventSink, processorEvents(aggregateId))

  private def processorLogAdapter: BidiFlow[V, ProducerRecord[K, V], Recovery[ConsumerRecord[K, V]], Recovery[V], NotUsed] =
    BidiFlow.fromFunctions(v => new ProducerRecord[K, V](topic, aggregate.aggregateId(v), v), {
      case Received(cr) => Received(cr.value)
      case Recovered => Recovered
    })

  private def live(aggregateId: K): Source[ConsumerRecord[K, V], NotUsed] =
    eventSource.filter(cr => aggregate.aggregateId(cr.value) == aggregateId)

  private def recovery(aggregateId: K, index: KafkaIndex[K, V]): Source[ConsumerRecord[K, V], NotUsed] =
    endOffsetsSource.flatMapConcat(index.aggregateEvents(aggregateId, _))

  private def dedupR(predicate: ConsumerRecord[K, V] => Boolean): Flow[ConsumerRecord[K, V], ConsumerRecord[K, V], NotUsed] =
    Flow[ConsumerRecord[K, V]].filter(predicate)

  private def dedupC(predicate: ConsumerRecord[K, V] => Boolean): Flow[Recovery[ConsumerRecord[K, V]], Recovery[ConsumerRecord[K, V]], NotUsed] =
    Flow[Recovery[ConsumerRecord[K, V]]].filter {
      case Received(cr) => predicate(cr)
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
