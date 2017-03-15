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
import akka.kafka.{ConsumerSettings, ProducerSettings}
import akka.stream.scaladsl.Flow
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

object Pipe {

  //
  // TODO: consider using Bijection for encoder/decoder pairs
  //

  def through[K, V, MS](producerSettings: ProducerSettings[K, V],
                        consumerSettings: ConsumerSettings[K, V],
                        messageSource: MS,
                        messageSequenceEncoder: (SequencedProducerRecord[K, V], MS, Long) => (K, V),
                        messageSourceDecoder: ConsumerRecord[K, V] => MS,
                        messageSequenceNrDecoder: ConsumerRecord[K, V] => Long)(
                        progressStore: ProgressStore[MS, TopicPartition],
                        storagePartitions: Set[TopicPartition]): Flow[SequencedProducerRecord[K, V], ResequencedConsumerRecord[K, V, MS] with Committable, NotUsed] = {
    val sequence = Sequence.from(
      consumerSettings,
      messageSourceDecoder,
      messageSequenceNrDecoder)(progressStore, storagePartitions)

    val sink = Produce.to(
      producerSettings,
      messageSource,
      messageSequenceEncoder,
      sequence,
      storagePartitions)

    val source = Resequence.from(
      consumerSettings,
      messageSourceDecoder,
      messageSequenceNrDecoder)(progressStore, storagePartitions)

    Flow.fromSinkAndSource(sink, source)
  }

  def through[K, V, MS](producerSettings: ProducerSettings[K, V],
                        consumerSettings: ConsumerSettings[K, V],
                        messageSource: MS,
                        messageSequenceEncoder: (SequencedProducerRecord[K, V], MS, Long) => (K, V),
                        messageSourceDecoder: ConsumerRecord[K, V] => MS,
                        messageSequenceNrDecoder: ConsumerRecord[K, V] => Long,
                        committedMessageSequenceNrs: Map[MS, Long],
                        committedStorageSequenceNrs: Map[TopicPartition, Long]): Flow[SequencedProducerRecord[K, V], ResequencedConsumerRecord[K, V, MS], NotUsed] = {
    val sequence = Sequence.from(
      consumerSettings,
      messageSourceDecoder,
      messageSequenceNrDecoder,
      committedMessageSequenceNrs,
      committedStorageSequenceNrs)

    val sink = Produce.to(
      producerSettings,
      messageSource,
      messageSequenceEncoder,
      sequence,
      committedStorageSequenceNrs.keySet)

    val source = Resequence.from(
      consumerSettings,
      messageSourceDecoder,
      messageSequenceNrDecoder,
      committedMessageSequenceNrs,
      committedStorageSequenceNrs)

    Flow.fromSinkAndSource(sink, source)
  }
}
