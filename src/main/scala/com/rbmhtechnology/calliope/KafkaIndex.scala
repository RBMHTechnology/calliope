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
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.collection.immutable.Map

object KafkaIndex {
  def inmem[K, V: Aggregate]: KafkaIndex[K, V] =
    new KafkaInmemIndex[K, V]

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

private class KafkaInmemIndex[K, V: Aggregate] extends KafkaIndex[K, V] {
  override def aggregateEvents(aggregateId: String, untilOffsets: Map[TopicPartition, Long]): Source[ConsumerRecord[K, V], NotUsed] = ???
}