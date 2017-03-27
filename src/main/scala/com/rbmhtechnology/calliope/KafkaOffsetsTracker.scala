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

import akka.stream.scaladsl.Flow
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.collection.immutable.Map

private class KafkaOffsetsTracker(ref: AtomicReference[Map[TopicPartition, Long]]) {
  def consumedOffsets: Map[TopicPartition, Long] = ref.get
}

private object KafkaOffsetsTracker {
  def flow[K, V](initialOffsets: Map[TopicPartition, Long]): Flow[ConsumerRecord[K, V], ConsumerRecord[K, V], KafkaOffsetsTracker] = {
    val currentOffsets = new AtomicReference[Map[TopicPartition, Long]](initialOffsets)
    Flow[ConsumerRecord[K, V]].map {
      cr => currentOffsets.updateAndGet(updateOffsets(_, cr))
      cr
    }.mapMaterializedValue(_ => new KafkaOffsetsTracker(currentOffsets))
  }

  private def updateOffsets[K, V](currentOffsets: Map[TopicPartition, Long], cr: ConsumerRecord[K, V]): Map[TopicPartition, Long] =
    currentOffsets.updated(new TopicPartition(cr.topic, cr.partition), cr.offset)
}

