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

package com.rbmhtechnology.calliope.javadsl;

import akka.NotUsed;
import akka.kafka.ConsumerMessage.CommittableMessage;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import com.rbmhtechnology.calliope.Aggregate;
import com.rbmhtechnology.calliope.Partitioned;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.function.Function;

public final class DeduplicationBatch {

  private DeduplicationBatch() {
  }

  public static <A> Flow<A, A, NotUsed> flow(final long batchSize, final Aggregate<A> aggregate, final Partitioned<A> partitioned) {
    return Flow.<A>create()
      .batch(batchSize, m -> DeduplicationMap.of(aggregate, partitioned, m), DeduplicationMap::append)
      .flatMapConcat(Source::from);
  }

  public static <K, V> Flow<ConsumerRecord<K, V>, ConsumerRecord<K, V>, NotUsed> plainFlow(final long batchSize,
                                                                                           final ConsumerRecordKeySelector<K, V> selector) {
    return DeduplicationBatch.flow(batchSize, selector::apply, partitionedConsumerRecord());
  }

  public static <K, V> Flow<CommittableMessage<K, V>, CommittableMessage<K, V>, NotUsed> committableFlow(final long batchSize,
                                                                                                         final CommittableMessageKeySelector<K, V> selector) {
    return DeduplicationBatch.flow(batchSize, selector::apply, partitionedCommittableMessage());
  }

  public interface ConsumerRecordKeySelector<K, V> extends Function<ConsumerRecord<K, V>, String> {
  }

  public interface CommittableMessageKeySelector<K, V> extends Function<CommittableMessage<K, V>, String> {
  }

  private static <K, V> Partitioned<ConsumerRecord<K, V>> partitionedConsumerRecord() {
    return record -> new TopicPartition(record.topic(), record.partition());
  }

  private static <K, V> Partitioned<CommittableMessage<K, V>> partitionedCommittableMessage() {
    return message -> new TopicPartition(message.record().topic(), message.record().partition());
  }
}
