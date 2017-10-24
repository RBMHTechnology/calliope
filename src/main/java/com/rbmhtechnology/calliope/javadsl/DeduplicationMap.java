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

import com.rbmhtechnology.calliope.Aggregate;
import com.rbmhtechnology.calliope.Partitioned;
import io.vavr.Tuple2;
import io.vavr.collection.LinkedHashMap;
import io.vavr.collection.Map;
import io.vavr.control.Option;
import org.apache.kafka.common.TopicPartition;

import java.util.Iterator;
import java.util.function.Supplier;

class DeduplicationMap<A> implements Iterable<A> {

  private final Map<TopicPartition, PartitionMap<A>> partitions;
  private final Aggregate<A> aggregate;
  private final Partitioned<A> partitioned;

  private DeduplicationMap(final Map<TopicPartition, PartitionMap<A>> partitions,
                           final Aggregate<A> aggregate,
                           final Partitioned<A> partitioned) {
    this.partitions = partitions;
    this.aggregate = aggregate;
    this.partitioned = partitioned;
  }

  static <A> DeduplicationMap<A> empty(final Aggregate<A> aggregate, final Partitioned<A> partitioned) {
    return new DeduplicationMap<>(LinkedHashMap.empty(), aggregate, partitioned);
  }

  static <A> DeduplicationMap<A> of(final Aggregate<A> aggregate, final Partitioned<A> partitioned, final A entry) {
    final TopicPartition topicPartition = partitioned.topicPartition(entry);
    final String aggregateId = aggregate.aggregateId(entry);

    return new DeduplicationMap<>(
      LinkedHashMap.of(topicPartition, PartitionMap.of(aggregateId, entry)),
      aggregate,
      partitioned
    );
  }

  DeduplicationMap<A> append(final A entry) {
    final TopicPartition topicPartition = partitioned.topicPartition(entry);
    final String aggregateId = aggregate.aggregateId(entry);

    return partitions.get(topicPartition)
      .map(pm -> pm.append(aggregateId, entry))
      .map(u -> batchOf(u.updated(), () -> partitions.put(topicPartition, u.result())))
      .getOrElse(() -> batchOf(partitions.put(topicPartition, PartitionMap.of(aggregateId, entry))));
  }

  private DeduplicationMap<A> batchOf(final Map<TopicPartition, PartitionMap<A>> partitions) {
    return new DeduplicationMap<>(partitions, aggregate, partitioned);
  }

  private DeduplicationMap<A> batchOf(final boolean shouldPerformUpdate,
                                      final Supplier<Map<TopicPartition, PartitionMap<A>>> updatedPartitions) {
    return shouldPerformUpdate
      ? batchOf(updatedPartitions.get())
      : this;
  }

  Option<A> get(final String topic, final int partition, final String aggregateId) {
    return get(new TopicPartition(topic, partition), aggregateId);
  }

  Option<A> get(final TopicPartition topicPartition, final String aggregateId) {
    return partitions.get(topicPartition)
      .flatMap(tp -> tp.get(aggregateId));
  }

  int size() {
    return partitions.foldLeft(0, (s, e) -> s + e._2().size());
  }

  @Override
  public Iterator<A> iterator() {
    return partitions.toStream()
      .flatMap(Tuple2::_2)
      .iterator();
  }

  static class PartitionMap<A> implements Iterable<A> {

    private final Map<String, A> entries;

    private PartitionMap(final Map<String, A> entries) {
      this.entries = entries;
    }

    static <A> PartitionMap<A> of(final String id, final A entry) {
      return new PartitionMap<>(LinkedHashMap.of(id, entry));
    }

    UpdateResult<A> append(final String id, final A entry) {
      return entries.containsKey(id)
        ? new UpdateResult<>(false, this)
        : new UpdateResult<>(true, new PartitionMap<>(entries.put(id, entry)));
    }

    Option<A> get(final String id) {
      return entries.get(id);
    }

    int size() {
      return entries.size();
    }

    @Override
    public Iterator<A> iterator() {
      return entries.values().iterator();
    }

    @Override
    public String toString() {
      return "PartitionMap{" +
        "entries=" + entries +
        '}';
    }

    static class UpdateResult<A> {
      private final boolean updated;
      private final PartitionMap<A> result;

      UpdateResult(final boolean updated, final PartitionMap<A> result) {
        this.updated = updated;
        this.result = result;
      }

      boolean updated() {
        return updated;
      }

      PartitionMap<A> result() {
        return result;
      }
    }
  }
}
