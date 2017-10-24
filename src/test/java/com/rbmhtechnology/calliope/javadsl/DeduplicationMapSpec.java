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

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.scalatest.junit.JUnitSuite;

import java.util.Objects;

import static io.vavr.control.Option.none;
import static io.vavr.control.Option.some;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class DeduplicationMapSpec extends JUnitSuite {

  private static final String TOPIC_1 = "topic1";
  private static final String TOPIC_2 = "topic2";

  @Test
  public void append_when_batchIsEmpty_must_appendAggregate() {
    final DeduplicationMap<Entry> batch = deduplicationBatch()
      .append(entry(TOPIC_1, 0, "agg1", 0));

    assertThat(batch.size(), is(1));
    assertThat(batch, contains(entry(TOPIC_1, 0, "agg1", 0)));
  }

  @Test
  public void append_when_batchContainsAggregateInSamePartition_must_discardEntry() {
    final DeduplicationMap<Entry> batch = deduplicationBatch()
      .append(entry(TOPIC_1, 0, "agg1", 0))
      .append(entry(TOPIC_1, 0, "agg1", 1));

    assertThat(batch.size(), is(1));
    assertThat(batch, contains(entry(TOPIC_1, 0, "agg1", 0)));
  }

  @Test
  public void append_when_batchDoesNotContainAggregate_must_appendEntry() {
    final DeduplicationMap<Entry> batch = deduplicationBatch()
      .append(entry(TOPIC_1, 0, "agg1", 0))
      .append(entry(TOPIC_1, 0, "agg2", 1));

    assertThat(batch.size(), is(2));
    assertThat(batch, contains(
      entry(TOPIC_1, 0, "agg1", 0),
      entry(TOPIC_1, 0, "agg2", 1)
    ));
  }

  @Test
  public void append_when_batchDoesNotContainAggregateInSamePartition_must_appendEntry() {
    final DeduplicationMap<Entry> batch = deduplicationBatch()
      .append(entry(TOPIC_1, 0, "agg1", 0))
      .append(entry(TOPIC_1, 1, "agg1", 1));

    assertThat(batch.size(), is(2));
    assertThat(batch, contains(
        entry(TOPIC_1, 0, "agg1", 0),
        entry(TOPIC_1, 1, "agg1", 1)
    ));
  }

  @Test
  public void append_when_batchDoesNotContainAggregateInSameTopic_must_appendEntry() {
    final DeduplicationMap<Entry> batch = deduplicationBatch()
      .append(entry(TOPIC_1, 0, "agg1", 0))
      .append(entry(TOPIC_2, 0, "agg1", 1));

    assertThat(batch.size(), is(2));
    assertThat(batch, contains(
      entry(TOPIC_1, 0, "agg1", 0),
      entry(TOPIC_2, 0, "agg1", 1)
    ));
  }

  @Test
  public void append_when_iteratedOverSinglePartition_must_keepStableOrderPerPartition() {
    final DeduplicationMap<Entry> batch = deduplicationBatch()
      .append(entry(TOPIC_1, 0, "agg1", 0))
      .append(entry(TOPIC_1, 0, "agg1", 1))
      .append(entry(TOPIC_1, 0, "agg2", 2))
      .append(entry(TOPIC_1, 0, "agg2", 3))
      .append(entry(TOPIC_1, 0, "agg3", 4))
      .append(entry(TOPIC_1, 0, "agg3", 5));

    assertThat(batch.size(), is(3));
    assertThat(batch, contains(
      entry(TOPIC_1, 0, "agg1", 0),
      entry(TOPIC_1, 0, "agg2", 2),
      entry(TOPIC_1, 0, "agg3", 4)
    ));
  }

  @Test
  public void append_when_iteratedOverMultiplePartitions_must_keepStableOrderPerPartition() {
    final DeduplicationMap<Entry> batch = deduplicationBatch()
      .append(entry(TOPIC_1, 0, "agg1", 0))
      .append(entry(TOPIC_1, 0, "agg1", 1))
      .append(entry(TOPIC_1, 0, "agg2", 2))
      .append(entry(TOPIC_1, 0, "agg2", 3))
      .append(entry(TOPIC_1, 1, "agg3", 4))
      .append(entry(TOPIC_1, 1, "agg3", 5))
      .append(entry(TOPIC_1, 1, "agg4", 6))
      .append(entry(TOPIC_1, 1, "agg4", 7));

    assertThat(batch.size(), is(4));
    assertThat(batch, contains(
      entry(TOPIC_1, 0, "agg1", 0),
      entry(TOPIC_1, 0, "agg2", 2),
      entry(TOPIC_1, 1, "agg3", 4),
      entry(TOPIC_1, 1, "agg4", 6)
    ));
  }

  @Test
  public void append_when_iteratedOverMultipleTopicPartitions_must_keepStableOrderPerPartition() {
    final DeduplicationMap<Entry> batch = deduplicationBatch()
      .append(entry(TOPIC_1, 0, "agg1", 0))
      .append(entry(TOPIC_1, 0, "agg1", 1))
      .append(entry(TOPIC_1, 0, "agg2", 2))
      .append(entry(TOPIC_1, 0, "agg2", 3))
      .append(entry(TOPIC_2, 0, "agg3", 4))
      .append(entry(TOPIC_2, 0, "agg3", 5))
      .append(entry(TOPIC_2, 0, "agg4", 6))
      .append(entry(TOPIC_2, 0, "agg4", 7));

    assertThat(batch.size(), is(4));
    assertThat(batch, contains(
      entry(TOPIC_1, 0, "agg1", 0),
      entry(TOPIC_1, 0, "agg2", 2),
      entry(TOPIC_2, 0, "agg3", 4),
      entry(TOPIC_2, 0, "agg4", 6)
    ));
  }

  @Test
  public void get_when_entryPresent_must_returnSomeEntry() {
    final DeduplicationMap<Entry> batch = deduplicationBatch()
      .append(entry(TOPIC_1, 0, "agg1", 0));

    assertThat(batch.get(TOPIC_1, 0, "agg1"), is(some(entry(TOPIC_1, 0, "agg1", 0))));
  }

  @Test
  public void get_when_entryNotPresent_must_returnNone() {
    final DeduplicationMap<Entry> batch = deduplicationBatch()
      .append(entry(TOPIC_1, 0, "agg1", 0));

    assertThat(batch.get(TOPIC_1, 0, "i_do_not_exist"), is(none()));
  }

  private DeduplicationMap<Entry> deduplicationBatch() {
    return DeduplicationMap.empty(Entry::aggregateId, Entry::topicPartition);
  }

  private Entry entry(final String topic, final int partition, final String aggregateId, int entryCount) {
    return Entry.create(topic, partition, aggregateId, aggregateId + "_" + TOPIC_1 + "_" + partition + "payload-" + entryCount);
  }

  private static class Entry {
    private final TopicPartition topicPartition;
    private final String aggregateId;
    private final String payload;

    private Entry(final TopicPartition topicPartition, final String aggregateId, final String payload) {
      this.topicPartition = topicPartition;
      this.aggregateId = aggregateId;
      this.payload = payload;
    }

    public static Entry create(final String topic, final int partition, final String aggregateId, final String payload) {
      return new Entry(new TopicPartition(topic, partition), aggregateId, payload);
    }

    public TopicPartition topicPartition() {
      return topicPartition;
    }

    public String aggregateId() {
      return aggregateId;
    }

    public String payload() {
      return payload;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      final Entry entry = (Entry) o;
      return Objects.equals(topicPartition, entry.topicPartition) &&
        Objects.equals(aggregateId, entry.aggregateId) &&
        Objects.equals(payload, entry.payload);
    }

    @Override
    public int hashCode() {
      return Objects.hash(topicPartition, aggregateId, payload);
    }

    @Override
    public String toString() {
      return "Entry{" +
        "topicPartition=" + topicPartition +
        ", aggregateId='" + aggregateId + '\'' +
        ", payload='" + payload + '\'' +
        '}';
    }
  }
}
