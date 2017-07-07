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

import akka.actor.ActorSystem;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import com.rbmhtechnology.calliope.javadsl.BoundSequenceStore;
import com.rbmhtechnology.calliope.javadsl.Deduplication;
import com.rbmhtechnology.calliope.Event;
import com.rbmhtechnology.calliope.javadsl.SequenceStore;
import com.rbmhtechnology.calliope.SequencedEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public class DeduplicationApiSpecification {

  private static final int maxPartitions = Integer.MAX_VALUE;

  public static void specification() {

    final ActorSystem system = ActorSystem.create("spec");
    final ActorMaterializer materializer = ActorMaterializer.create(system);

    final SequenceStore store = SequenceStore.create();
    final BoundSequenceStore<Event> eventStore = store.bind(Event::sourceId, Event::sequenceNr);
    final BoundSequenceStore<SequencedEvent<Event>> seqEventStore = SequenceStore.sequencedEvent();

    final Consumer.Control control =
      Consumer.<String, Event>plainPartitionedSource(null, Subscriptions.topics("topic"))
        .via(Deduplication.plain(maxPartitions, store::loadSequences, Event::sourceId, Event::sequenceNr))
        .map(x -> {
          // business logic
          store.persist(x, ConsumerRecord::value, m -> new TopicPartition(m.topic(), m.partition()), Event::sourceId, Event::sequenceNr);
          store.persistConsumerRecord(x, Event::sourceId, Event::sequenceNr);
          eventStore.persistConsumerRecord(x);
          return x;
        })
        .to(Sink.ignore())
        .run(materializer);

    final Consumer.Control control2 =
      Consumer.<String, Event>committablePartitionedSource(null, Subscriptions.topics("topic"))
        .via(Deduplication.committable(maxPartitions, store::loadSequences, Event::sourceId, Event::sequenceNr))
        .map(x -> {
          // business logic
          store.persistCommittableMessage(x, Event::sourceId, Event::sequenceNr);
          eventStore.persistCommittableMessage(x);
          return x;
        })
        .to(Sink.ignore())
        .run(materializer);
  }
}
