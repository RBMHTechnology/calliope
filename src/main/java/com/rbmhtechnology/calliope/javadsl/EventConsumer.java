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

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.kafka.ConsumerMessage;
import akka.kafka.ConsumerMessage.CommittableMessage;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.Attributes;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import com.rbmhtechnology.calliope.EventHandler;
import com.rbmhtechnology.calliope.PayloadDeserializationException;
import com.rbmhtechnology.calliope.StreamExecutor;
import com.rbmhtechnology.calliope.StreamExecutorSupervision;
import com.rbmhtechnology.calliope.javadsl.EventConsumer.Settings.BackoffSettings;
import com.rbmhtechnology.calliope.javadsl.EventConsumer.Settings.CommitSettings;
import com.rbmhtechnology.calliope.javadsl.EventConsumer.Settings.DeliverySettings;
import com.typesafe.config.Config;
import io.vavr.collection.List;
import io.vavr.collection.Seq;
import io.vavr.control.Try;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

import static akka.event.Logging.DebugLevel;
import static com.rbmhtechnology.calliope.javadsl.CompletableFutures.retry;
import static com.rbmhtechnology.calliope.StreamExecutorSupervision.Directive.RESTART;
import static com.rbmhtechnology.calliope.StreamExecutorSupervision.Directive.STOP;
import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.API.Match;
import static io.vavr.Predicates.instanceOf;

public class EventConsumer<K, V> {

  private final Seq<String> topics;
  private final Settings<K, V> settings;
  private final Flow<CommittableMessage<K, V>, CommittableMessage<K, V>, NotUsed> transformation;
  private final ActorSystem system;

  private EventConsumer(final Seq<String> topics,
                        final Settings<K, V> settings,
                        final Flow<CommittableMessage<K, V>, CommittableMessage<K, V>, NotUsed> transformation,
                        final ActorSystem system) {
    this.topics = topics;
    this.settings = settings;
    this.transformation = transformation;
    this.system = system;
  }

  public static <K, V> EventConsumer<K, V> committable(final Seq<String> topics,
                                                       final Settings<K, V> settings,
                                                       final ActorSystem system) {
    return new EventConsumer<>(topics, settings, Flow.create(), system);
  }

  public static <K, V> EventConsumer<K, V> committable(final String topic,
                                                       final Settings<K, V> settings,
                                                       final ActorSystem system) {
    return EventConsumer.committable(List.of(topic), settings, system);
  }

  public EventConsumer<K, V> via(final Flow<CommittableMessage<K, V>, CommittableMessage<K, V>, NotUsed> flow) {
    return new EventConsumer<>(topics, settings, transformation.via(flow), system);
  }

  public RunnableEventConsumer to(final EventHandler<V> eventHandler) {
    return new RunnableEventConsumer(createStreamExecutor(eventHandler));
  }

  public CompletionStage<Done> runWith(final EventHandler<V> eventHandler, final Duration timeout) {
    final RunnableEventConsumer runnable = new RunnableEventConsumer(createStreamExecutor(eventHandler));
    return runnable.run(timeout);
  }

  private ActorRef createStreamExecutor(final EventHandler<V> eventHandler) {
    final LoggingAdapter logger = Logging.getLogger(system, EventConsumer.class);

    final Props executorProps = StreamExecutor.props(
      graph(topics, eventHandler, settings, transformation, logger),
      supervision(settings.backoffSettings())
    );
    return system.actorOf(executorProps);
  }

  private RunnableGraph<CompletionStage<Done>> graph(final Seq<String> topic,
                                                     final EventHandler<V> eventHandler,
                                                     final Settings<K, V> settings,
                                                     final Flow<CommittableMessage<K, V>, CommittableMessage<K, V>, NotUsed> flow,
                                                     final LoggingAdapter logger) {
    final DeliverySettings delivery = settings.deliverySettings();
    final CommitSettings commit = settings.commitSettings();

    return Consumer.committableSource(settings.kafkaConsumerSettings(), Subscriptions.topics(topic.toJavaSet()))
      .log("Event consumed", logger).withAttributes(Attributes.logLevels(DebugLevel(), DebugLevel(), DebugLevel()))
      .map(m -> m.record().value()
        .map(v -> new CommittableMessage<>(recordOf(m.record(), v), m.committableOffset()))
        .getOrElseThrow(PayloadDeserializationException::new)
      )
      .via(flow)
      .mapAsync(delivery.parallelism(),
        r -> retry(delivery.retries(), () -> eventHandler.handleEvent(r.record().value(), delivery.timeout()))
          .thenApply(x -> r)
      )
      .log("Event processed", logger).withAttributes(Attributes.logLevels(DebugLevel(), DebugLevel(), DebugLevel()))
      .batch(commit.batchSize(),
        first -> ConsumerMessage.emptyCommittableOffsetBatch().updated(first.committableOffset()),
        (batch, message) -> batch.updated(message.committableOffset())
      )
      .mapAsync(commit.parallelism(), ConsumerMessage.Committable::commitJavadsl)
      .log("Events committed", logger).withAttributes(Attributes.logLevels(DebugLevel(), DebugLevel(), DebugLevel()))
      .toMat(Sink.ignore(), Keep.right());
  }

  private static <K, V> ConsumerRecord<K, V> recordOf(final ConsumerRecord<K, Try<V>> record, final V value) {
    return new ConsumerRecord<>(
      record.topic(),
      record.partition(),
      record.offset(),
      record.timestamp(),
      record.timestampType(),
      record.checksum(),
      record.serializedKeySize(),
      record.serializedValueSize(),
      record.key(),
      value
    );
  }

  private static StreamExecutorSupervision supervision(final BackoffSettings backoffSettings) {
    return StreamExecutorSupervision
      .withBackoff(backoffSettings.minBackoff(), backoffSettings.maxBackoff(), backoffSettings.randomFactor())
      .withDecider(err ->
        Match(err).of(
          Case($(instanceOf(PayloadDeserializationException.class)), x -> STOP),
          Case($(), x -> RESTART)
        )
      );
  }

  public static class Settings<K, V> {

    private final String bootstrapServers;
    private final DeliverySettings deliverySettings;
    private final CommitSettings commitSettings;
    private final BackoffSettings backoffSettings;
    private final Config kafkaConsumerConfig;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<Try<V>> valueDeserializer;

    private Settings(final String bootstrapServers,
                     final DeliverySettings deliverySettings,
                     final CommitSettings commitSettings,
                     final BackoffSettings backoffSettings,
                     final Config kafkaConsumerConfig,
                     final Deserializer<K> keyDeserializer,
                     final Deserializer<Try<V>> valueDeserializer) {
      this.bootstrapServers = bootstrapServers;
      this.deliverySettings = deliverySettings;
      this.commitSettings = commitSettings;
      this.backoffSettings = backoffSettings;
      this.kafkaConsumerConfig = kafkaConsumerConfig;
      this.keyDeserializer = keyDeserializer;
      this.valueDeserializer = valueDeserializer;
    }

    public static <K, V> Settings<K, V> create(final String bootstrapServers,
                                               final DeliverySettings deliverySettings,
                                               final CommitSettings commitSettings,
                                               final BackoffSettings backoffSettings,
                                               final Config kafkaConsumerConfig,
                                               final Deserializer<K> keyDeserializer,
                                               final Deserializer<Try<V>> valueDeserializer) {
      return new Settings<>(
        bootstrapServers,
        deliverySettings,
        commitSettings,
        backoffSettings,
        kafkaConsumerConfig,
        keyDeserializer,
        valueDeserializer
      );
    }

    public static <K, V> Settings<K, V> create(final Config config,
                                               final Deserializer<K> keyDeserializer,
                                               final Deserializer<Try<V>> valueDeserializer) {
      return Settings.create(
        config.getString("bootstrap-servers"),
        DeliverySettings.create(config.getConfig("delivery")),
        CommitSettings.create(config.getConfig("commit")),
        BackoffSettings.create(config.getConfig("failure-backoff")),
        config.getConfig("kafka-consumer"),
        keyDeserializer,
        valueDeserializer
      );
    }

    public static <K, V> Settings<K, V> create(final ActorSystem system,
                                               final Deserializer<K> keyDeserializer,
                                               final Deserializer<Try<V>> valueDeserializer) {
      return Settings.create(
        system.settings().config().getConfig("calliope.event-consumer"),
        keyDeserializer,
        valueDeserializer
      );
    }

    public DeliverySettings deliverySettings() {
      return deliverySettings;
    }

    public CommitSettings commitSettings() {
      return commitSettings;
    }

    public BackoffSettings backoffSettings() {
      return backoffSettings;
    }

    public ConsumerSettings<K, Try<V>> kafkaConsumerSettings() {
      return ConsumerSettings.create(kafkaConsumerConfig, keyDeserializer, valueDeserializer)
        .withBootstrapServers(bootstrapServers);
    }

    public Settings<K, V> withBootstrapServers(final String bootstrapServers) {
      return Settings.create(
        bootstrapServers,
        deliverySettings,
        commitSettings,
        backoffSettings,
        kafkaConsumerConfig,
        keyDeserializer,
        valueDeserializer
      );
    }

    public Settings<K, V> withDeliveryParallelism(final int parallelism) {
      return Settings.create(
        bootstrapServers,
        DeliverySettings.create(parallelism, deliverySettings.retries(), deliverySettings.timeout()),
        commitSettings,
        backoffSettings,
        kafkaConsumerConfig,
        keyDeserializer,
        valueDeserializer
      );
    }

    public Settings<K, V> withDeliveryRetries(final int retries) {
      return Settings.create(
        bootstrapServers,
        DeliverySettings.create(deliverySettings.parallelism(), retries, deliverySettings.timeout()),
        commitSettings,
        backoffSettings,
        kafkaConsumerConfig,
        keyDeserializer,
        valueDeserializer
      );
    }

    public Settings<K, V> withBackoffSettings(final BackoffSettings backoffSettings) {
      return Settings.create(
        bootstrapServers,
        deliverySettings,
        commitSettings,
        backoffSettings,
        kafkaConsumerConfig,
        keyDeserializer,
        valueDeserializer
      );
    }

    public static class DeliverySettings {

      private final int parallelism;
      private final int retries;
      private final Duration timeout;

      private DeliverySettings(final int parallelism, final int retries, final Duration timeout) {
        this.parallelism = parallelism;
        this.retries = retries;
        this.timeout = timeout;
      }

      public static DeliverySettings create(final int parallelism, final int retries, final Duration timeout) {
        return new DeliverySettings(parallelism, retries, timeout);
      }

      public static DeliverySettings create(final Config config) {
        return new DeliverySettings(
          config.getInt("parallelism"),
          config.getInt("retries"),
          config.getDuration("timeout")
        );
      }

      public int parallelism() {
        return parallelism;
      }

      public int retries() {
        return retries;
      }

      public Duration timeout() {
        return timeout;
      }
    }

    public static class CommitSettings {

      private final int parallelism;
      private final int batchSize;

      private CommitSettings(final int parallelism, final int batchSize) {
        this.parallelism = parallelism;
        this.batchSize = batchSize;
      }

      public static CommitSettings create(final int parallelism, final int batchSize) {
        return new CommitSettings(parallelism, batchSize);
      }

      public static CommitSettings create(final Config config) {
        return new CommitSettings(
          config.getInt("parallelism"),
          config.getInt("batch-size")
        );
      }

      public int parallelism() {
        return parallelism;
      }

      public int batchSize() {
        return batchSize;
      }
    }

    public static class BackoffSettings {

      private final Duration minBackoff;
      private final Duration maxBackoff;
      private final double randomFactor;

      private BackoffSettings(final Duration minBackoff, final Duration maxBackoff, final double randomFactor) {
        this.minBackoff = minBackoff;
        this.maxBackoff = maxBackoff;
        this.randomFactor = randomFactor;
      }

      public static BackoffSettings create(final Duration minBackoff, final Duration maxBackoff, final double randomFactor) {
        return new BackoffSettings(minBackoff, maxBackoff, randomFactor);
      }

      public static BackoffSettings create(final Config config) {
        return new BackoffSettings(
          config.getDuration("min-backoff"),
          config.getDuration("max-backoff"),
          config.getDouble("random-factor"));
      }

      public Duration minBackoff() {
        return minBackoff;
      }

      public Duration maxBackoff() {
        return maxBackoff;
      }

      public double randomFactor() {
        return randomFactor;
      }
    }
  }
}
