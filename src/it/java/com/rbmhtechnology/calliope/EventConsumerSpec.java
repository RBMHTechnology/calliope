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

package com.rbmhtechnology.calliope;

import akka.Done;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Status;
import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.Producer;
import akka.stream.Materializer;
import akka.stream.javadsl.Keep;
import akka.stream.testkit.TestPublisher;
import akka.stream.testkit.javadsl.TestSource;
import akka.testkit.JavaTestKit;
import akka.testkit.TestProbe;
import com.rbmhtechnology.calliope.javadsl.DeduplicationBatch;
import com.rbmhtechnology.calliope.javadsl.EventConsumer;
import com.rbmhtechnology.calliope.util.AkkaStreamsRule;
import info.batey.kafka.unit.KafkaUnitRule;
import io.vavr.collection.Seq;
import io.vavr.control.Try;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scalatest.junit.JUnitSuite;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import static akka.pattern.Patterns.ask;
import static com.rbmhtechnology.calliope.TestProbes.expectAnyMsgOf;
import static com.rbmhtechnology.calliope.TestProbes.receiveMsgClassWithin;
import static com.rbmhtechnology.calliope.javadsl.Durations.toFiniteDuration;
import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.API.Match;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static scala.compat.java8.FutureConverters.toJava;

public class EventConsumerSpec extends JUnitSuite {

  private static final String TEST_TOPIC = "test-topic";

  @ClassRule
  public static AkkaStreamsRule akka = new AkkaStreamsRule("application.conf");

  @Rule
  public KafkaUnitRule kafkaUnit = new KafkaUnitRule();

  private ProducerSettings<String, BaseEvent> producerSettings;
  private EventConsumer.Settings<String, BaseEvent> eventConsumerSettings;

  @Before
  public void setUp() throws Exception {
    producerSettings = producerSettings(kafkaUnit.getKafkaUnit().getKafkaConnect(), akka.system(), new EventSerializer());

    eventConsumerSettings = EventConsumer.Settings.create(akka.system(), new StringDeserializer(), new EventDeserializer())
      .withBootstrapServers(kafkaUnit.getKafkaUnit().getKafkaConnect());
  }

  @Test
  public void eventConsumer_when_eventsAvailable_must_publishToEventHandler() {
    new JavaTestKit(akka.system()) {{
      final TestProbe handler = new TestProbe(getSystem());
      final TestPublisher.Probe<BaseEvent> producer = eventProducer(TEST_TOPIC, producerSettings, getSystem(), akka.materializer());

      runEventConsumer(TEST_TOPIC, eventConsumerSettings, handler);

      producer.sendNext(event("event-1"));
      producer.sendNext(event("event-2"));
      producer.sendNext(event("event-3"));

      handler.expectMsg(event("event-1"));
      handler.expectMsg(event("event-2"));
      handler.expectMsg(event("event-3"));
    }};
  }

  @Test
  public void eventConsumer_when_eventsAvailable_must_publishEventsWithGivenParallelism() throws InterruptedException {
    new JavaTestKit(akka.system()) {{
      final TestProbe handler = new TestProbe(getSystem());
      final TestPublisher.Probe<BaseEvent> producer = eventProducer(TEST_TOPIC, producerSettings, getSystem(), akka.materializer());
      final EventConsumer.Settings<String, BaseEvent> settings = eventConsumerSettings.withDeliveryParallelism(1);

      runEventConsumer(TEST_TOPIC, settings, handler);

      producer.sendNext(event("event-1"));
      producer.sendNext(event("event-2"));

      final Seq<BaseEvent> events = receiveMsgClassWithin(handler, BaseEvent.class, Duration.ofSeconds(5));

      assertThat(events.size(), is(1));
      assertThat(events, contains(event("event-1")));
      handler.reply(done());

      handler.expectMsg(event("event-2"));
      handler.reply(done());
    }};
  }

  @Test
  public void eventConsumer_when_consumerStreamFails_must_restartConsumptionOfUncommittedEvents() throws InterruptedException {
    new JavaTestKit(akka.system()) {{
      final TestProbe handler = new TestProbe(getSystem());
      final TestPublisher.Probe<BaseEvent> producer = eventProducer(TEST_TOPIC, producerSettings, getSystem(), akka.materializer());
      final EventConsumer.Settings<String, BaseEvent> settings = eventConsumerSettings.withDeliveryRetries(0);

      runEventConsumer(TEST_TOPIC, settings, handler);

      producer.sendNext(event("event-1"));
      producer.sendNext(event("event-2"));

      handler.expectMsg(event("event-1"));
      handler.expectMsg(event("event-2"));
      handler.reply(new Status.Failure(new RuntimeException("err")));

      handler.expectMsg(event("event-1"));
      handler.reply(done());
      handler.expectMsg(event("event-2"));
      handler.reply(done());
    }};
  }

  @Test
  public void eventConsumer_when_eventDeliveryFails_must_retryFailedEvent() throws InterruptedException {
    new JavaTestKit(akka.system()) {{
      final TestProbe handler = new TestProbe(getSystem());
      final TestPublisher.Probe<BaseEvent> producer = eventProducer(TEST_TOPIC, producerSettings, getSystem(), akka.materializer());
      final EventConsumer.Settings<String, BaseEvent> settings = eventConsumerSettings.withDeliveryRetries(1);

      runEventConsumer(TEST_TOPIC, settings, handler);

      producer.sendNext(event("event-1"));
      producer.sendNext(event("event-2"));

      handler.expectMsg(event("event-1"));
      handler.reply(new Status.Failure(new RuntimeException("err")));

      expectAnyMsgOf(handler, event("event-1"), event("event-2"));
      handler.reply(done());

      expectAnyMsgOf(handler, event("event-1"), event("event-2"));
      handler.reply(done());

      handler.expectNoMsg(toFiniteDuration(Duration.ofSeconds(1)));
    }};
  }

  @Test
  public void eventConsumer_when_eventDeliveryFailsPermanently_must_restartConsumptionOfUncommittedEvents() throws InterruptedException {
    new JavaTestKit(akka.system()) {{
      final TestProbe handler = new TestProbe(getSystem());
      final TestPublisher.Probe<BaseEvent> producer = eventProducer(TEST_TOPIC, producerSettings, getSystem(), akka.materializer());
      final EventConsumer.Settings<String, BaseEvent> settings = eventConsumerSettings.withDeliveryRetries(1);

      runEventConsumer(TEST_TOPIC, settings, handler);

      producer.sendNext(event("event-1"));
      producer.sendNext(event("event-2"));

      handler.expectMsg(event("event-1"));
      handler.expectMsg(event("event-2"));
      handler.reply(new Status.Failure(new RuntimeException("err")));

      handler.expectMsg(event("event-2"));
      handler.reply(new Status.Failure(new RuntimeException("err")));

      handler.expectMsg(event("event-1"));
      handler.reply(done());
      handler.expectMsg(event("event-2"));
      handler.reply(done());
    }};
  }

  @Test
  public void eventConsumer_when_deduplicationEnabled_must_dropMessagesWithSameAggregateId() throws InterruptedException {
    new JavaTestKit(akka.system()) {{
      final TestProbe handler = new TestProbe(getSystem());
      final TestPublisher.Probe<BaseEvent> producer = eventProducer(TEST_TOPIC, producerSettings, getSystem(), akka.materializer());
      final EventConsumer.Settings<String, BaseEvent> settings = eventConsumerSettings
        .withDeliveryParallelism(1);

      eventConsumer(TEST_TOPIC, settings)
        .via(DeduplicationBatch.committableFlow(100, m -> m.record().key()))
        .to(actorEventHandler(handler.ref()))
        .run(akka.timeout());

      producer.sendNext(event("event-1"));
      producer.sendNext(event("event-1"));
      producer.sendNext(event("event-2"));
      producer.sendNext(event("event-2"));
      producer.sendNext(event("event-1"));
      producer.sendNext(event("event-3"));
      producer.sendNext(event("event-2"));

      // first entry is not handled by the deduplication-batch as it is placed in the initial buffer of Akka streams
      handler.expectMsg(event("event-1"));
      handler.reply(done());
      handler.expectMsg(event("event-1"));
      handler.reply(done());
      handler.expectMsg(event("event-2"));
      handler.reply(done());
      handler.expectMsg(event("event-3"));
      handler.reply(done());

      handler.expectNoMsg(toFiniteDuration(Duration.ofSeconds(1)));
    }};
  }

  @Test
  public void eventConsumer_when_deduplicationDisabled_must_consumeMessagesWithSameAggregateId() throws InterruptedException {
    new JavaTestKit(akka.system()) {{
      final TestProbe handler = new TestProbe(getSystem());
      final TestPublisher.Probe<BaseEvent> producer = eventProducer(TEST_TOPIC, producerSettings, getSystem(), akka.materializer());

      final EventHandler<BaseEvent> eventHandler = actorEventHandler(handler.ref());

      eventConsumer(TEST_TOPIC, eventConsumerSettings)
        .via(DeduplicationBatch.committableFlow(1, m -> m.record().key()))
        .to(eventHandler)
        .run(akka.timeout());

      producer.sendNext(event("event-1"));
      producer.sendNext(event("event-1"));
      producer.sendNext(event("event-2"));
      producer.sendNext(event("event-2"));
      producer.sendNext(event("event-1"));
      producer.sendNext(event("event-3"));
      producer.sendNext(event("event-2"));

      handler.expectMsg(event("event-1"));
      handler.reply(done());
      handler.expectMsg(event("event-1"));
      handler.reply(done());
      handler.expectMsg(event("event-2"));
      handler.reply(done());
      handler.expectMsg(event("event-2"));
      handler.reply(done());
      handler.expectMsg(event("event-1"));
      handler.reply(done());
      handler.expectMsg(event("event-3"));
      handler.reply(done());
      handler.expectMsg(event("event-2"));
      handler.reply(done());

      handler.expectNoMsg(toFiniteDuration(Duration.ofSeconds(1)));
    }};
  }

  @Test
  public void eventConsumer_when_deserializationFails_must_terminateConsumer() throws InterruptedException {
    new JavaTestKit(akka.system()) {{
      final TestProbe handler = new TestProbe(getSystem());
      final TestPublisher.Probe<BaseEvent> producer = eventProducer(TEST_TOPIC, producerSettings, getSystem(), akka.materializer());

      runEventConsumer(TEST_TOPIC, eventConsumerSettings, handler);

      producer.sendNext(new NonDeserializableEvent("event-1"));

      handler.expectNoMsg(toFiniteDuration(Duration.ofSeconds(2)));
    }};
  }

  private ProducerSettings<String, BaseEvent> producerSettings(final String bootstrapServers,
                                                               final ActorSystem system,
                                                               final Serializer<BaseEvent> serializer) {
    return ProducerSettings.create(system, new StringSerializer(), serializer)
      .withBootstrapServers(bootstrapServers);
  }

  private TestPublisher.Probe<BaseEvent> eventProducer(final String topic,
                                                       final ProducerSettings<String, BaseEvent> producerSettings,
                                                       final ActorSystem system,
                                                       final Materializer materializer) {
    return TestSource.<BaseEvent>probe(system)
      .zipWithIndex()
      .map(p -> new ProducerRecord<>(topic, 0, p.first().id(), p.first()))
      .toMat(Producer.plainSink(producerSettings), Keep.left())
      .run(materializer);
  }

  private CompletionStage<Done> runEventConsumer(final String topic,
                                                 final EventConsumer.Settings<String, BaseEvent> consumerSettings,
                                                 final TestProbe handler) {
    return eventConsumer(topic, consumerSettings)
      .to(actorEventHandler(handler.ref()))
      .run(akka.timeout());
  }

  private EventHandler<BaseEvent> actorEventHandler(final ActorRef eventReceiver) {
    return (event, timeout) -> toJava(ask(eventReceiver, event, timeout.toMillis()))
      .thenApply(x -> Done.getInstance());
  }

  private EventConsumer<String, BaseEvent> eventConsumer(final String topic,
                                                         final EventConsumer.Settings<String, BaseEvent> consumerSettings) {
    return EventConsumer.committable(topic, consumerSettings, akka.system());
  }

  private Event event(final String id) {
    return new Event(id);
  }

  private Done done() {
    return Done.getInstance();
  }

  public static abstract class BaseEvent {

    private final String id;

    protected BaseEvent(final String id) {
      this.id = id;
    }

    public String id() {
      return id;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      final BaseEvent that = (BaseEvent) o;
      return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id);
    }
  }

  public static class Event extends BaseEvent {

    public Event(final String id) {
      super(id);
    }
  }

  public static class NonDeserializableEvent extends BaseEvent {

    NonDeserializableEvent(final String id) {
      super(id);
    }
  }

  public static class EventSerializer implements Serializer<BaseEvent> {

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
    }

    @Override
    public byte[] serialize(final String topic, final BaseEvent data) {
      return (data.getClass().getName() + "|" + data.id()).getBytes();
    }

    @Override
    public void close() {
    }
  }

  public static class EventDeserializer implements Deserializer<Try<BaseEvent>> {

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
    }

    @Override
    public Try<BaseEvent> deserialize(final String topic, final byte[] data) {
      final String payload = new String(data);
      final String[] parts = payload.split("\\|");

      if (parts.length != 2) {
        return Try.failure(new IllegalArgumentException("Invalid payload for event [" + payload + "] given"));
      }

      final String eventClass = parts[0];
      final String id = parts[1];

      return Match(eventClass).<BaseEvent>option(
        Case($(Event.class.getName()), () -> new Event(id))
      ).toTry(() -> new IllegalArgumentException("No mapping for event [" + eventClass + "] defined"));
    }

    @Override
    public void close() {
    }
  }
}
