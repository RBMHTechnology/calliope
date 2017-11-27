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

import java.time.Duration
import java.util.concurrent.CompletionStage
import java.util.concurrent.TimeUnit.NANOSECONDS
import java.util.{UUID, Map => JMap}

import akka.Done
import akka.actor.ActorRef
import akka.actor.Status.Failure
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.ProducerSettings
import akka.kafka.javadsl.Producer
import akka.pattern.ask
import akka.stream.scaladsl.Keep
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit.TestProbe
import akka.util.Timeout
import com.rbmhtechnology.calliope.javadsl.DeduplicationBatch.CommittableMessageKeySelector
import com.rbmhtechnology.calliope.javadsl.{DeduplicationBatch, EventConsumer}
import io.vavr.control.Try
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer}
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.{BeforeAndAfterEach, MustMatchers}

import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object EventConsumerSpec {

  trait EventBase {
    def id: String
  }

  case class Event(id: String) extends EventBase
  case class NonDeserializableEvent(id: String) extends EventBase

  class EventSerializer extends Serializer[EventBase] {
    override def configure(configs: JMap[String, _], isKey: Boolean): Unit = {
    }

    override def serialize(topic: String, data: EventBase): Array[Byte] =
      (data.getClass.getName + "|" + data.id).getBytes

    override def close(): Unit = {
    }
  }

  class EventDeserializer extends Deserializer[Try[EventBase]] {
    private val EventClassManifest = classOf[Event].getName

    override def configure(configs: JMap[String, _], isKey: Boolean): Unit = {
    }

    override def deserialize(topic: String, data: Array[Byte]): Try[EventBase] = {
      val payload = new String(data)

      payload.split("\\|").toList match {
        case EventClassManifest :: id :: Nil => Try.success(Event(id))
        case _ => Try.failure(new IllegalArgumentException("Invalid payload for event [" + payload + "] given"))
      }
    }

    override def close(): Unit = {
    }
  }
}

class EventConsumerSpec extends KafkaSpec with MustMatchers with TypeCheckedTripleEquals with BeforeAndAfterEach {

  import EventConsumerSpec._

  var eventHandler: TestProbe = _
  var topic: String = _

  private val consumerStartTimeout: Duration = Duration.ofSeconds(5)
  private val eventProducerSettings = producerSettings(new EventSerializer())
  private val eventConsumerSettings = EventConsumer.Settings.create(system, new StringDeserializer(), new EventDeserializer())
    .withBootstrapServers(bootstrapServers)

  override protected def beforeEach(): Unit = {
    super.beforeEach()

    topic = topicName("event-consumer-spec")
    eventHandler = TestProbe()
  }

  def topicName(prefix: String): String =
    s"$prefix-${UUID.randomUUID()}"

  def eventProducer(topic: String, producerSettings: ProducerSettings[String, EventBase]): TestPublisher.Probe[EventBase] =
    TestSource.probe[EventBase]
      .map(e => new ProducerRecord[String, EventBase](topic, 0, e.id, e))
      .toMat(Producer.plainSink(producerSettings))(Keep.left)
      .run()

  def runEventConsumer(topic: String, consumerSettings: EventConsumer.Settings[String, EventBase], handler: TestProbe): CompletionStage[Done] =
    eventConsumer(topic, consumerSettings)
      .to(actorEventHandler(handler.ref))
      .run(consumerStartTimeout)

  def eventConsumer(topic: String, consumerSettings: EventConsumer.Settings[String, EventBase]): EventConsumer[String, EventBase] =
    EventConsumer.committable(topic, consumerSettings, system)

  def actorEventHandler(eventReceiver: ActorRef): EventHandler[EventBase] = new EventHandler[EventBase] {
    override def handleEvent(event: EventBase, timeout: Duration): CompletionStage[Done] = {
      implicit val t: Timeout = Timeout(timeout.toNanos, NANOSECONDS)
      implicit val ec: ExecutionContext = system.dispatcher

      (eventReceiver ? event)
        .map[Done](_ => Done)
        .toJava
    }
  }

  def keySelector[K, V](f: CommittableMessage[K, V] => String): CommittableMessageKeySelector[K, V] = new CommittableMessageKeySelector[K, V] {
    override def apply(m: CommittableMessage[K, V]) = f.apply(m)
  }

  "An EventConsumer" when {
    "uncommitted events are available" must {
      "publish events to the configured event-handler" in {
        val producer = eventProducer(topic, eventProducerSettings)

        runEventConsumer(topic, eventConsumerSettings, eventHandler)

        producer.sendNext(Event("event-1"))
        producer.sendNext(Event("event-2"))
        producer.sendNext(Event("event-3"))

        eventHandler.expectMsg(Event("event-1"))
        eventHandler.expectMsg(Event("event-2"))
        eventHandler.expectMsg(Event("event-3"))
      }
      "publish events with the configured parallelism" in {
        val producer = eventProducer(topic, eventProducerSettings)
        val settings = eventConsumerSettings.withDeliveryParallelism(1)

        runEventConsumer(topic, settings, eventHandler)

        producer.sendNext(Event("event-1"))
        producer.sendNext(Event("event-2"))

        val events = eventHandler.receiveWhile(5.seconds) {
          case e:EventBase => e
        }
        events must contain only Event("event-1")
        eventHandler.reply(Done)

        eventHandler.expectMsg(Event("event-2"))
        eventHandler.reply(Done)
      }
    }
    "consumer stream fails" must {
      "restart and continue consumption of uncommitted events" in {
        val producer = eventProducer(topic, eventProducerSettings)
        val settings = eventConsumerSettings.withDeliveryRetries(0)

        runEventConsumer(topic, settings, eventHandler)

        producer.sendNext(Event("event-1"))
        producer.sendNext(Event("event-2"))

        eventHandler.expectMsg(Event("event-1"))
        eventHandler.expectMsg(Event("event-2"))
        eventHandler.reply(Failure(new RuntimeException("error")))

        eventHandler.expectMsg(Event("event-1"))
        eventHandler.reply(Done)
        eventHandler.expectMsg(Event("event-2"))
        eventHandler.reply(Done)
      }
    }
    "event-delivery fails" must {
      "retry failed delivery" in {
        val producer = eventProducer(topic, eventProducerSettings)
        val settings = eventConsumerSettings.withDeliveryRetries(1)

        runEventConsumer(topic, settings, eventHandler)

        producer.sendNext(Event("event-1"))
        producer.sendNext(Event("event-2"))

        eventHandler.expectMsg(Event("event-1"))
        eventHandler.reply(Failure(new RuntimeException("error")))

        eventHandler.expectMsgAnyOf(Event("event-1"), Event("event-2"))
        eventHandler.reply(Done)

        eventHandler.expectMsgAnyOf(Event("event-1"), Event("event-2"))
        eventHandler.reply(Done)

        eventHandler.expectNoMsg(1.second)
      }
      "restart and continue consumption of uncommitted events if retries fail" in {
        val producer = eventProducer(topic, eventProducerSettings)
        val settings = eventConsumerSettings.withDeliveryRetries(1)

        runEventConsumer(topic, settings, eventHandler)

        producer.sendNext(Event("event-1"))
        producer.sendNext(Event("event-2"))

        eventHandler.expectMsg(Event("event-1"))
        eventHandler.expectMsg(Event("event-2"))
        eventHandler.reply(Failure(new RuntimeException("error")))

        eventHandler.expectMsg(Event("event-2"))
        eventHandler.reply(Failure(new RuntimeException("error")))

        eventHandler.expectMsg(Event("event-1"))
        eventHandler.reply(Done)
        eventHandler.expectMsg(Event("event-2"))
        eventHandler.reply(Done)
      }
    }
    "routed via a de-duplication batch" must {
      "drop messages containing the same aggregate-id" in {
        val producer = eventProducer(topic, eventProducerSettings)
        val settings = eventConsumerSettings.withDeliveryParallelism(1)

        eventConsumer(topic, settings)
          .via(DeduplicationBatch.committableFlow(100, keySelector(_.record.key())))
          .to(actorEventHandler(eventHandler.ref))
          .run(consumerStartTimeout)

        producer.sendNext(Event("event-1"))
        producer.sendNext(Event("event-1"))
        producer.sendNext(Event("event-2"))
        producer.sendNext(Event("event-2"))
        producer.sendNext(Event("event-1"))
        producer.sendNext(Event("event-3"))
        producer.sendNext(Event("event-2"))

        // first entry is not handled by the deduplication-batch as it is placed in the initial buffer of Akka streams
        eventHandler.expectMsg(Event("event-1"))
        eventHandler.reply(Done)
        eventHandler.expectMsg(Event("event-1"))
        eventHandler.reply(Done)
        eventHandler.expectMsg(Event("event-2"))
        eventHandler.reply(Done)
        eventHandler.expectMsg(Event("event-3"))
        eventHandler.reply(Done)

        eventHandler.expectNoMsg(1.second)
      }
      "process all messages if batch-size = 1" in {
        val producer = eventProducer(topic, eventProducerSettings)
        val settings = eventConsumerSettings.withDeliveryParallelism(1)

        eventConsumer(topic, settings)
          .via(DeduplicationBatch.committableFlow(1, keySelector(_.record.key())))
          .to(actorEventHandler(eventHandler.ref))
          .run(consumerStartTimeout)

        producer.sendNext(Event("event-1"))
        producer.sendNext(Event("event-1"))
        producer.sendNext(Event("event-2"))
        producer.sendNext(Event("event-2"))
        producer.sendNext(Event("event-1"))
        producer.sendNext(Event("event-3"))
        producer.sendNext(Event("event-2"))

        eventHandler.expectMsg(Event("event-1"))
        eventHandler.reply(Done)
        eventHandler.expectMsg(Event("event-1"))
        eventHandler.reply(Done)
        eventHandler.expectMsg(Event("event-2"))
        eventHandler.reply(Done)
        eventHandler.expectMsg(Event("event-2"))
        eventHandler.reply(Done)
        eventHandler.expectMsg(Event("event-1"))
        eventHandler.reply(Done)
        eventHandler.expectMsg(Event("event-3"))
        eventHandler.reply(Done)
        eventHandler.expectMsg(Event("event-2"))
        eventHandler.reply(Done)

        eventHandler.expectNoMsg(1.second)
      }
    }
    "deserialization fails" must {
      "terminate event-consumer" in {
        val producer = eventProducer(topic, eventProducerSettings)

        runEventConsumer(topic, eventConsumerSettings, eventHandler)

        producer.sendNext(NonDeserializableEvent("event-1"))

        eventHandler.expectNoMsg(5.seconds)
      }
    }
  }
}
