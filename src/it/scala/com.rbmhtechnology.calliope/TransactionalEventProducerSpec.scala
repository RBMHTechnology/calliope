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

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ProducerMessage, Subscriptions}
import akka.serialization.SerializerWithStringManifest
import akka.stream.scaladsl.Keep
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestProbe
import com.rbmhtechnology.calliope.scaladsl.TransactionalEventProducer.Settings
import com.rbmhtechnology.calliope.scaladsl.{EventStore, EventWriter, TransactionalEventProducer}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.{BeforeAndAfterEach, MustMatchers}

import scala.collection.immutable.{Seq, SortedMap}
import scala.concurrent.Await

object TransactionalEventProducerSpec {

  case class Event(id: String, aggregateId: String, payload: String)

  object Event {
    def apply(id: String, aggregateId: String): Event =
      new Event(id, aggregateId, s"payload-$id")

    implicit val aggregate = new Aggregate[Event] {
      override def aggregateId(event: Event): String = event.aggregateId
    }
  }

  class EventSerializer extends SerializerWithStringManifest {
    override def identifier: Int = 1337666

    override def manifest(o: AnyRef): String = "Event"

    override def toBinary(o: AnyRef): Array[Byte] = {
      val event = o.asInstanceOf[Event]
      s"${event.id}+${event.aggregateId}+${event.payload}".getBytes
    }

    override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
      val parts = new String(bytes).split('+')
      Event(parts(0), parts(1), parts(2))
    }
  }

  case class WritableEventStore() extends EventStore {
    var records: Map[Long, (String, String, Instant, Array[Byte])] = SortedMap.empty
    var deletedSequenceNrs = Vector.empty[Long]
    private var nextSnr = 1L

    override def readEvents(fromSequenceNr: Long, limit: Int): Seq[StoredEvent] = {
      records.dropWhile(_._1 < fromSequenceNr)
        .take(limit)
        .map { case (sequenceNr, record) =>
          StoredEvent(record._4, sequenceNr, record._3, record._1, record._2)
        }
        .toVector
    }

    override def writeEvent(event: Array[Byte], topic: String, aggregateId: String, onCommit: => Unit): Unit = {
      records = records + (nextSnr -> (topic, aggregateId, Instant.now(), event))
      nextSnr += 1
      onCommit
    }

    override def deleteEvents(toSequenceNr: Long): Unit = {
      deletedSequenceNrs = deletedSequenceNrs :+ toSequenceNr
      records = records.dropWhile(_._1 <= toSequenceNr)
    }

    def setNextSnr(sequenceNr: Long): Unit = {
      nextSnr = sequenceNr
    }
  }

  case class FixedResultEventStore(results: Seq[Unit => Seq[(Long, String, Event)]])(implicit system: ActorSystem)
    extends EventStore {

    private val serializer = PayloadSerializer()
    private var remaining = results

    override def readEvents(fromSequenceNr: Long, limit: Int): Seq[StoredEvent] = {
      val result = remaining.headOption
      remaining = remaining.drop(1)
      result.map(_.apply(Unit)).getOrElse(Seq.empty) map { case (sequenceNr, topic, event) =>
        StoredEvent(serializer.serialize(event), sequenceNr, Instant.now(), topic, event.aggregateId)
      }
    }

    override def writeEvent(event: Array[Byte], topic: String, aggregateId: String, onCommit: => Unit): Unit = ???

    override def deleteEvents(toSequenceNr: Long): Unit = {
    }
  }

}

class TransactionalEventProducerSpec extends KafkaSpec with MustMatchers with TypeCheckedTripleEquals with BeforeAndAfterEach {

  import TransactionalEventProducerSpec._

  import scala.concurrent.duration._

  private val sourceId = "source-1"
  private val group: String = "group-1"

  private val settings = Settings[Event](readBufferSize = 1, readInterval = 500.millis, deleteInterval = 10.seconds, transactionTimeout = 10.seconds, bootstrapServers)
  private var producer: TransactionalEventProducer[Event] = _

  override protected def afterEach(): Unit = {
    super.afterEach()

    if (producer != null) {
      Await.ready(producer.stop(), Duration(5, SECONDS))
    }
  }

  def kafkaConsumer(topic: String, groupId: String = group): TestSubscriber.Probe[ConsumerRecord[String, SequencedEvent[Event]]] = {
    val settings = consumerSettings[SequencedEvent[Event]](groupId, (m: AnyRef) => m.asInstanceOf[SequencedEvent[Event]])

    Consumer.plainSource(settings, Subscriptions.topics(topic))
      .toMat(TestSink.probe)(Keep.right)
      .run()
  }

  def topicConsumer(topicName: String = "topic"): (String, TestSubscriber.Probe[ConsumerRecord[String, SequencedEvent[Event]]]) = {
    val topic = s"$topicName-${UUID.randomUUID()}"
    (topic, kafkaConsumer(topic))
  }

  def runTransactionalEventProducer(topic: String, eventStore: EventStore, settings: Settings[Event]): EventWriter[Event] = {
    producer = TransactionalEventProducer(sourceId, topic, eventStore, settings, err => fail("onFailure triggered", err))
    producer.run()
  }

  def sleep(duration: Duration): Unit = {
    Thread.sleep(duration.toMillis)
  }

  "A TransactionalEventProducer" when {
    "events are committed to the underlying event-store" must {
      "produce a single event to the configured default topic" in {
        val (topic, consumer) = topicConsumer()
        val writer = runTransactionalEventProducer(topic, WritableEventStore(), settings)

        writer.writeEvent(Event("1", "agg1"))

        consumer.requestNext().value().payload mustBe Event("1", "agg1")
      }
      "use the aggregate id as key in the topic" in {
        val (topic, consumer) = topicConsumer()
        val writer = runTransactionalEventProducer(topic, WritableEventStore(), settings)

        writer.writeEvent(Event("1", "agg1"))

        consumer.requestNext().key() mustBe "agg1"
      }
      "wrap the event in an instance of `SequencedEvent`" in {
        import CustomSequencedEventEquality._

        val (topic, consumer) = topicConsumer()
        val writer = runTransactionalEventProducer(topic, WritableEventStore(), settings)

        writer.writeEvent(Event("1", "agg1"))
        writer.writeEvent(Event("2", "agg1"))

        consumer.request(2)
        assert(consumer.expectNext().value() === SequencedEvent(Event("1", "agg1"), sourceId, 1L, Instant.MIN))
        assert(consumer.expectNext().value() === SequencedEvent(Event("2", "agg1"), sourceId, 2L, Instant.MIN))
      }
      "produce multiple consecutive events for the same aggregate to the configured default topic" in {
        val (topic, consumer) = topicConsumer()
        val writer = runTransactionalEventProducer(topic, WritableEventStore(), settings)

        writer.writeEvent(Event("1", "agg1"))
        writer.writeEvent(Event("2", "agg1"))
        writer.writeEvent(Event("3", "agg1"))

        consumer.request(3)
        consumer.expectNextN(3).map(_.value().payload) must contain inOrder(Event("1", "agg1"), Event("2", "agg1"), Event("3", "agg1"))
      }
      "produce multiple consecutive events for different aggregates to the configured default topic" in {
        val (topic, consumer) = topicConsumer()
        val writer = runTransactionalEventProducer(topic, WritableEventStore(), settings)

        writer.writeEvent(Event("1", "agg1"))
        writer.writeEvent(Event("2", "agg2"))
        writer.writeEvent(Event("3", "agg1"))
        writer.writeEvent(Event("4", "agg2"))

        consumer.request(4)
        consumer.expectNextN(4).groupBy(_.key()).mapValues(_.map(_.value().payload)) must contain allOf(
          "agg1" -> Seq(Event("1", "agg1"), Event("3", "agg1")),
          "agg2" -> Seq(Event("2", "agg2"), Event("4", "agg2"))
        )
      }
      "produce events to different topics if specified on write" in {
        val (topic, consumer) = topicConsumer()
        val (otherTopic, otherTopicConsumer) = topicConsumer("otherTopic")
        val writer = runTransactionalEventProducer(topic, WritableEventStore(), settings)

        writer.writeEvent(Event("1", "agg1"))
        writer.writeEventToTopic(Event("2", "agg1"), otherTopic)
        writer.writeEventToTopic(Event("3", "agg2"), otherTopic)
        writer.writeEvent(Event("4", "agg2"))

        consumer.request(2)
        consumer.expectNextN(2).map(_.value().payload) must contain allOf(Event("1", "agg1"), Event("4", "agg2"))

        otherTopicConsumer.request(2)
        otherTopicConsumer.expectNextN(2).map(_.value().payload) must contain allOf(Event("2", "agg1"), Event("3", "agg2"))
      }
      "delete produced events from the underlying event store" in {
        val (topic, consumer) = topicConsumer()
        val eventStore = WritableEventStore()
        val deleteInterval = 500.millis
        val writer = runTransactionalEventProducer(topic, eventStore, settings.withReadBufferSize(10).withDeleteInterval(deleteInterval))

        writer.writeEvent(Event("1", "agg1"))
        consumer.requestNext()

        sleep(deleteInterval * 1.5)
        writer.writeEvent(Event("2", "agg1"))
        writer.writeEvent(Event("3", "agg1"))

        sleep(deleteInterval * 1.5)
        writer.writeEvent(Event("4", "agg1"))
        writer.writeEvent(Event("5", "agg1"))
        writer.writeEvent(Event("6", "agg1"))

        sleep(deleteInterval * 1.5)
        writer.writeEvent(Event("7", "agg1"))
        sleep(deleteInterval / 4)
        writer.writeEvent(Event("8", "agg1"))

        sleep(deleteInterval * 1.5)
        writer.writeEvent(Event("9", "agg1"))
        writer.writeEvent(Event("10", "agg1"))

        consumer.request(9)
        consumer.expectNextN(9)

        sleep(deleteInterval * 2)

        eventStore.records mustBe empty
        eventStore.deletedSequenceNrs mustBe Seq(1L, 3L, 6L, 8L, 10L)
      }
    }
    "gaps are detected in the event sequence" must {
      "persist the gap after transaction timeout has elapsed" in {
        val (topic, consumer) = topicConsumer()
        val eventStore = WritableEventStore()
        val writer = runTransactionalEventProducer(topic, eventStore, settings.withTransactionTimeout(5.seconds))

        writer.writeEvent(Event("1", "agg1"))

        eventStore.setNextSnr(3)
        writer.writeEvent(Event("3", "agg1"))

        consumer.request(3)
        consumer.expectNext().value().payload mustBe Event("1", "agg1")
        consumer.expectNoMsg(1.second)

        consumer.expectNext(10.seconds).value().payload mustBe Event("3", "agg1")
      }
      "persist the gap after the missing element was committed" in {
        val (topic, consumer) = topicConsumer()
        val eventStore = WritableEventStore()
        val writer = runTransactionalEventProducer(topic, eventStore, settings.withTransactionTimeout(1.hour))

        writer.writeEvent(Event("1", "agg1"))

        eventStore.setNextSnr(3)
        writer.writeEvent(Event("3", "agg1"))

        consumer.request(3)
        consumer.expectNext().value().payload mustBe Event("1", "agg1")
        consumer.expectNoMsg(1.second)

        eventStore.setNextSnr(2)
        writer.writeEvent(Event("2", "agg1"))

        consumer.expectNext().value().payload mustBe Event("2", "agg1")
        consumer.expectNext().value().payload mustBe Event("3", "agg1")
      }
    }
    "an error occurs while reading events from the underlying event-store" must {
      "continue reading from the beginning of the store" in {
        val (topic, consumer) = topicConsumer()
        val eventStore = FixedResultEventStore(Vector(
          _ => Seq((1L, topic, Event("1", "agg1"))),
          _ => throw new RuntimeException("read failure"),
          _ => Seq((1L, topic, Event("1", "agg1")), (2L, topic, Event("2", "agg2")))
        ))
        runTransactionalEventProducer(topic, eventStore, settings.withReadBufferSize(5))

        consumer.request(3)
        consumer.expectNext().value().payload mustBe Event("1", "agg1")
        consumer.expectNext().value().payload mustBe Event("1", "agg1")
        consumer.expectNext().value().payload mustBe Event("2", "agg2")
      }
    }
    "created with settings from the ActorSystem" must {
      "use the producer-settings from the configuration file" in {
        val (topic, consumer) = topicConsumer()
        val writer = runTransactionalEventProducer(topic, WritableEventStore(), Settings().withBootstrapServers(bootstrapServers))

        writer.writeEvent(Event("1", "agg1"))

        consumer.requestNext().value().payload mustBe Event("1", "agg1")
      }
      "use the producer-flow from the configuration file" in {
        val (topic, consumer) = topicConsumer()
        val writer = runTransactionalEventProducer(topic, WritableEventStore(), Settings().withBootstrapServers(bootstrapServers))

        writer.writeEvent(Event("1", "agg1"))

        consumer.requestNext().value().payload mustBe Event("1", "agg1")
      }
    }
    "created with custom settings" must {
      "use the producer-provider configured in the settings" in {
        val flowProbe = TestProbe()
        val (topic, consumer) = topicConsumer()
        val writer = runTransactionalEventProducer(topic, WritableEventStore(), Settings()
          .withBootstrapServers(bootstrapServers)
          .withProducerProvider(s =>
            Producer.flow(s).map { (ev: ProducerMessage.Result[String, SequencedEvent[Event], Unit]) =>
              flowProbe.ref ! ev.message.record.value().payload
              ev
            }
          ))

        writer.writeEvent(Event("1", "agg1"))

        consumer.requestNext().value().payload mustBe Event("1", "agg1")
        flowProbe.expectMsg(Event("1", "agg1"))
      }
    }
  }
}
