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
import akka.stream.scaladsl.{Flow, Keep, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually

import scala.collection.immutable.Seq

object KafkaEventHubSpec {
  import KafkaSpec._

  trait ExampleRequest { def aggregateId: String }
  case class ExampleQuery(aggregateId: String) extends ExampleRequest
  case class ExampleCommand(aggregateId: String, payload: String) extends ExampleRequest
  case class ExampleResponse(aggregateId: String, state: Seq[String])

  implicit val requestAggregate = new Aggregate[ExampleRequest, String] {
    override def aggregateId(a: ExampleRequest): String = a.aggregateId
  }

  def processorLogic(aggregateId: String) = new ProcessorLogic[ExampleEvent, ExampleRequest, ExampleResponse] {
    private var eventPayloads: Seq[String] = Seq.empty
    private var ctr: Int = 1

    override def onRequest(c: ExampleRequest): (Seq[ExampleEvent], () => ExampleResponse) = c match {
      case ExampleQuery(_) =>
        (Seq(), () => ExampleResponse(aggregateId, eventPayloads))
      case ExampleCommand(_, payload) =>
        (Seq(ExampleEvent(s"$aggregateId#$ctr", aggregateId, payload)),
          () => ExampleResponse(aggregateId, eventPayloads))
    }

    override def onEvent(e: ExampleEvent): Unit = {
      eventPayloads = eventPayloads :+ e.payload
      ctr = ctr + 1
    }
  }
}

class KafkaEventHubSpec extends KafkaSpec with BeforeAndAfterEach with Eventually {
  import KafkaEventHubSpec._
  import KafkaSpec._

  var producer: KafkaProducer[String, ExampleEvent] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    producer = producerSettings.createKafkaProducer()
  }

  override def afterAll(): Unit = {
    producer.close()
    super.afterAll()
  }

  def send(topic: String)(event: ExampleEvent): Unit =
    producer.send(new ProducerRecord[String, ExampleEvent](topic, eventAggregate.aggregateId(event), event))

  def processorProbes(processor: Flow[ExampleRequest, ExampleResponse, NotUsed]): (TestPublisher.Probe[ExampleRequest], TestSubscriber.Probe[ExampleResponse]) =
    TestSource.probe[ExampleRequest].via(processor).toMat(TestSink.probe[ExampleResponse])(Keep.both).run()

  def eventProbe(source: Source[ConsumerRecord[String, ExampleEvent], NotUsed]): TestSubscriber.Probe[ExampleEvent] =
    source.map(_.value).toMat(TestSink.probe[ExampleEvent])(Keep.right).run()

  def eventHub(topic: String): KafkaEventHub[String, ExampleEvent] =
    KafkaEvents.hub(consumerSettings(group), producerSettings, topic, index(topic))

  def index(topic: String): KafkaIndex[String, ExampleEvent] = {
    val index = KafkaIndex.inmem(eventAggregate, system)
    index.connect(consumerSettings(group), topic)
    index
  }

  "An aggregate event source" must {
    "consume past and live aggregate events" in {
      val e1 = ExampleEvent("a1#1", "a1", "u")
      val e2 = ExampleEvent("a2#1", "a2", "v")
      val e3 = ExampleEvent("a1#2", "a1", "w")
      val e4 = ExampleEvent("a2#2", "a2", "x")
      val e5 = ExampleEvent("a1#3", "a1", "y")

      Seq(e1, e2, e3, e4).foreach(send(topic))

      val hub = eventHub(topic)
      val sub = eventProbe(hub.aggregateEvents("a1"))

      sub.request(3)
      sub.expectNextN(2) should be(Seq(e1, e3))

      send(topic)(e5)
      sub.expectNext(e5)
    }
  }
  "A request processor" must {
    "create aggregate request processors dynamically" in {
      val topic = "es-1"
      val hub = eventHub(topic)
      val esub = eventProbe(hub.events)
      val (rpub, rsub) = processorProbes(hub.requestProcessorN(10, processorLogic))

      rsub.request(3)
      esub.request(3)

      rpub.sendNext(ExampleCommand("a1", "a"))
      rpub.sendNext(ExampleCommand("a2", "b"))
      rpub.sendNext(ExampleCommand("a1", "c"))

      // -----------------------------------------------
      // Response order corresponds to request order !!!
      // -----------------------------------------------

      rsub.expectNextN(3) should be(Seq(
        ExampleResponse("a1", Seq("a")),
        ExampleResponse("a2", Seq("b")),
        ExampleResponse("a1", Seq("a", "c"))))

      val actualEvents = esub.expectNextN(3)

      actualEvents.filter(_.aggregateId == "a1") should be(Seq(
        ExampleEvent("a1#1", "a1", "a"),
        ExampleEvent("a1#2", "a1", "c")))

      actualEvents.filter(_.aggregateId == "a2") should be(Seq(
        ExampleEvent("a2#1", "a2", "b")))
    }
    "collaborate with request processors of same aggregate id in other hubs" in {
      val topic = "es-2"
      val hub1 = eventHub(topic)
      val hub2 = eventHub(topic)

      val (rpub1, rsub1) = processorProbes(hub1.requestProcessorN(10, processorLogic))
      val (rpub2, rsub2) = processorProbes(hub2.requestProcessorN(10, processorLogic))

      rsub1.request(1)
      rpub1.sendNext(ExampleCommand("a1", "a"))
      rsub1.requestNext(ExampleResponse("a1", Seq("a")))

      eventually {
        rsub2.request(1)
        rpub2.sendNext(ExampleQuery("a1"))
        rsub2.requestNext(ExampleResponse("a1", Seq("a")))
      }

      rsub2.request(1)
      rpub2.sendNext(ExampleCommand("a1", "b"))
      rsub2.requestNext(ExampleResponse("a1", Seq("a", "b")))

      eventually {
        rsub1.request(1)
        rpub1.sendNext(ExampleQuery("a1"))
        rsub1.requestNext(ExampleResponse("a1", Seq("a", "b")))
      }
    }
  }
}
