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
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{BidiFlow, Flow, Keep, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import akka.testkit.TestKit
import org.scalatest._

import scala.collection.immutable.Seq
import scala.concurrent.duration._

object ProcessorSpec {
  case class ExampleEvent(id: String, increment: String)
  case class ExampleRequest(id: String, increment: String)
  case class ExampleReply(id: String, state: Seq[String])

  def logic = new ProcessorLogic[ExampleEvent, ExampleRequest, ExampleReply] {
    private var state: Seq[String] = Seq()

    override def onRequest(c: ExampleRequest): (Seq[ExampleEvent], () => ExampleReply) = c match {
      case ExampleRequest(id, "get") => (Nil, () => ExampleReply(id, state))
      case ExampleRequest(id, incr) => (Seq(ExampleEvent(id, incr)), () => ExampleReply(id, state))
    }

    override def onEvent(e: ExampleEvent): Unit =
      state = state :+ e.increment
  }

  implicit val E = new Event[ExampleEvent, String] {
    override def eventId(a: ExampleEvent): String = a.id
  }
}

class ProcessorSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with BeforeAndAfterAll {
  import ProcessorSpec._
  import Processor._

  implicit val materializer = ActorMaterializer()

  override def afterAll(): Unit = {
    materializer.shutdown()
    TestKit.shutdownActorSystem(system)
  }

  def requestProbes(requestProcessor: Flow[ExampleRequest, ExampleReply, NotUsed]): (TestPublisher.Probe[ExampleRequest], TestSubscriber.Probe[ExampleReply]) =
    TestSource.probe[ExampleRequest].via(requestProcessor).toMat(TestSink.probe[ExampleReply])(Keep.both).run()

  def processorProbes[I1, O1, I2, O2](processor: BidiFlow[I1, O1, I2, O2, NotUsed]): (TestPublisher.Probe[I1], TestSubscriber.Probe[O2], TestPublisher.Probe[I2], TestSubscriber.Probe[O1]) =
    TestSource.probe[I1].viaMat(processor.joinMat(log[O1, I2])(Keep.right))(Keep.both).toMat(TestSink.probe[O2])(Keep.both).mapMaterializedValue { case ((rpub, (esub, epub)), rsub) => (rpub, rsub, epub, esub) }.run()

  def recoveredProcessor: BidiFlow[ExampleRequest, ExampleEvent, ExampleEvent, ExampleReply, NotUsed] =
    unrecoveredProcessor.atop(BidiFlow.fromFlows(Flow[ExampleEvent], Flow[ExampleEvent].map(Received(_)).prepend(Source.single(Recovered))))

  def unrecoveredProcessor: BidiFlow[ExampleRequest, ExampleEvent, Recovery[ExampleEvent], ExampleReply, NotUsed] =
    BidiFlow.fromGraph(Processor(logic))

  def log(entries: Seq[ExampleEvent]): Flow[ExampleEvent, ExampleEvent, NotUsed] =
    Flow[ExampleEvent].prepend(Source(entries))

  def log[I1, O1]: Flow[I1, O1, (TestSubscriber.Probe[I1], TestPublisher.Probe[O1])] =
    Flow.fromSinkAndSourceMat(TestSink.probe[I1], TestSource.probe[O1])(Keep.both)

  "A processor" must {
    "process query requests" in {
      val (pub, sub) = requestProbes(recoveredProcessor.join(log(Nil)))

      sub.request(1)

      pub.sendNext(ExampleRequest("1", "get"))
      sub.expectNext(ExampleReply("1", Seq()))
    }
    "process update requests" in {
      val (pub, sub) = requestProbes(recoveredProcessor.join(log(Nil)))

      sub.request(2)

      pub.sendNext(ExampleRequest("1", "foo"))
      pub.sendNext(ExampleRequest("2", "bar"))

      sub.expectNext(ExampleReply("1", Seq("foo")))
      sub.expectNext(ExampleReply("2", Seq("foo", "bar")))
    }
    "not process requests while another request is currently processed" in {
      val (rpub, rsub, epub, esub) = processorProbes(recoveredProcessor)

      rsub.request(3)
      esub.request(3)

      rpub.sendNext(ExampleRequest("1", "foo"))
      rpub.sendNext(ExampleRequest("2", "get"))

      esub.expectNext(ExampleEvent("1", "foo"))
      epub.sendNext(ExampleEvent("1", "foo"))

      rsub.expectNext(ExampleReply("1", Seq("foo")))
      rsub.expectNext(ExampleReply("2", Seq("foo")))
    }
    "recover initial state from logged events" in {
      val (rpub, rsub, epub, esub) = processorProbes(recoveredProcessor)

      epub.sendNext(ExampleEvent("1", "foo"))
      epub.sendNext(ExampleEvent("2", "bar"))

      rsub.request(1)
      esub.request(1)

      rpub.sendNext(ExampleRequest("3", "get"))
      rsub.expectNext(ExampleReply("3", Seq("foo", "bar")))
    }
    "only request commands after recovery" in {
      val (rpub, rsub, epub, esub) = processorProbes(unrecoveredProcessor)

      rsub.request(1)
      esub.request(1)

      rpub.sendNext(ExampleRequest("3", "baz"))
      epub.sendNext(Received(ExampleEvent("1", "foo")))
      esub.expectNoMsg(100.millis)
      epub.sendNext(Received(ExampleEvent("2", "bar")))
      esub.expectNoMsg(100.millis)
      epub.sendNext(Recovered)
      esub.expectNext(ExampleEvent("3", "baz"))
    }
  }
}
