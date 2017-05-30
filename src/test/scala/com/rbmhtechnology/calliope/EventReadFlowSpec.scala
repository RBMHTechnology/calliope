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
import akka.stream.scaladsl.Flow
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.TestSubscriber.OnSubscribe
import akka.testkit.TestKit
import org.scalatest.{MustMatchers, WordSpecLike}

import scala.collection.immutable._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class EventReadFlowSpec extends TestKit(ActorSystem("test")) with WordSpecLike with MustMatchers with StreamSpec with FlowSpec {
  import EventRecords._
  import TestEventReader.Extensions._

  implicit val eventReader = TestEventReader()

  def eventReadFlow(reader: TestEventReader, bufferSize: Int): Flow[Unit, EventRecord[String], NotUsed] =
    EventReadFlow[Unit, String](reader, bufferSize)

  "An EventReadFlow" when {
    "elements demanded from downstream" must {
      "fetch and push elements from the event reader" in {
        val (pub, sub) = runFlow(eventReadFlow(eventReader, 5))

        sub.request(1)

        pub.sendNextResult(1L -> Success(eventRecords(1, 5)))

        sub.expectNext(eventRecord(1))
      }
      "push records from buffer" in {
        val (pub, sub) = runFlow(eventReadFlow(eventReader, 5))

        sub.request(5)

        pub.sendNextResult(1L -> Success(eventRecords(1, 5)))

        sub.expectNextN(eventRecords(1, 5))
      }
      "fetch the next sequence of records from the event reader when buffer is empty" in {
        val (pub, sub) = runFlow(eventReadFlow(eventReader, 5))

        sub.request(10)

        pub.sendNextResult(1L -> Success(eventRecords(1, 5)))
        pub.sendNextResult(6L -> Success(eventRecords(6, 10)))

        sub.expectNextN(eventRecords(1, 10))
      }
      "apply buffer size to the result of the event reader" in {
        val (pub, sub) = runFlow(eventReadFlow(eventReader, 5))

        sub.request(10)

        pub.sendNextResult(1L -> Success(eventRecords(1, 5)))
        pub.sendNextResult(6L -> Success(eventRecords(6, 20)))

        sub.expectNextN(eventRecords(1, 10))
      }
    }
    "downstream demand cannot be fulfilled" must {
      "not push elements if event reader is empty" in {
        val (pub, sub) = runFlow(eventReadFlow(eventReader, 10))

        sub.request(1)

        eventReader.setResult(1L -> Success(Seq.empty))
        pub.sendNext(Unit)

        sub.expectNoMsg(1.second)
      }
      "stop pushing elements once the event reader is empty" in {
        val (pub, sub) = runFlow(eventReadFlow(eventReader, 5))

        sub.request(10)

        eventReader.setResult(1L -> Success(eventRecords(1, 5)))
        pub.sendNext(Unit)

        sub.expectNextN(eventRecords(1, 5))

        eventReader.setResult(6L -> Success(Seq.empty))
        pub.sendNext(Unit)

        sub.expectNoMsg(1.second)
      }
      "fetch data from the event reader on upstream emission" in {
        val (pub, sub) = runFlow(eventReadFlow(eventReader, 5))

        eventReader.setResult(1L -> Success(Seq.empty))
        sub.request(1)
        sub.expectNoMsg(1.second)

        eventReader.setResult(1L -> Success(eventRecords(1, 5)))
        pub.sendNext(Unit)
        sub.expectNext(eventRecord(1))
      }
      "fetch data from the event reader for each upstream emission until data available" in {
        val (pub, sub) = runFlow(eventReadFlow(eventReader, 5))

        sub.request(10)

        eventReader.setResult(1L -> Success(eventRecords(1, 5)))
        pub.sendNext(Unit)
        eventReader.setResult(6L -> Success(eventRecords(6, 7)))
        pub.sendNext(Unit)

        sub.expectNextN(eventRecords(1, 7))

        eventReader.setResult(8L -> Success(Seq.empty))
        pub.sendNext(Unit)

        sub.expectNoMsg(1.second)

        eventReader.setResult(8L -> Success(eventRecords(9, 10)))
        pub.sendNext(Unit)

        sub.expectNextN(eventRecords(9, 10))
      }
      "ignore upstream emissions until demand is propagated from downstream" in {
        val (pub, sub) = runFlow(eventReadFlow(eventReader, 5))

        sub.ensureSubscription()

        eventReader.setResult(1L -> Failure(new RuntimeException("should not be called yet")))
        pub.sendNext(Unit)
        sub.expectNoMsg(1.second)

        eventReader.setResult(1L -> Success(Seq.empty))
        pub.sendNext(Unit)
        sub.request(1)
        sub.expectNoMsg(1.second)

        eventReader.setResult(1L -> Success(Seq(eventRecord(1))))
        pub.sendNext(Unit)
        sub.expectNext(eventRecord(1))
      }
    }
  }
  "event reader call fails" must {
    "fail the flow stage" in {
      val (pub, sub) = runFlow(eventReadFlow(eventReader, 5))

      sub.request(1)

      pub.sendNextResult(1L -> Failure(new RuntimeException("event reader call failed")))

      sub.expectError()
    }
  }
}
