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

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Source, SourceQueueWithComplete}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.scalatest.{MustMatchers, WordSpecLike}

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class EventSourceSpec extends TestKit(ActorSystem("test")) with WordSpecLike with MustMatchers with StopSystemAfterAll {
  import EventRecords._

  implicit val materializer = ActorMaterializer()
  val eventReader = TestEventReader()

  def runSource(source: Source[EventRecord[String], SourceQueueWithComplete[Unit]]): (SourceQueueWithComplete[Unit], TestSubscriber.Probe[EventRecord[String]]) =
    source.toMat(TestSink.probe)(Keep.both).run()

  "An EventSource" when {
    "elements demanded from downstream" must {
      "fetch and push elements from the event reader" in {
        val (_, sub) = runSource(EventSource(eventReader, 5, 100.millis))

        eventReader.setResult(1L -> Success(eventRecords(1, 5)))

        sub.request(1)
        sub.expectNext(eventRecord(1))
      }
      "push records from buffer" in {
        val (_, sub) = runSource(EventSource(eventReader, 5, 100.millis))

        eventReader.setResult(1L -> Success(eventRecords(1, 5)))

        sub.request(5)
        sub.expectNextN(eventRecords(1, 5))
      }
      "fetch the next sequence of records from the event reader when buffer is empty" in {
        val (_, sub) = runSource(EventSource(eventReader, 5, 100.millis))

        eventReader.setResult(1L -> Success(eventRecords(1, 5)))
        eventReader.setResult(6L -> Success(eventRecords(6, 10)))

        sub.request(10)
        sub.expectNextN(eventRecords(1, 10))
      }
    }
    "downstream demand cannot be fulfilled" must {
      "push no records if event reader returns no events" in {
        val (_, sub) = runSource(EventSource(eventReader, 5, 100.millis))

        eventReader.setResult(1L -> Success(eventRecords(1, 5)))
        eventReader.setResult(6L -> Success(Seq.empty))

        sub.request(10)
        sub.expectNextN(eventRecords(1, 5))
        sub.expectNoMsg(1.second)
      }
      "try to fetch new records on polling interval only" in {
        val (_, sub) = runSource(EventSource(eventReader, 5, 2.seconds))

        eventReader.setResult(1L -> Success(Seq.empty))

        sub.request(5)
        sub.expectNoMsg(1.second)

        eventReader.setResult(1L -> Success(eventRecords(1, 5)))

        sub.expectNextN(eventRecords(1, 5))
      }
      "try to fetch new records on committed transaction" in {
        val (onCommit, sub) = runSource(EventSource(eventReader, 5, 10.seconds))

        eventReader.setResult(1L -> Success(Seq.empty))

        sub.request(5)
        sub.expectNoMsg(1.second)

        eventReader.setResult(1L -> Success(eventRecords(1, 5)))

        onCommit.offer(Unit)
        sub.expectNextN(eventRecords(1, 5))
      }
      "try to fetch new records on multiple transaction commits" in {
        val (onCommit, sub) = runSource(EventSource(eventReader, 5, 10.seconds))

        eventReader.setResult(1L -> Success(Seq.empty))

        sub.request(5)
        sub.expectNoMsg(1.second)

        eventReader.setResult(1L -> Success(eventRecords(1, 5)))

        onCommit.offer(Unit)
        onCommit.offer(Unit)
        sub.expectNextN(eventRecords(1, 5))
      }
    }
    "no elements demanded from downstream" must {
      "not fetch new records on transaction commit" in {
        val (onCommit, sub) = runSource(EventSource(eventReader, 5, 10.seconds))

        sub.ensureSubscription()

        eventReader.setResult(1L -> Failure(new RuntimeException("should not be called")))

        onCommit.offer(Unit)
        onCommit.offer(Unit)
        onCommit.offer(Unit)
        sub.expectNoMsg(2.seconds)
      }
      "not fetch new records on polling interval" in {
        val (_, sub) = runSource(EventSource(eventReader, 5, 100.millis))

        sub.ensureSubscription()

        eventReader.setResult(1L -> Failure(new RuntimeException("should not be called")))
        sub.expectNoMsg(2.seconds)
      }
    }
    "event reader call fails" must {
      "stop the stream with failure" in {
        val (_, sub) = runSource(EventSource(eventReader, 5, 100.millis))

        eventReader.setResult(1L -> Failure(new RuntimeException("event reader call failed")))

        sub.request(1)
        sub.expectError()
      }
    }
  }
}
