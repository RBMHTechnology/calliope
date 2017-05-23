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

import akka.Done
import akka.actor.ActorSystem
import akka.pattern._
import akka.stream.ActorMaterializerSettings
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpecLike}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object EventDeletionSpec {

  case class SequenceNr(sequenceNr: Long)

  case object SequenceNr {
    implicit object Sequenced extends Sequenced[SequenceNr] {
      override def sequenceNr(event: SequenceNr): Long = event.sequenceNr
    }
  }

  implicit class EventDeletionTestProbe(probe: TestProbe) {
    import TestProbeExtensions._

    def expectSequenceNrAndReply(sequenceNr: Long, reply: Try[Unit] = Success(Unit)): Unit = {
      probe.expectNextAndReply(sequenceNr, reply)
    }
  }
}

class EventDeletionSpec extends TestKit(ActorSystem("test"))
  with WordSpecLike with MustMatchers with StreamSpec with SinkSpec with BeforeAndAfterEach with ScalaFutures {

  import EventDeletionSpec._

  import scala.concurrent.ExecutionContext.Implicits.global

  override def materializerSettings = Some(ActorMaterializerSettings(system).withInputBuffer(initialSize = 1, maxSize = 1))

  implicit val timeout = Timeout(5.seconds)
  val is: AfterWord = afterWord("is")
  var eventDeletionProbe: TestProbe = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    eventDeletionProbe = TestProbe()
  }

  def actorEventDeleter(deletionProbe: TestProbe = eventDeletionProbe): EventDeleter = new EventDeleter {
    override def deleteEvents(toSequenceNr: Long): Future[Unit] = (deletionProbe.ref ? toSequenceNr).map(_.asInstanceOf[Unit])
  }

  "An EventDeletionSink" when {
    "elements are pushed from upstream" when {
      "deleteEvents of the underlying EventDeleter returns a result" that is {
        "a success" must {
          "invoke deleteEvents for each pushed element with group-size 1" in {
            val (pub, _) = runSink(EventDeletion.sink[SequenceNr](actorEventDeleter(), perSize = 1))

            pub.expectRequest()
            pub.sendNext(SequenceNr(1L))

            eventDeletionProbe.expectSequenceNrAndReply(1L)

            pub.expectRequest()
            pub.sendNext(SequenceNr(2L))
            eventDeletionProbe.expectSequenceNrAndReply(2L)

            pub.expectRequest()
          }
          "invoke deleteEvents for each time the given size has been reached" in {
            val (pub, _) = runSink(EventDeletion.sink[SequenceNr](actorEventDeleter(), perSize = 2))

            pub.sendNext(SequenceNr(1L))
            pub.sendNext(SequenceNr(2L))

            eventDeletionProbe.expectSequenceNrAndReply(2L)

            pub.sendNext(SequenceNr(3L))
            pub.sendNext(SequenceNr(4L))

            eventDeletionProbe.expectSequenceNrAndReply(4L)

            pub.expectRequest()
          }
          "invoke deleteEvents for each time frame" in {
            val (pub, _) = runSink(EventDeletion.sink[SequenceNr](actorEventDeleter(), perDuration = 500.millis))

            pub.sendNext(SequenceNr(1L))

            pub.sendNext(SequenceNr(2L))
            pub.sendNext(SequenceNr(3L))
            pub.sendNext(SequenceNr(4L))
            eventDeletionProbe.expectSequenceNrAndReply(1L)
            Thread.sleep(750.millis.toMillis)

            pub.sendNext(SequenceNr(5L))
            pub.sendNext(SequenceNr(6L))
            eventDeletionProbe.expectSequenceNrAndReply(4L)
            Thread.sleep(750.millis.toMillis)

            pub.sendNext(SequenceNr(7L))
            pub.sendNext(SequenceNr(8L))
            eventDeletionProbe.expectSequenceNrAndReply(6L)
            eventDeletionProbe.expectSequenceNrAndReply(8L)

            pub.expectRequest()
          }
        }
        "a failure" must {
          "cancel the subscription" in {
            val (pub, _) = runSink(EventDeletion.sink[SequenceNr](actorEventDeleter(), perSize = 1))

            pub.expectRequest()
            pub.sendNext(SequenceNr(1L))

            eventDeletionProbe.expectSequenceNrAndReply(sequenceNr = 1L, reply = Failure(new IllegalStateException("delete failed")))

            pub.expectCancellation()
          }
          "complete the materialized future with a failure" in {
            val (pub, done) = runSink(EventDeletion.sink[SequenceNr](actorEventDeleter(), perSize = 1))

            pub.expectRequest()
            pub.sendNext(SequenceNr(1L))

            eventDeletionProbe.expectSequenceNrAndReply(sequenceNr = 1L, reply = Failure(new IllegalStateException("delete failed")))

            whenReady(done.failed) { result =>
              result mustBe an[IllegalStateException]
            }
          }
        }
      }
    }
    "no elements are pushed from upstream" must {
      "not invoke the underlying EventDeleter" in {
        val (pub, _) = runSink(EventDeletion.sink[SequenceNr](actorEventDeleter(), perSize = 1))

        pub.expectRequest()
        eventDeletionProbe.expectNoMsg(1.second)
      }
    }
    "upstream signals stream completion" that {
      "is a success" must {
        "complete the materialized future with a success" in {
          val (pub, done) = runSink(EventDeletion.sink[SequenceNr](actorEventDeleter(), perSize = 1))

          pub.sendComplete()

          whenReady(done) { result =>
            result mustBe Done
          }
        }
      }
      "is a failure" must {
        "complete the materialized future with a failure" in {
          val (pub, done) = runSink(EventDeletion.sink[SequenceNr](actorEventDeleter(), perSize = 1))

          pub.sendError(new IllegalStateException("upstream failed"))

          whenReady(done.failed) { result =>
            result mustBe an[IllegalStateException]
          }
        }
      }
    }
  }
}
