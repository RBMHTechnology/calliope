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

import org.scalatest.{MustMatchers, WordSpecLike}

import scala.collection.immutable.Seq

object TimestampGapDetectionSpec {

  case class Event(sequenceNr: Long, timestamp: Instant)

  object Event {

    implicit val Sequenced = new Sequenced[Event] {
      override def sequenceNr(event: Event): Long = event.sequenceNr
    }

    implicit val Timed = new Timestamped[Event] {
      override def timestamp(event: Event): Instant = event.timestamp
    }

    implicit val Ord: Ordering[Event] = Ordering.by(_.sequenceNr)
  }

  object GapCollection {
    def apply(gaps: Long*): GapCollection =
      new GapCollection(gaps.toVector)

    val empty: GapCollection =
      new GapCollection(Seq.empty)
  }

  case class GapCollection(gaps: Seq[Long])

  def gaps(elements: Long*): GapCollection =
    GapCollection(elements: _*)

  def sequenceNrs(items: Seq[Long])(implicit gapCollection: GapCollection = GapCollection.empty): Seq[Long] =
    items.filterNot(gapCollection.gaps.contains(_))

  def events(items: Seq[Long])(implicit gapCollection: GapCollection = GapCollection.empty): Seq[Event] =
    sequenceNrs(items).map(Event(_, Instant.now()))

  def now(): Long =
    System.nanoTime()
}

class TimestampGapDetectionSpec extends WordSpecLike with MustMatchers with TimestampGapDetection {
  import TimestampGapDetectionSpec._

  import scala.concurrent.duration._
  import scala.language.postfixOps

  override val persistenceTimeout: FiniteDuration = 100 millis

  def waitForPersistTimeout(): Unit = {
    Thread.sleep(persistenceTimeout.toMillis * 2)
  }

  "A TimestampGapDetection" must {
    "return all events if no gaps exist" in {
      implicit val gaps = GapCollection.empty

      events(1L to 5L).filter(gapLess(1)).map(_.sequenceNr) mustBe sequenceNrs(1L to 5L)
    }
    "return events up to a given gap" in {
      implicit val g = gaps(4)

      events(1L to 5L).filter(gapLess(1)).map(_.sequenceNr) mustBe sequenceNrs(1L to 3L)
    }
    "return events up to the first temporary gap" in {
      implicit val g = gaps(3, 4)

      events(1L to 5L).filter(gapLess(1)).map(_.sequenceNr) mustBe sequenceNrs(1L to 2L)
    }
    "return no events if a temporary gap at the beginning is detected" in {
      implicit val g = gaps(1)

      events(1L to 5L).filter(gapLess(1)).map(_.sequenceNr) mustBe empty
    }
    "persist a temporary gap after the persistence timeout has elapsed" in {
      implicit val g = gaps(6)
      val testEvents = events(1L to 10L)

      testEvents.filter(gapLess(1)).map(_.sequenceNr) mustBe (1L to 5L)

      waitForPersistTimeout()

      testEvents.filter(gapLess(1)).map(_.sequenceNr) mustBe sequenceNrs(1L to 10L)
    }
    "persist consecutive temporary gaps" in {
      implicit val g = gaps(6, 7, 8)
      val testEvents = events(1L to 10L)

      testEvents.filter(gapLess(1)).map(_.sequenceNr) mustBe sequenceNrs(1L to 5L)

      waitForPersistTimeout()

      testEvents.filter(gapLess(1)).map(_.sequenceNr) mustBe sequenceNrs(1L to 10L)
    }
    "return a missing event if it appears within the persistence timeout" in {
      val testEvents = events(1L to 5L)(gaps(3))

      testEvents.filter(gapLess(1)).map(_.sequenceNr) mustBe sequenceNrs(1L to 2L)

      val updated = (testEvents :+ Event(3, Instant.now())).sorted
      updated.filter(gapLess(3)).map(_.sequenceNr) mustBe sequenceNrs(3L to 5L)
    }
    "return a missing event if it appears after the persistence timeout" in {
      val testEvents = events(1L to 5L)(gaps(3))

      testEvents.filter(gapLess(1)).map(_.sequenceNr) mustBe sequenceNrs(1L to 2L)

      waitForPersistTimeout()

      val updated = (testEvents :+ Event(3, Instant.now())).sorted
      updated.filter(gapLess(3)).map(_.sequenceNr) mustBe sequenceNrs(3L to 5L)
    }
    "return events up to the persistent gap even if temporary gaps exist" in {
      implicit val g = gaps(4, 8)
      val testEvents = events(1L to 7L)

      testEvents.filter(gapLess(1)).map(_.sequenceNr) mustBe sequenceNrs(1L to 3L)

      waitForPersistTimeout()

      val updated = testEvents ++ events(9L to 10L)
      updated.filter(gapLess(4)).map(_.sequenceNr) mustBe sequenceNrs(4L to 8L)
      updated.filter(gapLess(1)).map(_.sequenceNr) mustBe sequenceNrs(1L to 8L)
    }
    "persist non-consecutive temporary gaps" in {
      implicit val g = gaps(4, 7)
      val testEvents = events(1L to 10L)

      testEvents.filter(gapLess(1)).map(_.sequenceNr) mustBe sequenceNrs(1L to 3L)

      waitForPersistTimeout()

      testEvents.filter(gapLess(4)).map(_.sequenceNr) mustBe sequenceNrs(4L to 10L)
    }
    "persist highest known gap" in {
      implicit val g = gaps(4, 8)
      val testEvents = events(1L to 10L)

      testEvents.filter(gapLess(1)).map(_.sequenceNr) mustBe sequenceNrs(1L to 3L)

      waitForPersistTimeout()

      testEvents.filter(gapLess(1)).map(_.sequenceNr) mustBe sequenceNrs(1L to 10L)
    }
  }
}
