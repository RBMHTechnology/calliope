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

import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpecLike}

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration

object GapDetectionSpec {

  object GapCollection {
    def apply(gaps: Long*): GapCollection =
      new GapCollection(gaps.toVector)

    val empty: GapCollection =
      new GapCollection(immutable.Seq.empty)
  }

  case class GapCollection(gaps: immutable.Seq[Long])

  case class Event(sequenceNr: Long)

  case object Event {
    implicit val sequenced = new Sequenced[Event] {
      override def sequenceNr(event: Event): Long = event.sequenceNr
    }
  }

  def sleep(duration: FiniteDuration): Unit = {
    Thread.sleep(duration.toMillis)
  }

  def gaps(elements: Long*): GapCollection =
    GapCollection(elements: _*)

  def events(sequenceNrs: immutable.Seq[Int])(implicit gapCollection: GapCollection = GapCollection.empty): immutable.Seq[Event] =
    sequenceNrs.filterNot(gapCollection.gaps.contains).map(Event(_))
}

class BatchGapDetectionSpec extends WordSpecLike with MustMatchers with BeforeAndAfterEach {

  import GapDetectionSpec._

  import scala.concurrent.duration._
  import scala.language.postfixOps

  val timeout: FiniteDuration = 100 millis

  implicit var gd: GapDetection = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    gd = gapDetection()
  }

  def gapDetection(): GapDetection = new BatchGapDetection {
    override def persistenceTimeout: FiniteDuration = timeout
  }

  def waitForPersistTimeout(): Unit = {
    sleep(timeout)
  }

  def filterGapLess[E: Sequenced](fromSeqNr: Long, events: immutable.Seq[E])(implicit gapDetection: GapDetection): immutable.Seq[E] =
    events.filter(gapDetection.gapLess(fromSeqNr))

  "A batch gap-detection" must {
    "select all events if no gaps exist" in {
      implicit val gaps = GapCollection.empty

      filterGapLess(1, events(1 to 5)) mustBe events(1 to 5)
    }
    "select events up to a given gap" in {
      implicit val g = gaps(4)

      filterGapLess(1, events(1 to 5)) mustBe events(1 to 3)
    }
    "select events up to the first gap given consecutive gaps" in {
      implicit val g = gaps(3, 4)

      filterGapLess(1, events(1 to 5)) mustBe events(1 to 2)
    }
    "select no events if a gap at the beginning is detected" in {
      implicit val g = gaps(1)

      filterGapLess(1, events(1 to 5)) mustBe empty
    }
    "persist a gap after the persistence timeout" in {
      implicit val g = gaps(6)

      filterGapLess(1, events(1 to 10)) mustBe events(1 to 5)

      waitForPersistTimeout()

      filterGapLess(1, events(1 to 10)) mustBe events(1 to 10)
    }
    "persist a gap only if already detected" in {
      implicit val g = gaps(6)

      waitForPersistTimeout()

      filterGapLess(1, events(1 to 10)) mustBe events(1 to 5)
    }
    "persist consecutive gaps after the persistence timeout" in {
      implicit val g = gaps(6, 7, 8)

      filterGapLess(1, events(1 to 10)) mustBe events(1 to 5)

      waitForPersistTimeout()

      filterGapLess(1, events(1 to 10)) mustBe events(1 to 10)
    }
    "select a missing event if it appears within the persistence timeout" in {
      filterGapLess(1, events(1 to 5)(gaps(3))) mustBe events(1 to 2)

      filterGapLess(3, events(3 to 5)) mustBe events(3 to 5)
    }
    "select a missing event within a consecutive gap if it appears within the persistence timeout" in {
      filterGapLess(1, events(1 to 5)(gaps(3, 4))) mustBe events(1 to 2)

      filterGapLess(3, events(3 to 5)(gaps(4))) mustBe events(3 to 3)

      waitForPersistTimeout()

      filterGapLess(4, events(4 to 5)(gaps(4))) mustBe events(5 to 5)
    }
    "select a missing event if it appears after the persistence timeout" in {
      filterGapLess(1, events(1 to 5)(gaps(3))) mustBe events(1 to 2)

      waitForPersistTimeout()

      filterGapLess(3, events(3 to 5)) mustBe events(3 to 5)
    }
    "select all events after at once after all gaps were persisted in a previous invocation" in {
      implicit val g = gaps(4, 8)

      filterGapLess(1, events(1 to 5)) mustBe events(1 to 3)

      waitForPersistTimeout()

      filterGapLess(4, events(4 to 9)) mustBe events(5 to 7)

      waitForPersistTimeout()

      filterGapLess(1, events(1 to 10)) mustBe events(1 to 10)
    }
    "select events up to a persisted gap even if gaps exist" in {
      implicit val g = gaps(4, 8)

      filterGapLess(1, events(1 to 5)) mustBe events(1 to 3)

      waitForPersistTimeout()

      filterGapLess(4, events(4 to 10)) mustBe events(5 to 7)
      filterGapLess(1, events(1 to 5)) mustBe events(1 to 5)
    }
    "persist non-consecutive gaps" in {
      implicit val g = gaps(4, 7)

      filterGapLess(1, events(1 to 10)) mustBe events(1 to 3)

      waitForPersistTimeout()

      filterGapLess(4, events(4 to 10)) mustBe events(4 to 10)
    }
    "persist non-consecutive gaps for multiple selects" in {
      implicit val g = gaps(3, 5, 8, 9)

      filterGapLess(1, events(1 to 6)) mustBe events(1 to 2)
      filterGapLess(1, events(1 to 10)) mustBe events(1 to 2)

      waitForPersistTimeout()

      filterGapLess(3, events(3 to 6)) mustBe events(3 to 6)
      filterGapLess(3, events(3 to 10)) mustBe events(3 to 10)
    }
    "persist highest known gap" in {
      implicit val g = gaps(4, 8)

      filterGapLess(1, events(1 to 10)) mustBe events(1 to 3)
      filterGapLess(1, events(1 to 5)) mustBe events(1 to 3)

      waitForPersistTimeout()

      filterGapLess(1, events(1 to 10)) mustBe events(1 to 10)
      filterGapLess(1, events(1 to 5)) mustBe events(1 to 5)
    }
    "persist gaps as soon as the persistence-timeout has been reached even if newer gaps are found" in {
      implicit val g = gaps(3, 4, 7, 11)

      filterGapLess(1, events(1 to 10)) mustBe events(1 to 2)

      sleep(timeout / 2)

      filterGapLess(3, events(3 to 12)) mustBe empty

      sleep(timeout / 2)

      filterGapLess(3, events(3 to 12)) mustBe events(3 to 10)

      waitForPersistTimeout()

      filterGapLess(11, events(11 to 12)) mustBe events(11 to 12)
    }
  }
}

