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

  case class Gaps(sequenceNrs: Int*)

  case class Event(sequenceNr: Long)

  case object Event {
    implicit val sequenced = new Sequenced[Event] {
      override def sequenceNr(event: Event): Long = event.sequenceNr
    }
  }

  val NoGaps = Gaps()

  def sleep(duration: FiniteDuration): Unit = {
    Thread.sleep(duration.toMillis)
  }

  def eventSource(sequenceNrs: immutable.Seq[Int])(implicit gaps: Gaps): immutable.Seq[Event] =
    sequenceNrs.filterNot(gaps.sequenceNrs.contains).map(Event(_))

  def events(sequenceNrs: immutable.Seq[Int]): immutable.Seq[Event] =
    sequenceNrs.map(Event(_))

  def event(sequenceNr: Int): immutable.Seq[Event] =
    events(immutable.Seq(sequenceNr))
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
      implicit val gaps = NoGaps

      filterGapLess(fromSeqNr = 1, eventSource(1 to 5)) mustBe events(1 to 5)
    }
    "select events up to a given gap" in {
      implicit val gaps = Gaps(4)

      filterGapLess(fromSeqNr = 1, eventSource(1 to 5)) mustBe events(1 to 3)
    }
    "select events up to the first gap given consecutive gaps" in {
      implicit val gaps = Gaps(3, 4)

      filterGapLess(fromSeqNr = 1, eventSource(1 to 5)) mustBe events(1 to 2)
    }
    "select no events if a gap at the beginning is detected" in {
      implicit val gaps = Gaps(1)

      filterGapLess(fromSeqNr = 1, eventSource(1 to 5)) mustBe empty
    }
    "persist a gap after the persistence timeout" in {
      implicit val gaps = Gaps(6)

      filterGapLess(fromSeqNr = 1, eventSource(1 to 10)) mustBe events(1 to 5)

      waitForPersistTimeout()

      filterGapLess(fromSeqNr = 1, eventSource(1 to 10)) mustBe events(1 to 5) ++ events(7 to 10)
    }
    "persist a gap only if already detected" in {
      implicit val gaps = Gaps(6)

      waitForPersistTimeout()

      filterGapLess(fromSeqNr = 1, eventSource(1 to 10)) mustBe events(1 to 5)
    }
    "persist consecutive gaps after the persistence timeout" in {
      implicit val gaps = Gaps(6, 7, 8)

      filterGapLess(fromSeqNr = 1, eventSource(1 to 10)) mustBe events(1 to 5)

      waitForPersistTimeout()

      filterGapLess(fromSeqNr = 1, eventSource(1 to 10)) mustBe events(1 to 5) ++ events(9 to 10)
    }
    "select a missing event if it appears within the persistence timeout" in {
      filterGapLess(fromSeqNr = 1, eventSource(1 to 5)(Gaps(3))) mustBe events(1 to 2)

      filterGapLess(fromSeqNr = 3, eventSource(3 to 5)(NoGaps)) mustBe events(3 to 5)
    }
    "select a missing event within a consecutive gap if it appears within the persistence timeout" in {
      filterGapLess(fromSeqNr = 1, eventSource(1 to 5)(Gaps(3, 4))) mustBe events(1 to 2)

      filterGapLess(fromSeqNr = 3, eventSource(3 to 5)(Gaps(4))) mustBe event(3)

      waitForPersistTimeout()

      filterGapLess(fromSeqNr = 4, eventSource(4 to 5)(Gaps(4))) mustBe event(5)
    }
    "select a missing event if it appears after the persistence timeout" in {
      filterGapLess(fromSeqNr = 1, eventSource(1 to 5)(Gaps(3))) mustBe events(1 to 2)

      waitForPersistTimeout()

      filterGapLess(fromSeqNr = 3, eventSource(3 to 5)(NoGaps)) mustBe events(3 to 5)
    }
    "select all events after at once after all gaps were persisted in a previous invocation" in {
      implicit val gaps = Gaps(4, 8)

      filterGapLess(fromSeqNr = 1, eventSource(1 to 5)) mustBe events(1 to 3)

      waitForPersistTimeout()

      filterGapLess(fromSeqNr = 4, eventSource(4 to 9)) mustBe events(5 to 7)

      waitForPersistTimeout()

      filterGapLess(fromSeqNr = 1, eventSource(1 to 10)) mustBe events(1 to 3) ++ events(5 to 7) ++ events(9 to 10)
    }
    "select events up to a persisted gap even if gaps exist" in {
      implicit val gaps = Gaps(4, 8)

      filterGapLess(fromSeqNr = 1, eventSource(1 to 5)) mustBe events(1 to 3)

      waitForPersistTimeout()

      filterGapLess(fromSeqNr = 4, eventSource(4 to 10)) mustBe events(5 to 7)

      filterGapLess(fromSeqNr = 1, eventSource(1 to 5)) mustBe events(1 to 3) ++ event(5)
    }
    "persist non-consecutive gaps" in {
      implicit val gaps = Gaps(4, 7)

      filterGapLess(fromSeqNr = 1, eventSource(1 to 10)) mustBe events(1 to 3)

      waitForPersistTimeout()

      filterGapLess(fromSeqNr = 4, eventSource(4 to 10)) mustBe events(5 to 6) ++ events(8 to 10)
    }
    "persist non-consecutive gaps for multiple selects" in {
      implicit val gaps = Gaps(3, 5, 8, 9)

      filterGapLess(fromSeqNr = 1, eventSource(1 to 6)) mustBe events(1 to 2)
      filterGapLess(fromSeqNr = 1, eventSource(1 to 10)) mustBe events(1 to 2)

      waitForPersistTimeout()

      filterGapLess(fromSeqNr = 3, eventSource(3 to 6)) mustBe event(4) ++ event(6)
      filterGapLess(fromSeqNr = 3, eventSource(3 to 10)) mustBe event(4) ++ events(6 to 7) ++ event(10)
    }
    "persist highest known gap" in {
      implicit val gaps = Gaps(4, 8)

      filterGapLess(fromSeqNr = 1, eventSource(1 to 10)) mustBe events(1 to 3)
      filterGapLess(fromSeqNr = 1, eventSource(1 to 5)) mustBe events(1 to 3)

      waitForPersistTimeout()

      filterGapLess(fromSeqNr = 1, eventSource(1 to 10)) mustBe events(1 to 3) ++ events(5 to 7) ++ events(9 to 10)

      filterGapLess(fromSeqNr = 1, eventSource(1 to 5)) mustBe events(1 to 3) ++ event(5)
    }
    "persist gaps as soon as the persistence-timeout has been reached even if newer gaps are found" in {
      implicit val gaps = Gaps(3, 4, 7, 11)

      filterGapLess(fromSeqNr = 1, eventSource(1 to 10)) mustBe events(1 to 2)

      sleep(timeout / 2)

      filterGapLess(fromSeqNr = 3, eventSource(3 to 12)) mustBe empty

      sleep(timeout / 2)

      filterGapLess(fromSeqNr = 3, eventSource(3 to 12)) mustBe events(5 to 6) ++ events(8 to 10)

      sleep(timeout / 2)

      filterGapLess(fromSeqNr = 11, eventSource(11 to 12)) mustBe event(12)
    }
  }
}

