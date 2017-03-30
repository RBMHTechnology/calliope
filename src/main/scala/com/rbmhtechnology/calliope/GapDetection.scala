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

import scala.concurrent.duration.FiniteDuration

/**
  * A gap-detection filters all events containing successive sequence numbers up to the first gap found in those sequence numbers.
  *
  * If a gap is older than the given persistence timeout, the gap will be persisted and the next event will be included in the result set.
  * If a gap is younger than the persistence timeout, all events appearing after the gap will be dropped.
  */
trait GapDetection {
  def persistenceTimeout: FiniteDuration

  def gapLess[A: Sequenced : Timestamped](fromSeqNr: Long)(implicit sn: Sequenced[A], ts: Timestamped[A]): A => Boolean
}

/**
  * A gap-detection algorithm which uses timestamps of events to detect and persist gaps.
  *
  * This algorithm should only be used if the application can guarantee that the clock used to create the event-timestamps
  * and the clock used to apply the gap-detection are the same or synchronized.
  */
trait TimestampGapDetection extends GapDetection {
  def gapLess[A: Sequenced : Timestamped](fromSeqNr: Long)(implicit sn: Sequenced[A], ts: Timestamped[A]): A => Boolean = {
    def olderThan(duration: FiniteDuration)(event: A)(implicit referenceTs: Instant): Boolean =
      referenceTs.toEpochMilli - ts.timestamp(event).toEpochMilli >= duration.toMillis

    implicit val now = Instant.now()
    var lastSeqNr = fromSeqNr - 1

    event => {
      val seqNr = sn.sequenceNr(event)

      if (seqNr <= lastSeqNr) {
        false
      }
      else if (seqNr == lastSeqNr + 1 || olderThan(persistenceTimeout)(event)) {
        lastSeqNr = seqNr
        true
      }
      else {
        false
      }
    }
  }
}
