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

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration

/**
  * A gap-detection filters all events containing successive sequence numbers up to the first gap found in those sequence numbers.
  *
  * If a gap is older than the given persistence timeout, the gap will be persisted and the next event will be included in the result set.
  * If a gap is younger than the persistence timeout, all events appearing after the gap will be dropped.
  */
trait GapDetection {
  def persistenceTimeout: FiniteDuration

  def gapLess[A: Sequenced](fromSeqNr: Long)(implicit sn: Sequenced[A]): A => Boolean
}

/**
  * A gap-detection algorithm which uses processing-timestamps of observed gaps in event-sequences to detect and persist gaps.
  *
  * This algorithm uses a batched approach (i.e. all gaps within an event batch will be persisted at once) providing lower latency per gap.
  *
  * The algorithm selects events from the event-sequence as long as the sequence-numbers are monotonically increasing or gaps found in the
  * sequence-numbers have already been observed at a previous point in time and the given '''persistence timeout''' has elapsed since the
  * first occurrence. These gaps are denoted as '''persistent gaps''' and will not prevent selection of subsequent events in the event-sequence.
  *
  * New gaps within a batch are detected by determining the gap with the highest sequence-number and registering the gap as a '''temporary gap'''
  * together with the timestamp of the observation.
  * Additional temporary gaps may be registered with their respective timestamp by subsequent invocations of the algorithm,
  * even if the current temporary gap has not yet been persisted.
  * This is done to keep the latency produced by gaps with different observation timestamps minimal.
  * With this approach events which are delayed by an earlier temporary gap will not be delayed by detecting a newer temporary gap
  * in a subsequent invocation.
  *
  * Temporary gaps with an occurrence-timestamp older than the persistence timeout are persisted. Only the gap with the highest sequence-number
  * is stored as the current persistent gap.
  * All gaps with a sequence-number smaller than the latest persistent gap are assumed to be missing in the event-sequence and will
  * not stop selection of subsequent events.
  *
  * '''Important:'''
  * [[BatchGapDetection]] and invocations of [[GapDetection.gapLess()]] and the returned [[Function]] are '''not thread-safe'''
  * and perform side-effects as they mutate internal state.
  * An instance of [[BatchGapDetection]] should only be used for '''a single event-source''' as sharing the instance between different
  * event-sources may lead to loss of events.
  */
trait BatchGapDetection extends GapDetection {

  type SequenceNr = Long
  type NanoTimestamp = Long

  private case class GapMarker(sequenceNr: SequenceNr, timestamp: NanoTimestamp)

  private var persistentGap: Option[GapMarker] = None
  private var temporaryGaps: immutable.Seq[GapMarker] = Vector.empty

  def gapLess[A: Sequenced](fromSeqNr: SequenceNr)(implicit sn: Sequenced[A]): A => Boolean = {
    val now = currentTime()
    var lastSeqNr = fromSeqNr - 1
    var gapLess = true

    val (updatedPersistentGaps, updatedTemporaryGaps) = temporaryGaps.partition(timedOutBefore(now))

    persistentGap = updatedPersistentGaps.lastOption.orElse(persistentGap)
    temporaryGaps = updatedTemporaryGaps

    val isPersistedGap = isKnownGapWithin(persistentGap)(_)

    event => {
      val seqNr = sn.sequenceNr(event)

      if (seqNr <= lastSeqNr) {
        false
      } else if (seqNr == lastSeqNr + 1L || isPersistedGap(gap(seqNr))) {
        lastSeqNr = seqNr
        gapLess
      } else {
        temporaryGaps = registerGap(temporaryGaps, gap(seqNr), now)
        lastSeqNr = seqNr
        gapLess = false
        false
      }
    }
  }

  private def currentTime(): NanoTimestamp =
    System.nanoTime()

  private def timedOutBefore(targetTimestamp: NanoTimestamp): GapMarker => Boolean =
    m => m.timestamp + persistenceTimeout.toNanos < targetTimestamp

  private def isKnownGapWithin(gaps: Traversable[GapMarker])(gap: SequenceNr): Boolean =
    gaps.exists(_.sequenceNr >= gap)

  private def gap(sequenceNr: SequenceNr): SequenceNr =
    sequenceNr - 1

  private def registerGap(gaps: immutable.Seq[GapMarker], gap: SequenceNr, ts: NanoTimestamp): immutable.Seq[GapMarker] = {
    if (isKnownGapWithin(gaps)(gap))
      gaps
    else
      gaps.filterNot(_.timestamp == ts) :+ GapMarker(gap, ts)
  }
}
