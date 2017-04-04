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
import akka.stream.scaladsl.{BidiFlow, Flow}

import scala.collection.immutable.{Seq, SortedMap}

case class Sequenced[A](message: A, sequenceNr: Long)

object Sequenced {
  def apply[A, B]: BidiFlow[A, Sequenced[A], Sequenced[B], B, NotUsed] =
    BidiFlow.fromFlows(sequencer, resequencer)

  def sequencer[A]: Flow[A, Sequenced[A], NotUsed] =
    Flow[A].map {
      var sequenceNr: Long = -1L
      a => {
        sequenceNr = sequenceNr + 1L
        Sequenced(a, sequenceNr)
      }
    }

  def resequencer[A]: Flow[Sequenced[A], A, NotUsed] =
    Flow[Sequenced[A]].mapConcat {
      var resequencer = Resequencer[A]()
      sa => {
        val (r, resequenced) = resequencer.resequence(sa)
        resequencer = r
        resequenced
      }
    }
}

private case class Resequencer[A](candidates: SortedMap[Long, Sequenced[A]] = SortedMap.empty[Long, Sequenced[A]], lastDeliveredSequenceNr: Long = -1L) {
  def resequence(sequenced: Sequenced[A]): (Resequencer[A], Seq[A]) =
    resequence(sequenced, candidates, lastDeliveredSequenceNr)

  private def resequence(sequenced: Sequenced[A], candidates: SortedMap[Long, Sequenced[A]], lastDeliveredSequenceNr: Long, resequenced: Seq[A] = Seq.empty): (Resequencer[A], Seq[A]) = {
    val currSequenceNr = sequenced.sequenceNr
    val nextSequenceNr = currSequenceNr + 1L

    if (sequenced.sequenceNr == lastDeliveredSequenceNr + 1L) {
      val resequencedUpdated = resequenced :+ sequenced.message
      candidates.get(nextSequenceNr) match {
        case Some(s) => resequence(s, candidates - nextSequenceNr, currSequenceNr, resequencedUpdated)
        case None    => (copy(candidates, currSequenceNr), resequencedUpdated)
      }
    } else (copy(candidates.updated(sequenced.sequenceNr, sequenced)), Seq.empty)
  }
}

