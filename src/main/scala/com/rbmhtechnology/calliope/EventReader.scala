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

import scala.concurrent.{ExecutionContext, Future}

/**
  * Reads events from arbitrary sources.
  */
trait EventReader[A] {
  def readEvents(fromSequenceNr: Long, maxItems: Int): Future[Seq[EventRecord[A]]]
}

/**
  * Mixin for applying an arbitrary gap-detection to an [[EventReader]].
  */
trait GapDetectionEventReader[A] extends EventReader[A] with GapDetection {
  implicit def ec: ExecutionContext

  abstract override def readEvents(fromSequenceNr: Long, maxItems: Int): Future[Seq[EventRecord[A]]] = {
    super.readEvents(fromSequenceNr, maxItems).map(_.filter(gapLess(fromSequenceNr)))
  }
}

/**
  * Mixin for applying a gap-detection based on [[TimestampGapDetection]] to an [[EventReader]].
  */
trait ReaderTimestampGapDetection[A] extends GapDetectionEventReader[A] with TimestampGapDetection