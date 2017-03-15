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

import org.scalatest._

import scala.collection.immutable.SortedSet

class SequenceSpec extends WordSpec with Matchers {
  import Sequence._

  "A sequence stream" must {
    "generate sequence numbers from scratch" in {
      stream(-1).take(3).toList should be(List(0L, 1L, 2L))
    }
    "generate sequence numbers from specified messageSequenceNrSnapshot" in {
      stream(4).take(3).toList should be(List(5L, 6L, 7L))
    }
    "generate sequence numbers from specified messageSequenceNrSnapshot and messageSequenceNrs" in {
      stream(4, SortedSet(6, 9)).take(4).toList should be(List(5L, 7L, 8L, 10L))
    }
    "generate sequence numbers from specified messageSequenceNrSnapshot and messageSequenceNrs with duplicates" in {
      stream(4, SortedSet(1, 4, 6, 4)).take(3).toList should be(List(5L, 7L, 8L))
    }
  }
}
