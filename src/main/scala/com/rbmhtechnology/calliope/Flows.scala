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
import akka.stream.scaladsl.{Flow, Source}

object Flows {
  def groupBy[A, B, K](maxSubStreams: Int, key: A => K, via: K => Flow[A, B, NotUsed]): Flow[A, B, NotUsed] =
    Flow[A].groupBy(maxSubStreams, key).prefixAndTail(1).flatMapMerge(maxSubStreams, {
      case (h, t) => Source(h).concat(t).via(via(key(h.head)))
    }).mergeSubstreams
}
