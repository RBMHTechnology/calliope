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

import akka.stream.ThrottleMode
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
  * The EventDeletion factory offers Akka Streams Flows and Sinks which delete events through an instance of [[EventDeleter]].
  * The stages accept elements which implement [[Sequenced]] and pass the sequence number of the element to the deletion request sent to the [[EventDeleter]].
  */
object EventDeletion {
  import scala.concurrent.ExecutionContext.Implicits.global

  def flow[A: Sequenced](eventDeleter: EventDeleter, perSize: Int)(implicit sequenced: Sequenced[A]): Flow[A, A, NotUsed] =
    Flow[A]
      .grouped(perSize)
      .mapAsync(1)(elems => eventDeleter.deleteEvents(sequenced.sequenceNr(elems.last)).map(_ => elems))
      .mapConcat(identity)

  def sink[A: Sequenced](eventDeleter: EventDeleter, perSize: Int): Sink[A, Future[Done]] =
    flow(eventDeleter, perSize)
      .toMat(Sink.ignore)(Keep.right)

  def sink[A: Sequenced](eventDeleter: EventDeleter, perDuration: FiniteDuration): Sink[A, Future[Done]] =
    Flow[A]
      .conflate((_, latest) => latest)
      .throttle(1, perDuration, 1, ThrottleMode.Shaping)
      .via(flow(eventDeleter, 1))
      .toMat(Sink.ignore)(Keep.right)
}
