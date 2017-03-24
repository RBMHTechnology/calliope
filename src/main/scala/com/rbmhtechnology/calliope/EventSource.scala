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
import akka.actor.Cancellable
import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, Source, SourceQueueWithComplete}
import akka.stream.stage._

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * An EventSource is an Akka Streams Source which reads events from an [[EventReader]] and emits those events downstream on demand.
  *
  * The source may be materialized to a SourceQueue which can be used to signal new events at the source.
  * If downstream demand exists and no source updates are emitted through the SourceQueue, periodic polling is used to query for event updates.
  */
object EventSource {
  def apply[Out](eventReader: EventReader[Out], bufferSize: Int, pollingInterval: FiniteDuration): Source[EventRecord[Out], SourceQueueWithComplete[Unit]] = {
    val ticks = Source.tick(pollingInterval, pollingInterval, Unit)
    val commitQueue = Source.queue[Unit](1, OverflowStrategy.dropHead)

    ticks.mergeMat(commitQueue)(Keep.right)
      .buffer(1, OverflowStrategy.backpressure)
      .via(EventReadFlow(eventReader, bufferSize))
  }
}

object EventReadFlow {
  def apply[In, Out](eventReader: EventReader[Out], bufferSize: Int): Flow[In, EventRecord[Out], NotUsed] =
    Flow.fromGraph(new EventReadFlow[In, Out](eventReader, bufferSize))
}

final class EventReadFlow[In, Out](eventReader: EventReader[Out], bufferSize: Int) extends GraphStage[FlowShape[In, EventRecord[Out]]] {
  private[this] val in = Inlet[In]("EventReadFlow.in")
  private[this] val out = Outlet[EventRecord[Out]]("EventReadFlow.out")

  override def shape: FlowShape[In, EventRecord[Out]] = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private[this] var lastSequenceNr = 0L
      private[this] var fetchCallback: (Try[Seq[EventRecord[Out]]]) => Unit = _

      override def preStart(): Unit = {
        val ac = getAsyncCallback[Try[Seq[EventRecord[Out]]]] {
          case Failure(err) => fail(out, err)
          case Success(events) =>
            if (events.isEmpty) {
              pull(in)
            } else {
              lastSequenceNr = events.last.sequenceNr
              emitMultiple(out, events.toIterator)
            }
        }
        fetchCallback = ac.invoke
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          fetchRecords(lastSequenceNr)
        }
      })

      private def fetchRecords(snr: Long): Unit = {
        implicit val ec = materializer.executionContext
        eventReader.readEvents(snr + 1, bufferSize).map(_.take(bufferSize)).onComplete(fetchCallback)
      }
    }
}
