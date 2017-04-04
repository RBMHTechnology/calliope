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
import akka.stream.scaladsl.BidiFlow
import akka.stream.{Attributes, BidiShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.collection.immutable.Seq

trait ProcessorLogic[EVT, REQ, RES] {
  def onRequest(r: REQ): (Seq[EVT], () => RES)
  def onEvent(e: EVT): Unit
}

import Processor._
object Processor {
  sealed trait Recovery[+A]
  case object Recovered extends Recovery[Nothing]
  case class Received[A](event: A) extends Recovery[A]

  private case class Cycle[EVT, RES](events: Seq[EVT], reply: () => Sequenced[RES])

  def apply[ID, EVT, REQ, RES](logic: ProcessorLogic[EVT, REQ, RES])(implicit event: Event[EVT, ID]): BidiFlow[REQ, EVT, Recovery[EVT], RES, NotUsed] =
    BidiFlow.fromFunctions[REQ, Sequenced[REQ], Sequenced[RES], RES](Sequenced(_, 0L), _.message).atop(BidiFlow.fromGraph(new Processor(logic)))
}

private class Processor[ID, EVT, REQ, RES](logic: ProcessorLogic[EVT, REQ, RES])(implicit event: Event[EVT, ID]) extends GraphStage[BidiShape[Sequenced[REQ], EVT, Recovery[EVT], Sequenced[RES]]] {
  import Processor._

  val i1 = Inlet[Sequenced[REQ]]("Processor.i1")
  val i2 = Inlet[Recovery[EVT]]("Processor.i2")
  val o1 = Outlet[EVT]("Processor.o1")
  val o2 = Outlet[Sequenced[RES]]("Processor.o2")

  val shape = BidiShape.of(i1, o1, i2, o2)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private var cycle: Option[Cycle[EVT, RES]] = None
      private var recovered = false

      import event._

      setHandler(i1, new InHandler {
        override def onPush(): Unit = {
          val seq = grab(i1)
          val (evts, reply) = logic.onRequest(seq.message)
          val sreply = () => Sequenced(reply(), seq.sequenceNr)
          if (evts.isEmpty)
            push(o2, sreply())
          else {
            cycle = Some(Cycle(evts, sreply))
            emitMultiple(o1, evts)
          }
        }
      })

      setHandler(i2, new InHandler {
        override def onPush(): Unit = {
          val grabbed = grab(i2)
          pull(i2)
          grabbed match {
            case Received(evt) =>
              logic.onEvent(evt)
              cycle.foreach { p =>
                if (eventId(p.events.last) == eventId(evt)) {
                  push(o2, p.reply())
                  cycle = None
                }
              }
            case Recovered =>
              if (!recovered) {
                recovered = true
                if (isAvailable(o1) && isAvailable(o2)) pull(i1)
              }
          }
        }
      })

      setHandler(o1, new OutHandler {
        override def onPull(): Unit = {
          if (recovered && isAvailable(o2) && cycle.isEmpty) pull(i1)
        }
      })

      setHandler(o2, new OutHandler {
        override def onPull(): Unit = {
          if (recovered && isAvailable(o1) && cycle.isEmpty) pull(i1)
        }
      })

      override def preStart(): Unit = {
        pull(i2)
      }
    }
}

