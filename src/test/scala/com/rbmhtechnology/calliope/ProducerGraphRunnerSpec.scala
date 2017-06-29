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

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, ActorSystem, Kill, OneForOneStrategy, Props, SupervisorStrategy}
import akka.pattern
import akka.pattern.ask
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{KillSwitches, OverflowStrategy}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import com.rbmhtechnology.calliope.scaladsl.ProducerGraphRunner
import com.rbmhtechnology.calliope.scaladsl.TransactionalEventProducer.ProducerGraph
import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object ProducerGraphRunnerSpec {

  object RestartingGraphRunnerSupervisor {
    def props(graph: ProducerGraph): Props =
      Props(new RestartingGraphRunnerSupervisor(graph))
  }

  class RestartingGraphRunnerSupervisor(graph: ProducerGraph) extends Actor {

    override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
      case _ => Restart
    }

    val graphRunner = context.actorOf(ProducerGraphRunner.props(graph))

    override def receive: Receive = {
      case "fail" =>
        graphRunner ! Kill

      case msg =>
        graphRunner forward msg
    }
  }

}

class ProducerGraphRunnerSpec extends TestKit(ActorSystem("test"))
  with WordSpecLike with MustMatchers with StopSystemAfterAll with BeforeAndAfterEach with ImplicitSender {

  import ProducerGraphRunner._
  import ProducerGraphRunnerSpec._
  import system.dispatcher

  implicit val timeout = Timeout(1.second)

  var eventProbe: TestProbe = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    eventProbe = TestProbe()
  }

  def producerGraph(probe: TestProbe): ProducerGraph =
    Source.queue[Unit](1, OverflowStrategy.backpressure)
      .viaMat(KillSwitches.single)(Keep.both)
      .toMat(Sink.foreach { ev => probe.ref ! ev })(Keep.both)

  def producerGraphRunner(probe: TestProbe): ActorRef =
    system.actorOf(RestartingGraphRunnerSupervisor.props(producerGraph(probe)))

  def runningProducerGraphRunner(probe: TestProbe): ActorRef = {
    val runner = producerGraphRunner(probe)
    runner ! RunGraph
    expectMsg(GraphStarted)
    runner
  }

  def pollForState(runner: ActorRef, state: State, interval: FiniteDuration = 100.millis)(implicit timeout: Timeout): Future[State] = {
    (runner ? GetState).mapTo[State]
      .flatMap {
        case `state` => Future.successful(state)
        case _ => pattern.after(interval, system.scheduler) {
          pollForState(runner, state)
        }
      }
  }

  "A ProducerGraphRunner" when {
    "running" must {
      "run the event producer stream" in {
        val runner = runningProducerGraphRunner(eventProbe)

        runner ! NotifyCommit
        eventProbe.receiveN(1)
      }
      "stop the event producer stream if requests" in {
        val runner = runningProducerGraphRunner(eventProbe)

        runner ! StopGraph
        expectMsg(GraphStopped)

        runner ! NotifyCommit
        eventProbe.expectNoMsg(1.second)
      }
      "continue running the event producer stream if started again" in {
        val runner = runningProducerGraphRunner(eventProbe)

        runner ! RunGraph
        expectMsg(GraphStarted)

        runner ! NotifyCommit
        eventProbe.receiveN(1)
      }
    }
    "stopped" must {
      "stop the event producer stream" in {
        val runner = producerGraphRunner(eventProbe)

        runner ! NotifyCommit
        eventProbe.expectNoMsg(1.second)
      }
      "start the event producer stream if requested" in {
        val runner = producerGraphRunner(eventProbe)

        runner ! RunGraph
        expectMsg(GraphStarted)

        runner ! NotifyCommit
        eventProbe.receiveN(1)
      }
      "continue to stop the event producer stream if stopped again" in {
        val runner = producerGraphRunner(eventProbe)

        runner ! StopGraph
        expectMsg(GraphStopped)

        runner ! NotifyCommit
        eventProbe.expectNoMsg(1.second)
      }
    }
    "failing" must {
      "restart the event producer stream" in {
        val runner = runningProducerGraphRunner(eventProbe)

        runner ! NotifyCommit
        eventProbe.receiveN(1)

        runner ! "fail"

        Await.ready(pollForState(runner, Running), 1.second)

        runner ! NotifyCommit
        runner ! NotifyCommit
        eventProbe.receiveN(2)
      }
    }
  }
}
