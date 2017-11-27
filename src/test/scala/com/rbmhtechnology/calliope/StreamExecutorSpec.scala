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

import java.time.Duration
import java.util.concurrent.CompletionStage

import akka.{Done, NotUsed}
import akka.actor.{ActorRef, ActorSystem}
import akka.stream.javadsl.{Keep, RunnableGraph, Sink}
import akka.stream.scaladsl.Source
import akka.testkit.{TestKit, TestProbe}
import com.rbmhtechnology.calliope.StreamExecutor.{RunStream, StreamStarted}
import com.rbmhtechnology.calliope.StreamExecutorSupervision.Decider
import com.rbmhtechnology.calliope.StreamExecutorSupervision.Directive.STOP
import org.reactivestreams.{Publisher, Subscriber}
import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpecLike}

import scala.collection.immutable
import scala.concurrent.duration._

object StreamExecutorSpec {

  case class ReplayProducer[A]() extends Publisher[A] {

    private var subscribers: immutable.Seq[Subscriber[_ >: A]] = Vector.empty
    private var emissions: immutable.Seq[A] = Vector.empty

    override def subscribe(s: Subscriber[_ >: A]): Unit = {
      subscribers = subscribers :+ s
      emissions.foreach(s.onNext(_))
    }

    def sendNext(element: A): Unit = {
      emissions = emissions :+ element
      subscribers.foreach(_.onNext(element))
    }

    def sendError(err: Throwable): Unit = {
      subscribers.foreach(_.onError(err))
    }

    def sendComplete(): Unit = {
      subscribers.foreach(_.onComplete())
    }
  }
}

class StreamExecutorSpec extends TestKit(ActorSystem("test")) with WordSpecLike with MustMatchers with BeforeAndAfterEach with StreamSpec {

  import StreamExecutorSpec._

  private val supervision = StreamExecutorSupervision.withBackoff(Duration.ofMillis(50), Duration.ofMillis(100), 0.2)

  var consumer: TestProbe = _
  var producer: ReplayProducer[String] = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()

    consumer = TestProbe()
    producer = ReplayProducer[String]()
  }

  def streamExecutor[A](producer: Publisher[A], consumer: TestProbe, supervision: StreamExecutorSupervision = supervision): ActorRef =
    system.actorOf(StreamExecutor.props(actorConsumerGraph(producer, consumer.ref), supervision))

  def actorConsumerGraph[A](producer: Publisher[A], consumer: ActorRef): RunnableGraph[CompletionStage[Done]] =
    Source.fromPublisher(producer)
      .map { m =>
        consumer.tell(m, ActorRef.noSender)
        m
      }
      .asJava
      .toMat(Sink.ignore, Keep.right[NotUsed, CompletionStage[Done]])

  def runStream(streamExecutor: ActorRef): ActorRef = {
    val complete = TestProbe()
    streamExecutor.tell(RunStream.instance, complete.ref)
    complete.expectMsg(StreamStarted.instance)
    streamExecutor
  }

  "A StreamExecutor" when {
    "started" must {
      "run the underlying stream" in {
        runStream(streamExecutor(producer, consumer))

        producer.sendNext("event-1")
        consumer.expectMsg("event-1")
      }
    }
    "stream completes with failure" must {
      "restart the stream" in {
        runStream(streamExecutor(producer, consumer))

        producer.sendNext("event-1")
        consumer.expectMsg("event-1")

        producer.sendError(new RuntimeException("error"))

        producer.sendNext("event-2")
        consumer.expectMsg("event-1")
        consumer.expectMsg("event-2")
      }
    }
    "stream completes successfully" must {
      "restart the stream" in {
        runStream(streamExecutor(producer, consumer))

        producer.sendNext("event-1")
        consumer.expectMsg("event-1")

        producer.sendComplete()

        producer.sendNext("event-2")
        consumer.expectMsg("event-1")
        consumer.expectMsg("event-2")
      }
    }
    "stopping-directive selected by supervision-decider" must {
      "stop the stream" in {
        val stoppingSupervision = supervision.withDecider(new Decider {
          override def apply(t: Throwable): StreamExecutorSupervision.Directive = STOP
        })
        runStream(streamExecutor(producer, consumer, stoppingSupervision))

        producer.sendNext("event-1")
        consumer.expectMsg("event-1")

        producer.sendError(new RuntimeException("error"))

        producer.sendNext("event-2")
        consumer.expectNoMsg(1.second)
      }
    }
  }
}
