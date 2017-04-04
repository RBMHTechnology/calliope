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

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, Suite}

trait StreamSpec extends BeforeAndAfterAll with StopSystemAfterAll { this: TestKit with Suite =>
  implicit val materializer = ActorMaterializer()

  override protected def afterAll(): Unit = {
    materializer.shutdown()
    super.afterAll()
  }
}

trait FlowSpec { this: TestKit with StreamSpec =>

  def runFlow[In, Out, M](flow: Flow[In, Out, M]): (TestPublisher.Probe[In], TestSubscriber.Probe[Out]) =
    TestSource.probe[In]
      .via(flow)
      .toMat(TestSink.probe)(Keep.both)
      .run()

  def processNext[In, Out](in: In)(implicit pub: TestPublisher.Probe[In], sub: TestSubscriber.Probe[Out]): Out = {
    sub.request(1)
    pub.sendNext(in)
    sub.expectNext()
  }
}

trait SourceSpec { this: TestKit with StreamSpec =>

  def runSource[Out, M](source: Source[Out, M]): (M, TestSubscriber.Probe[Out]) =
    source.toMat(TestSink.probe)(Keep.both).run()
}
