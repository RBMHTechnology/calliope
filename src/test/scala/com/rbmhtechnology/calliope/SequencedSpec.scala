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

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.collection.immutable.Seq

class SequencedSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with BeforeAndAfterAll {
  implicit val materializer = ActorMaterializer()

  override def afterAll(): Unit = {
    materializer.shutdown()
    TestKit.shutdownActorSystem(system)
  }

  def resequencerProbes: (TestPublisher.Probe[Sequenced[String]], TestSubscriber.Probe[String]) =
    TestSource.probe[Sequenced[String]].via(Sequenced.resequencer[String]).toMat(TestSink.probe[String])(Keep.both).run()

  "A resequencer flow" must {
    "resequence sequenced elements" in {
      val (pub, sub) = resequencerProbes

      sub.request(10)

      pub.sendNext(Sequenced("h", 7))
      pub.sendNext(Sequenced("a", 0))
      pub.sendNext(Sequenced("b", 1))
      pub.sendNext(Sequenced("f", 5))
      pub.sendNext(Sequenced("d", 3))
      pub.sendNext(Sequenced("e", 4))
      pub.sendNext(Sequenced("j", 9))
      pub.sendNext(Sequenced("g", 6))
      pub.sendNext(Sequenced("c", 2))
      pub.sendNext(Sequenced("i", 8))

      sub.expectNextN(Seq("a", "b", "c", "d", "e", "f", "g", "h", "i", "j"))
    }
  }
}
