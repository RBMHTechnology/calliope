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
import org.scalatest._

import scala.collection.immutable.{Map, Seq, SortedSet}

object ResequenceSpec {
  case class Message(partition: Int, offset: Long, payload: Payload)
  case class Payload(emitter: String, sequenceNr: Long)

  def messageSourceDecoder(m: Message): String =
    m.payload.emitter

  def messageSequenceNrDecoder(m: Message): Long =
    m.payload.sequenceNr

  def storagePartitionDecoder(m: Message): Int =
    m.partition

  def storageSequenceNrDecoder(m: Message): Long =
    m.offset
}

class ResequenceSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  import ResequenceSpec._

  implicit val materializer = ActorMaterializer()

  override def afterAll(): Unit = {
    materializer.shutdown()
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  def probes(committedMessageSequenceNrs: Map[String, Long],
             committedStorageSequenceNr: Map[Int, Long]): (TestPublisher.Probe[Message], TestSubscriber.Probe[(Message, ResequenceProgressIncrement[Int])]) = {
    TestSource.probe[Message]
      .via(new Resequence(messageSourceDecoder, messageSequenceNrDecoder, storagePartitionDecoder, storageSequenceNrDecoder, committedMessageSequenceNrs, committedStorageSequenceNr))
      .toMat(TestSink.probe[(Message, ResequenceProgressIncrement[Int])])(Keep.both)
      .run()
  }

  "A Resequence stage" must {
    "resequence a stream using sequence numbers from a single source" in {
      val messages = Seq(
        Message(0, 0, Payload("e1", 0)),
        Message(0, 1, Payload("e1", 3)),
        Message(0, 2, Payload("e1", 4)),
        Message(0, 3, Payload("e1", 1)),
        Message(0, 4, Payload("e1", 2)),
        Message(0, 5, Payload("e1", 5)))

      val (src, snk) = probes(Map.empty, Map.empty)

      snk.request(messages.size)
      messages.foreach(src.sendNext)

      val actual = snk.expectNextN(messages.size)
      val expected = Seq(
        (Message(0, 0, Payload("e1", 0)), ResequenceProgressIncrement(0, 0)),
        (Message(0, 3, Payload("e1", 1)), ResequenceProgressIncrement(0, 0)),
        (Message(0, 4, Payload("e1", 2)), ResequenceProgressIncrement(0, 0)),
        (Message(0, 1, Payload("e1", 3)), ResequenceProgressIncrement(0, 1)),
        (Message(0, 2, Payload("e1", 4)), ResequenceProgressIncrement(0, 4)),
        (Message(0, 5, Payload("e1", 5)), ResequenceProgressIncrement(0, 5)))

      actual should be(expected)
    }
    "resequence a stream using sequence numbers from multiple sources" in {
      val messages = Seq(
        Message(0, 0, Payload("e1", 1)),
        Message(0, 1, Payload("e2", 0)),
        Message(0, 2, Payload("e1", 0)),
        Message(0, 3, Payload("e2", 2)),
        Message(0, 4, Payload("e1", 2)),
        Message(0, 5, Payload("e2", 1)))

      val (src, snk) = probes(Map.empty, Map.empty)

      snk.request(messages.size)
      messages.foreach(src.sendNext)

      val actual = snk.expectNextN(messages.size)
      val expected = Seq(
        (Message(0, 1, Payload("e2", 0)), ResequenceProgressIncrement(0, -1)),
        (Message(0, 2, Payload("e1", 0)), ResequenceProgressIncrement(0, -1)),
        (Message(0, 0, Payload("e1", 1)), ResequenceProgressIncrement(0, 2)),
        (Message(0, 4, Payload("e1", 2)), ResequenceProgressIncrement(0, 2)),
        (Message(0, 5, Payload("e2", 1)), ResequenceProgressIncrement(0, 2)),
        (Message(0, 3, Payload("e2", 2)), ResequenceProgressIncrement(0, 5)))

      actual should be(expected)
    }
  }
}

class ResequenceTrackerSpec extends WordSpec with Matchers {
  "A ResequenceTracker" must {
    "track highest gapless offset" in {
      val tracker = new ResequenceTracker(-1L)

      tracker.delivered(0L)
      tracker.highestGaplessStorageSequenceNr should be(0L)
      tracker.buffer should be(SortedSet.empty[Long])

      tracker.delivered(3L)
      tracker.highestGaplessStorageSequenceNr should be(0L)
      tracker.buffer should be(SortedSet(3L))

      tracker.delivered(4L)
      tracker.highestGaplessStorageSequenceNr should be(0L)
      tracker.buffer should be(SortedSet(3L, 4L))

      tracker.delivered(1L)
      tracker.highestGaplessStorageSequenceNr should be(1L)
      tracker.buffer should be(SortedSet(3L, 4L))

      tracker.delivered(2L)
      tracker.highestGaplessStorageSequenceNr should be(4L)
      tracker.buffer should be(SortedSet.empty[Long])

      tracker.delivered(5L)
      tracker.highestGaplessStorageSequenceNr should be(5L)
      tracker.buffer should be(SortedSet.empty[Long])
    }
    "track highest gapless offset starting from a custom offset" in {
      val tracker = new ResequenceTracker(1L)

      tracker.delivered(3L)
      tracker.highestGaplessStorageSequenceNr should be(1L)
      tracker.buffer should be(SortedSet(3L))

      tracker.delivered(4L)
      tracker.highestGaplessStorageSequenceNr should be(1L)
      tracker.buffer should be(SortedSet(3L, 4L))

      tracker.delivered(2L)
      tracker.highestGaplessStorageSequenceNr should be(4L)
      tracker.buffer should be(SortedSet.empty[Long])

      tracker.delivered(5L)
      tracker.highestGaplessStorageSequenceNr should be(5L)
      tracker.buffer should be(SortedSet.empty[Long])

      // stored (offset, snr)
      // (0, 0)
      // (1, 3)
      // (2, 4)
      // (3, 1)
      // (4, 2)
      // (5, 5)

      // resequenced (offset, snr) + tracked
      // (0, 0) 0
      // (3, 1) 0
      // (4, 2) 0
      // (1, 3) 1 *
      // (2, 4) 4
      // (5, 5) 5

      // replayed from offset = 2 (highest delivered offset = 1 and highest delivered snr = 3)
      // (2, 4)
      // (3, 1)
      // (4, 2)
      // (5, 5)

      // resequenced (offset, snr)
      // (3, 1) * dup snr
      // (4, 2) * dup snr
      // (2, 4)
      // (5, 5)
    }
  }
}


