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

import akka.stream.testkit.TestPublisher

import scala.collection.immutable.{Map, Seq}
import scala.concurrent.Future
import scala.util.Try

object TestEventReader {
  def apply(): TestEventReader =
    new TestEventReader(Map.empty)

  def apply(results: (Long, Try[Seq[EventRecord[String]]])*): TestEventReader =
    new TestEventReader(Map(results: _*))

  object Extensions {

    implicit class TestEventReaderPublisher[A](pub: TestPublisher.Probe[A]) {
      def sendNextResult(result: (Long, Try[Seq[EventRecord[String]]]))(implicit reader: TestEventReader): Unit = {
        reader.setResult(result)
        pub.sendNext(Unit.asInstanceOf[A])
      }
    }
  }
}

class TestEventReader(initial: Map[Long, Try[Seq[EventRecord[String]]]]) extends EventReader[String] {
  private var results = initial

  override def readEvents(fromSequenceNr: Long, maxItems: Int): Future[Seq[EventRecord[String]]] = {
    results.get(fromSequenceNr)
      .map(Future.fromTry)
      .getOrElse(Future.failed(new IllegalStateException(s"No result defined for sequence number $fromSequenceNr")))
  }

  def setResult(result: (Long, Try[Seq[EventRecord[String]]])): Unit = {
    results = results + result
  }
}
