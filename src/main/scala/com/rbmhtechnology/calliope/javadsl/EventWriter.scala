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

package com.rbmhtechnology.calliope.javadsl

import com.rbmhtechnology.calliope.scaladsl
import java.util.function.{ Function => JFunction }

class EventWriter[A] private[javadsl](delegate: scaladsl.EventWriter[A]) {

  def writeEventToTopic(event: A, aggregateId: String, topic: String): Unit = {
    delegate.writeEventToTopic(event, aggregateId, topic)
  }

  def writeEvent(event: A, aggregateId: String): Unit = {
    delegate.writeEvent(event, aggregateId)
  }

  def bindAggregateId(aggregateId: JFunction[A, String]): AggregateAwareEventWriter[A] =
    new AggregateAwareEventWriter[A](delegate, aggregateId)
}

class AggregateAwareEventWriter[A] private[javadsl](delegate: scaladsl.EventWriter[A], aggregateId: JFunction[A, String]) {

  def writeEventToTopic(event: A, topic: String): Unit = {
    delegate.writeEventToTopic(event, aggregateId.apply(event), topic)
  }

  def writeEvent(event: A): Unit = {
    delegate.writeEvent(event, aggregateId.apply(event))
  }
}