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
import akka.stream.scaladsl.Source

import scala.collection.immutable.Map

trait ProgressStore[MS, SP] {
  def read(groupId: String): ResequenceProgress[MS, SP]
  def update(groupId: String, resequenceProgress: ResequenceProgress[MS, SP]): Unit

  def source(groupId: String): Source[ResequenceProgress[MS, SP], NotUsed] =
    Source.lazily(() => Source.single(read(groupId))).mapMaterializedValue(_ => NotUsed)
}

class InmemProgressStore[MS, SP] extends ProgressStore[MS, SP] {
  private var committedMessageSequenceNrs: Map[String, Map[MS, Long]] = Map.empty.withDefaultValue(Map.empty)
  private var committedStorageSequenceNrs: Map[String, Map[SP, Long]] = Map.empty.withDefaultValue(Map.empty)

  override def read(groupId: String): ResequenceProgress[MS, SP] = this.synchronized {
    ResequenceProgress(
      committedMessageSequenceNrs(groupId),
      committedStorageSequenceNrs(groupId))
  }

  override def update(groupId: String, resequenceProgress: ResequenceProgress[MS, SP]): Unit = this.synchronized {
    committedMessageSequenceNrs = committedMessageSequenceNrs.updated(groupId, resequenceProgress.messageSequenceNrs)
    committedStorageSequenceNrs = committedStorageSequenceNrs.updated(groupId, resequenceProgress.storageSequenceNrs)
  }
}
