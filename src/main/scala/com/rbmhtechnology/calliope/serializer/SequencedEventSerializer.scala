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

package com.rbmhtechnology.calliope.serializer

import java.time.Instant

import akka.actor.ExtendedActorSystem
import akka.serialization.SerializerWithStringManifest
import com.rbmhtechnology.calliope.SequencedEvent
import com.rbmhtechnology.calliope.serializer.SequencedEventFormats.SequencedEventFormat

class DelegatingSequencedEventSerializer(system: ExtendedActorSystem)
  extends SequencedEventSerializer(system, DelegatingStringManifestPayloadSerializer(system))

object SequencedEventSerializer {
  val SequencedEventManifest = "com.rbmhtechnology.calliope.v1.SequencedEventManifest"
}

abstract class SequencedEventSerializer(system: ExtendedActorSystem, payloadSerializer: PayloadSerializer) extends SerializerWithStringManifest {

  import SequencedEventSerializer._

  override def identifier: Int = 996248934

  override def manifest(o: AnyRef): String = SequencedEventManifest

  override def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case s: SequencedEvent[_] =>
      sequencedEventFormatBuilder(s).build().toByteArray
    case _ =>
      throw new IllegalArgumentException(s"Serialization of objects for type '${obj.getClass}' is not supported")
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
    case SequencedEventManifest =>
      sequencedEvent(SequencedEventFormat.parseFrom(bytes))
    case "" =>
      throw new IllegalArgumentException(s"Deserialization with empty manifest is not supported")
    case _ =>
      throw new IllegalArgumentException(s"Deserialization of objects with manifest '$manifest' is not supported")
  }

  def sequencedEventFormatBuilder(sequencedEvent: SequencedEvent[_]): SequencedEventFormat.Builder =
    SequencedEventFormat.newBuilder()
      .setPayload(payloadSerializer.payloadFormatBuilder(sequencedEvent.payload.asInstanceOf[AnyRef]))
      .setSourceId(sequencedEvent.sourceId)
      .setSequenceNr(sequencedEvent.sequenceNr)
      .setCreationTimestamp(sequencedEvent.creationTimestamp.toEpochMilli)

  def sequencedEvent(format: SequencedEventFormat): SequencedEvent[_] =
    SequencedEvent(
      payloadSerializer.payload(format.getPayload),
      format.getSourceId,
      format.getSequenceNr,
      Instant.ofEpochMilli(format.getCreationTimestamp)
    )
}
