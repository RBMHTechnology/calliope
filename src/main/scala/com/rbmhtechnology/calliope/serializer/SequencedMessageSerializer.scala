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

import akka.actor.ExtendedActorSystem
import akka.serialization.SerializerWithStringManifest
import com.rbmhtechnology.calliope.SequencedMessage
import com.rbmhtechnology.calliope.serializer.SequencedMessageFormats.SequencedMessageFormat

class DelegatingSequencedMessageSerializer(system: ExtendedActorSystem)
  extends SequencedMessageSerializer(system, DelegatingStringManifestPayloadSerializer(system))

object SequencedMessageSerializer {
  val SequencedMessageManifest = "com.rbmhtechnology.calliope.v1.SequencedMessageManifest"
}

abstract class SequencedMessageSerializer(system: ExtendedActorSystem, payloadSerializer: PayloadSerializer) extends SerializerWithStringManifest {
  import SequencedMessageSerializer._

  override def identifier: Int = 996248934

  override def manifest(o: AnyRef): String = SequencedMessageManifest

  override def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case s: SequencedMessage[_] =>
      sequencedMessageFormatBuilder(s).build().toByteArray
    case _ =>
      throw new IllegalArgumentException(s"Serialization of objects for type '${obj.getClass}' is not supported")
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
    case SequencedMessageManifest =>
      sequencedMessage(SequencedMessageFormat.parseFrom(bytes))
    case "" =>
      throw new IllegalArgumentException(s"Deserialization with empty manifest is not supported")
    case _ =>
      throw new IllegalArgumentException(s"Deserialization of objects with manifest '$manifest' is not supported")
  }

  def sequencedMessageFormatBuilder(sequencedEvent: SequencedMessage[_]): SequencedMessageFormat.Builder =
    SequencedMessageFormat.newBuilder()
      .setPayload(payloadSerializer.payloadFormatBuilder(sequencedEvent.payload.asInstanceOf[AnyRef]))
      .setSourceId(sequencedEvent.sourceId)
      .setSequenceNo(sequencedEvent.sequenceNo)
      .setCreationTimestamp(sequencedEvent.creationTimestamp)

  def sequencedMessage(format: SequencedMessageFormat): SequencedMessage[_] =
    SequencedMessage(
      payloadSerializer.payload(format.getPayload),
      format.getSourceId,
      format.getSequenceNo,
      format.getCreationTimestamp
    )
}
