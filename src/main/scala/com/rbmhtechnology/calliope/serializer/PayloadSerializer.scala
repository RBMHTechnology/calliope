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

import akka.actor.ActorSystem
import akka.serialization.{NullSerializer, SerializationExtension, SerializerWithStringManifest}
import com.google.protobuf.ByteString
import com.rbmhtechnology.calliope.serializer.CommonFormats.PayloadFormat

trait PayloadSerializer {
  def payloadFormatBuilder(payload: AnyRef): PayloadFormat.Builder
  def payload(payloadFormat: PayloadFormat): AnyRef
}

object DelegatingStringManifestPayloadSerializer {
  def apply(system: ActorSystem): DelegatingStringManifestPayloadSerializer =
    new DelegatingStringManifestPayloadSerializer(system)
}

class DelegatingStringManifestPayloadSerializer(system: ActorSystem) extends PayloadSerializer {

  override def payloadFormatBuilder(payload: AnyRef): PayloadFormat.Builder = {
    val serializer = SerializationExtension(system).findSerializerFor(payload) match {
      case s: SerializerWithStringManifest => s
      case _: NullSerializer => throw new IllegalArgumentException(s"No serializer found for payload of ${payload.getClass}")
      case s => throw new IllegalArgumentException(s"Serializer ${s.getClass} for ${payload.getClass} does not implement the required type 'SerializerWithStringManifest'")
    }

    val builder = PayloadFormat.newBuilder()
    builder.setSerializerId(serializer.identifier)
    builder.setPayloadManifest(serializer.manifest(payload))
    builder.setPayload(ByteString.copyFrom(serializer.toBinary(payload)))
    builder
  }

  override def payload(payloadFormat: PayloadFormat): AnyRef = {
    if (payloadFormat.getPayloadManifest.isEmpty) {
      throw new IllegalArgumentException(s"No payload manifest provided in payload format")
    }
    SerializationExtension(system).deserialize(
      payloadFormat.getPayload.toByteArray,
      payloadFormat.getSerializerId,
      payloadFormat.getPayloadManifest).get
  }
}
