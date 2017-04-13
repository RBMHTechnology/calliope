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
import com.rbmhtechnology.calliope.serializer.CommonFormats.PayloadFormat
import com.rbmhtechnology.calliope.serializer.DelegatingStringManifestPayloadSerializer

object PayloadSerializer {
  def apply()(implicit system: ActorSystem): PayloadSerializer =
    new PayloadSerializer()
}

class PayloadSerializer(implicit system: ActorSystem) {

  private val serializer = DelegatingStringManifestPayloadSerializer(system)

  def deserialize(eventBytes: Array[Byte]): Any =
    serializer.payload(PayloadFormat.parseFrom(eventBytes))

  def serialize(event: Any): Array[Byte] =
    serializer.payloadFormatBuilder(event.asInstanceOf[AnyRef]).build().toByteArray
}