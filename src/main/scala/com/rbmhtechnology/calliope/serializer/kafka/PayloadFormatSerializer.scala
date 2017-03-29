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

package com.rbmhtechnology.calliope.serializer.kafka

import java.util

import akka.actor.ActorSystem
import com.rbmhtechnology.calliope.serializer.CommonFormats.PayloadFormat
import com.rbmhtechnology.calliope.serializer.{DelegatingStringManifestPayloadSerializer, PayloadSerializer}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

trait NoOpConfiguration {
  def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
}

trait NoOpClose {
  def close(): Unit = {}
}

object PayloadFormatSerializer {
  def apply[A <: AnyRef](implicit system: ActorSystem): PayloadFormatSerializer[A] =
    new PayloadFormatSerializer[A](DelegatingStringManifestPayloadSerializer(system))
}

class PayloadFormatSerializer[A <: AnyRef] private(serializer: PayloadSerializer) extends Serializer[A]
  with NoOpConfiguration with NoOpClose {

  override def serialize(topic: String, data: A): Array[Byte] =
    serializer.payloadFormatBuilder(data).build().toByteArray
}

object PayloadFormatDeserializer {
  def apply[A](payloadMapper: AnyRef => A)(implicit system: ActorSystem): PayloadFormatDeserializer[A] =
    new PayloadFormatDeserializer[A](DelegatingStringManifestPayloadSerializer(system), payloadMapper)

  def apply(implicit system: ActorSystem): PayloadFormatDeserializer[AnyRef] =
    apply[AnyRef](identity)
}

class PayloadFormatDeserializer[A] private(serializer: PayloadSerializer, payloadMapper: AnyRef => A) extends Deserializer[A]
  with NoOpConfiguration with NoOpClose {

  override def deserialize(topic: String, data: Array[Byte]): A =
    payloadMapper(serializer.payload(PayloadFormat.parseFrom(data)))
}