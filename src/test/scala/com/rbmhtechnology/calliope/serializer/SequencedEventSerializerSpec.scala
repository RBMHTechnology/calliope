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

import akka.actor.ActorSystem
import akka.serialization.{SerializationExtension, SerializerWithStringManifest}
import akka.testkit.TestKit
import com.rbmhtechnology.calliope.{SequencedEvent, SpecWords, StopSystemAfterAll}
import org.scalatest.{MustMatchers, WordSpecLike}

class SequencedEventSerializerSpec extends TestKit(ActorSystem("test"))
  with WordSpecLike with MustMatchers with StopSystemAfterAll with SpecWords {

  import Payloads._
  import SequencedEventSerializer._

  def sequencedEvent(payload: AnyRef): SequencedEvent[AnyRef] =
    SequencedEvent(payload, "sourceId", 1, Instant.ofEpochMilli(1000))

  val serialization = SerializationExtension(system)
  val serializer: SerializerWithStringManifest = serialization.serializerFor(classOf[SequencedEvent[_]]).asInstanceOf[SerializerWithStringManifest]

  "A SequencedEventSerializer" when invoked {
    "with a SequencedEvent" must {
      "serialize and deserialize a payload backed by a StringManifestSerializer" in {
        val original = sequencedEvent(StringManifestSerializablePayload("payload"))

        val bytes = serializer.toBinary(original)
        val deserialized = serializer.fromBinary(bytes, SequencedEventManifest)

        deserialized mustBe original
      }
      "throw an IllegalArgumentException for a payload backed by a plain Serializer" in {
        val original = sequencedEvent(PlainSerializablePayload("payload"))

        intercept[IllegalArgumentException] {
          serializer.toBinary(original)
        }
      }
    }
    "without a string-manifest" must {
      "throw an IllegalArgumentException" in {
        val original = sequencedEvent(StringManifestSerializablePayload("payload"))
        val bytes = serializer.toBinary(original)

        intercept[IllegalArgumentException] {
          serializer.fromBinary(bytes)
        }
      }
    }
    "with an invalid type" must {
      "throw an IllegalArgumentException on serialization" in {
        intercept[IllegalArgumentException] {
          serializer.toBinary("invalid-for-this-serializer")
        }
      }
      "throw an IllegalArgumentException on deserialization" in {
        val original = sequencedEvent(StringManifestSerializablePayload("payload"))
        val bytes = serializer.toBinary(original)

        intercept[IllegalArgumentException] {
          serializer.fromBinary(bytes, "non-sequenced-message-manifest")
        }
      }
    }
  }
}
