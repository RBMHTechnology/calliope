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
import akka.serialization.{SerializationExtension, SerializerWithStringManifest}
import akka.testkit.TestKit
import com.rbmhtechnology.calliope.{SequencedMessage, SpecWords, StopSystemAfterAll}
import org.scalatest.{MustMatchers, WordSpecLike}

class SequencedMessageSerializerSpec extends TestKit(ActorSystem("test"))
  with WordSpecLike with MustMatchers with StopSystemAfterAll with SpecWords {

  import Payloads._
  import SequencedMessageSerializer._

  def sequencedMessage(payload: AnyRef): SequencedMessage[AnyRef] =
    SequencedMessage(payload, "sourceId", 1, 1000)

  val serialization = SerializationExtension(system)
  val serializer: SerializerWithStringManifest = serialization.serializerFor(classOf[SequencedMessage[_]]).asInstanceOf[SerializerWithStringManifest]

  "A SequencedMessageSerializer" when invoked {
    "with a SequencedMessage" must {
      "serialize and deserialize a payload backed by a StringManifestSerializer" in {
        val original = sequencedMessage(StringManifestSerializablePayload("payload"))

        val bytes = serializer.toBinary(original)
        val deserialized = serializer.fromBinary(bytes, SequencedMessageManifest)

        deserialized mustBe original
      }
      "throw an IllegalArgumentException for a payload backed by a plain Serializer" in {
        val original = sequencedMessage(PlainSerializablePayload("payload"))

        intercept[IllegalArgumentException] {
          serializer.toBinary(original)
        }
      }
    }
    "without a string-manifest" must {
      "throw an IllegalArgumentException" in {
        val original = sequencedMessage(StringManifestSerializablePayload("payload"))
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
        val original = sequencedMessage(StringManifestSerializablePayload("payload"))
        val bytes = serializer.toBinary(original)

        intercept[IllegalArgumentException] {
          serializer.fromBinary(bytes, "non-sequenced-message-manifest")
        }
      }
    }
  }
}
