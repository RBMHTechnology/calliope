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

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.rbmhtechnology.calliope.{SpecWords, StopSystemAfterAll}
import org.scalatest.{MustMatchers, WordSpecLike}

class PayloadFormatSerializerSpec extends TestKit(ActorSystem("test"))
  with WordSpecLike with MustMatchers with StopSystemAfterAll with SpecWords {

  import com.rbmhtechnology.calliope.serializer.Payloads._

  private val isDeserializedAsAnInstanceOf = afterWord("is deserialized as an instance of")

  def serializeDeserialize[A](deserializer: PayloadFormatDeserializer[A], obj: AnyRef): A =
    deserializer.deserialize("topic", PayloadFormatSerializer[AnyRef].serialize("topic", obj))

  "A Kafka PayloadFormatSerializer/Deserializer" when invokedWith {
    "a payload backed by a StringManifestSerializer" must {
      "serialize and deserialize the payload" which isDeserializedAsAnInstanceOf {
        "AnyRef if no payload-mapper given" in {
          val original = StringManifestSerializablePayload("inner-payload")
          val deserializer = PayloadFormatDeserializer.apply

          val deserialized: AnyRef = serializeDeserialize(deserializer, original)
          deserialized mustBe original
        }
        "the type of the payload-mapper" in {
          val original = StringManifestSerializablePayload("inner-payload")
          val deserializer = PayloadFormatDeserializer(_.asInstanceOf[StringManifestSerializablePayload])

          val deserialized: StringManifestSerializablePayload = serializeDeserialize(deserializer, original)
          deserialized mustBe original
        }
      }
      "apply the payload mapper to the payload on deserialization" in {
        val original = StringManifestSerializablePayload("inner-payload")
        val deserializer = PayloadFormatDeserializer[String]({
          case p: StringManifestSerializablePayload => p.payload
          case _ => throw new IllegalStateException("invalid payload given")
        })

        val deserialized: String = serializeDeserialize(deserializer, original)
        deserialized mustBe "inner-payload"
      }
    }
    "a payload backed by a plain Serializer" must {
      "throw an IllegalArgumentException" in {
        intercept[IllegalArgumentException] {
          PayloadFormatSerializer[AnyRef].serialize("topic", PlainSerializablePayload("inner-payload"))
        }
      }
    }
  }
}
