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

import java.time.Instant

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.google.protobuf.{ByteString, InvalidProtocolBufferException}
import com.rbmhtechnology.calliope.CustomMatchers.failWith
import com.rbmhtechnology.calliope.serializer.CommonFormats.PayloadFormat
import com.rbmhtechnology.calliope.serializer.SequencedEventFormats.SequencedEventFormat
import com.rbmhtechnology.calliope.serializer.{PayloadNotDeserializableException, SequencedEventSerializer}
import com.rbmhtechnology.calliope.{SpecWords, StopSystemAfterAll}
import org.scalatest.{MustMatchers, WordSpecLike}

import scala.util.Random
import scala.compat.java8.FunctionConverters._

class PayloadFormatSerializerSpec extends TestKit(ActorSystem("test"))
  with WordSpecLike with MustMatchers with StopSystemAfterAll with SpecWords {

  import com.rbmhtechnology.calliope.serializer.Payloads._

  private val isDeserializedAsAnInstanceOf = afterWord("is deserialized as an instance of")

  val topic = "topic"

  def serializeDeserialize[A](deserializer: PayloadFormatDeserializer[A], obj: AnyRef): A =
    deserializer.deserialize(topic, PayloadFormatSerializer[AnyRef].serialize(topic, obj))

  private def payloadFormatWithUnknownManifest =
    PayloadFormat.newBuilder().setPayloadManifest("unknown-manifest").setSerializerId(42).setPayload(ByteString.copyFrom(randomBytes)).build

  private def sequencedEventUnknownPayload = {
    val payload = payloadFormatWithUnknownManifest
    val sequencedEvent = SequencedEventFormat.newBuilder().setSourceId("one").setSequenceNr(1).setCreationTimestamp(Instant.now.getEpochSecond).setPayload(payload).build().toByteString
    PayloadFormat.newBuilder().setPayloadManifest(SequencedEventSerializer.SequencedEventManifest).setSerializerId(996248934).setPayload(sequencedEvent).build
  }

  private def randomBytes = {
    val bytes = new Array[Byte](20)
    Random.nextBytes(bytes)
    bytes
  }

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
          PayloadFormatSerializer[AnyRef].serialize(topic, PlainSerializablePayload("inner-payload"))
        }
      }
    }
    "a payload not backed by a protocol buffer definition" must {
      "throw an InvalidProtocolBufferException exception" in {
        val deserializer = PayloadFormatDeserializer.apply

        intercept[InvalidProtocolBufferException] {
          deserializer.deserialize(topic, randomBytes)
        }
      }
    }
    "a PayloadFormat payload not being backed by an akka deserializer" must {
      "throw an PayloadNotDeserializableException exception" in {
        val deserializer = PayloadFormatDeserializer.apply

        intercept[PayloadNotDeserializableException] {
          deserializer.deserialize(topic, payloadFormatWithUnknownManifest.toByteArray)
        }
      }
      "return a Failure with attempt" in {
        val deserializer = PayloadFormatDeserializer.attempt(identity)

        deserializer.deserialize(topic, payloadFormatWithUnknownManifest.toByteArray) must failWith[PayloadNotDeserializableException]
      }
    }
    "a SequencedEvent payload not being backed by an akka deserializer" must {
      "throw an PayloadNotDeserializableException exception" in {
        val deserializer = PayloadFormatDeserializer.apply

        intercept[PayloadNotDeserializableException] {
          deserializer.deserialize(topic, sequencedEventUnknownPayload.toByteArray)
        }
      }
      "return a Failure with attempt" in {
        val deserializer = PayloadFormatDeserializer.attempt(identity)

        deserializer.deserialize(topic, sequencedEventUnknownPayload.toByteArray) must failWith[PayloadNotDeserializableException]
      }
      "return a vavr.Failure with attempt using Java API" in {
        val deserializer = PayloadFormatDeserializer.createAttempt(asJavaFunction((x: AnyRef) => x), system)

        deserializer.deserialize(topic, sequencedEventUnknownPayload.toByteArray).isFailure mustBe true
      }
    }
  }
}
