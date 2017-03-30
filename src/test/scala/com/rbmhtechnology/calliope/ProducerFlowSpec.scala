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

import java.util.concurrent.CompletionStage

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage
import akka.kafka.ConsumerMessage.{CommittableMessage, GroupTopicPartition, PartitionOffset}
import akka.stream.scaladsl.Flow
import akka.testkit.TestKit
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.{MustMatchers, WordSpecLike}

import scala.concurrent.Future

object ProducerFlowSpec {
  import ProducerFlow._

  case class ProducibleMessage(topic: String, id: Long, payload: String, timestamp: Option[Long] = None)

  object ProducibleMessage {
    implicit val producible: Producible[ProducibleMessage, Long, String] =
      new Producible[ProducibleMessage, Long, String] {
        override def topic(message: ProducibleMessage): String = message.topic

        override def key(message: ProducibleMessage): Long = message.id

        override def value(message: ProducibleMessage): String = message.payload

        override def timestamp(message: ProducibleMessage): Option[Long] = message.timestamp
      }

    implicit val committable: Committable[ProducibleMessage] =
      (_: ProducibleMessage) => committableOffset(PartitionOffset(GroupTopicPartition("group", "topic", 0), 1L))
  }

  def committableOffset(offset: PartitionOffset): ConsumerMessage.CommittableOffset =
    new ConsumerMessage.CommittableOffset {
      override def partitionOffset: PartitionOffset = offset
      override def commitScaladsl(): Future[Done] = ???
      override def commitJavadsl(): CompletionStage[Done] = ???
    }
}

trait ProducerFlowBehaviours { this: WordSpecLike with MustMatchers with StreamSpec with FlowSpec =>

  import ProducerFlowSpec._

  def producerRecordConverter(message: ProducibleMessage, flow: Flow[ProducibleMessage, ProducerRecord[Long, String], NotUsed]): Unit = {

    "contains the mapped input properties" in {
      implicit val (pub, sub) = runFlow(flow)

      val processed = processNext(message)

      processed.topic() mustBe message.topic
      processed.key() mustBe message.id
      processed.value() mustBe message.payload
      processed.timestamp() mustBe message.timestamp.get
    }
    "leaves the timestamp empty if not provided by the input" in {
      implicit val (pub, sub) = runFlow(flow)

      val processed = processNext(message.copy(timestamp = None))

      processed.timestamp() mustBe null
    }
    "leaves the partition assignment of the ProducerRecord empty" in {
      implicit val (pub, sub) = runFlow(flow)

      val processed = processNext(message)

      processed.partition() must be(null)
    }
  }
}

class ProducerFlowSpec extends TestKit(ActorSystem("test"))
  with WordSpecLike with MustMatchers with StreamSpec with FlowSpec with ProducerFlowBehaviours {

  import ProducerFlowSpec._

  private def invokedWith = afterWord("invoked with")
  private def createAFlowThat = afterWord("create a flow that")

  val inputMessage = ProducibleMessage(topic = "topic", id = 1L, payload = "payload", timestamp = Some(1000L))
  val offset = PartitionOffset(GroupTopicPartition("groupId", "sourceTopic", 0), 42)
  val committableMessage = CommittableMessage(new ConsumerRecord[Unit, ProducibleMessage]("sourceTopic", 0, 42, Unit, inputMessage), committableOffset(offset))

  "A ProducerFlow" when invokedWith {
    "toRecord" must createAFlowThat {
      "maps an arbitrary input to a 'ProducerRecord'" that {
        behave like producerRecordConverter(inputMessage, ProducerFlow[ProducibleMessage].toRecord)
      }
    }
    "toMessage" must createAFlowThat {
      "maps an arbitrary input to a 'ProducerMessage' containing a 'ProducerRecord'" that {
        behave like producerRecordConverter(inputMessage, ProducerFlow[ProducibleMessage].toMessage.map(_.record))
      }
      "leaves the passThrough property of the 'ProducerMessage' empty if not specified" in {
        implicit val (pub, sub) = runFlow(ProducerFlow[ProducibleMessage].toMessage)

        val processed = processNext(inputMessage)

        processed.passThrough mustBe ()
      }
      "sets the passThrough property of the 'ProducerMessage' if provided" in {
        implicit val (pub, sub) = runFlow(ProducerFlow[ProducibleMessage].toMessage(m => (m.id, m.payload)))

        val processed = processNext(inputMessage)

        processed.passThrough must be (inputMessage.id, inputMessage.payload)
      }
    }
    "toCommittableMessage" must createAFlowThat {
      "maps an arbitrary input to a committable 'ProducerMessage' containing a 'ProducerRecord'" that {
        behave like producerRecordConverter(inputMessage, ProducerFlow[ProducibleMessage].toCommittableMessage.map(_.record))
      }
      "maps an input wrapped in a committable 'ConsumerMessage' to a 'ProducerMessage'" in {
        implicit val (pub, sub) = runFlow(ProducerFlow[CommittableMessage[Unit, ProducibleMessage]].toCommittableMessage)

        val processed = processNext(committableMessage)

        processed.record.topic() mustBe committableMessage.record.value().topic
        processed.record.key() mustBe committableMessage.record.value().id
        processed.record.value() mustBe committableMessage.record.value().payload
      }
      "preserves the committable offset of the 'ConsumerMessage'" in {
        implicit val (pub, sub) = runFlow(ProducerFlow[CommittableMessage[Unit, ProducibleMessage]].toCommittableMessage)

        val processed = processNext(committableMessage)

        processed.passThrough.partitionOffset mustBe offset
      }
    }
  }
}
