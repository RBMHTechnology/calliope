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

import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import org.apache.kafka.common.TopicPartition

class KafkaPipeSpec extends KafkaSpec {
  import KafkaSpec._

  type PUB = SequencedProducerRecord[Long, Message]
  type SUB = ResequencedConsumerRecord[Long, Message, String] with Committable

  def probes(messageSource: String, progressStore: ProgressStore[String, TopicPartition]): (TestPublisher.Probe[PUB], TestSubscriber.Probe[SUB]) = {
    val pipe = Pipe.through(
      producerSettings,
      consumerSettings,
      messageSource,
      messageSequenceEncoder,
      messageSourceDecoder,
      messageSequenceNrDecoder)(progressStore, topicPartitions.toSet)
    TestSource.probe[PUB].via(pipe).toMat(TestSink.probe[SUB])(Keep.both).run()
  }

  "A pipe" must {
    "write consumed messages to given topic partitions and consume written messages from these partitions in sequential order" in {
      val prototype = SequencedProducerRecord(-1L, Message("a", -1L))
      val progressStore = new InmemProgressStore[String, TopicPartition]

      // collaborating emitters
      val (pub1, sub1) = probes("e1", progressStore)
      val (pub2, sub2) = probes("e2", progressStore)

      pub1.sendNext(prototype)
      pub1.sendNext(prototype)

      sub1.request(2)
      sub2.request(2)

      sub1.expectNextN(2).map(_.consumerRecord.value) should be(Seq(Message("e1", 0L), Message("e1", 1L)))
      sub2.expectNextN(2).map(_.consumerRecord.value) should be(Seq(Message("e1", 0L), Message("e1", 1L)))

      pub2.sendNext(prototype)
      pub2.sendNext(prototype)

      sub1.request(2)
      sub2.request(2)

      sub1.expectNextN(2).map(_.consumerRecord.value) should be(Seq(Message("e2", 0L), Message("e2", 1L)))
      sub2.expectNextN(2).map(_.consumerRecord.value) should be(Seq(Message("e2", 0L), Message("e2", 1L)))
    }
  }
}
