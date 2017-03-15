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

import akka.kafka.scaladsl.Producer
import akka.kafka.{ConsumerSettings, ProducerSettings}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition

import scala.collection.immutable.{Map, SortedSet}
import scala.concurrent.Future

case class SequencedProducerRecord[K, V](key: K, value: V, timestamp: Long = -1L)

object Sequence {
  def from[K, V, MS](consumerSettings: ConsumerSettings[K, V],
                     messageSourceDecoder: ConsumerRecord[K, V] => MS,
                     messageSequenceNrDecoder: ConsumerRecord[K, V] => Long)
                    (progressStore: ProgressStore[MS, TopicPartition],
                     storagePartitions: Set[TopicPartition]): Source[Map[MS, Source[Long, NotUsed]], NotUsed] = {
    val groupId = consumerSettings.getProperty(ConsumerConfig.GROUP_ID_CONFIG)
    progressStore.source(groupId)
      .flatMapConcat(progress => from(
        consumerSettings,
        messageSourceDecoder,
        messageSequenceNrDecoder,
        progress.messageSequenceNrs,
        storagePartitions.map(sp => sp -> progress.storageSequenceNrs.getOrElse(sp, -1L)).toMap))
  }

  def from[K, V, MS](consumerSettings: ConsumerSettings[K, V],
                     messageSourceDecoder: ConsumerRecord[K, V] => MS,
                     messageSequenceNrDecoder: ConsumerRecord[K, V] => Long,
                     committedMessageSequenceNrs: Map[MS, Long],
                     committedStorageSequenceNrs: Map[TopicPartition, Long]): Source[Map[MS, Source[Long, NotUsed]], NotUsed] = {
    Consume.fromSnapshot(consumerSettings, committedStorageSequenceNrs).fold(Map.empty[MS, SortedSet[Long]]) {
      case (acc, cr) => acc.get(messageSourceDecoder(cr)) match {
        case Some(ss) => acc.updated(messageSourceDecoder(cr), ss + messageSequenceNrDecoder(cr))
        case None     => acc.updated(messageSourceDecoder(cr), SortedSet(messageSequenceNrDecoder(cr)))
      }
    } map { remainingMessageSequenceNrs =>
      (committedMessageSequenceNrs.keySet ++ remainingMessageSequenceNrs.keySet).map { ms =>
        val sequenceStream = stream(
          committedMessageSequenceNrs.getOrElse(ms, -1L),
          remainingMessageSequenceNrs.getOrElse(ms, SortedSet.empty))
        ms -> Source(sequenceStream)
      }.toMap
    }
  }

  def stream(committedMessageSequenceNr: Long, remainingMessageSequenceNrs: SortedSet[Long] = SortedSet.empty): Stream[Long] =
    remainingMessageSequenceNrs.headOption match {
      case Some(snr) if snr <= committedMessageSequenceNr + 1L =>
        stream(snr max committedMessageSequenceNr, remainingMessageSequenceNrs.tail)
      case _ =>
        val next = committedMessageSequenceNr + 1L
        next #:: stream(next, remainingMessageSequenceNrs)
    }
}

object Produce {
  def to[K, V, MS](producerSettings: ProducerSettings[K, V],
                   messageSource: MS,
                   messageSequenceEncoder: (SequencedProducerRecord[K, V], MS, Long) => (K, V),
                   messageSequenceNrSources: Source[Map[MS, Source[Long, NotUsed]], NotUsed],
                   storagePartitions: Set[TopicPartition]): Sink[SequencedProducerRecord[K, V], Future[Done]] = {
    val spa = storagePartitions.toArray
    val len = storagePartitions.size

    def storagePartition(snr: Long): TopicPartition =
      spa((snr % len).toInt)

    val messageSequenceNrSource: Source[Long, NotUsed] =
      messageSequenceNrSources.flatMapConcat(_.getOrElse(messageSource, Source(Sequence.stream(-1L))))

    Flow[SequencedProducerRecord[K, V]].zipWith(messageSequenceNrSource) { (spr, snr) =>
      val (k, v) = messageSequenceEncoder(spr, messageSource, snr)
      val sp = storagePartition(snr)
      new ProducerRecord[K, V](sp.topic, sp.partition, if (spr.timestamp == -1L) null else spr.timestamp, k, v)
    }.toMat(Producer.plainSink[K, V](producerSettings))(Keep.right)
  }
}
