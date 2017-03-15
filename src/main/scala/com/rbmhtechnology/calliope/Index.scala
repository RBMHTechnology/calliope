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

import akka.NotUsed
import akka.kafka.ConsumerSettings
import akka.stream.Materializer
import akka.stream.scaladsl.{BroadcastHub, Flow, Keep, Sink, Source}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

case class IndexProgress[MS, SP](committedMessageSequenceNrs: Map[MS, Long],
                                 committedStorageSequenceNrs: Map[SP, Long])

trait IndexConnection[I, M] {
  def subscribe: Source[M, NotUsed]
  def subscribe(indexKey: I): Source[M, NotUsed]
  def close(): Unit
}

object IndexStore {
  def inmemStore[I, K, V, MS]: IndexStore[I, ConsumerRecord[K, V], MS, TopicPartition] =
    new InmemIndexStore

  def connect[I, K, V, MS](consumerSettings: ConsumerSettings[K, V],
                           storagePartitions: Set[TopicPartition],
                           indexStore: IndexStore[I, ConsumerRecord[K, V], MS, TopicPartition],
                           indexKeyDecoder: ConsumerRecord[K, V] => I,
                           messageSourceDecoder: ConsumerRecord[K, V] => MS,
                           messageSequenceNrDecoder: ConsumerRecord[K, V] => Long)(implicit materializer: Materializer): IndexConnection[I, ConsumerRecord[K, V]] =
    new IndexConnection[I, ConsumerRecord[K, V]] {
      val progress: IndexProgress[MS, TopicPartition] =
        indexStore.progress(storagePartitions)

      /**
       * Subscribe to all consumer records.
       */
      val subscribe: Source[ConsumerRecord[K, V], NotUsed] =
        Resequence
          .fromInc(consumerSettings,
            messageSourceDecoder,
            messageSequenceNrDecoder,
            progress.committedMessageSequenceNrs,
            progress.committedStorageSequenceNrs)
          .via(writer(
            indexStore,
            indexKeyDecoder,
            messageSourceDecoder,
            messageSequenceNrDecoder))
          .toMat(BroadcastHub.sink)(Keep.right)
          .mapMaterializedValue { src =>
            // consume hub buffer so that index store still
            // gets updated when there are no subscribers
            src.to(Sink.ignore)
            src
          }.run()

      /**
       * Subscribe to consumer records of given `indexKey`.
       */
      override def subscribe(indexKey: I): Source[ConsumerRecord[K, V], NotUsed] =
        subscribe.prepend(indexStore.messages(indexKey)).filter(recordOf(indexKey))

      override def close(): Unit =
        ??? // FIXME: shutdown source

      private def recordOf(key: I): ConsumerRecord[K, V] => Boolean = {
        var highestMessageSequenceNrs: Map[MS, Long] = Map.empty.withDefaultValue(-1L)
        cr => if (indexKeyDecoder(cr) == key && messageSequenceNrDecoder(cr) > highestMessageSequenceNrs(messageSourceDecoder(cr))) {
          highestMessageSequenceNrs = highestMessageSequenceNrs.updated(messageSourceDecoder(cr), messageSequenceNrDecoder(cr))
          true
        } else false
      }
    }

  def writer[I, K, V, MS](indexStore: IndexStore[I, ConsumerRecord[K, V], MS, TopicPartition],
                          indexKeyDecoder: ConsumerRecord[K, V] => I,
                          messageSourceDecoder: ConsumerRecord[K, V] => MS,
                          messageSequenceNrDecoder: ConsumerRecord[K, V] => Long): Flow[(ConsumerRecord[K, V], ResequenceProgressIncrement[TopicPartition]), ConsumerRecord[K, V], NotUsed] =
    Flow[(ConsumerRecord[K, V], ResequenceProgressIncrement[TopicPartition])].map {
      case (message, progress) =>
        indexStore.append(indexKeyDecoder(message),
          message,
          messageSourceDecoder(message),
          messageSequenceNrDecoder(message),
          storagePartitionDecoder(message),
          message.offset)
        message
    }
}

trait IndexStore[I, M, MS, SP] {
  def progress(storagePartitions: Set[SP]): IndexProgress[MS, SP]
  def messages(indexKey: I): Source[M, NotUsed]
  def append(indexKey: I,
             message: M,
             messageSource: MS,
             messageSequenceNr: Long,
             storagePartition: SP,
             storageSequenceNr: Long): Unit
}

private class InmemIndexStore[I, M, MS, SP] extends IndexStore[I, M, MS, SP] {
  private var committedMessageSequenceNrs: Map[MS, Long] = Map.empty
  private var committedStorageSequenceNrs: Map[SP, Long] = Map.empty
  private var index: Map[I, List[M]] = Map.empty

  override def messages(indexKey: I): Source[M, NotUsed] = this.synchronized {
    Source(index.getOrElse(indexKey, Nil).reverse)
  }

  override def progress(storagePartitions: Set[SP]): IndexProgress[MS, SP] = this.synchronized {
    IndexProgress(committedMessageSequenceNrs, storagePartitions.map(sp => sp -> committedStorageSequenceNrs.getOrElse(sp, -1L)).toMap)
  }

  /**
   * Expects increasing `messageSequenceNr` and `storageSequenceNr`.
   */
  override def append(indexKey: I,
                      message: M,
                      messageSource: MS,
                      messageSequenceNr: Long,
                      storagePartition: SP,
                      storageSequenceNr: Long): Unit = this.synchronized {
    index.get(indexKey) match {
      case Some(messages) => index = index.updated(indexKey, message :: messages)
      case None           => index = index.updated(indexKey, message :: Nil)
    }
    committedMessageSequenceNrs = committedMessageSequenceNrs.updated(messageSource, messageSequenceNr)
    committedStorageSequenceNrs = committedStorageSequenceNrs.updated(storagePartition, storageSequenceNr)
  }
}