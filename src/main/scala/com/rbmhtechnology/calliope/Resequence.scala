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
import akka.stream._
import akka.stream.scaladsl.Source
import akka.stream.stage._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.collection.immutable.{Map, SortedSet}
import scala.collection.{immutable, mutable}

case class ResequenceProgressIncrement[SP](storagePartition: SP,
                                           storageSequenceNr: Long)

case class ResequenceProgress[MS, SP](messageSequenceNrs: Map[MS, Long] = Map.empty[MS, Long],
                                      storageSequenceNrs: Map[SP, Long] = Map.empty[SP, Long]) {
  def update(messageSource: MS,
             messageSequenceNr: Long,
             storagePartition: SP,
             storageSequenceNr: Long): ResequenceProgress[MS, SP] = copy(
      messageSequenceNrs.updated(messageSource, messageSequenceNr),
      storageSequenceNrs.updated(storagePartition, storageSequenceNr))
}

case class ResequencedConsumerRecord[K, V, MS](consumerRecord: ConsumerRecord[K, V],
                                               resequenceProgress: ResequenceProgress[MS, TopicPartition])

object Resequence {
  def from[K, V, MS](consumerSettings: ConsumerSettings[K, V],
                     messageSourceDecoder: ConsumerRecord[K, V] => MS,
                     messageSequenceNrDecoder: ConsumerRecord[K, V] => Long)
                    (progressStore: ProgressStore[MS, TopicPartition],
                     storagePartitions: Set[TopicPartition]): Source[ResequencedConsumerRecord[K, V, MS] with Committable, NotUsed] = {
    val groupId = consumerSettings.getProperty(ConsumerConfig.GROUP_ID_CONFIG)
    progressStore.source(groupId)
      .flatMapConcat(progress => from(
        consumerSettings,
        messageSourceDecoder,
        messageSequenceNrDecoder,
        progress.messageSequenceNrs,
        storagePartitions.map(sp => sp -> progress.storageSequenceNrs.getOrElse(sp, -1L)).toMap))
      .map { rcr =>
        new ResequencedConsumerRecord[K, V, MS](rcr.consumerRecord, rcr.resequenceProgress) with Committable {
          override def commit(): Unit = progressStore.update(groupId, resequenceProgress)
        }
      }
  }

  def from[K, V, MS](consumerSettings: ConsumerSettings[K, V],
                     messageSourceDecoder: ConsumerRecord[K, V] => MS,
                     messageSequenceNrDecoder: ConsumerRecord[K, V] => Long,
                     committedMessageSequenceNrs: Map[MS, Long],
                     committedStorageSequenceNrs: Map[TopicPartition, Long]): Source[ResequencedConsumerRecord[K, V, MS], NotUsed] = {
    val groupId = consumerSettings.getProperty(ConsumerConfig.GROUP_ID_CONFIG)
    fromInc(
      consumerSettings,
      messageSourceDecoder,
      messageSequenceNrDecoder,
      committedMessageSequenceNrs,
      committedStorageSequenceNrs)
    .scan(ResequencedConsumerRecord[K, V, MS](null, ResequenceProgress[MS, TopicPartition]())) {
      case (acc, crp) =>
        val msg = crp._1
        val prg = crp._2
        ResequencedConsumerRecord[K, V, MS](msg, acc.resequenceProgress.update(
          messageSourceDecoder(msg),
          messageSequenceNrDecoder(msg),
          prg.storagePartition,
          prg.storageSequenceNr))
    }.drop(1)
  }

  def fromInc[K, V, MS](consumerSettings: ConsumerSettings[K, V],
                        messageSourceDecoder: ConsumerRecord[K, V] => MS,
                        messageSequenceNrDecoder: ConsumerRecord[K, V] => Long,
                        committedMessageSequenceNrs: Map[MS, Long],
                        committedStorageSequenceNrs: Map[TopicPartition, Long]): Source[(ConsumerRecord[K, V], ResequenceProgressIncrement[TopicPartition]), NotUsed] =
    Consume.from(consumerSettings, committedStorageSequenceNrs)
      .via(new Resequence(
        messageSourceDecoder,
        messageSequenceNrDecoder,
        storagePartitionDecoder,
        storageSequenceNrDecoder,
        committedMessageSequenceNrs,
        committedStorageSequenceNrs))
}

class Resequence[M, MS, SP](messageSourceDecoder: M => MS,
                            messageSequenceNrDecoder: M => Long,
                            storagePartitionDecoder: M => SP,
                            storageSequenceNrDecoder: M => Long,
                            committedMessageSequenceNrs: immutable.Map[MS, Long],
                            committedStorageSequenceNrs: immutable.Map[SP, Long]) extends GraphStage[FlowShape[M, (M, ResequenceProgressIncrement[SP])]] {

  val in = Inlet[M]("Resequence.in")
  val out = Outlet[(M, ResequenceProgressIncrement[SP])]("Resequence.out")

  override val shape = FlowShape.of(in, out)

  // ----------------------------------------------------------
  //  TODO: FAIL STAGE IF INPUT STORAGE SEQUENCE HAS GAPS !!!!
  // ----------------------------------------------------------

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private val resequenceBuffers: mutable.Map[MS, ResequenceBuffer[M]] = mutable.Map.empty
      private val resequenceTrackers: mutable.Map[SP, ResequenceTracker] = mutable.Map.empty

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val m = grab(in)

          val msnr = messageSequenceNrDecoder(m)
          val ssnr = storageSequenceNrDecoder(m)

          val pt = getOrCreateResequenceTracker(storagePartitionDecoder(m))
          val rb = getOrCreateResequenceBuffer(messageSourceDecoder(m))

          if (pt.duplicate(ssnr)) {
            pull(in)
          } else if (rb.duplicate(msnr)) {
            pt.delivered(ssnr)
            pull(in)
          } else if (rb.deliverable(msnr)) {
            rb.delivered(msnr)
            pt.delivered(ssnr)
            push(out, (m, ResequenceProgressIncrement(storagePartitionDecoder(m), pt.highestGaplessStorageSequenceNr)))
          } else {
            rb.add(msnr, m)
            pull(in)
          }
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          nextMessage match {
            case None =>
              pull(in)
            case Some(m) =>
              val sp = storagePartitionDecoder(m)
              val pt = resequenceTrackers(sp)

              resequenceBuffers(messageSourceDecoder(m)).delivered(messageSequenceNrDecoder(m))
              pt.delivered(storageSequenceNrDecoder(m))
              push(out, (m, ResequenceProgressIncrement(sp, pt.highestGaplessStorageSequenceNr)))
          }
        }
      })

      private def nextMessage: Option[M] =
        resequenceBuffers.collectFirst { case (_, r) if r.hasNextMessage => r.nextMessage }

      private def getOrCreateResequenceBuffer(messageSource: MS): ResequenceBuffer[M] =
        if (resequenceBuffers.contains(messageSource)) resequenceBuffers(messageSource) else {
          val resequenceBuffer = new ResequenceBuffer[M](committedMessageSequenceNrs.getOrElse(messageSource, -1L))
          resequenceBuffers.update(messageSource, resequenceBuffer)
          resequenceBuffer
        }

      private def getOrCreateResequenceTracker(storagePartition: SP): ResequenceTracker =
        if (resequenceTrackers.contains(storagePartition)) resequenceTrackers(storagePartition) else {
          val resequenceTracker = new ResequenceTracker(committedStorageSequenceNrs.getOrElse(storagePartition, -1L))
          resequenceTrackers.update(storagePartition, resequenceTracker)
          resequenceTracker
        }
    }
}

private class ResequenceTracker(var highestGaplessStorageSequenceNr: Long) {
  var buffer: SortedSet[Long] = SortedSet.empty

  @tailrec
  final def delivered(storageSequenceNr: Long): Unit = {
    if (storageSequenceNr == highestGaplessStorageSequenceNr + 1L) {
      highestGaplessStorageSequenceNr = storageSequenceNr
      if (buffer.nonEmpty) {
        val hd = buffer.head
        buffer = buffer.tail
        delivered(hd)
      }
    } else buffer = buffer + storageSequenceNr
  }

  def duplicate(storageSequenceNr: Long): Boolean =
    storageSequenceNr <= highestGaplessStorageSequenceNr || buffer.contains(storageSequenceNr)
}

private class ResequenceBuffer[M](var highestDeliveredMessageSequenceNr: Long) {
  val buffer: mutable.SortedMap[Long, M] = mutable.SortedMap.empty

  def hasNextMessage: Boolean =
    buffer.contains(nextMessageSequenceNr)

  def nextMessage: M =
    buffer(nextMessageSequenceNr)

  def nextMessageSequenceNr: Long =
    highestDeliveredMessageSequenceNr + 1L

  def add(messageSequenceNr: Long, message: M): Unit =
    buffer.update(messageSequenceNr, message)

  def delivered(messageSequenceNr: Long): Unit = {
    buffer.remove(messageSequenceNr)
    highestDeliveredMessageSequenceNr = messageSequenceNr
  }

  def deliverable(messageSequenceNr: Long): Boolean =
    messageSequenceNr == highestDeliveredMessageSequenceNr + 1L

  def duplicate(messageSequenceNr: Long): Boolean =
    messageSequenceNr <= highestDeliveredMessageSequenceNr || buffer.contains(messageSequenceNr)
}
