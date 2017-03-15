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

import java.util.concurrent.atomic.AtomicBoolean

import akka.NotUsed
import akka.kafka.ConsumerSettings
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.immutable.Map
import scala.concurrent.Future
import scala.util._

object Consume {
  def fromSnapshot[K, V](consumerSettings: ConsumerSettings[K, V],
                         committedStorageSequenceNrs: Map[TopicPartition, Long]): Source[ConsumerRecord[K, V], NotUsed] =
    Offsets.endOffsets(consumerSettings, committedStorageSequenceNrs.keySet).flatMapConcat(range(consumerSettings, committedStorageSequenceNrs, _))

  def from[K, V](consumerSettings: ConsumerSettings[K, V],
                 committedStorageSequenceNrs: Map[TopicPartition, Long]): Source[ConsumerRecord[K, V], NotUsed] =
    range(consumerSettings, committedStorageSequenceNrs, Map.empty)

  def range[K, V](consumerSettings: ConsumerSettings[K, V],
                  committedStorageSequenceNrs: Map[TopicPartition, Long],
                  currentStorageSequenceNrs: Map[TopicPartition, Long]): Source[ConsumerRecord[K, V], NotUsed] =
    Source.fromGraph(new Consume(consumerSettings, committedStorageSequenceNrs, currentStorageSequenceNrs.withDefaultValue(Long.MaxValue)))
}

class Consume[K, V](consumerSettings: ConsumerSettings[K, V],
                    committedStorageSequenceNrs: Map[TopicPartition, Long],
                    currentStorageSequenceNrs: Map[TopicPartition, Long]) extends GraphStage[SourceShape[ConsumerRecord[K, V]]] {

  val out: Outlet[ConsumerRecord[K, V]] =
    Outlet("Consume")

  override val shape: SourceShape[ConsumerRecord[K, V]] =
    SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private val stopped = new AtomicBoolean(false)
      private var consumer: KafkaConsumer[K, V] = _
      private var records: Iterator[ConsumerRecord[K, V]] = Iterator.empty
      private var paused: Set[TopicPartition] = Set.empty

      setHandler(out, new OutHandler {
        override def onPull(): Unit = process()
      })

      @tailrec
      private def process(): Unit =
        if (records.hasNext) {
          val cr = records.next()
          val tp = storagePartitionDecoder(cr)

          if (paused.contains(tp)) process()
          else if (withinRange(tp, cr.offset)) {
            push(out, cr)
            if (!withinRange(tp, cr.offset + 1L)) pause(tp)
          } else process()
        } else poll.onComplete(getAsyncCallback(handle).invoke)(materializer.executionContext)

      private def handle(result: Try[Iterator[ConsumerRecord[K, V]]]): Unit = result match {
        case Success(crs) => records = crs; process()
        case Failure(thr) => failStage(thr)
      }

      private def poll: Future[Iterator[ConsumerRecord[K, V]]] =
        pollOnce.flatMap(iter => if (iter.hasNext) Future.successful(iter) else poll)(materializer.executionContext)

      private def pollOnce: Future[Iterator[ConsumerRecord[K, V]]] =
        Future(pollOnceSync)(materializer.executionContext)

      private def pollOnceSync: Iterator[ConsumerRecord[K, V]] = {
        // --------------------------------------------------------------------
        // TODO: consider replacing the concatenating iterator over poll result
        // The concatenating iterator brings elements further out of order and
        // therefore increases load on the resequencer (this must be verified).
        // --------------------------------------------------------------------
        try if (stopped.get) Iterator.empty else asScalaIterator(consumer.poll(500L).iterator)
        catch { case e: WakeupException => consumer.close(); Iterator.empty }
      }


      private def withinRange(storagePartition: TopicPartition, storageSequenceNr: Long): Boolean =
        storageSequenceNr < currentStorageSequenceNrs(storagePartition)

      private def pause(tp: TopicPartition): Unit = {
        consumer.pause(List(tp).asJava)
        paused = paused + tp
        if (paused == committedStorageSequenceNrs.keySet) completeStage()
      }

      override def preStart(): Unit = {
        consumer = consumerSettings.createKafkaConsumer()
        consumer.assign(committedStorageSequenceNrs.keySet.asJava)
        committedStorageSequenceNrs.foreach {
          case (tp, snr) if withinRange(tp, snr + 1L) => consumer.seek(tp, snr + 1L)
          case (tp, _)                                => pause(tp)
        }
      }

      override def postStop(): Unit = {
        stopped.set(true)
        consumer.wakeup()
      }
    }
}
