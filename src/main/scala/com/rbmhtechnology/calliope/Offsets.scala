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
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.collection.immutable.Map

object Offsets {
  def endOffsets[K, V](consumerSettings: ConsumerSettings[K, V], storagePartitions: Set[TopicPartition]): Source[Map[TopicPartition, Long], NotUsed] =
    Source.fromGraph(new Offsets(consumerSettings)(_.endOffsets(storagePartitions.asJava).asScala.mapValues(_.toLong).toMap))
}

private class Offsets[K, V](consumerSettings: ConsumerSettings[K, V])(f: KafkaConsumer[K, V] => Map[TopicPartition, Long]) extends GraphStage[SourceShape[Map[TopicPartition, Long]]] {
  val out: Outlet[Map[TopicPartition, Long]] =
    Outlet("Offsets")

  override val shape: SourceShape[Map[TopicPartition, Long]] =
    SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      var consumer: KafkaConsumer[K, V] = _

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          push(out, f(consumer))
          completeStage()
        }
      })

      override def preStart(): Unit =
        consumer = consumerSettings.createKafkaConsumer()

      override def postStop(): Unit =
        consumer.close()
    }
}
