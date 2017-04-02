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

object KafkaMetadata {
  def beginOffsets[K, V](consumerSettings: ConsumerSettings[K, V], topicPartitions: Set[TopicPartition]): Source[Map[TopicPartition, Long], NotUsed] =
    offsets[K, V](consumerSettings, beginOffsets(_, topicPartitions))

  def endOffsets[K, V](consumerSettings: ConsumerSettings[K, V], topic: String): Source[Map[TopicPartition, Long], NotUsed] =
    offsets[K, V](consumerSettings, endOffsets(_, topic))

  def committedOffsets[K, V](consumerSettings: ConsumerSettings[K, V], topic: String): Source[Map[TopicPartition, Long], NotUsed] =
    offsets[K, V](consumerSettings, committedOffsets(_, topic))

  def topicPartitions[K, V](consumerSettings: ConsumerSettings[K, V], topic: String): Source[Set[TopicPartition], NotUsed] =
    Source.fromGraph(new KafkaMetadata(consumerSettings)(topicPartitions(_, topic)))

  private def offsets[K, V](consumerSettings: ConsumerSettings[K, V], f: KafkaConsumer[K, V] => Map[TopicPartition, Long]): Source[Map[TopicPartition, Long], NotUsed] =
    Source.fromGraph(new KafkaMetadata(consumerSettings)(f))

  private def beginOffsets[K, V](consumer: KafkaConsumer[K, V], topicPartitions: Set[TopicPartition]): Map[TopicPartition, Long] =
    consumer.beginningOffsets(topicPartitions.asJava).asScala.mapValues(_.toLong).toMap

  private def endOffsets[K, V](consumer: KafkaConsumer[K, V], topic: String): Map[TopicPartition, Long] =
    consumer.endOffsets(topicPartitions(consumer, topic).asJava).asScala.mapValues(_.toLong).toMap

  private def committedOffsets[K, V](consumer: KafkaConsumer[K, V], topic: String): Map[TopicPartition, Long] =
    topicPartitions(consumer, topic).foldLeft(Map.empty[TopicPartition, Long]) { case (acc, tp) => acc.updated(tp, Option(consumer.committed(tp)).map(_.offset).getOrElse(0L)) }

  private def topicPartitions[K, V](consumer: KafkaConsumer[K, V], topic: String): Set[TopicPartition] =
    consumer.partitionsFor(topic).asScala.map(pi => new TopicPartition(topic, pi.partition)).toSet
}

private class KafkaMetadata[K, V, R](consumerSettings: ConsumerSettings[K, V])(f: KafkaConsumer[K, V] => R) extends GraphStage[SourceShape[R]] {
  val out: Outlet[R] =
    Outlet("KafkaMetadata.out")

  override val shape: SourceShape[R] =
    SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      var consumer: KafkaConsumer[K, V] = _

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          push(out, f(consumer))
        }
      })

      override def preStart(): Unit =
        consumer = consumerSettings.createKafkaConsumer()

      override def postStop(): Unit =
        consumer.close()
    }
}
