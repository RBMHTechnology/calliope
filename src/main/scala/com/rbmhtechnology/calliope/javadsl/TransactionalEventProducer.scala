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

package com.rbmhtechnology.calliope.javadsl

import java.time.{Duration => JDuration}
import java.util.concurrent.CompletableFuture
import java.util.function.{Consumer, Function => JFunction}

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.stream.javadsl.Flow
import com.rbmhtechnology.calliope.DurationConverters._
import com.rbmhtechnology.calliope.{SequencedEvent, scaladsl}
import com.typesafe.config.Config

object TransactionalEventProducer {

  import EventStore._

  type ProducerFlow[A] = Flow[ProducerMessage.Message[String, SequencedEvent[A], Unit], ProducerMessage.Result[String, SequencedEvent[A], Unit], NotUsed]
  type ProducerProvider[A] = JFunction[ProducerSettings[String, SequencedEvent[A]], ProducerFlow[A]]

  def create[A](sourceId: String, topic: String, eventStore: EventStore, system: ActorSystem): TransactionalEventProducer[A] = {
    new TransactionalEventProducer[A](scaladsl.TransactionalEventProducer[A](sourceId, topic, eventStore.asScala)(system))
  }

  def create[A](sourceId: String, topic: String, eventStore: EventStore, settings: Settings[A], system: ActorSystem): TransactionalEventProducer[A] = {
    new TransactionalEventProducer[A](scaladsl.TransactionalEventProducer[A](sourceId, topic, eventStore.asScala, settings.delegate)(system))
  }

  def create[A](sourceId: String, topic: String, eventStore: EventStore, onFailure: Consumer[Throwable], system: ActorSystem): TransactionalEventProducer[A] = {
    new TransactionalEventProducer[A](scaladsl.TransactionalEventProducer[A](sourceId, topic, eventStore.asScala, onFailure.accept _)(system))
  }

  def create[A](sourceId: String, topic: String, eventStore: EventStore, settings: Settings[A], onFailure: Consumer[Throwable], system: ActorSystem): TransactionalEventProducer[A] = {
    new TransactionalEventProducer[A](scaladsl.TransactionalEventProducer[A](sourceId, topic, eventStore.asScala, settings.delegate, onFailure.accept _)(system))
  }

  def settings[A](system: ActorSystem): Settings[A] =
    new Settings(scaladsl.TransactionalEventProducer.Settings[A]()(system))

  def settings[A](config: Config, system: ActorSystem): Settings[A] =
    new Settings(scaladsl.TransactionalEventProducer.Settings[A](config)(system))

  def settings[A](readBufferSize: Int,
                  readInterval: JDuration,
                  deleteInterval: JDuration,
                  transactionTimeout: JDuration,
                  bootstrapServers: String,
                  system: ActorSystem): Settings[A] =
    new Settings[A](scaladsl.TransactionalEventProducer.Settings(
      readBufferSize,
      readInterval.toFiniteDuration,
      deleteInterval.toFiniteDuration,
      transactionTimeout.toFiniteDuration,
      bootstrapServers)(system))

  def settings[A](readBufferSize: Int,
                  readInterval: JDuration,
                  deleteInterval: JDuration,
                  transactionTimeout: JDuration,
                  bootstrapServers: String,
                  producerConfig: Config,
                  system: ActorSystem): Settings[A] =
    new Settings(scaladsl.TransactionalEventProducer.Settings(
      readBufferSize,
      readInterval.toFiniteDuration,
      deleteInterval.toFiniteDuration,
      transactionTimeout.toFiniteDuration,
      bootstrapServers,
      producerConfig)(system))

  class Settings[A] private[javadsl](private[javadsl] val delegate: scaladsl.TransactionalEventProducer.Settings[A]) {

    def withReadBufferSize(readBufferSize: Int): Settings[A] =
      copy(delegate.withReadBufferSize(readBufferSize = readBufferSize))

    def withReadInterval(readInterval: JDuration): Settings[A] =
      copy(delegate.withReadInterval(readInterval.toFiniteDuration))

    def withDeleteInterval(deleteInterval: JDuration): Settings[A] =
      copy(delegate.withDeleteInterval(deleteInterval.toFiniteDuration))

    def withTransactionTimeout(transactionTimeout: JDuration): Settings[A] =
      copy(delegate.withTransactionTimeout(transactionTimeout.toFiniteDuration))

    def withBootstrapServers(bootstrapServers: String): Settings[A] =
      copy(delegate.withBootstrapServers(bootstrapServers))

    def withProducerCloseTimeout(closeTimeout: JDuration): Settings[A] =
      copy(delegate.withProducerCloseTimeout(closeTimeout.toFiniteDuration))

    def withProducerDispatcher(dispatcher: String): Settings[A] =
      copy(delegate.withProducerDispatcher(dispatcher))

    def withProducerParallelism(parallelism: Int): Settings[A] =
      copy(delegate.withProducerParallelism(parallelism))

    def withProducerProperty(key: String, value: String): Settings[A] =
      copy(delegate.withProducerProperty(key, value))

    def withProducerProvider(producerProvider: ProducerProvider[A]): Settings[A] =
      copy(delegate.withProducerProvider(s => producerProvider.apply(s).asScala))

    private def copy(delegate: scaladsl.TransactionalEventProducer.Settings[A]): Settings[A] =
      new Settings(delegate)
  }
}

class TransactionalEventProducer[A] private(delegate: scaladsl.TransactionalEventProducer[A]) {

  import scala.compat.java8.FutureConverters._

  /**
    * Starts producing events from the underlying [[EventStore]] to the configured Kafka endpoint.
    *
    * This is an asynchronous operation which returns a [[CompletableFuture]] that is resolved once event production has started or fails if the
    * given timeout was exceeded.
    */
  def run(timeout: JDuration): CompletableFuture[Done] =
    delegate.run()(timeout.toFiniteDuration).toJava.toCompletableFuture

  /**
    * Starts producing events from the underlying [[EventStore]] to the configured Kafka endpoint.
    *
    * This is an asynchronous operation which returns a [[CompletableFuture]] that is resolved once event production has started or fails if the
    * default-timeout was exceeded.
    *
    * <p><b>Uses a default-timeout of 5 seconds</b><p>
    */
  def run(): CompletableFuture[Done] =
    delegate.run().toJava.toCompletableFuture

  /**
    * Stops producing events from the underlying [[EventStore]].
    *
    * This is an asynchronous operation which returns a [[CompletableFuture]] that is resolved once event production has stopped or fails if the
    * given timeout was exceeded.
    *
    * The TransactionalEventProducer may be started afterwards by invoking `start()` to continue event production.
    *
    * <p>
    * <b>IMPORTANT:</b>
    * This method will stop event-production without removing already produced events from the underlying store. Duplicates may be produced
    * by a subsequent call to `run()` as a consequence.
    * </p>
    */
  def stop(timeout: JDuration): CompletableFuture[Done] =
    delegate.stop()(timeout.toFiniteDuration).toJava.toCompletableFuture

  /**
    * Stops producing events from the underlying [[EventStore]].
    *
    * This is an asynchronous operation which returns a [[CompletableFuture]] that is resolved once event production has stopped or fails if the
    * timeout was exceeded.
    *
    * The TransactionalEventProducer may be started afterwards by invoking `start()` to continue event production.
    *
    * <p><b>Uses a default-timeout of 5 seconds</b></p>
    *
    * <p>
    * <b>IMPORTANT:</b>
    * This method will stop event-production without removing already produced events from the underlying store. Duplicates may be produced
    * by a subsequent call to `run()` as a consequence.
    * </p>
    */
  def stop(): CompletableFuture[Done] =
    delegate.stop().toJava.toCompletableFuture

  /**
    * Terminates the TransactionalEventProducer. No event production can be performed after invoking this method.
    * Should be used for cleanup purposes.
    *
    * This is an asynchronous operation which returns a [[CompletableFuture]] that is resolved once the component has been terminated or fails if the
    * given timeout was exceeded.
    *
    * <p>
    * <b>IMPORTANT:</b>
    * This method will stop event-production without removing already produced events from the underlying store. Duplicates may be produced
    * by a subsequent incarnation of the [[TransactionalEventProducer]] as a consequence.
    * </p>
    */
  def terminate(timeout: JDuration): CompletableFuture[Done] =
    delegate.terminate()(timeout.toFiniteDuration).toJava.toCompletableFuture

  /**
    * Terminates the TransactionalEventProducer. No event production can be performed after invoking this method.
    * Should be used for cleanup purposes.
    *
    * This is an asynchronous operation which returns a [[CompletableFuture]] that is resolved once the component has been terminated or fails if the
    * default-timeout was exceeded.
    *
    * <p><b>Uses a default-timeout of 5 seconds</b></p>
    *
    * <p>
    * <b>IMPORTANT:</b>
    * This method will stop event-production without removing already produced events from the underlying store. Duplicates may be produced
    * by a subsequent incarnation of the [[TransactionalEventProducer]] as a consequence.
    * </p>
    */
  def terminate(): CompletableFuture[Done] =
    delegate.terminate().toJava.toCompletableFuture

  /**
    * Creates an [[EventWriter]] which is used to store events to the underlying [[EventStore]] and consecutively produce them to the configured
    * Kafka endpoint.
    */
  def eventWriter(): EventWriter[A] =
    new EventWriter[A](delegate.eventWriter())
}
