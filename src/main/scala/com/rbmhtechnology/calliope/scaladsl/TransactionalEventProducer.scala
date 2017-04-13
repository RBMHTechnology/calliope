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

package com.rbmhtechnology.calliope.scaladsl

import akka.Done
import akka.actor.SupervisorStrategy.{Escalate, Stop}
import akka.actor.{Actor, ActorLogging, ActorSystem, OneForOneStrategy, Props, Stash, SupervisorStrategy}
import akka.event.{Logging, LoggingAdapter}
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.pattern.gracefulStop
import akka.stream.scaladsl.{Keep, RunnableGraph, SourceQueueWithComplete}
import akka.stream.{ActorMaterializer, Attributes}
import com.rbmhtechnology.calliope._
import com.rbmhtechnology.calliope.scaladsl.TransactionalEventProducer.{FailureHandler, ProducerGraph, Settings, UnexpectedStreamCompletionException}
import com.rbmhtechnology.calliope.serializer.kafka.PayloadFormatSerializer
import com.typesafe.config.Config
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.Future
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.{Failure, Success}

object TransactionalEventProducer {

  class UnexpectedStreamCompletionException(message: String) extends RuntimeException(message)

  class StreamFailedException(message: String) extends RuntimeException(message: String)

  type ProducerGraph = RunnableGraph[(SourceQueueWithComplete[Unit], Future[Done])]
  type FailureHandler = (Throwable) => Unit
  private val EmptyFailureHandler: FailureHandler = _ => {}

  def apply[A](sourceId: String, topic: String, eventStore: EventStore, settings: Settings[A])
              (implicit system: ActorSystem): TransactionalEventProducer[A] =
    apply(sourceId, topic, eventStore, settings, EmptyFailureHandler)

  def apply[A](sourceId: String, topic: String, eventStore: EventStore, onFailure: FailureHandler = EmptyFailureHandler)
              (implicit system: ActorSystem): TransactionalEventProducer[A] =
    apply(sourceId, topic, eventStore, Settings(), onFailure)

  def apply[A](sourceId: String, topic: String, eventStore: EventStore, settings: Settings[A], onFailure: FailureHandler)
              (implicit system: ActorSystem): TransactionalEventProducer[A] =
    new TransactionalEventProducer(sourceId, topic, eventStore, onFailure, settings)

  object Settings {
    import DurationConverters._

    def apply[A]()(implicit system: ActorSystem): Settings[A] =
      apply(system.settings.config.getConfig("calliope.transactional-event-producer"))

    def apply[A](config: Config)(implicit system: ActorSystem): Settings[A] =
      apply(config.getInt("read-buffer-size"),
        config.getDuration("read-interval").toFiniteDuration,
        config.getDuration("delete-interval").toFiniteDuration,
        config.getDuration("transaction-timeout").toFiniteDuration,
        config.getString("bootstrap-servers"),
        config.getConfig("producer"))

    def apply[A](readBufferSize: Int,
                 readInterval: FiniteDuration,
                 deleteInterval: FiniteDuration,
                 transactionTimeout: FiniteDuration,
                 bootstrapServers: String)(implicit system: ActorSystem): Settings[A] =
      apply(readBufferSize, readInterval, deleteInterval, transactionTimeout, bootstrapServers,
        system.settings.config.getConfig("calliope.transactional-event-producer.producer"))

    def apply[A](readBufferSize: Int,
                 readInterval: FiniteDuration,
                 deleteInterval: FiniteDuration,
                 transactionTimeout: FiniteDuration,
                 bootstrapServers: String,
                 producerConfig: Config)(implicit system: ActorSystem): Settings[A] =
      new Settings[A](readBufferSize, readInterval, deleteInterval, transactionTimeout,
        ProducerSettings(producerConfig, new StringSerializer(), PayloadFormatSerializer.apply[SequencedEvent[A]])
          .withBootstrapServers(bootstrapServers))
  }

  class Settings[A] private(val readBufferSize: Int,
                            val readInterval: FiniteDuration,
                            val deleteInterval: FiniteDuration,
                            val transactionTimeout: FiniteDuration,
                            val producerSettings: ProducerSettings[String, SequencedEvent[A]]) {
    lazy val producer: KafkaProducer[String, SequencedEvent[A]] = producerSettings.createKafkaProducer()

    def withReadBufferSize(readBufferSize: Int): Settings[A] =
      copy(readBufferSize = readBufferSize)

    def withReadInterval(readInterval: FiniteDuration): Settings[A] =
      copy(readInterval = readInterval)

    def withDeleteInterval(deleteInterval: FiniteDuration): Settings[A] =
      copy(deleteInterval = deleteInterval)

    def withTransactionTimeout(transactionTimeout: FiniteDuration): Settings[A] =
      copy(transactionTimeout = transactionTimeout)

    def withBootstrapServers(bootstrapServers: String): Settings[A] =
      copy(producerSettings = producerSettings.withBootstrapServers(bootstrapServers))

    def withProducerCloseTimeout(closeTimeout: FiniteDuration): Settings[A] =
      copy(producerSettings = producerSettings.withCloseTimeout(closeTimeout))

    def withProducerDispatcher(dispatcher: String): Settings[A] =
      copy(producerSettings = producerSettings.withDispatcher(dispatcher))

    def withProducerParallelism(parallelism: Int): Settings[A] =
      copy(producerSettings = producerSettings.withParallelism(parallelism))

    def withProducerProperty(key: String, value: String): Settings[A] =
      copy(producerSettings = producerSettings.withProperty(key, value))

    private def copy(readBufferSize: Int = readBufferSize,
                     readInterval: FiniteDuration = readInterval,
                     deleteInterval: FiniteDuration = deleteInterval,
                     transactionTimeout: FiniteDuration = transactionTimeout,
                     producerSettings: ProducerSettings[String, SequencedEvent[A]] = producerSettings): Settings[A] =
      new Settings(readBufferSize, readInterval, deleteInterval, transactionTimeout, producerSettings)
  }
}

class TransactionalEventProducer[A] private(sourceId: String, topic: String, eventStore: EventStore, onFailure: FailureHandler, settings: Settings[A])
                                           (implicit val system: ActorSystem) extends IoDispatcher {

  import ProducerGraphRunner._

  private implicit val loggingAdapter = Logging(system, getClass)

  private var running = false

  private val graph = ProducerGraph(
    EventStoreReader.withTimestampGapDetection(sourceId, eventStore, settings.transactionTimeout),
    new EventStoreDeleter(eventStore),
    settings)

  private lazy val graphRunner = system.actorOf(Props(new GraphRunnerSupervisor(graph, onFailure)))

  def run(): EventWriter[A] = {
    running = true
    val runner = graphRunner
    new EventStoreWriter[A](topic, eventStore, runner ! NotifyCommit)
  }

  def stop(timeout: FiniteDuration = 5.seconds): Future[Boolean] = {
    if (running)
      gracefulStop(graphRunner, timeout)
    else
      Future.successful(true)
  }
}

private object ProducerGraph {
  def apply[A](eventReader: EventReader[A], eventDeleter: EventDeleter, settings: Settings[A])(implicit logging: LoggingAdapter): ProducerGraph =
    EventSource(eventReader, settings.readBufferSize, settings.readInterval)
      .viaMat(ProducerFlow[EventRecord[A]].toMessage)(Keep.left)
      .viaMat(Producer.flow(settings.producerSettings, settings.producer))(Keep.left)
      .log("produced", identity).withAttributes(Attributes.logLevels(onFailure = Logging.DebugLevel))
      .map(_.message.record.value())
      .toMat(EventDeletion.sink(eventDeleter, settings.deleteInterval))(Keep.both)
}

private class GraphRunnerSupervisor(graph: ProducerGraph, onFailure: FailureHandler) extends Actor {
  import SupervisorStrategyExtensions._

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy()(SupervisorStrategy.defaultDecider.foreach {
    case (err, Stop) =>
      onFailure.apply(err)
    case (err, Escalate) =>
      onFailure.apply(err)
  })

  private val graphRunner = context.actorOf(ProducerGraphRunner.props(graph))

  override def receive: Receive = {
    case msg => graphRunner forward msg
  }
}

private object ProducerGraphRunner {

  case object NotifyCommit

  private[ProducerGraphRunner] case object StartGraph
  private[ProducerGraphRunner] case class RestartGraph(failure: Throwable)

  def props(graph: ProducerGraph): Props =
    Props(new ProducerGraphRunner(graph))
}

private class ProducerGraphRunner(graph: ProducerGraph) extends Actor with ActorLogging with Stash {

  import ProducerGraphRunner._
  import context.dispatcher

  implicit val materializer = ActorMaterializer()

  private def runGraph(): SourceQueueWithComplete[Unit] = {
    val (queue, ft) = graph.run()

    ft onComplete { result =>
      val cause = result match {
        case Failure(err) => err
        case Success(_) => new UnexpectedStreamCompletionException("Transactional producer stream stopped unexpectedly")
      }
      self ! RestartGraph(cause)
    }
    queue
  }

  private var commitQueue: SourceQueueWithComplete[Unit] = _

  override def preStart(): Unit = {
    self ! StartGraph
  }

  override def postStop(): Unit = {
    materializer.shutdown()
    super.postStop()
  }

  private def initializing: Receive = {
    case StartGraph =>
      log.info("Starting transactional producer.")
      commitQueue = runGraph()
      unstashAll()
      context.become(initialized)

    case RestartGraph(_) =>
      log.info("Restart of transactional producer requested while initialization in progress. Restart request is ignored.")

    case NotifyCommit =>
      stash()
  }

  private def initialized: Receive = {
    case NotifyCommit =>
      commitQueue.offer(Unit)

    case RestartGraph(failure) =>
      log.warning("Transactional producer was shutdown with cause: {}. Producer will be restarted", failure)
      commitQueue = runGraph()
  }

  override def receive: Receive =
    initializing
}
