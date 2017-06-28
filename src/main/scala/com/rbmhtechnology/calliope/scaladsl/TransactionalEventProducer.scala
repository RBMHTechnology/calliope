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

import akka.actor.SupervisorStrategy.{Escalate, Stop}
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, FSM, OneForOneStrategy, Props, Stash, SupervisorStrategy}
import akka.event.{Logging, LoggingAdapter}
import akka.kafka.scaladsl.Producer
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.pattern.{gracefulStop, _}
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, SourceQueueWithComplete}
import akka.stream.{ActorMaterializer, Attributes, KillSwitches, UniqueKillSwitch}
import akka.util.Timeout
import akka.{Done, NotUsed}
import com.rbmhtechnology.calliope._
import com.rbmhtechnology.calliope.scaladsl.TransactionalEventProducer.{FailureHandler, ProducerGraph, Settings}
import com.rbmhtechnology.calliope.serializer.kafka.PayloadFormatSerializer
import com.typesafe.config.Config
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.{Failure, Success}

object TransactionalEventProducer {

  class UnexpectedStreamCompletionException(message: String) extends RuntimeException(message)

  class StreamFailedException(message: String) extends RuntimeException(message: String)

  type ProducerGraph = RunnableGraph[((SourceQueueWithComplete[Unit], UniqueKillSwitch), Future[Done])]
  type FailureHandler = (Throwable) => Unit
  type ProducerFlow[A] = Flow[ProducerMessage.Message[String, SequencedEvent[A], Unit], ProducerMessage.Result[String, SequencedEvent[A], Unit], NotUsed]
  type ProducerProvider[A] = ProducerSettings[String, SequencedEvent[A]] => ProducerFlow[A]

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
        ProducerSettings(producerConfig, new StringSerializer(), PayloadFormatSerializer.apply[SequencedEvent[A]]).withBootstrapServers(bootstrapServers),
        Producer.flow(_))
  }

  class Settings[A] private(val readBufferSize: Int,
                            val readInterval: FiniteDuration,
                            val deleteInterval: FiniteDuration,
                            val transactionTimeout: FiniteDuration,
                            val producerSettings: ProducerSettings[String, SequencedEvent[A]],
                            private val producerProvider: ProducerProvider[A]) {

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

    def withProducerProvider(producerProvider: ProducerProvider[A]): Settings[A] =
      copy(producerProvider = producerProvider)

    private[calliope] def producerFlow(): ProducerFlow[A] =
      producerProvider(producerSettings)

    private def copy(readBufferSize: Int = readBufferSize,
                     readInterval: FiniteDuration = readInterval,
                     deleteInterval: FiniteDuration = deleteInterval,
                     transactionTimeout: FiniteDuration = transactionTimeout,
                     producerSettings: ProducerSettings[String, SequencedEvent[A]] = producerSettings,
                     producerProvider: ProducerProvider[A] = producerProvider): Settings[A] =
      new Settings(readBufferSize, readInterval, deleteInterval, transactionTimeout, producerSettings, producerProvider)
  }
}

class TransactionalEventProducer[A] private(sourceId: String, topic: String, eventStore: EventStore, onFailure: FailureHandler, settings: Settings[A])
                                           (implicit val system: ActorSystem) extends IoDispatcher {

  import ProducerGraphRunner._
  import system.dispatcher

  private implicit val loggingAdapter = Logging(system, getClass)

  private val graph = ProducerGraph(
    EventStoreReader.withTimestampGapDetection(sourceId, eventStore, settings.transactionTimeout)(system, ioDispatcher),
    new EventStoreDeleter(eventStore)(ioDispatcher),
    settings)

  private val graphRunner = system.actorOf(Props(new GraphRunnerSupervisor(graph, onFailure)))

  /**
    * Creates an [[EventWriter]] which is used to store events to the underlying [[EventStore]] and consecutively produce them to the configured
    * Kafka endpoint.
    */
  def eventWriter(): EventWriter[A] =
    new EventStoreWriter[A](topic, eventStore, graphRunner ! NotifyCommit)

  /**
    * Starts producing events from the underlying [[EventStore]] to the configured Kafka endpoint.
    *
    * This is an asynchronous operation which returns a [[Future]] that is resolved once event production has started.
    * The future may fail with an [[AskTimeoutException]] if the producer was not able to respond withing the given timeout.
    */
  def run()(implicit timeout: FiniteDuration = 5.seconds): Future[Done] =
    askGraphRunnerTo(RunGraph)

  /**
    * Stops producing events from the underlying [[EventStore]].
    *
    * This is an asynchronous operation which returns a [[Future]] that is resolved once event production has stopped.
    * The future may fail with an [[AskTimeoutException]] if the producer was not able to respond withing the given timeout.
    *
    * The TransactionalEventProducer may be started afterwards by invoking `start()` to continue event production.
    *
    * <p>
    * <b>IMPORTANT:</b>
    * This method will stop event-production without removing already produced events from the underlying store. Duplicates may be produced
    * by a subsequent call to `run()` as a consequence.
    * </p>
    */
  def stop()(implicit timeout: FiniteDuration = 5.seconds): Future[Done] =
    askGraphRunnerTo(StopGraph)

  /**
    * Terminates the TransactionalEventProducer. No event production can be performed after invoking this method.
    * Should be used for cleanup purposes.
    *
    * This is an asynchronous operation which returns a [[Future]] that is resolved once the component has been terminated.
    * The future may fail with an [[AskTimeoutException]] if the producer was not able to respond withing the given timeout.
    *
    * <p>
    * <b>IMPORTANT:</b>
    * This method will stop event-production without removing already produced events from the underlying store. Duplicates may be produced
    * by a subsequent incarnation of the [[TransactionalEventProducer]] as a consequence.
    * </p>
    */
  def terminate()(implicit timeout: FiniteDuration = 5.seconds): Future[Done] = {
    val deadline = Deadline.now + timeout
    stop().flatMap(_ => gracefulStop(graphRunner, deadline.timeLeft)).map(_ => Done)
  }

  private def askGraphRunnerTo(msg: Any)(implicit timeout: FiniteDuration): Future[Done] = {
    ask(graphRunner, msg)(Timeout(timeout)).map(_ => Done)
  }
}

private object ProducerGraph {
  def apply[A](eventReader: EventReader[A], eventDeleter: EventDeleter, settings: Settings[A])(implicit logging: LoggingAdapter): ProducerGraph =
    EventSource(eventReader, settings.readBufferSize, settings.readInterval)
      .viaMat(KillSwitches.single)(Keep.both)
      .viaMat(ProducerFlow[EventRecord[A]].toMessage)(Keep.left)
      .viaMat(settings.producerFlow())(Keep.left)
      .log("produced", identity).withAttributes(Attributes.logLevels(onFailure = Logging.DebugLevel))
      .map(_.message.record.value())
      .toMat(EventDeletion.sink(eventDeleter, settings.deleteInterval))(Keep.both)
}

private class GraphRunnerSupervisor(graph: ProducerGraph, onFailure: FailureHandler) extends Actor {
  import SupervisorStrategyExtensions._

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy()(SupervisorStrategy.defaultDecider.foreach {
    case (err, Stop) =>
      onFailure(err)
    case (err, Escalate) =>
      onFailure(err)
  })

  private val graphRunner = context.actorOf(ProducerGraphRunner.props(graph))

  override def receive: Receive = {
    case msg => graphRunner forward msg
  }
}

private[calliope] object ProducerGraphRunner {

  case object NotifyCommit
  case object RunGraph
  case object StopGraph

  case object GraphStarted
  case object GraphStopped

  private[ProducerGraphRunner] case class RestartGraph(failure: Throwable)

  private[ProducerGraphRunner] case class GraphAbortedException() extends RuntimeException

  private[ProducerGraphRunner] sealed trait State
  private[ProducerGraphRunner] case object Stopped extends State
  private[ProducerGraphRunner] case object Stopping extends State
  private[ProducerGraphRunner] case object Running extends State

  private[ProducerGraphRunner] case class Data(commitQueue: Option[SourceQueueWithComplete[Unit]] = None,
                                               killSwitch: Option[UniqueKillSwitch] = None,
                                               notifyOnStop: immutable.Seq[ActorRef] = Vector.empty) {
    def withQueue(queue: SourceQueueWithComplete[Unit]): Data =
      copy(commitQueue = Some(queue))

    def withKillSwitch(killSwitch: UniqueKillSwitch): Data =
      copy(killSwitch = Some(killSwitch))

    def withNotificationFor(target: ActorRef): Data =
      copy(notifyOnStop = notifyOnStop :+ target)

    def withoutNotifications(): Data =
      copy(notifyOnStop = Vector.empty)
  }

  def props(graph: ProducerGraph): Props =
    Props(new ProducerGraphRunner(graph))
}

private[calliope] class ProducerGraphRunner(graph: ProducerGraph) extends Actor
  with ActorLogging with Stash with FSM[ProducerGraphRunner.State, ProducerGraphRunner.Data] {

  import ProducerGraphRunner._
  import context.dispatcher

  implicit val materializer = ActorMaterializer()

  private def runGraph(): (SourceQueueWithComplete[Unit], UniqueKillSwitch) = {
    val (graphControl, ft) = graph.run()

    ft recover {
      case _: GraphAbortedException => Done
    } onComplete {
      case Failure(err) => self ! RestartGraph(err)
      case Success(_) => self ! GraphStopped
    }
    graphControl
  }

  startWith(Stopped, Data())

  when(Stopped) {
    case Event(RunGraph, data) =>
      log.info("Starting transactional producer stream.")
      val (queue, ks) = runGraph()
      goto(Running) using data.withQueue(queue).withKillSwitch(ks) replying GraphStarted

    case Event(StopGraph, _) =>
      stay replying GraphStopped
  }

  when(Running) {
    case Event(RunGraph, _) =>
      stay replying GraphStarted

    case Event(StopGraph, data) =>
      log.info("Shutting down transactional producer stream.")
      data.killSwitch.foreach(_.abort(GraphAbortedException()))
      goto(Stopping) using data.withNotificationFor(sender())

    case Event(RestartGraph(failure), data) =>
      log.warning("Transactional producer stream was shutdown with cause: {}. Producer will be restarted", failure)
      val (queue, ks) = runGraph()
      stay using data.withQueue(queue).withKillSwitch(ks)

    case Event(NotifyCommit, data) =>
      data.commitQueue.foreach(_.offer(Unit))
      stay
  }

  when(Stopping) {
    case Event(RunGraph | StopGraph, _) =>
      stash()
      stay

    case Event(GraphStopped, data) =>
      log.info("Transactional producer stream stopped.")
      data.notifyOnStop.foreach(_ ! GraphStopped)
      unstashAll()
      goto(Stopped) using data.withoutNotifications()
  }

  whenUnhandled {
    case Event(ev, _) =>
      stay
  }

  initialize()

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    if (stateName == Running) {
      self ! RunGraph
    }
    super.preRestart(reason, message)
  }

  override def postStop(): Unit = {
    materializer.shutdown()
    super.postStop()
  }
}
