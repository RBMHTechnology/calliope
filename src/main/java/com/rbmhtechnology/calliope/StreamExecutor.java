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

package com.rbmhtechnology.calliope;

import akka.Done;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.stream.javadsl.RunnableGraph;
import io.vavr.control.Option;

import java.util.concurrent.CompletionStage;

import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.API.Match;
import static io.vavr.API.run;
import static io.vavr.control.Option.some;

public class StreamExecutor extends AbstractActor {

  private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  private final AbstractActor.Receive initializing;

  private Option<ActorRef> runner = Option.none();

  private StreamExecutor(final Props runnerProps, final StreamExecutorSupervision supervision) {
    final AbstractActor.Receive running =
      receiveBuilder()
        .match(RunStream.class, x ->
          sender().tell(StreamStarted.instance(), self())
        )
        .build();

    initializing =
      receiveBuilder()
        .match(RunStream.class, x -> {
          runner = some(deployRunner(runnerProps, supervision));
          sender().tell(StreamStarted.instance(), self());
          getContext().become(running);
        })
        .build();
  }

  public static Props props(final RunnableGraph<CompletionStage<Done>> graph, final StreamExecutorSupervision supervision) {
    return Props.create(StreamExecutor.class, () -> new StreamExecutor(Runner.props(graph), supervision));
  }

  private ActorRef deployRunner(final Props runnerProps, final StreamExecutorSupervision supervision) {
    final Props supervisorProps = supervision.supervisorProps(runnerProps, "stream-executor-runner", (err, decision) ->
      Match(decision).of(
        Case($(SupervisorStrategy.restart()), x -> run(() ->
          log.warning("Stream executor encountered an error [{}]. The executor will be restarted.", err))
        ),
        Case($(), x -> run(() ->
          log.error(err, "Stream executor terminated because of an unrecoverable failure."))
        )
      ));
    return context().actorOf(supervisorProps);
  }

  @Override
  public Receive createReceive() {
    return initializing;
  }

  @Override
  public void preRestart(final Throwable reason, final scala.Option<Object> message) throws Exception {
    runner.forEach(x -> self().tell(RunStream.instance(), self()));
    super.preRestart(reason, message);
  }

  public static class RunStream {

    private static final RunStream INSTANCE = new RunStream();

    private RunStream() {
    }

    public static RunStream instance() {
      return INSTANCE;
    }
  }

  public static class StreamStarted {

    private static final StreamStarted INSTANCE = new StreamStarted();

    private StreamStarted() {
    }

    public static StreamStarted instance() {
      return INSTANCE;
    }
  }
}
