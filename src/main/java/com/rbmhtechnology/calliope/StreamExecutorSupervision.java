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

import akka.actor.ActorInitializationException;
import akka.actor.ActorKilledException;
import akka.actor.DeathPactException;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.pattern.BackoffSupervisor;

import java.time.Duration;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static akka.pattern.Backoff.onFailure;
import static com.rbmhtechnology.calliope.javadsl.Durations.toFiniteDuration;
import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.API.Match;
import static io.vavr.Predicates.anyOf;
import static io.vavr.Predicates.instanceOf;
import static scala.concurrent.duration.Duration.Inf;

public abstract class StreamExecutorSupervision {

  abstract Props supervisorProps(final Props childProps, final String childName, final FailureHandler onFailure);

  public static Backoff withBackoff(final Duration minBackoff, final Duration maxBackoff, final double randomFactor) {
    return new Backoff(minBackoff, maxBackoff, randomFactor, Decider.restartDecider());
  }

  interface FailureHandler extends BiConsumer<Throwable, SupervisorStrategy.Directive> {
  }

  public interface Decider extends Function<Throwable, Directive> {

    static Decider restartDecider() {
      return x -> Directive.RESTART;
    }
  }

  public enum Directive {
    STOP(SupervisorStrategy.stop()),
    RESTART(SupervisorStrategy.restart());

    private final SupervisorStrategy.Directive supervisorDirective;

    Directive(final SupervisorStrategy.Directive supervisorDirective) {
      this.supervisorDirective = supervisorDirective;
    }

    SupervisorStrategy.Directive supervisorDirective() {
      return supervisorDirective;
    }
  }

  public static class Backoff extends StreamExecutorSupervision {

    private final Duration minBackoff;
    private final Duration maxBackoff;
    private final double randomFactor;
    private final Decider decider;

    private Backoff(final Duration minBackoff, final Duration maxBackoff, final double randomFactor, final Decider decider) {
      this.minBackoff = minBackoff;
      this.maxBackoff = maxBackoff;
      this.randomFactor = randomFactor;
      this.decider = decider;
    }

    public Backoff withDecider(final Decider decider) {
      return new Backoff(minBackoff, maxBackoff, randomFactor, decider);
    }

    @Override
    Props supervisorProps(final Props childProps, final String childName, final FailureHandler onFailure) {
      return BackoffSupervisor.props(
        onFailure(childProps, childName, toFiniteDuration(minBackoff), toFiniteDuration(maxBackoff), randomFactor)
          .withSupervisorStrategy(new OneForOneStrategy(-1, Inf(), err -> {
              final SupervisorStrategy.Directive decision = Match(err).of(
                Case($(anyOf(
                  instanceOf(ActorInitializationException.class),
                  instanceOf(ActorKilledException.class),
                  instanceOf(DeathPactException.class))), e -> SupervisorStrategy.stop()),
                Case($(instanceOf(StreamExecutorException.ExecutionFailed.class)), e -> decider.apply(e.getCause()).supervisorDirective()),
                Case($(instanceOf(StreamExecutorException.UnexpectedStreamCompletion.class)), e -> decider.apply(e).supervisorDirective()),
                Case($(instanceOf(Exception.class)), e -> SupervisorStrategy.restart()),
                Case($(), e -> SupervisorStrategy.escalate())
              );
              onFailure.accept(err, decision);
              return decision;
            }, false)
          )
      );
    }
  }
}
