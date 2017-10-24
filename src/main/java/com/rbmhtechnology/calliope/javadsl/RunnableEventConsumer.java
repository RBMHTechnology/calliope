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

package com.rbmhtechnology.calliope.javadsl;

import akka.Done;
import akka.actor.ActorRef;
import com.rbmhtechnology.calliope.StreamExecutor.RunStream;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

import static akka.pattern.Patterns.ask;
import static scala.compat.java8.FutureConverters.toJava;

public class RunnableEventConsumer {

  private final ActorRef consumerStreamExecutor;

  RunnableEventConsumer(final ActorRef consumerStreamExecutor) {
    this.consumerStreamExecutor = consumerStreamExecutor;
  }

  public CompletionStage<Done> run(final Duration timeout) {
    return toJava(ask(consumerStreamExecutor, RunStream.instance(), timeout.toMillis()))
      .thenApply(x -> Done.getInstance());
  }
}
