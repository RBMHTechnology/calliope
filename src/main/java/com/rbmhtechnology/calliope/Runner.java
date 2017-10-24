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
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.RunnableGraph;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.util.concurrent.CompletionStage;

class Runner extends AbstractActor {

  private final RunnableGraph<CompletionStage<Done>> graph;
  private final ActorMaterializer materializer;

  private Runner(final RunnableGraph<CompletionStage<Done>> graph) {
    this.graph = graph;
    this.materializer = ActorMaterializer.create(context());
  }

  static Props props(final RunnableGraph<CompletionStage<Done>> graph) {
    return Props.create(Runner.class, () -> new Runner(graph));
  }

  @Override
  public void preStart() throws Exception {
    runGraph();
  }

  private void runGraph() {
    final CompletionStage<Done> done = graph.run(materializer);

    done.whenCompleteAsync((x, err) -> {
      final Throwable cause = err != null ? err : new StreamExecutorException.UnexpectedStreamCompletion();
      self().tell(new Fail(cause), self());
    }, context().dispatcher());
  }

  @Override
  public PartialFunction<Object, BoxedUnit> receive() {
    return ReceiveBuilder.create()
      .match(Fail.class, f -> {
        throw new StreamExecutorException.ExecutionFailed(f.cause());
      })
      .build();
  }

  @Override
  public void postStop() throws Exception {
    materializer.shutdown();
    super.postStop();
  }

  static class Fail {
    private final Throwable cause;

    Fail(final Throwable cause) {
      this.cause = cause;
    }

    Throwable cause() {
      return cause;
    }
  }
}
