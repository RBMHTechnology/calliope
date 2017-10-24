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
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.JavaTestKit;
import akka.testkit.TestProbe;
import com.rbmhtechnology.calliope.StreamExecutor.RunStream;
import com.rbmhtechnology.calliope.StreamExecutor.StreamStarted;
import io.vavr.collection.Seq;
import io.vavr.collection.Vector;
import org.junit.ClassRule;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.scalatest.junit.JUnitSuite;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

import static com.rbmhtechnology.calliope.StreamExecutorSupervision.Directive.STOP;
import static com.rbmhtechnology.calliope.javadsl.Durations.toFiniteDuration;

public class StreamExecutorSpec extends JUnitSuite {

  @ClassRule
  public static AkkaStreamsRule akka = new AkkaStreamsRule("reference.conf");

  private final ReplayProducer<String> producer = ReplayProducer.create();

  private final StreamExecutorSupervision.Backoff supervision =
    StreamExecutorSupervision.withBackoff(Duration.ofMillis(50), Duration.ofMillis(100), 0.2);

  @Test
  public void executor_when_run_must_startStream() {
    new JavaTestKit(akka.system()) {{
      final TestProbe consumer = new TestProbe(getSystem());
      final ActorRef executor = getSystem().actorOf(StreamExecutor.props(actorConsumerGraph(producer, consumer.ref()), supervision));

      runStream(getSystem(), executor);

      producer.sendNext("event-1");
      consumer.expectMsg("event-1");
    }};
  }

  @Test
  public void executor_when_streamCompletesWithFailure_must_restartStream() throws InterruptedException {
    new JavaTestKit(akka.system()) {{
      final TestProbe consumer = new TestProbe(getSystem());
      final ActorRef executor = getSystem().actorOf(StreamExecutor.props(actorConsumerGraph(producer, consumer.ref()), supervision));

      runStream(getSystem(), executor);

      producer.sendNext("event-1");
      consumer.expectMsg("event-1");

      producer.sendError(new RuntimeException("err"));

      producer.sendNext("event-2");
      consumer.expectMsg("event-1");
      consumer.expectMsg("event-2");
    }};
  }

  @Test
  public void executor_when_streamCompletesSuccessfully_must_restartStream() throws InterruptedException {
    new JavaTestKit(akka.system()) {{
      final TestProbe consumer = new TestProbe(getSystem());
      final ActorRef executor = getSystem().actorOf(StreamExecutor.props(actorConsumerGraph(producer, consumer.ref()), supervision));

      runStream(getSystem(), executor);

      producer.sendNext("event-1");
      consumer.expectMsg("event-1");

      producer.sendComplete();

      producer.sendNext("event-2");
      consumer.expectMsg("event-1");
      consumer.expectMsg("event-2");
    }};
  }

  @Test
  public void executor_when_stoppingDirectiveSelectedBySupervisionDecider_must_stopStream() throws InterruptedException {
    new JavaTestKit(akka.system()) {{
      final TestProbe consumer = new TestProbe(getSystem());
      final StreamExecutorSupervision stoppingSupervision = supervision.withDecider(err -> STOP);

      final ActorRef executor = getSystem()
        .actorOf(StreamExecutor.props(actorConsumerGraph(producer, consumer.ref()), stoppingSupervision));

      runStream(getSystem(), executor);

      producer.sendNext("event-1");
      consumer.expectMsg("event-1");

      producer.sendError(new RuntimeException("err"));

      producer.sendNext("event-2");
      consumer.expectNoMsg(toFiniteDuration(Duration.ofSeconds(1)));
    }};
  }

  private <A> RunnableGraph<CompletionStage<Done>> actorConsumerGraph(final Publisher<A> producer, final ActorRef consumer) {
    return Source.fromPublisher(producer)
      .map(em -> {
        consumer.tell(em, ActorRef.noSender());
        return em;
      })
      .toMat(Sink.ignore(), Keep.right());
  }

  private ActorRef runStream(final ActorSystem system, final ActorRef streamExecutor) {
    final TestProbe complete = new TestProbe(system);
    streamExecutor.tell(RunStream.instance(), complete.ref());
    complete.expectMsg(StreamStarted.instance());
    return streamExecutor;
  }

  private static class ReplayProducer<A> implements Publisher<A> {

    private Seq<Subscriber<? super A>> subscribers;
    private Seq<A> emissions;

    private ReplayProducer(final Seq<Subscriber<? super A>> subscribers, final Seq<A> emissions) {
      this.subscribers = subscribers;
      this.emissions = emissions;
    }

    private static <A> ReplayProducer<A> create() {
      return new ReplayProducer<>(Vector.empty(), Vector.empty());
    }

    @Override
    public void subscribe(final Subscriber<? super A> s) {
      subscribers = subscribers.append(s);
      emissions.forEach(s::onNext);
    }

    public void sendNext(final A element) {
      emissions = emissions.append(element);
      subscribers.forEach(s -> s.onNext(element));
    }

    public void sendError(final Throwable error) {
      subscribers.forEach(s -> s.onError(error));
    }

    public void sendComplete() {
      subscribers.forEach(Subscriber::onComplete);
    }
  }
}
