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

import akka.testkit.TestProbe;
import io.vavr.collection.Seq;
import io.vavr.collection.Vector;
import io.vavr.control.Option;
import scala.runtime.AbstractPartialFunction;

import java.time.Duration;
import java.util.function.Function;

import static akka.japi.Util.immutableSeq;
import static com.rbmhtechnology.calliope.javadsl.Durations.asScala;
import static scala.collection.JavaConversions.seqAsJavaList;

public final class TestProbes {

  private TestProbes() {
  }

  public static <A> Seq<A> receiveWhile(final TestProbe probe,
                                        final Duration max,
                                        final Duration idle,
                                        final int messages,
                                        final Function<Object, Option<A>> f) {
    final scala.collection.immutable.Seq<A> results = probe.receiveWhile(asScala(max), asScala(idle), messages,
      new AbstractPartialFunction<Object, A>() {

        @Override
        public A apply(final Object o) {
          return f.apply(o).get();
        }

        @Override
        public boolean isDefinedAt(final Object o) {
          return f.apply(o).isDefined();
        }
      }
    );
    return Vector.ofAll(seqAsJavaList(results));
  }

  public static <A> Seq<A> receiveMsgClassWithin(final TestProbe probe, final Class<A> clazz, final Duration duration) {
    return receiveWhile(probe, duration, duration, Integer.MAX_VALUE, ev ->
      Option.when(clazz.isAssignableFrom(ev.getClass()), () -> clazz.cast(ev))
    );
  }

  @SafeVarargs
  public static <A> A expectAnyMsgOf(final TestProbe probe, final A... messages) {
    return probe.expectMsgAnyOf(immutableSeq(messages));
  }
}
