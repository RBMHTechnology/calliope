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

import io.vavr.control.Option;
import io.vavr.control.Try;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

public final class CompletableFutures {

  private CompletableFutures() {
  }

  public static <A> CompletableFuture<A> failedFuture(final Throwable error) {
    final CompletableFuture<A> ft = new CompletableFuture<>();
    ft.completeExceptionally(error);
    return ft;
  }

  public static <A> CompletionStage<A> recoverWith(final Supplier<? extends CompletionStage<A>> ft,
                                                   final Function<Throwable, Option<CompletionStage<A>>> recover) {
    return ft.get()
      .<Try<A>>handle((res, err) -> err == null ? Try.success(res) : Try.failure(err))
      .thenCompose(res -> res
        .<CompletionStage<A>>map(CompletableFuture::completedFuture)
        .recoverWith(err -> recover.apply(err).toTry(() -> err))
        .getOrElseGet(CompletableFutures::failedFuture)
      );
  }

  public static <A> CompletionStage<A> retry(final int retries, final Supplier<? extends CompletionStage<A>> ft) {
    return recoverWith(ft, err -> Option.when(retries > 0, () -> retry(retries - 1, ft)));
  }
}
