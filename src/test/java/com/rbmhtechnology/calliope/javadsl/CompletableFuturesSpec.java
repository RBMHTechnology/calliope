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

import io.vavr.control.Try;
import org.junit.Test;
import org.scalatest.junit.JUnitSuite;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.rbmhtechnology.calliope.javadsl.CompletableFutures.failedFuture;
import static com.rbmhtechnology.calliope.javadsl.CompletableFutures.recoverWith;
import static com.rbmhtechnology.calliope.javadsl.CompletableFutures.retry;
import static io.vavr.control.Option.none;
import static io.vavr.control.Option.some;
import static io.vavr.control.Try.failure;
import static io.vavr.control.Try.success;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class CompletableFuturesSpec extends JUnitSuite {

  private static final Duration TIMEOUT = Duration.ofSeconds(5);
  private static final RuntimeException FAILURE = new RuntimeException("error");
  private static final String SUCCESS = "success";

  @Test
  public void failedFuture_must_createExceptionallyCompletedFuture() {
    final Try<Object> futureResult = await(TIMEOUT,
      failedFuture(FAILURE)
    );

    assertThat(futureResult, is(failure(FAILURE)));
  }

  @Test
  public void recoverWith_whenSuccessfulRecoverySupplied_must_returnSuccessfulFuture() {
    final Try<String> futureResult = await(TIMEOUT,
      recoverWith(() -> failedFuture(FAILURE), err -> some(completedFuture(SUCCESS)))
    );

    assertThat(futureResult, is(success(SUCCESS)));
  }

  @Test
  public void recoverWith_whenFailingRecoverySupplied_must_returnFailedFuture() {
    final RuntimeException customFailure = new RuntimeException("err");

    final Try<String> futureResult = await(TIMEOUT,
      recoverWith(() -> failedFuture(FAILURE), err -> some(failedFuture(customFailure)))
    );

    assertThat(futureResult, is(failure(customFailure)));
  }

  @Test
  public void recoverWith_whenNoRecoverySupplied_must_returnFailedFuture() {
    final Try<String> futureResult = await(TIMEOUT,
      recoverWith(() -> failedFuture(FAILURE), err -> none())
    );

    assertThat(futureResult, is(failure(FAILURE)));
  }

  @Test
  public void retry_when_futureSucceeds_must_returnSuccessfulFuture() {
    final Try<String> futureResult = await(TIMEOUT,
      retry(1, succeedOnAttempt(1, () -> completedFuture(SUCCESS)))
    );

    assertThat(futureResult, is(success(SUCCESS)));
  }

  @Test
  public void retry_when_firstRetrySucceeds_must_returnSuccessfulFuture() {
    final Try<String> futureResult = await(TIMEOUT,
      retry(1, succeedOnAttempt(2, () -> completedFuture(SUCCESS)))
    );

    assertThat(futureResult, is(success(SUCCESS)));
  }

  @Test
  public void retry_when_consecutiveRetrySucceeds_must_returnSuccessfulFuture() {
    final Try<String> futureResult = await(TIMEOUT,
      retry(5, succeedOnAttempt(3, () -> completedFuture(SUCCESS)))
    );

    assertThat(futureResult, is(success(SUCCESS)));
  }

  @Test
  public void retry_when_allRetriesFail_must_returnFailedFuture() {
    final Try<String> futureResult = await(TIMEOUT,
      retry(1, succeedOnAttempt(3, () -> completedFuture(SUCCESS)))
    );

    assertThat(futureResult, is(failure(FAILURE)));
  }

  @Test
  public void retry_when_noRetriesGiven_must_notPerformRetry() {
    final Try<String> futureResult = await(TIMEOUT,
      retry(0, succeedOnAttempt(2, () -> completedFuture(SUCCESS)))
    );

    assertThat(futureResult, is(failure(FAILURE)));
  }

  @Test
  public void retry_when_invalidRetriesGiven_must_notPerformRetry() {
    final Try<String> futureResult = await(TIMEOUT,
      retry(-1, succeedOnAttempt(2, () -> completedFuture(SUCCESS)))
    );

    assertThat(futureResult, is(failure(FAILURE)));
  }

  private <A> Try<A> await(final Duration timeout, final CompletionStage<A> ft) {
    try {
      return success(ft.toCompletableFuture().get(timeout.toNanos(), NANOSECONDS));
    } catch (InterruptedException | TimeoutException e) {
      return failure(e);
    } catch (ExecutionException e) {
      return failure(e.getCause());
    }
  }

  private <A> Supplier<CompletableFuture<A>> succeedOnAttempt(final int successAttempt, final Supplier<CompletableFuture<A>> result) {
    final AtomicInteger attempts = new AtomicInteger(0);

    return () -> {
      final int attempt = attempts.incrementAndGet();

      return attempt < successAttempt
        ? failedFuture(FAILURE)
        : result.get();
    };
  }
}
