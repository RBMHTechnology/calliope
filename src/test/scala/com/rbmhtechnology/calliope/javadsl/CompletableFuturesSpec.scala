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

package com.rbmhtechnology.calliope.javadsl

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CompletableFuture, CompletionStage}
import java.util.function.Supplier

import io.vavr.control.{Option => JOption}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{AsyncWordSpecLike, MustMatchers}

import scala.compat.java8.FunctionConverters._
import scala.compat.java8.FutureConverters._

object CompletableFuturesSpec {

  class TestException extends RuntimeException

  val Success: String = "success"
}

class CompletableFuturesSpec extends AsyncWordSpecLike with MustMatchers with ScalaFutures {

  import CompletableFuture._
  import JOption._

  import CompletableFutures._
  import CompletableFuturesSpec._

  def invokedWith: AfterWord = afterWord("invoked with")

  def recoverWith[A](ft: => CompletionStage[A])(recover: Throwable => JOption[CompletionStage[A]]): CompletionStage[A] =
    CompletableFutures.recoverWith[A](new Supplier[CompletionStage[A]] {
      override def get(): CompletionStage[A] = ft
    }, recover.asJava)

  def retry[A](retries: Int, ft: () => CompletionStage[A]): CompletionStage[A] =
    CompletableFutures.retry(retries, new Supplier[CompletionStage[A]] {
      override def get(): CompletionStage[A] = ft()
    })

  def succeedOnAttempt[A](successAttempt: Int, ft: => CompletableFuture[A]): () => CompletionStage[A] = {
    val attempts = new AtomicInteger(0)
    () => {
      val attempt = attempts.incrementAndGet()
      if (attempt < successAttempt)
        failedFuture(new TestException)
      else
        ft
    }
  }

  "CompletableFutures" when invokedWith {
    "failedFuture" must {
      "create a failing future" in {
        failedFuture[String](new TestException())
          .toScala.failed
          .map {
            err => err mustBe a[TestException]
          }
      }
    }
    "recoverWith" must {
      "succeed future" when {
        "successful recovery function supplied" in {
          recoverWith(failedFuture[String](new TestException())) { _ => some(completedFuture(Success)) }
            .toScala
            .map {
              res => res mustBe Success
            }
        }
      }
      "fail future" when {
        "failing recovery function supplied" in {
          recoverWith(failedFuture[String](new TestException())) { _ => some(failedFuture(new IllegalArgumentException)) }
            .toScala.failed
            .map {
              err => err.getCause mustBe a[IllegalArgumentException]
            }
        }
        "no recovery function supplied" in {
          recoverWith(failedFuture[String](new TestException())) { _ => none() }
            .toScala.failed
            .map {
              err => err.getCause mustBe a[TestException]
            }
        }
      }
    }
    "retry" must {
      "succeed future" when {
        "future succeeds" in {
          retry(1, succeedOnAttempt(1, completedFuture(Success)))
            .toScala
            .map {
              res => res mustBe Success
            }
        }
        "future succeeds on first retry" in {
          retry(1, succeedOnAttempt(2, completedFuture(Success)))
            .toScala
            .map {
              res => res mustBe Success
            }
        }
        "future succeeds on consecutive retry" in {
          retry(5, succeedOnAttempt(3, completedFuture(Success)))
            .toScala
            .map {
              res => res mustBe Success
            }
        }
      }
      "fail future" when {
        "all retries fail" in {
          retry(1, succeedOnAttempt(3, completedFuture(Success)))
            .toScala.failed
            .map {
              err => err.getCause mustBe a[TestException]
            }
        }
        "no retries given" in {
          retry(0, succeedOnAttempt(2, completedFuture(Success)))
            .toScala.failed
            .map {
              err => err.getCause mustBe a[TestException]
            }
        }
        "invalid retries given" in {
          retry(-1, succeedOnAttempt(2, completedFuture(Success)))
            .toScala.failed
            .map {
              err => err.getCause mustBe a[TestException]
            }
        }
      }
    }
  }
}
