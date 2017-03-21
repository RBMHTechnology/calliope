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

package com.rbmhtechnology.calliope

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}

import scala.concurrent.{Future, Promise}

object Barrier {
  /**
    * Creates a `Flow[A]` that opens after the given future `f` successfully completes.
    */
  def apply[A](f: Future[_]): Flow[A, A, NotUsed] =
    Flow[A].prepend(Source.fromFuture(f).flatMapConcat(_ => Source.empty[A]))

  /**
    * Creates a `Flow[A]` that opens after the materialized `Promise[Any]` successfully completes.
    */
  def apply[A](): Flow[A, A, Promise[Any]] = withPromise { p =>
    apply(p.future).mapMaterializedValue(_ => p)
  }

  private def withPromise[A](block: Promise[Any] => A): A =
    block(Promise[Any]())
}
