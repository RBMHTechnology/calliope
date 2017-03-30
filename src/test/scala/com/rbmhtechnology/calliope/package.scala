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

package com.rbmhtechnology

import akka.actor.Status.{Failure => ActorFailure}
import akka.testkit.TestProbe

import scala.util.{Failure, Success, Try}

package object calliope {

  object TestProbeExtensions {

    implicit class ExtendedTestProbe(probe: TestProbe) {
      def expectNextAndReply[A, B](expected: A, reply: Try[B]): Unit = {
        probe.expectMsg(expected)
        val r = reply match {
          case Failure(err) => ActorFailure(err)
          case Success(suc) => suc
        }
        probe.reply(r)
      }
    }
  }
}
