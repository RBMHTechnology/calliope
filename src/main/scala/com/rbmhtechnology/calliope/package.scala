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

import java.time.{Duration => JDuration}

import akka.actor.SupervisorStrategy.Directive
import akka.actor.{ActorContext, SupervisorStrategy}

package object calliope {

  object SupervisorStrategyExtensions {

    implicit class ExtendedSupervisorStrategyDecider(decider: SupervisorStrategy.Decider) {

      def foreach(c: PartialFunction[(Throwable, Directive), Unit])(implicit context: ActorContext): SupervisorStrategy.Decider = {
        case err if decider.isDefinedAt(err) =>
          val decision = decider.apply(err)
          try {
            c.applyOrElse((err, decision), (_: (Throwable, Directive)) => Unit)
          } catch {
            case e: Throwable =>
              context.system.log.warning("Exception {} occurred in handler of ExtendedSupervisorStrategyDecider.foreach", e)
          }
          decision
      }
    }
  }

  object DurationConverters {
    import scala.concurrent.duration._

    implicit class DurationToFiniteDurationConverter(duration: JDuration) {
      def toFiniteDuration: FiniteDuration =
        duration.toNanos.nanos
    }
  }
}
