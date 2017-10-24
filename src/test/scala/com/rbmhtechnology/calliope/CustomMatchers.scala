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

import org.scalatest.matchers.{MatchResult, Matcher}

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

trait CustomMatchers {
  def failWith[A <: Throwable](implicit classTag: ClassTag[A]) =
    new TryFailureTypeMatcher[A]()

  case class TryFailureTypeMatcher[A <: Throwable](implicit classTag: ClassTag[A]) extends Matcher[Try[AnyRef]] {
    override def apply(left: Try[AnyRef]): MatchResult = left match {
      case Failure(err) =>
        MatchResult(classTag.runtimeClass.equals(err.getClass),
          s"Failure class [${err.getClass.getName}] did not match expected class [${classTag.runtimeClass.getName}]",
          s"Failure class [${err.getClass.getName}] did match expected class [${classTag.runtimeClass.getName}]"
        )
      case Success(res) =>
        MatchResult(matches = false, s"Unexpected success: $res", s"Unexpected success: $res")
    }
  }
}

object CustomMatchers extends CustomMatchers