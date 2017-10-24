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

package com.rbmhtechnology.calliope.util;

import akka.actor.ActorSystem;
import com.typesafe.config.ConfigFactory;
import org.junit.rules.ExternalResource;

import java.time.Duration;

public class AkkaSystemRule extends ExternalResource {

  private static final String DEFAULT_SYSTEM_NAME = "test";

  private final String configResourceName;

  private ActorSystem system;
  private Duration timeout;

  public AkkaSystemRule() {
    this("application.conf");
  }

  public AkkaSystemRule(String configResourceName) {
    this.configResourceName = configResourceName;
  }

  @Override
  protected void before() throws Throwable {
    this.system = ActorSystem.create(DEFAULT_SYSTEM_NAME, ConfigFactory.load(configResourceName));
    this.timeout = system.settings().config().getDuration("akka.test.single-expect-default");
  }

  @Override
  protected void after() {
    system.terminate();
  }

  public ActorSystem system() {
    return system;
  }


  public Duration timeout() {
    return timeout;
  }
}
