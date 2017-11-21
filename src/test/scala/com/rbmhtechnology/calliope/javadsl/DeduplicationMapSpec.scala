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

import com.rbmhtechnology.calliope.{Aggregate, Partitioned}
import org.apache.kafka.common.TopicPartition
import org.scalatest.{MustMatchers, WordSpecLike}
import io.vavr.control.{ Option => JOption }

import scala.collection.JavaConverters._

object DeduplicationMapSpec {
  val Topic1 = "topic1"
  val Topic2 = "topic2"

  val EmptyDeduplicationMap: DeduplicationMap[Entry] = DeduplicationMap.empty(
    new Aggregate[Entry] {
      override def aggregateId(entry: Entry) = entry.aggregateId
    },
    new Partitioned[Entry] {
      override def topicPartition(entry: Entry): TopicPartition = entry.tp
    }
  )

  object Entry {
    def apply(topic: String, partition: Int, aggregateId: String, payload: String): Entry =
      apply(new TopicPartition(topic, partition), aggregateId, aggregateId + "_" + topic + "_" + partition + "payload-" + payload)
  }

  case class Entry(tp: TopicPartition, aggregateId: String, payload: String)
}

class DeduplicationMapSpec extends WordSpecLike with MustMatchers {

  import DeduplicationMapSpec._
  import JOption._

  "A DeduplicationMap" when {
    "an entry is appended to the map and" when {
      "the map is empty" must {
        "append entry" in {
          val map = EmptyDeduplicationMap
            .append(Entry(Topic1, 0, "agg1", "p-1"))

          map.size must be(1)
          map.asScala must contain only Entry(Topic1, 0, "agg1", "p-1")
        }
      }
      "the map contains an entry for the same aggregate in the same partition" must {
        "discard entry" in {
          val map = EmptyDeduplicationMap
            .append(Entry(Topic1, 0, "agg1", "p-1"))
            .append(Entry(Topic1, 0, "agg1", "p-2"))

          map.asScala must contain only Entry(Topic1, 0, "agg1", "p-1")
        }
      }
      "the map does not contain the aggregate of the entry" must {
        "append entry" in {
          val map = EmptyDeduplicationMap
            .append(Entry(Topic1, 0, "agg1", "p-1"))
            .append(Entry(Topic1, 0, "agg2", "p-2"))

          map.asScala must contain allOf(Entry(Topic1, 0, "agg1", "p-1"), Entry(Topic1, 0, "agg2", "p-2"))
        }
      }
      "the map does not contain aggregate of the entry in the same partition" must {
        "append entry" in {
          val map = EmptyDeduplicationMap
            .append(Entry(Topic1, 0, "agg1", "p-1"))
            .append(Entry(Topic1, 1, "agg1", "p-2"))

          map.asScala must contain allOf(Entry(Topic1, 0, "agg1", "p-1"), Entry(Topic1, 1, "agg1", "p-2"))
        }
      }
      "the map does not contain aggregate of the entry in the same topic" must {
        "append entry" in {
          val map = EmptyDeduplicationMap
            .append(Entry(Topic1, 0, "agg1", "p-1"))
            .append(Entry(Topic2, 0, "agg1", "p-2"))

          map.asScala must contain allOf(Entry(Topic1, 0, "agg1", "p-1"), Entry(Topic2, 0, "agg1", "p-2"))
        }
      }
      "iterated over single partition" must {
        "keep a stable order per partition" in {
          val map = EmptyDeduplicationMap
            .append(Entry(Topic1, 0, "agg1", "p-0"))
            .append(Entry(Topic1, 0, "agg1", "p-1"))
            .append(Entry(Topic1, 0, "agg2", "p-2"))
            .append(Entry(Topic1, 0, "agg2", "p-3"))
            .append(Entry(Topic1, 0, "agg3", "p-4"))
            .append(Entry(Topic1, 0, "agg3", "p-5"))

          map.size must be(3)
          map.asScala.toList must contain inOrderOnly(
            Entry(Topic1, 0, "agg1", "p-0"),
            Entry(Topic1, 0, "agg2", "p-2"),
            Entry(Topic1, 0, "agg3", "p-4")
          )
        }
      }
      "iterated over multiple partitions" must {
        "keep a stable order per partition" in {
          val map = EmptyDeduplicationMap
            .append(Entry(Topic1, 0, "agg1", "p-0"))
            .append(Entry(Topic1, 0, "agg1", "p-1"))
            .append(Entry(Topic1, 0, "agg2", "p-2"))
            .append(Entry(Topic1, 0, "agg2", "p-3"))
            .append(Entry(Topic1, 1, "agg3", "p-4"))
            .append(Entry(Topic1, 1, "agg3", "p-5"))
            .append(Entry(Topic1, 1, "agg4", "p-6"))
            .append(Entry(Topic1, 1, "agg4", "p-7"))

          map.size must be(4)
          map.asScala.toList must contain inOrderOnly(
            Entry(Topic1, 0, "agg1", "p-0"),
            Entry(Topic1, 0, "agg2", "p-2"),
            Entry(Topic1, 1, "agg3", "p-4"),
            Entry(Topic1, 1, "agg4", "p-6")
          )
        }
      }
      "iterated over multiple topics and partitions" must {
        "keep a stable order per partition" in {
          val map = EmptyDeduplicationMap
            .append(Entry(Topic1, 0, "agg1", "p-0"))
            .append(Entry(Topic1, 0, "agg1", "p-1"))
            .append(Entry(Topic1, 0, "agg2", "p-2"))
            .append(Entry(Topic1, 0, "agg2", "p-3"))
            .append(Entry(Topic2, 0, "agg3", "p-4"))
            .append(Entry(Topic2, 0, "agg3", "p-5"))
            .append(Entry(Topic2, 0, "agg4", "p-6"))
            .append(Entry(Topic2, 0, "agg4", "p-7"))

          map.size must be(4)
          map.asScala.toList must contain inOrderOnly(
            Entry(Topic1, 0, "agg1", "p-0"),
            Entry(Topic1, 0, "agg2", "p-2"),
            Entry(Topic2, 0, "agg3", "p-4"),
            Entry(Topic2, 0, "agg4", "p-6")
          )
        }
      }
    }
    "an entry is retrieved from the map and" when {
      "when the requested entry is present" must {
        "return `Some` entry" in {
          val map = EmptyDeduplicationMap
            .append(Entry(Topic1, 0, "agg1", "p-0"))

          map.get(Topic1, 0, "agg1") must be(some(Entry(Topic1, 0, "agg1", "p-0")))
        }
      }
      "when the requested entry is not present" must {
        "return `None`" in {
          val map = EmptyDeduplicationMap
            .append(Entry(Topic1, 0, "agg1", "p-0"))

          map.get(Topic1, 0, "i_do_not_exist") must be(none())
        }
      }
    }
  }
}
