/*
 * Copyright 2019 is-land
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

package com.island.ohara.connector.perf
import java.util.Collections

import com.island.ohara.common.data.{Column, DataType}
import com.island.ohara.common.rule.SmallTest
import com.island.ohara.kafka.connector.TaskSetting
import com.island.ohara.kafka.connector.json.ConnectorFormatter
import org.junit.Test
import org.scalatest.Matchers

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class TestPerfSourceProps extends SmallTest with Matchers {
  private[this] val props = PerfSourceProps(10, 10 seconds)
  private[this] val topics = Seq("TestPerfSourceProps")
  private[this] val schema = Seq(Column.builder().name("name").dataType(DataType.SHORT).order(1).build())

  @Test
  def testPlainMap(): Unit = {
    val props = PerfSourceProps(123, 10 seconds)
    val copy = PerfSourceProps(TaskSetting.of(props.toMap.asJava))
    props shouldBe copy
  }

  @Test
  def testEmptyTopics(): Unit = {
    val source = new PerfSource

    an[NoSuchElementException] should be thrownBy source.start(
      ConnectorFormatter.of().name(methodName()).columns(schema.asJava).settings(props.toMap.asJava).raw())
  }

  @Test
  def testEmptySchemaOnSource(): Unit = {
    val source = new PerfSource

    source.start(
      ConnectorFormatter.of().name(methodName()).topicNames(topics.asJava).settings(props.toMap.asJava).raw())
  }

  @Test
  def testEmptySchemaOnSourceTask(): Unit = {
    val task = new PerfSourceTask

    task.start(ConnectorFormatter.of().name(methodName()).topicNames(topics.asJava).settings(props.toMap.asJava).raw())

    task.schema shouldBe DEFAULT_SCHEMA
  }

  @Test
  def testEmptyBatchToDefault(): Unit = {
    PerfSourceProps(TaskSetting.of(Collections.emptyMap())).batch shouldBe DEFAULT_BATCH
  }

  @Test
  def testEmptyFrequenceToDefault(): Unit = {
    PerfSourceProps(TaskSetting.of(Collections.emptyMap())).freq shouldBe DEFAULT_FREQUENCE
  }

  @Test
  def testInvalidFrequence(): Unit = {
    an[NumberFormatException] should be thrownBy PerfSourceProps(
      TaskSetting.of(Map(PERF_BATCH -> "1", PERF_FREQUENCE -> "abc").asJava))
  }

  @Test
  def testInvalidProps(): Unit = {
    val source = new PerfSource
    an[IllegalArgumentException] should be thrownBy source.start(
      ConnectorFormatter
        .of()
        .name(methodName())
        .topicNames(topics.asJava)
        .columns(schema.asJava)
        .settings(props.copy(batch = -1).toMap.asJava)
        .raw())
  }
}
