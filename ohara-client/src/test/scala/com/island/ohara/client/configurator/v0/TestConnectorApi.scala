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

package com.island.ohara.client.configurator.v0

import com.island.ohara.client.configurator.v0.ConnectorApi.ConnectorState._
import com.island.ohara.client.configurator.v0.ConnectorApi.{Creation, _}
import com.island.ohara.client.configurator.v0.MetricsApi.Metrics
import com.island.ohara.common.data.{Column, DataType, Serializer}
import com.island.ohara.common.rule.SmallTest
import com.island.ohara.common.util.CommonUtils
import com.island.ohara.kafka.connector.json.{PropGroups, SettingDefinition}
import org.junit.Test
import org.scalatest.Matchers
import spray.json.{JsArray, JsString, _}
import scala.concurrent.ExecutionContext.Implicits.global
class TestConnectorApi extends SmallTest with Matchers {

  // TODO: remove this test after ohara manager starts to use new APIs
  @Test
  def testStaleCreationApis(): Unit = {
    val name = CommonUtils.randomString()
    val workerClusterName = CommonUtils.randomString()
    val className = CommonUtils.randomString()
    val column = Column
      .builder()
      .name(CommonUtils.randomString())
      .newName(CommonUtils.randomString())
      .dataType(DataType.DOUBLE)
      .build()
    val topicNames = Seq(CommonUtils.randomString())
    val numberOfTasks = 10
    val configs = Map("aaa" -> "cccc")
    val anotherKey = CommonUtils.randomString()
    val anotherValue = CommonUtils.randomString()
    val request =
      CONNECTOR_CREATION_JSON_FORMAT.read(s"""
                                               |{
                                               |  "name": ${JsString(name).toString()},
                                               |  "workerClusterName": ${JsString(workerClusterName).toString()},
                                               |  "connector.class": ${JsString(className).toString()},
                                               |  "schema": ${JsArray(COLUMN_JSON_FORMAT.write(column)).toString()},
                                               |  "topics": ${JsArray(topicNames.map(v => JsString(v)).toVector)
                                               .toString()},
                                               |  "numberOfTasks": ${JsNumber(numberOfTasks).toString()},
                                               |  "configs": ${JsObject(configs.map { case (k, v) => k -> JsString(v) })
                                               .toString()},
                                                  "$anotherKey": "$anotherValue"
                                               |}
                                            """.stripMargin.parseJson)
    request.workerClusterName.get shouldBe workerClusterName
    request.className shouldBe className
    request.columns shouldBe Seq.empty
    request.topicNames shouldBe topicNames
    request.numberOfTasks shouldBe 1
    // this key is deprecated so json converter will replace it by new one
    request.settings.contains("className") shouldBe false
    request.settings.contains("aaa") shouldBe false
    request.settings(anotherKey).asInstanceOf[JsString].value shouldBe anotherValue
    CONNECTOR_CREATION_JSON_FORMAT.read(CONNECTOR_CREATION_JSON_FORMAT.write(request)) shouldBe request
  }

  @Test
  def testSerialization(): Unit = {
    val request = Creation(
      settings = Map(
        "abc" -> JsString("Asdasdasd"),
        "ccc" -> JsNumber(312313),
        "bbb" -> JsArray(JsString("Asdasdasd"), JsString("aaa")),
        "ddd" -> JsObject("asdasd" -> JsString("Asdasdasd"))
      ))
    request shouldBe Serializer.OBJECT.from(Serializer.OBJECT.to(request)).asInstanceOf[Creation]
  }

  // TODO: remove this test after ohara manager starts to use new APIs
  @Test
  def testStaleCreationResponseApis(): Unit = {
    val desc = ConnectorDescription(
      settings = Map(
        SettingDefinition.CONNECTOR_NAME_DEFINITION.key() -> JsString(CommonUtils.randomString()),
        SettingDefinition.CONNECTOR_CLASS_DEFINITION.key() -> JsString(CommonUtils.randomString()),
        SettingDefinition.COLUMNS_DEFINITION.key() -> JsNull,
        SettingDefinition.TOPIC_NAMES_DEFINITION.key() -> JsArray(JsString(CommonUtils.randomString())),
        SettingDefinition.NUMBER_OF_TASKS_DEFINITION.key() -> JsNumber(1231),
        SettingDefinition.WORKER_CLUSTER_NAME_DEFINITION.key() -> JsString(CommonUtils.randomString()),
      ),
      state = None,
      error = None,
      metrics = Metrics(Seq.empty),
      lastModified = CommonUtils.current()
    )
    val jsonString = CONNECTOR_DESCRIPTION_JSON_FORMAT.write(desc).toString()
    jsonString.contains("id") shouldBe true
    jsonString.contains(SettingDefinition.CONNECTOR_NAME_DEFINITION.key()) shouldBe true
    jsonString.contains(SettingDefinition.CONNECTOR_CLASS_DEFINITION.key()) shouldBe true
    jsonString.contains("className") shouldBe false
    jsonString.contains(SettingDefinition.COLUMNS_DEFINITION.key()) shouldBe true
    jsonString.contains("schema") shouldBe false
    jsonString.contains("topics") shouldBe true
    jsonString.contains(SettingDefinition.NUMBER_OF_TASKS_DEFINITION.key()) shouldBe true
    jsonString.contains("numberOfTasks") shouldBe false
    jsonString.contains("settings") shouldBe true
    jsonString.contains(SettingDefinition.WORKER_CLUSTER_NAME_DEFINITION.key()) shouldBe true
    jsonString.contains("state") shouldBe false
    jsonString.contains("error") shouldBe false
    jsonString.contains("lastModified") shouldBe true
    jsonString.contains("configs") shouldBe false
  }
  @Test
  def testState(): Unit = {
    ConnectorState.all shouldBe Seq(
      UNASSIGNED,
      RUNNING,
      PAUSED,
      FAILED,
      DESTROYED
    ).sortBy(_.name)
  }

  @Test
  def testStateJson(): Unit = {
    ConnectorState.all.foreach(
      state =>
        ConnectorApi.CONNECTOR_STATE_JSON_FORMAT
          .read(ConnectorApi.CONNECTOR_STATE_JSON_FORMAT.write(state)) shouldBe state)
  }

  @Test
  def renderJsonWithoutAnyRequiredFields(): Unit = {
    val response = ConnectorDescription(
      settings = Map(
        CommonUtils.randomString() -> JsString(CommonUtils.randomString()),
        SettingDefinition.CONNECTOR_NAME_DEFINITION.key() -> JsString(CommonUtils.randomString())
      ),
      state = None,
      error = None,
      metrics = Metrics(Seq.empty),
      lastModified = CommonUtils.current()
    )
    // pass
    ConnectorApi.CONNECTOR_DESCRIPTION_JSON_FORMAT.write(response)
  }

  @Test
  def renderJsonWithConnectorClass(): Unit = {
    val className = CommonUtils.randomString()
    val response = ConnectorDescription(
      settings = Map(
        SettingDefinition.CONNECTOR_CLASS_DEFINITION.key() -> JsString(className),
        SettingDefinition.CONNECTOR_NAME_DEFINITION.key() -> JsString(CommonUtils.randomString())
      ),
      state = None,
      error = None,
      metrics = Metrics(Seq.empty),
      lastModified = CommonUtils.current()
    )
    ConnectorApi.CONNECTOR_DESCRIPTION_JSON_FORMAT
      .write(response)
      .asInstanceOf[JsObject]
      // previous name
      .fields
      .contains("className") shouldBe false
  }

  @Test
  def parsePreviousKeyOfClassNameFromConnectorCreationRequest(): Unit = {
    import spray.json._
    val className = CommonUtils.randomString()
    val connectorCreationRequest = ConnectorApi.CONNECTOR_CREATION_JSON_FORMAT.read(s"""
                                                                                               | {
                                                                                               | \"className\": \"$className\"
                                                                                               | }
     """.stripMargin.parseJson)
    an[NoSuchElementException] should be thrownBy connectorCreationRequest.className
  }

  @Test
  def parsePreviousKeyOfClassNameFromConnectorDescription(): Unit = {
    import spray.json._
    val className = CommonUtils.randomString()
    val connectorDescription = ConnectorApi.CONNECTOR_DESCRIPTION_JSON_FORMAT.read(s"""
                                                                                      | {
                                                                                      | \"id\": \"asdasdsad\",
                                                                                      | \"lastModified\": 123,
                                                                                      | \"settings\": {
                                                                                      | \"className\": \"$className\"
                                                                                      | },
                                                                                      | \"metrics\": {
                                                                                      |   "meters":[]
                                                                                      | }
                                                                                      | }
     """.stripMargin.parseJson)
    an[NoSuchElementException] should be thrownBy connectorDescription.className
  }

  @Test
  def parsePropGroups(): Unit = {
    val creationRequest = ConnectorApi.CONNECTOR_CREATION_JSON_FORMAT.read(s"""
                                                                                      | {
                                                                                      | \"columns\": [
                                                                                      |   {
                                                                                      |     "order": 1,
                                                                                      |     "name": "abc",
                                                                                      |     "newName": "ccc",
                                                                                      |     "dataType": "STRING"
                                                                                      |   }
                                                                                      | ]
                                                                                      | }
     """.stripMargin.parseJson)
    val column = PropGroups.ofJson(creationRequest.settings("columns").toString()).toColumns.get(0)
    column.order() shouldBe 1
    column.name() shouldBe "abc"
    column.newName() shouldBe "ccc"
    column.dataType().name() shouldBe "STRING"
  }

  @Test
  def parseStaleConfigs(): Unit = {
    val creationRequest = ConnectorApi.CONNECTOR_CREATION_JSON_FORMAT.read(s"""
                                                                                      | {
                                                                                      |  "name": "ftp source",
                                                                                      |  "schema": [
                                                                                      |    {
                                                                                      |      "name": "col1",
                                                                                      |      "newName": "col1",
                                                                                      |      "dataType": "STRING",
                                                                                      |      "order": 1
                                                                                      |    },
                                                                                      |    {
                                                                                      |      "name": "col2",
                                                                                      |      "newName": "col2",
                                                                                      |      "dataType": "STRING",
                                                                                      |      "order": 2
                                                                                      |    },
                                                                                      |    {
                                                                                      |      "name": "col3",
                                                                                      |      "newName": "col3",
                                                                                      |      "dataType": "STRING",
                                                                                      |      "order": 3
                                                                                      |    }
                                                                                      |  ],
                                                                                      |  "className": "com.island.ohara.connector.ftp.FtpSource",
                                                                                      |  "topics": [
                                                                                      |    "47e45b56-6cee-4bc5-83e4-62e872552880"
                                                                                      |  ],
                                                                                      |  "numberOfTasks": 1,
                                                                                      |  "configs": {
                                                                                      |    "ftp.input.folder": "/demo_folder/input",
                                                                                      |    "ftp.completed.folder": "/demo_folder/complete",
                                                                                      |    "ftp.error.folder": "/demo_folder/error",
                                                                                      |    "ftp.encode": "UTF-8",
                                                                                      |    "ftp.hostname": "10.2.0.28",
                                                                                      |    "ftp.port": "21",
                                                                                      |    "ftp.user.name": "ohara",
                                                                                      |    "ftp.user.password": "island123",
                                                                                      |    "currTask": "1"
                                                                                      |  }
                                                                                      |}
                                                                                      |     """.stripMargin.parseJson)
    // the deprecated APIs should not be supported now!!!
    creationRequest.settings.contains("ftp.input.folder") shouldBe false
    creationRequest.settings.contains("ftp.completed.folder") shouldBe false
    creationRequest.settings.contains("ftp.error.folder") shouldBe false
    creationRequest.settings.contains("ftp.encode") shouldBe false
    creationRequest.settings.contains("ftp.hostname") shouldBe false
    creationRequest.settings.contains("ftp.port") shouldBe false
  }

  @Test
  def ignoreNameOnCreation(): Unit = an[NullPointerException] should be thrownBy ConnectorApi.access
    .hostname(CommonUtils.randomString())
    .port(CommonUtils.availablePort())
    .request
    .create()

  @Test
  def ignoreNameOnUpdate(): Unit = an[NullPointerException] should be thrownBy ConnectorApi.access
    .hostname(CommonUtils.randomString())
    .port(CommonUtils.availablePort())
    .request
    .update()

  @Test
  def emptyName(): Unit = an[IllegalArgumentException] should be thrownBy ConnectorApi.access.request.name("")

  @Test
  def nullName(): Unit = an[NullPointerException] should be thrownBy ConnectorApi.access.request.name(null)

  @Test
  def emptyClassName(): Unit =
    an[IllegalArgumentException] should be thrownBy ConnectorApi.access.request.className("")

  @Test
  def nullClassName(): Unit =
    an[NullPointerException] should be thrownBy ConnectorApi.access.request.className(null)

  @Test
  def emptyColumns(): Unit =
    an[IllegalArgumentException] should be thrownBy ConnectorApi.access.request.columns(Seq.empty)

  @Test
  def nullColumns(): Unit = an[NullPointerException] should be thrownBy ConnectorApi.access.request.columns(null)

  @Test
  def emptyWorkerClusterName(): Unit =
    an[IllegalArgumentException] should be thrownBy ConnectorApi.access.request.workerClusterName("")

  @Test
  def nullWorkerClusterName(): Unit =
    an[NullPointerException] should be thrownBy ConnectorApi.access.request.workerClusterName(null)

  @Test
  def emptyTopicNames(): Unit =
    an[IllegalArgumentException] should be thrownBy ConnectorApi.access.request.topicNames(Seq.empty)

  @Test
  def nullTopicNames(): Unit =
    an[NullPointerException] should be thrownBy ConnectorApi.access.request.topicName(null)

  @Test
  def emptySettings(): Unit =
    an[IllegalArgumentException] should be thrownBy ConnectorApi.access.request.settings(Map.empty)

  @Test
  def nullSettings(): Unit = an[NullPointerException] should be thrownBy ConnectorApi.access.request.settings(null)

  @Test
  def testCreation(): Unit = {
    val name = CommonUtils.randomString(10)
    val className = CommonUtils.randomString(10)
    val topicNames = Seq(CommonUtils.randomString(10))
    val map = Map(
      CommonUtils.randomString(10) -> CommonUtils.randomString(10),
      CommonUtils.randomString(10) -> CommonUtils.randomString(10),
      CommonUtils.randomString(10) -> CommonUtils.randomString(10)
    )
    val creation =
      ConnectorApi.access.request.name(name).className(className).topicNames(topicNames).settings(map).creation
    creation.name shouldBe name
    creation.className shouldBe className
    creation.topicNames shouldBe topicNames
    map.foreach {
      case (k, v) => creation.plain(k) shouldBe v
    }
  }

  @Test
  def testDefaultNumberOfTasks(): Unit =
    ConnectorApi.CONNECTOR_CREATION_JSON_FORMAT
      .read(s"""
                                                                | {
                                                                |  "name": "ftp source"
                                                                |}
                                                                |     """.stripMargin.parseJson)
      .numberOfTasks shouldBe 1

  @Test
  def parseColumn(): Unit = {
    val name = CommonUtils.randomString()
    val newName = CommonUtils.randomString()
    val dataType = DataType.BOOLEAN
    val order = 1
    val column = ConnectorApi.COLUMN_JSON_FORMAT.read(s"""
                                            | {
                                            |  "name": "$name",
                                            |  "newName": "$newName",
                                            |  "dataType": "${dataType.name}",
                                            |  "order": $order
                                            |}
                                            |     """.stripMargin.parseJson)
    column.name shouldBe name
    column.newName shouldBe newName
    column.dataType shouldBe dataType
    column.order shouldBe order

    val column2 = ConnectorApi.COLUMN_JSON_FORMAT.read(s"""
                                                         | {
                                                         |  "name": "$name",
                                                         |  "dataType": "${dataType.name}",
                                                         |  "order": $order
                                                         |}
                                                         |     """.stripMargin.parseJson)
    column2.name shouldBe name
    column2.newName shouldBe name
    column2.dataType shouldBe dataType
    column2.order shouldBe order
  }

  @Test
  def emptyStringForColumn(): Unit =
    an[DeserializationException] should be thrownBy ConnectorApi.COLUMN_JSON_FORMAT.read(s"""
                                                                              | {
                                                                              |  "name": "",
                                                                              |  "dataType": "Boolean",
                                                                              |  "order": 1
                                                                              |}
                                                                              |     """.stripMargin.parseJson)

  @Test
  def negativeOrderForColumn(): Unit =
    an[DeserializationException] should be thrownBy ConnectorApi.COLUMN_JSON_FORMAT.read(s"""
       | {
       |  "name": "aaa",
       |  "dataType": "Boolean",
       |  "order": -1
       |}
       |     """.stripMargin.parseJson)
}
