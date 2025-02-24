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

import com.island.ohara.common.rule.SmallTest
import com.island.ohara.common.util.CommonUtils
import org.junit.Test
import org.scalatest.Matchers
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
class TestHadoopApi extends SmallTest with Matchers {

  @Test
  def testNullUriInUpdate(): Unit = {
    val update = HadoopApi.HDFS_UPDATE_JSON_FORMAT.read("""
        |{
        | "uri": null
        |}
      """.stripMargin.parseJson)
    update.uri shouldBe None
  }

  @Test
  def testEmptyUriInUpdate(): Unit = {
    an[DeserializationException] should be thrownBy HadoopApi.HDFS_UPDATE_JSON_FORMAT.read("""
        |{
        | "uri": ""
        |}
      """.stripMargin.parseJson)
  }

  @Test
  def testEmptyNameInCreation(): Unit = {
    an[DeserializationException] should be thrownBy HadoopApi.HDFS_CREATION_JSON_FORMAT.read("""
        |{
        | "name": "",
        | "uri": "file:///tmp"
        |}
      """.stripMargin.parseJson)
  }

  @Test
  def testEmptyUriInCreation(): Unit = {
    an[DeserializationException] should be thrownBy HadoopApi.HDFS_CREATION_JSON_FORMAT.read("""
        |{
        | "name": "hdfs_name1",
        | "uri": ""
        |}
      """.stripMargin.parseJson)

  }

  @Test
  def testNullUriInCreation(): Unit = {
    an[DeserializationException] should be thrownBy HadoopApi.HDFS_CREATION_JSON_FORMAT.read("""
        |{
        | "name": "hdfs_name1",
        | "uri": null
        |}
        |""".stripMargin.parseJson)
  }

  @Test
  def testNullNameInCreation(): Unit = {
    an[DeserializationException] should be thrownBy HadoopApi.HDFS_CREATION_JSON_FORMAT.read("""
        |{
        | "name": null,
        | "uri": "file:///tmp"
        |}
        |""".stripMargin.parseJson)
  }

  @Test
  def testParserUpdate(): Unit = {
    val uri = s"file:///tmp/${CommonUtils.randomString()}"
    val update = HadoopApi.HDFS_UPDATE_JSON_FORMAT.read(s"""
         |{
         | "uri": "${uri}"
         |}
       """.stripMargin.parseJson)
    update.uri.get shouldBe uri
  }

  @Test
  def testParseCreation(): Unit = {
    val name = CommonUtils.randomString()
    val uri = s"file:///tmp/${CommonUtils.randomString()}"
    val creation = HadoopApi.HDFS_CREATION_JSON_FORMAT.read(s"""
         |{
         | "name": "${name}",
         | "uri": "${uri}"
         |}
       """.stripMargin.parseJson)
    creation.name shouldBe name
    creation.uri shouldBe uri
  }

  @Test
  def ignoreNameOnCreation(): Unit = an[NullPointerException] should be thrownBy HadoopApi
    .access()
    .hostname(CommonUtils.randomString())
    .port(CommonUtils.availablePort())
    .request()
    .uri(CommonUtils.randomString())
    .create()

  @Test
  def ignoreNameOnUpdate(): Unit = an[NullPointerException] should be thrownBy HadoopApi
    .access()
    .hostname(CommonUtils.randomString())
    .port(CommonUtils.availablePort())
    .request()
    .uri(CommonUtils.randomString())
    .update()

  @Test
  def emptyName(): Unit = an[IllegalArgumentException] should be thrownBy HadoopApi.access().request().name("")

  @Test
  def nullName(): Unit = an[NullPointerException] should be thrownBy HadoopApi.access().request().name(null)

  @Test
  def ignoreUriOnCreation(): Unit = an[NullPointerException] should be thrownBy HadoopApi
    .access()
    .hostname(CommonUtils.randomString())
    .port(CommonUtils.availablePort())
    .request()
    .name(CommonUtils.randomString())
    .create()

  @Test
  def emptyUri(): Unit = an[IllegalArgumentException] should be thrownBy HadoopApi.access().request().uri("")

  @Test
  def nullUri(): Unit = an[NullPointerException] should be thrownBy HadoopApi.access().request().uri(null)
}
