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

package com.island.ohara.configurator.route

import com.island.ohara.client.configurator.v0.NodeApi
import com.island.ohara.client.configurator.v0.NodeApi.{Node, Request}
import com.island.ohara.common.rule.SmallTest
import com.island.ohara.common.util.{CommonUtils, Releasable}
import com.island.ohara.configurator.Configurator
import org.junit.{After, Test}
import org.scalatest.Matchers

import scala.concurrent.ExecutionContext.Implicits.global
class TestNodeRoute extends SmallTest with Matchers {
  private[this] val numberOfCluster = 1
  private[this] val configurator = Configurator.builder().fake(numberOfCluster, numberOfCluster).build()

  /**
    * a fake cluster has 3 fake node.
    */
  private[this] val numberOfDefaultNodes = 3 * numberOfCluster
  private[this] val nodeApi = NodeApi.access().hostname(configurator.hostname).port(configurator.port)

  private[this] def compare(lhs: Node, rhs: Node): Unit = {
    lhs.name shouldBe rhs.name
    lhs.port shouldBe rhs.port
    lhs.user shouldBe rhs.user
    lhs.password shouldBe rhs.password
  }

  @Test
  def testServices(): Unit = {
    val nodes = result(nodeApi.list)
    nodes.isEmpty shouldBe false
    nodes.foreach(_.services.isEmpty shouldBe false)
  }

  @Test
  def testAdd(): Unit = {
    val name = CommonUtils.randomString()
    val port = CommonUtils.availablePort()
    val user = CommonUtils.randomString()
    val password = CommonUtils.randomString()
    val res = result(nodeApi.request().name(name).port(port).user(user).password(password).create())
    res.name shouldBe name
    res.port shouldBe port
    res.user shouldBe user
    res.password shouldBe password

    result(nodeApi.list).size shouldBe (1 + numberOfDefaultNodes)
    compare(result(nodeApi.list).find(_.name == name).get, res)

    an[IllegalArgumentException] should be thrownBy result(
      nodeApi.request().name(res.name).port(port).user(user).password(password).create())
  }

  @Test
  def testDelete(): Unit = {
    val res = result(
      nodeApi
        .request()
        .name(CommonUtils.randomString())
        .port(CommonUtils.availablePort())
        .user(CommonUtils.randomString())
        .password(CommonUtils.randomString())
        .create())

    result(nodeApi.list).size shouldBe (1 + numberOfDefaultNodes)

    result(nodeApi.delete(res.name))
    result(nodeApi.list).size shouldBe numberOfDefaultNodes
  }

  @Test
  def disableToDeleteNodeRunningService(): Unit = {
    val nodes = result(nodeApi.list)
    val runningNode = nodes.filter(_.services.exists(_.clusterNames.nonEmpty)).head
    an[IllegalArgumentException] should be thrownBy result(nodeApi.delete(runningNode.id))
  }

  @Test
  def testUpdate(): Unit = {
    val res = result(
      nodeApi
        .request()
        .name(CommonUtils.randomString())
        .port(CommonUtils.availablePort())
        .user(CommonUtils.randomString())
        .password(CommonUtils.randomString())
        .create())

    result(nodeApi.list).size shouldBe (1 + numberOfDefaultNodes)

    result(
      nodeApi
        .request()
        .name(res.name)
        .port(CommonUtils.availablePort())
        .user(CommonUtils.randomString())
        .password(CommonUtils.randomString())
        .update())

    result(nodeApi.list).size shouldBe (1 + numberOfDefaultNodes)
  }

  @Test
  def duplicateDelete(): Unit =
    (0 to 10).foreach(_ => result(nodeApi.delete(CommonUtils.randomString(5))))

  @Test
  def duplicateUpdate(): Unit = {
    val init = result(nodeApi.list).size
    val count = 10
    (0 until count).foreach { _ =>
      result(
        nodeApi
          .request()
          .name(CommonUtils.randomString())
          .port(CommonUtils.availablePort())
          .user(CommonUtils.randomString())
          .password(CommonUtils.randomString())
          .create())
    }
    result(nodeApi.list).size shouldBe count + init
  }

  @Test
  def testInvalidNameOnCreation(): Unit = {
    val invalidStrings = Seq("a@", "a=", "a\\", "a~", "a//")
    invalidStrings.foreach { invalidString =>
      an[IllegalArgumentException] should be thrownBy result(
        nodeApi
          .request()
          .name(invalidString)
          .port(CommonUtils.availablePort())
          .user(CommonUtils.randomString())
          .password(CommonUtils.randomString())
          .create())
    }
  }

  @Test
  def testUpdatePort(): Unit = {
    val port = CommonUtils.availablePort()
    updatePartOfField(_.port(port), _.copy(port = port))
  }

  @Test
  def testUpdateUser(): Unit = {
    val user = CommonUtils.randomString()
    updatePartOfField(_.user(user), _.copy(user = user))
  }

  @Test
  def testUpdatePassword(): Unit = {
    val password = CommonUtils.randomString()
    updatePartOfField(_.password(password), _.copy(password = password))
  }

  private[this] def updatePartOfField(req: Request => Request, _expected: Node => Node): Unit = {
    val previous = result(
      nodeApi
        .request()
        .name(CommonUtils.randomString())
        .port(CommonUtils.availablePort())
        .user(CommonUtils.randomString())
        .password(CommonUtils.randomString())
        .update())
    val updated = result(req(nodeApi.request().name(previous.name)).update())
    val expected = _expected(previous)
    updated.name shouldBe expected.name
    updated.port shouldBe expected.port
    updated.user shouldBe expected.user
    updated.password shouldBe expected.password
  }

  @Test
  def failToCreateNodeWithoutPort(): Unit =
    an[IllegalArgumentException] should be thrownBy result(
      nodeApi
        .request()
        .name(CommonUtils.randomString())
        .user(CommonUtils.randomString())
        .password(CommonUtils.randomString())
        .update())

  @Test
  def failToCreateNodeWithoutUser(): Unit =
    an[IllegalArgumentException] should be thrownBy result(
      nodeApi
        .request()
        .name(CommonUtils.randomString())
        .port(CommonUtils.availablePort())
        .password(CommonUtils.randomString())
        .update())

  @Test
  def failToCreateNodeWithoutPassword(): Unit =
    an[IllegalArgumentException] should be thrownBy result(
      nodeApi
        .request()
        .name(CommonUtils.randomString())
        .port(CommonUtils.availablePort())
        .user(CommonUtils.randomString())
        .update())

  @After
  def tearDown(): Unit = Releasable.close(configurator)
}
