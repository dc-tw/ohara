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

package com.island.ohara.agent

import com.island.ohara.client.configurator.v0.NodeApi.Node
import com.island.ohara.client.configurator.v0.ZookeeperApi
import com.island.ohara.client.configurator.v0.ZookeeperApi.ZookeeperClusterInfo
import com.island.ohara.common.rule.SmallTest
import com.island.ohara.common.util.CommonUtils
import org.junit.Test
import org.scalatest.Matchers

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
class TestZookeeperCreator extends SmallTest with Matchers {
  private[this] val TIMEOUT: FiniteDuration = 30 seconds

  private[this] def zkCreator(): ZookeeperCollie.ClusterCreator =
    (executionContext, clusterName, imageName, clientPort, peerPort, electionPort, nodeNames) => {
      // the inputs have been checked (NullPointerException). Hence, we throw another exception here.
      if (executionContext == null) throw new AssertionError()
      if (clusterName == null || clusterName.isEmpty) throw new AssertionError()
      if (imageName == null || imageName.isEmpty) throw new AssertionError()
      if (clientPort <= 0) throw new AssertionError()
      if (peerPort <= 0) throw new AssertionError()
      if (electionPort <= 0) throw new AssertionError()
      if (nodeNames == null || nodeNames.isEmpty) throw new AssertionError()
      Future.successful(
        ZookeeperClusterInfo(
          name = clusterName,
          imageName = imageName,
          clientPort = clientPort,
          peerPort = peerPort,
          electionPort = electionPort,
          nodeNames = nodeNames
        ))
    }

  @Test
  def nullImage(): Unit = {
    an[NullPointerException] should be thrownBy zkCreator().imageName(null)
  }

  @Test
  def emptyImage(): Unit = {
    an[IllegalArgumentException] should be thrownBy zkCreator().imageName("")
  }

  @Test
  def nullClusterName(): Unit = {
    an[NullPointerException] should be thrownBy zkCreator().clusterName(null)
  }

  @Test
  def emptyClusterName(): Unit = {
    an[IllegalArgumentException] should be thrownBy zkCreator().clusterName("")
  }

  @Test
  def negativeClientPort(): Unit = {
    an[IllegalArgumentException] should be thrownBy zkCreator().clientPort(-1)
  }

  @Test
  def negativePeerPort(): Unit = {
    an[IllegalArgumentException] should be thrownBy zkCreator().peerPort(-1)
  }

  @Test
  def negativeElectionPort(): Unit = {
    an[IllegalArgumentException] should be thrownBy zkCreator().electionPort(-1)
  }

  @Test
  def nullNodes(): Unit = {
    an[NullPointerException] should be thrownBy zkCreator().nodeNames(null)
  }

  @Test
  def emptyNodes(): Unit = {
    an[IllegalArgumentException] should be thrownBy zkCreator().nodeNames(Set.empty)
  }

  @Test
  def testNameLength(): Unit = zkCreator()
    .clusterName(CommonUtils.randomString(10))
    .imageName(CommonUtils.randomString(10))
    .peerPort(CommonUtils.availablePort())
    .clientPort(CommonUtils.availablePort())
    .electionPort(CommonUtils.availablePort())
    .nodeName(CommonUtils.randomString())
    .create()

  @Test
  def testInvalidName(): Unit = an[IllegalArgumentException] should be thrownBy zkCreator()
    .clusterName(CommonUtils.randomString(Collie.LIMIT_OF_NAME_LENGTH + 1))
    .imageName(CommonUtils.randomString(10))
    .peerPort(CommonUtils.availablePort())
    .clientPort(CommonUtils.availablePort())
    .electionPort(CommonUtils.availablePort())
    .nodeName(CommonUtils.randomString())
    .create()

  @Test
  def testCopy(): Unit = {
    val zookeeperClusterInfo = ZookeeperClusterInfo(
      name = CommonUtils.randomString(10),
      imageName = CommonUtils.randomString(),
      clientPort = 10,
      peerPort = 10,
      electionPort = 10,
      nodeNames = Set(CommonUtils.randomString())
    )
    Await.result(zkCreator().copy(zookeeperClusterInfo).create(), 30 seconds) shouldBe zookeeperClusterInfo
  }

  @Test
  def testZKCreator(): Unit = {
    val node1Name = "node1"
    val node1 = Node(name = node1Name,
                     port = 22,
                     user = "user1",
                     password = "123456",
                     services = Seq.empty,
                     lastModified = CommonUtils.current())
    val node2Name = "node2"
    val node2 = node1.copy(name = node2Name)

    val zookeeperCollie = new FakeZookeeperCollie(NodeCollie(Seq(node1, node2)))

    val zkCreator: Future[ZookeeperClusterInfo] = zookeeperCollie
      .creator()
      .clusterName("cluster1")
      .imageName(ZookeeperApi.IMAGE_NAME_DEFAULT)
      .clientPort(2181)
      .peerPort(2182)
      .electionPort(2183)
      .nodeNames(Set(node1Name, node2Name))
      .create()

    val zookeeperClusterInfo = Await.result(zkCreator, TIMEOUT)
    zookeeperClusterInfo.name shouldBe "cluster1"
    zookeeperClusterInfo.clientPort shouldBe 2181
    zookeeperClusterInfo.peerPort shouldBe 2182
    zookeeperClusterInfo.electionPort shouldBe 2183
    zookeeperClusterInfo.nodeNames.size shouldBe 2
    zookeeperClusterInfo.nodeNames.contains(node1Name) shouldBe true
    zookeeperClusterInfo.nodeNames.contains(node2Name) shouldBe true
    zookeeperClusterInfo.ports.size shouldBe 3
  }
}
