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

package com.island.ohara.agent.k8s

import com.island.ohara.agent.{ContainerCollie, NodeCollie, ZookeeperCollie}
import com.island.ohara.client.configurator.v0.ContainerApi.ContainerInfo
import com.island.ohara.client.configurator.v0.NodeApi.Node
import com.island.ohara.client.configurator.v0.ZookeeperApi.ZookeeperClusterInfo

import scala.concurrent.{Await, ExecutionContext, Future}
import com.island.ohara.common.rule.SmallTest
import com.island.ohara.common.util.CommonUtils
import org.junit.Test
import org.scalatest.Matchers

import scala.concurrent.ExecutionContext.Implicits
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

class TestK8SBasicCollieImpl extends SmallTest with Matchers {
  private[this] val TIMEOUT: FiniteDuration = 30 seconds

  @Test
  def testClusterName(): Unit = {
    val cluster1ContainerName = ContainerCollie.format(PREFIX_KEY, "cluster1", "zk")
    zookeeperClusterName(cluster1ContainerName) shouldBe "cluster1"

    val cluster2ContainerName = ContainerCollie.format(PREFIX_KEY, "zk", "zk")
    zookeeperClusterName(cluster2ContainerName) shouldBe "zk"

    val cluster3ContainerName = ContainerCollie.format(PREFIX_KEY, "zkzk", "zk")
    zookeeperClusterName(cluster3ContainerName) shouldBe "zkzk"

    val cluster4ContainerName = s"${PREFIX_KEY}${DIVIDER}zk${DIVIDER}zk"
    zookeeperClusterName(cluster4ContainerName) shouldBe "zk"
  }

  private[this] def zookeeperClusterName(containerName: String): String = {
    val node1Name = "node1"
    val node1: Node = Node(node1Name, 22, "", "", services = Seq.empty, lastModified = CommonUtils.current())
    val nodes: Seq[Node] = Seq(node1)
    val nodeCollie = NodeCollie(nodes)
    val k8sClient = new FakeK8SClient(true, None, containerName)

    val k8sBasicCollieImpl =
      new K8SBasicCollieImpl[ZookeeperClusterInfo, ZookeeperCollie.ClusterCreator](nodeCollie, k8sClient) {
        override protected def toClusterDescription(clusterName: String, containers: Seq[ContainerInfo])(
          implicit executionContext: ExecutionContext): Future[ZookeeperClusterInfo] = {
          Future {
            ZookeeperClusterInfo(clusterName, containers(0).imageName, 2181, 2182, 2183, Set.empty)
          }
        }
        override def creator(): ZookeeperCollie.ClusterCreator =
          throw new UnsupportedOperationException("Test doesn't support creator function")
      }

    val containers = k8sBasicCollieImpl.clusterWithAllContainers(Implicits.global)
    Await.result(containers, TIMEOUT).head._1.name
  }
}
