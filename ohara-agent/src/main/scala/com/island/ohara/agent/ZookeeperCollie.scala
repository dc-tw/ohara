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
import java.util.Objects

import com.island.ohara.client.configurator.v0.ClusterInfo
import com.island.ohara.client.configurator.v0.ContainerApi.{ContainerInfo, PortMapping, PortPair}
import com.island.ohara.client.configurator.v0.NodeApi.Node
import com.island.ohara.client.configurator.v0.ZookeeperApi.ZookeeperClusterInfo
import com.island.ohara.common.annotations.Optional
import com.island.ohara.common.util.CommonUtils

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

/**
  * An interface of controlling zookeeper cluster.
  * It isolates the implementation of container manager from Configurator.
  */
trait ZookeeperCollie extends Collie[ZookeeperClusterInfo, ZookeeperCollie.ClusterCreator] {

  /**
    * This is a complicated process. We must address following issues.
    * 1) check the existence of cluster
    * 2) check the existence of nodes
    * 3) Each zookeeper container has got to export peer port, election port, and client port
    * 4) Each zookeeper container should use "docker host name" to replace "container host name".
    * 4) Add routes to all zookeeper containers
    * @return creator of broker cluster
    */
  override def creator(): ZookeeperCollie.ClusterCreator =
    (executionContext, clusterName, imageName, clientPort, peerPort, electionPort, nodeNames) => {
      implicit val exec: ExecutionContext = executionContext
      clusters.flatMap(clusters => {
        if (clusters.keys.filter(_.isInstanceOf[ZookeeperClusterInfo]).exists(_.name == clusterName))
          Future.failed(new IllegalArgumentException(s"zookeeper cluster:$clusterName exists!"))
        else
          nodeCollie
            .nodes(nodeNames)
            .map(_.map(node => node -> ContainerCollie.format(prefixKey, clusterName, serviceName)).toMap)
            .flatMap {
              nodes =>
                // add route in order to make zk node can connect to each other.
                val route: Map[String, String] = routeInfo(nodes)

                val zkServers: String = nodes.keys.map(_.name).mkString(" ")
                // ssh connection is slow so we submit request by multi-thread
                Future
                  .sequence(nodes.zipWithIndex.map {
                    case ((node, containerName), index) =>
                      Future {
                        val containerInfo = ContainerInfo(
                          nodeName = node.name,
                          id = ContainerCollie.UNKNOWN,
                          imageName = imageName,
                          created = ContainerCollie.UNKNOWN,
                          state = ContainerCollie.UNKNOWN,
                          kind = ContainerCollie.UNKNOWN,
                          name = containerName,
                          size = ContainerCollie.UNKNOWN,
                          portMappings = Seq(PortMapping(
                            hostIp = ContainerCollie.UNKNOWN,
                            portPairs = Seq(
                              PortPair(
                                hostPort = clientPort,
                                containerPort = clientPort
                              ),
                              PortPair(
                                hostPort = peerPort,
                                containerPort = peerPort
                              ),
                              PortPair(
                                hostPort = electionPort,
                                containerPort = electionPort
                              )
                            )
                          )),
                          environments = Map(
                            ZookeeperCollie.ID_KEY -> index.toString,
                            ZookeeperCollie.CLIENT_PORT_KEY -> clientPort.toString,
                            ZookeeperCollie.PEER_PORT_KEY -> peerPort.toString,
                            ZookeeperCollie.ELECTION_PORT_KEY -> electionPort.toString,
                            ZookeeperCollie.SERVERS_KEY -> zkServers
                          ),
                          // zookeeper doesn't have advertised hostname/port so we assign the "docker host" directly
                          hostname = node.name
                        )
                        doCreator(executionContext, clusterName, containerName, containerInfo, node, route)
                        Some(containerInfo)
                      }
                  })
                  .map(_.flatten.toSeq)
                  .map {
                    successfulContainers =>
                      if (successfulContainers.isEmpty)
                        throw new IllegalArgumentException(s"failed to create $clusterName on $serviceName")
                      val clusterInfo = ZookeeperClusterInfo(
                        name = clusterName,
                        imageName = imageName,
                        clientPort = clientPort,
                        peerPort = peerPort,
                        electionPort = electionPort,
                        nodeNames = successfulContainers.map(_.nodeName).toSet
                      )
                      postCreateZookeeperCluster(clusterInfo, successfulContainers)
                      clusterInfo
                  }
            }
      })
    }

  /**
    * Please implement nodeCollie
    * @return
    */
  protected def nodeCollie: NodeCollie

  /**
    * The prefix name for paltform
    * @return
    */
  protected def prefixKey: String

  /**
    * return service name
    * @return
    */
  protected def serviceName: String

  protected def doCreator(executionContext: ExecutionContext,
                          clusterName: String,
                          containerName: String,
                          containerInfo: ContainerInfo,
                          node: Node,
                          route: Map[String, String]): Unit

  protected def postCreateZookeeperCluster(clusterInfo: ClusterInfo, successfulContainers: Seq[ContainerInfo]): Unit = {
    //Default Nothing
  }

  protected def routeInfo(nodes: Map[Node, String]): Map[String, String] =
    nodes.map {
      case (node, _) =>
        node.name -> CommonUtils.address(node.name)
    }

  private[agent] def toZookeeperCluster(clusterName: String,
                                        containers: Seq[ContainerInfo]): Future[ZookeeperClusterInfo] = {
    val first = containers.head
    Future.successful(
      ZookeeperClusterInfo(
        name = clusterName,
        imageName = first.imageName,
        clientPort = first.environments(ZookeeperCollie.CLIENT_PORT_KEY).toInt,
        peerPort = first.environments(ZookeeperCollie.PEER_PORT_KEY).toInt,
        electionPort = first.environments(ZookeeperCollie.ELECTION_PORT_KEY).toInt,
        nodeNames = containers.map(_.nodeName).toSet
      ))
  }
}

object ZookeeperCollie {
  trait ClusterCreator extends Collie.ClusterCreator[ZookeeperClusterInfo] {
    private[this] var clientPort: Int = CommonUtils.availablePort()
    private[this] var peerPort: Int = CommonUtils.availablePort()
    private[this] var electionPort: Int = CommonUtils.availablePort()

    override protected def doCopy(clusterInfo: ZookeeperClusterInfo): Unit = {
      clientPort(clusterInfo.clientPort)
      peerPort(clusterInfo.peerPort)
      electionPort(clusterInfo.electionPort)
    }

    @Optional("default is random port")
    def clientPort(clientPort: Int): ClusterCreator = {
      this.clientPort = CommonUtils.requireConnectionPort(clientPort)
      this
    }

    @Optional("default is random port")
    def peerPort(peerPort: Int): ClusterCreator = {
      this.peerPort = CommonUtils.requireConnectionPort(peerPort)
      this
    }

    @Optional("default is random port")
    def electionPort(electionPort: Int): ClusterCreator = {
      this.electionPort = CommonUtils.requireConnectionPort(electionPort)
      this
    }

    override def create()(implicit executionContext: ExecutionContext): Future[ZookeeperClusterInfo] = doCreate(
      executionContext = Objects.requireNonNull(executionContext),
      clusterName = CommonUtils.requireNonEmpty(clusterName),
      imageName = CommonUtils.requireNonEmpty(imageName),
      clientPort = CommonUtils.requireConnectionPort(clientPort),
      peerPort = CommonUtils.requireConnectionPort(peerPort),
      electionPort = CommonUtils.requireConnectionPort(electionPort),
      nodeNames = CommonUtils.requireNonEmpty(nodeNames.asJava).asScala.toSet
    )

    protected def doCreate(executionContext: ExecutionContext,
                           clusterName: String,
                           imageName: String,
                           clientPort: Int,
                           peerPort: Int,
                           electionPort: Int,
                           nodeNames: Set[String]): Future[ZookeeperClusterInfo]
  }

  private[agent] val CLIENT_PORT_KEY: String = "ZK_CLIENT_PORT"

  private[agent] val PEER_PORT_KEY: String = "ZK_PEER_PORT"

  private[agent] val ELECTION_PORT_KEY: String = "ZK_ELECTION_PORT"

  private[agent] val DATA_DIRECTORY_KEY: String = "ZK_DATA_DIR"
  private[agent] val SERVERS_KEY: String = "ZK_SERVERS"
  private[agent] val ID_KEY: String = "ZK_ID"
}
