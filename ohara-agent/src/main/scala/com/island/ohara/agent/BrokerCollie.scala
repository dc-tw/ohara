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

import com.island.ohara.client.configurator.v0.BrokerApi.BrokerClusterInfo
import com.island.ohara.client.configurator.v0.ClusterInfo
import com.island.ohara.client.configurator.v0.ContainerApi.{ContainerInfo, PortMapping, PortPair}
import com.island.ohara.client.configurator.v0.NodeApi.Node
import com.island.ohara.client.configurator.v0.ZookeeperApi.ZookeeperClusterInfo
import com.island.ohara.client.kafka.TopicAdmin
import com.island.ohara.common.annotations.Optional
import com.island.ohara.common.util.CommonUtils
import com.island.ohara.metrics.BeanChannel
import com.island.ohara.metrics.kafka.TopicMeter

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

trait BrokerCollie extends Collie[BrokerClusterInfo, BrokerCollie.ClusterCreator] {

  /**
    * This is a complicated process. We must address following issues.
    * 1) check the existence of cluster
    * 2) check the existence of nodes
    * 3) Each broker container has got to export exporter port and client port
    * 4) Each broker container should assign "docker host name/port" to advertised name/port
    * 5) add zookeeper routes to all broker containers (broker needs to connect to zookeeper cluster)
    * 6) Add broker routes to all broker containers
    * 7) update existed containers (if we are adding new node into a running cluster)
    * @return creator of broker cluster
    */
  override def creator(): BrokerCollie.ClusterCreator =
    (executionContext, clusterName, imageName, zookeeperClusterName, clientPort, exporterPort, jmxPort, nodeNames) => {
      implicit val exec: ExecutionContext = executionContext
      clusters.flatMap(clusters => {
        clusters
          .filter(_._1.isInstanceOf[BrokerClusterInfo])
          .map {
            case (cluster, containers) => cluster.asInstanceOf[BrokerClusterInfo] -> containers
          }
          .find(_._1.name == clusterName)
          .map(_._2)
          .map(containers =>
            nodeCollie
              .nodes(containers.map(_.nodeName).toSet)
              .map(_.map(node => node -> containers.find(_.nodeName == node.name).get).toMap))
          .getOrElse(Future.successful(Map.empty))
          .map {
            existNodes =>
              // if there is a running cluster already, we should check the consistency of configuration
              existNodes.values.foreach {
                container =>
                  def checkValue(previous: String, newValue: String): Unit =
                    if (previous != newValue) throw new IllegalArgumentException(s"previous:$previous new:$newValue")

                  def check(key: String, newValue: String): Unit = {
                    val previous = container.environments(key)
                    if (previous != newValue) throw new IllegalArgumentException(s"previous:$previous new:$newValue")
                  }

                  checkValue(container.imageName, imageName)
                  check(BrokerCollie.CLIENT_PORT_KEY, clientPort.toString)
                  check(BrokerCollie.ZOOKEEPER_CLUSTER_NAME, zookeeperClusterName)
              }
              existNodes
          }
          .flatMap(existNodes =>
            nodeCollie
              .nodes(nodeNames)
              .map(_.map(node => node -> ContainerCollie.format(prefixKey, clusterName, serviceName)).toMap)
              .map((existNodes, _)))
          .map {
            case (existNodes, newNodes) =>
              existNodes.keys.foreach(node =>
                if (newNodes.keys.exists(_.name == node.name))
                  throw new IllegalArgumentException(s"${node.name} is running on a $clusterName"))

              (existNodes, newNodes, zookeeperContainers(zookeeperClusterName))
          }
          .flatMap {
            case (existNodes, newNodes, zkContainers) =>
              zkContainers
                .flatMap(zkContainers => {
                  if (zkContainers.isEmpty)
                    throw new IllegalArgumentException(s"$clusterName zookeeper container doesn't exist")
                  val zookeepers = zkContainers
                    .map(c => s"${c.nodeName}:${c.environments(ZookeeperCollie.CLIENT_PORT_KEY).toInt}")
                    .mkString(",")

                  //Use asInstanceOf function to solve compiler data type error
                  val route = ContainerCollie.preSettingEnvironment(existNodes.asInstanceOf[Map[Node, ContainerInfo]],
                                                                    newNodes.asInstanceOf[Map[Node, String]],
                                                                    zkContainers,
                                                                    resolveHostName,
                                                                    hookUpdate)
                  // the new broker node can't take used id so we find out the max id which is used by current cluster
                  val maxId: Int =
                    if (existNodes.isEmpty) 0
                    else existNodes.values.map(_.environments(BrokerCollie.ID_KEY).toInt).toSet.max + 1

                  // ssh connection is slow so we submit request by multi-thread
                  Future.sequence(newNodes.zipWithIndex.map {
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
                                hostPort = exporterPort,
                                containerPort = exporterPort
                              ),
                              PortPair(
                                hostPort = jmxPort,
                                containerPort = jmxPort
                              )
                            )
                          )),
                          environments = Map(
                            BrokerCollie.ID_KEY -> (maxId + index).toString,
                            BrokerCollie.CLIENT_PORT_KEY -> clientPort.toString,
                            BrokerCollie.ZOOKEEPERS_KEY -> zookeepers,
                            BrokerCollie.ADVERTISED_HOSTNAME_KEY -> node.name,
                            BrokerCollie.EXPORTER_PORT_KEY -> exporterPort.toString,
                            BrokerCollie.ADVERTISED_CLIENT_PORT_KEY -> clientPort.toString,
                            BrokerCollie.ZOOKEEPER_CLUSTER_NAME -> zookeeperClusterName,
                            BrokerCollie.JMX_HOSTNAME_KEY -> node.name,
                            BrokerCollie.JMX_PORT_KEY -> jmxPort.toString
                          ),
                          hostname = containerName
                        )
                        doCreator(executionContext, clusterName, containerName, containerInfo, node, route)
                        Some(containerInfo)
                      }
                  })
                })
                .map(_.flatten.toSeq)
                .map {
                  successfulContainers =>
                    if (successfulContainers.isEmpty)
                      throw new IllegalArgumentException(s"failed to create $clusterName on $serviceName")
                    val clusterInfo = BrokerClusterInfo(
                      name = clusterName,
                      imageName = imageName,
                      zookeeperClusterName = zookeeperClusterName,
                      exporterPort = exporterPort,
                      clientPort = clientPort,
                      jmxPort = jmxPort,
                      nodeNames = (successfulContainers.map(_.nodeName) ++ existNodes.map(_._1.name)).toSet
                    )
                    postCreateBrokerCluster(clusterInfo, successfulContainers)
                    clusterInfo
                }
          }
      })
    }

  /**
    * Please setting nodeCollie to implement class
    * @return
    */
  protected def nodeCollie: NodeCollie

  /**
    *  Implement prefix name for the platform
    * @return
    */
  protected def prefixKey: String

  /**
    * Setting service name
    * @return
    */
  protected def serviceName: String

  /**
    * Please implement this function to get Zookeeper cluster information
    * @param executionContext
    * @return
    */
  protected def zookeeperClusters(
    implicit executionContext: ExecutionContext): Future[Map[ClusterInfo, Seq[ContainerInfo]]]

  /**
    * Update exist node info
    * @param node
    * @param container
    * @param route
    */
  protected def hookUpdate(node: Node, container: ContainerInfo, route: Map[String, String]): Unit = {
    //Nothing
  }

  /**
    * Hostname resolve to IP address
    * @param nodeName
    * @return
    */
  protected def resolveHostName(nodeName: String): String = {
    CommonUtils.address(nodeName)
  }

  /**
    * Please implement this function to create the container to a different platform
    * @param executionContext
    * @param clusterName
    * @param containerName
    * @param containerInfo
    * @param node
    * @param route
    */
  protected def doCreator(executionContext: ExecutionContext,
                          clusterName: String,
                          containerName: String,
                          containerInfo: ContainerInfo,
                          node: Node,
                          route: Map[String, String]): Unit

  /**
    * After creating  the Broker, need to processor other things
    * @param clusterInfo
    * @param successfulContainers
    */
  protected def postCreateBrokerCluster(clusterInfo: ClusterInfo, successfulContainers: Seq[ContainerInfo]): Unit = {
    //Default Nothing
  }

  /**
    * Create a topic admin according to passed cluster name.
    * Noted: if target cluster doesn't exist, an future with exception will return
    * @param clusterName target cluster
    * @return cluster info and topic admin
    */
  def topicAdmin(clusterName: String)(
    implicit executionContext: ExecutionContext): Future[(BrokerClusterInfo, TopicAdmin)] = cluster(clusterName).map {
    case (c, _) => (c, topicAdmin(c))
  }

  /**
    * Create a topic admin according to passed cluster.
    * @param cluster target cluster
    * @return topic admin
    */
  def topicAdmin(cluster: BrokerClusterInfo): TopicAdmin = TopicAdmin(cluster.connectionProps)

  /**
    * Get all meter beans from specific broker cluster
    * @param cluster cluster
    * @return meter beans
    */
  def topicMeters(cluster: BrokerClusterInfo): Seq[TopicMeter] = cluster.nodeNames.flatMap { node =>
    BeanChannel.builder().hostname(node).port(cluster.jmxPort).build().topicMeters().asScala
  }.toSeq

  private[agent] def toBrokerCluster(clusterName: String, containers: Seq[ContainerInfo]): Future[BrokerClusterInfo] = {
    val first = containers.head
    Future.successful(
      BrokerClusterInfo(
        name = clusterName,
        imageName = first.imageName,
        zookeeperClusterName = first.environments(BrokerCollie.ZOOKEEPER_CLUSTER_NAME),
        exporterPort = first.environments(BrokerCollie.EXPORTER_PORT_KEY).toInt,
        clientPort = first.environments(BrokerCollie.CLIENT_PORT_KEY).toInt,
        jmxPort = first.environments(BrokerCollie.JMX_PORT_KEY).toInt,
        nodeNames = containers.map(_.nodeName).toSet
      ))
  }

  /**
    * For check, zookeeper cluster exist. You can override this function to check zookeeper cluster info
    * @param zkClusterName
    * @param executionContext
    * @return
    */
  private def zookeeperContainers(zkClusterName: String)(
    implicit executionContext: ExecutionContext): Future[Seq[ContainerInfo]] = {
    zookeeperClusters.map(
      _.filter(_._1.isInstanceOf[ZookeeperClusterInfo])
        .find(_._1.name == zkClusterName)
        .map(_._2)
        .getOrElse(throw new NoSuchClusterException(s"zookeeper cluster:$zkClusterName doesn't exist"))
    )
  }
}

object BrokerCollie {
  trait ClusterCreator extends Collie.ClusterCreator[BrokerClusterInfo] {
    private[this] var clientPort: Int = CommonUtils.availablePort()
    private[this] var zookeeperClusterName: String = _
    private[this] var exporterPort: Int = CommonUtils.availablePort()
    private[this] var jmxPort: Int = CommonUtils.availablePort()

    override protected def doCopy(clusterInfo: BrokerClusterInfo): Unit = {
      zookeeperClusterName(clusterInfo.zookeeperClusterName)
      clientPort(clusterInfo.clientPort)
      exporterPort(clusterInfo.exporterPort)
      jmxPort(clusterInfo.jmxPort)
    }

    def zookeeperClusterName(zookeeperClusterName: String): ClusterCreator = {
      this.zookeeperClusterName = CommonUtils.requireNonEmpty(zookeeperClusterName)
      this
    }

    @Optional("default is random port")
    def clientPort(clientPort: Int): ClusterCreator = {
      this.clientPort = CommonUtils.requireConnectionPort(clientPort)
      this
    }

    @Optional("default is random port")
    def exporterPort(exporterPort: Int): ClusterCreator = {
      this.exporterPort = CommonUtils.requireConnectionPort(exporterPort)
      this
    }

    @Optional("default is random port")
    def jmxPort(jmxPort: Int): ClusterCreator = {
      this.jmxPort = CommonUtils.requireConnectionPort(jmxPort)
      this
    }

    override def create()(implicit executionContext: ExecutionContext): Future[BrokerClusterInfo] = doCreate(
      executionContext = Objects.requireNonNull(executionContext),
      clusterName = CommonUtils.requireNonEmpty(clusterName),
      imageName = CommonUtils.requireNonEmpty(imageName),
      zookeeperClusterName = CommonUtils.requireNonEmpty(zookeeperClusterName),
      clientPort = CommonUtils.requireConnectionPort(clientPort),
      exporterPort = CommonUtils.requireConnectionPort(exporterPort),
      jmxPort = CommonUtils.requireConnectionPort(jmxPort),
      nodeNames = CommonUtils.requireNonEmpty(nodeNames.asJava).asScala.toSet
    )

    protected def doCreate(executionContext: ExecutionContext,
                           clusterName: String,
                           imageName: String,
                           zookeeperClusterName: String,
                           clientPort: Int,
                           exporterPort: Int,
                           jmxPort: Int,
                           nodeNames: Set[String]): Future[BrokerClusterInfo]
  }

  private[agent] val ID_KEY: String = "BROKER_ID"
  private[agent] val DATA_DIRECTORY_KEY: String = "BROKER_DATA_DIR"
  private[agent] val ZOOKEEPERS_KEY: String = "BROKER_ZOOKEEPERS"
  private[agent] val CLIENT_PORT_KEY: String = "BROKER_CLIENT_PORT"
  private[agent] val ADVERTISED_HOSTNAME_KEY: String = "BROKER_ADVERTISED_HOSTNAME"
  private[agent] val ADVERTISED_CLIENT_PORT_KEY: String = "BROKER_ADVERTISED_CLIENT_PORT"
  private[agent] val EXPORTER_PORT_KEY: String = "PROMETHEUS_EXPORTER_PORT"
  private[agent] val JMX_HOSTNAME_KEY: String = "JMX_HOSTNAME"
  private[agent] val JMX_PORT_KEY: String = "JMX_PORT"

  /**
    * internal key used to save the zookeeper cluster name.
    * All nodes of broker cluster should have this environment variable.
    */
  private[agent] val ZOOKEEPER_CLUSTER_NAME: String = "CCI_ZOOKEEPER_CLUSTER_NAME"
}
