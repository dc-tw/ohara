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

package com.island.ohara.configurator.fake

import java.util.concurrent.ConcurrentHashMap

import com.island.ohara.agent.{NodeCollie, WorkerCollie}
import com.island.ohara.client.configurator.v0.{ClusterInfo, ContainerApi, NodeApi}
import com.island.ohara.client.configurator.v0.ContainerApi.ContainerInfo
import com.island.ohara.client.configurator.v0.WorkerApi.WorkerClusterInfo
import com.island.ohara.client.kafka.WorkerClient
import com.island.ohara.metrics.BeanChannel
import com.island.ohara.metrics.basic.CounterMBean

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

private[configurator] class FakeWorkerCollie(node: NodeCollie, wkConnectionProps: String)
    extends FakeCollie[WorkerClusterInfo, WorkerCollie.ClusterCreator](node)
    with WorkerCollie {

  override def counters(cluster: WorkerClusterInfo): Seq[CounterMBean] =
    // we don't care for the fake mode since both fake mode and embedded mode are run on local jvm
    BeanChannel.local().counterMBeans().asScala

  /**
    * cache all connectors info in-memory so we should keep instance for each fake cluster.
    */
  private[this] val fakeClientCache = new ConcurrentHashMap[WorkerClusterInfo, FakeWorkerClient]
  override def creator(): WorkerCollie.ClusterCreator =
    (_,
     clusterName,
     imageName,
     brokerClusterName,
     clientPort,
     jmxPort,
     groupId,
     offsetTopicName,
     offsetTopicReplications,
     offsetTopicPartitions,
     statusTopicName,
     statusTopicReplications,
     statusTopicPartitions,
     configTopicName,
     configTopicReplications,
     _,
     nodeNames) =>
      Future.successful(
        addCluster(
          WorkerClusterInfo(
            name = clusterName,
            imageName = imageName,
            brokerClusterName = brokerClusterName,
            clientPort = clientPort,
            jmxPort = jmxPort,
            groupId = groupId,
            offsetTopicName = offsetTopicName,
            offsetTopicPartitions = offsetTopicPartitions,
            offsetTopicReplications = offsetTopicReplications,
            configTopicName = configTopicName,
            configTopicPartitions = 1,
            configTopicReplications = configTopicReplications,
            statusTopicName = statusTopicName,
            statusTopicPartitions = statusTopicPartitions,
            statusTopicReplications = statusTopicReplications,
            jarInfos = Seq.empty,
            connectors = Seq.empty,
            nodeNames = nodeNames
          )))

  override protected def doRemoveNode(previousCluster: WorkerClusterInfo, beRemovedContainer: ContainerInfo)(
    implicit executionContext: ExecutionContext): Future[Boolean] = Future
    .successful(
      addCluster(WorkerClusterInfo(
        name = previousCluster.name,
        imageName = previousCluster.imageName,
        brokerClusterName = previousCluster.brokerClusterName,
        clientPort = previousCluster.clientPort,
        jmxPort = previousCluster.jmxPort,
        groupId = previousCluster.groupId,
        statusTopicName = previousCluster.statusTopicName,
        statusTopicPartitions = previousCluster.statusTopicPartitions,
        statusTopicReplications = previousCluster.statusTopicReplications,
        configTopicName = previousCluster.configTopicName,
        configTopicPartitions = previousCluster.configTopicPartitions,
        configTopicReplications = previousCluster.configTopicReplications,
        offsetTopicName = previousCluster.offsetTopicName,
        offsetTopicPartitions = previousCluster.offsetTopicPartitions,
        offsetTopicReplications = previousCluster.offsetTopicReplications,
        jarInfos = previousCluster.jarInfos,
        connectors = Seq.empty,
        nodeNames = previousCluster.nodeNames.filterNot(_ == beRemovedContainer.nodeName)
      )))
    .map(_ => true)

  override def workerClient(cluster: WorkerClusterInfo): WorkerClient =
    if (wkConnectionProps == null) {
      val fake = new FakeWorkerClient
      val r = fakeClientCache.putIfAbsent(cluster, fake)
      if (r == null) fake else r
    } else WorkerClient(wkConnectionProps)

  override protected def doAddNode(
    previousCluster: WorkerClusterInfo,
    previousContainers: Seq[ContainerApi.ContainerInfo],
    newNodeName: String)(implicit executionContext: ExecutionContext): Future[WorkerClusterInfo] =
    Future.successful(
      addCluster(
        WorkerClusterInfo(
          name = previousCluster.name,
          imageName = previousCluster.imageName,
          brokerClusterName = previousCluster.brokerClusterName,
          clientPort = previousCluster.clientPort,
          jmxPort = previousCluster.jmxPort,
          groupId = previousCluster.groupId,
          statusTopicName = previousCluster.statusTopicName,
          statusTopicPartitions = previousCluster.statusTopicPartitions,
          statusTopicReplications = previousCluster.statusTopicReplications,
          configTopicName = previousCluster.configTopicName,
          configTopicPartitions = previousCluster.configTopicPartitions,
          configTopicReplications = previousCluster.configTopicReplications,
          offsetTopicName = previousCluster.offsetTopicName,
          offsetTopicPartitions = previousCluster.offsetTopicPartitions,
          offsetTopicReplications = previousCluster.offsetTopicReplications,
          jarInfos = previousCluster.jarInfos,
          connectors = Seq.empty,
          nodeNames = previousCluster.nodeNames ++ Set(newNodeName)
        )))

  override protected def doCreator(executionContext: ExecutionContext,
                                   clusterName: String,
                                   containerName: String,
                                   containerInfo: ContainerInfo,
                                   node: NodeApi.Node,
                                   route: Map[String, String]): Unit =
    throw new UnsupportedOperationException("FakeWorkerCollie doesn't support doCreator function")

  override protected def brokerClusters(
    implicit executionContext: ExecutionContext): Future[Map[ClusterInfo, Seq[ContainerInfo]]] =
    throw new UnsupportedOperationException("FakeWorkerCollie doesn't support brokerClusters function")

  /**
    * Please implement nodeCollie
    *
    * @return
    */
  override protected def nodeCollie: NodeCollie = node

  /**
    * Implement prefix name for paltform
    *
    * @return
    */
  override protected def prefixKey: String = "fakeworker"
}
