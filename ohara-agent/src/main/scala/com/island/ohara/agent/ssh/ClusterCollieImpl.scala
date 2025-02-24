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

package com.island.ohara.agent.ssh

import java.util.concurrent.ExecutorService

import com.island.ohara.agent._
import com.island.ohara.client.configurator.v0.ClusterInfo
import com.island.ohara.client.configurator.v0.ContainerApi.ContainerInfo
import com.island.ohara.client.configurator.v0.NodeApi.Node
import com.island.ohara.common.util.{Releasable, ReleaseOnce}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

// accessible to configurator
private[ohara] class ClusterCollieImpl(cacheTimeout: Duration, nodeCollie: NodeCollie, cacheThreadPool: ExecutorService)
    extends ReleaseOnce
    with ClusterCollie {

  private[this] val dockerCache = DockerClientCache()

  private[this] val clusterCache: ClusterCache = ClusterCache.builder
    .frequency(cacheTimeout)
    // TODO: 5 * timeout is enough ??? by chia
    .supplier(() => Await.result(doClusters(ExecutionContext.fromExecutor(cacheThreadPool)), cacheTimeout * 5))
    // Giving some time to process to complete the build and then we can remove it from cache safety.
    .lazyRemove(cacheTimeout)
    .build()

  private[this] val zkCollie: ZookeeperCollieImpl = new ZookeeperCollieImpl(nodeCollie, dockerCache, clusterCache)

  private[this] val bkCollie: BrokerCollieImpl = new BrokerCollieImpl(nodeCollie, dockerCache, clusterCache)

  private[this] val wkCollie: WorkerCollieImpl = new WorkerCollieImpl(nodeCollie, dockerCache, clusterCache)

  private[this] val _streamCollie: StreamCollieImpl = new StreamCollieImpl(nodeCollie, dockerCache, clusterCache)

  override def zookeeperCollie(): ZookeeperCollie = zkCollie
  override def brokerCollie(): BrokerCollie = bkCollie
  override def workerCollie(): WorkerCollie = wkCollie
  override def streamCollie(): StreamCollie = _streamCollie

  private[this] def doClusters(
    implicit executionContext: ExecutionContext): Future[Map[ClusterInfo, Seq[ContainerInfo]]] = nodeCollie
    .nodes()
    .flatMap(Future
      .traverse(_) { node =>
        // multi-thread to seek all containers from multi-nodes
        // Note: we fetch all containers (include exited and running) here
        dockerCache.exec(node, _.containers(containerName => containerName.startsWith(PREFIX_KEY))).recover {
          case e: Throwable =>
            LOG.error(s"failed to get active containers from $node", e)
            Seq.empty
        }
      }
      .map(_.flatten))
    .flatMap { allContainers =>
      def parse(serviceName: String,
                f: (String, Seq[ContainerInfo]) => Future[ClusterInfo]): Future[Map[ClusterInfo, Seq[ContainerInfo]]] =
        Future
          .sequence(
            allContainers
              .filter(_.name.contains(s"$DIVIDER$serviceName$DIVIDER"))
              // form: PREFIX_KEY-CLUSTER_NAME-SERVICE-HASH
              .map(container => container.name.split(DIVIDER)(1) -> container)
              .groupBy(_._1)
              .map {
                case (clusterName, value) => clusterName -> value.map(_._2)
              }
              .map {
                case (clusterName, containers) => f(clusterName, containers).map(_ -> containers)
              })
          .map(_.toMap)

      for {
        zkMap <- parse(ContainerCollie.ZK_SERVICE_NAME, zkCollie.toZookeeperCluster)
        bkMap <- parse(ContainerCollie.BK_SERVICE_NAME, bkCollie.toBrokerCluster)
        wkMap <- parse(ContainerCollie.WK_SERVICE_NAME, wkCollie.toWorkerCluster)
        streamMap <- parse(ContainerCollie.STREAM_SERVICE_NAME, _streamCollie.toStreamCluster)
      } yield zkMap ++ bkMap ++ wkMap ++ streamMap
    }

  override protected def doClose(): Unit = {
    Releasable.close(dockerCache)
    Releasable.close(clusterCache)
  }

  override def images(nodes: Seq[Node])(implicit executionContext: ExecutionContext): Future[Map[Node, Seq[String]]] =
    Future.traverse(nodes)(node => Future(dockerCache.exec(node, node -> _.imageNames()))).map(_.toMap)
}
