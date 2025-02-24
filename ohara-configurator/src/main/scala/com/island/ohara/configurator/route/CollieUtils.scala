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
import com.island.ohara.agent.{BrokerCollie, WorkerCollie}
import com.island.ohara.client.configurator.v0.BrokerApi.BrokerClusterInfo
import com.island.ohara.client.configurator.v0.ClusterInfo
import com.island.ohara.client.configurator.v0.WorkerApi.WorkerClusterInfo
import com.island.ohara.client.kafka.{TopicAdmin, WorkerClient}

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.{ClassTag, classTag}

/**
  * TODO: this is just a workaround in ohara 0.3. It handles the following trouble:
  * 1) UI doesn't support user to host zk and bk cluster so most request won't carry the information of broker. Hence
  *    we need to handle the request with "default" broker cluster
  * 2) make ops to cluster be "blocking"
  */
object CollieUtils {

  private[route] def topicAdmin[T](clusterName: Option[String])(
    implicit brokerCollie: BrokerCollie,
    cleaner: AdminCleaner,
    executionContext: ExecutionContext): Future[(BrokerClusterInfo, TopicAdmin)] = clusterName
    .map(brokerCollie.topicAdmin)
    .getOrElse(brokerCollie.clusters
      .map { clusters =>
        clusters.size match {
          case 0 =>
            throw new IllegalArgumentException(
              s"we can't choose default broker cluster since there is no broker cluster")
          case 1 => clusters.keys.head
          case _ =>
            throw new IllegalArgumentException(
              s"we can't choose default broker cluster since there are too many broker cluster:${clusters.keys.map(_.name).mkString(",")}")
        }
      }
      .map(c => (c, cleaner.add(brokerCollie.topicAdmin(c)))))

  private[route] def workerClient[T](clusterName: Option[String])(
    implicit workerCollie: WorkerCollie,
    executionContext: ExecutionContext): Future[(WorkerClusterInfo, WorkerClient)] = clusterName
    .map(workerCollie.workerClient)
    .getOrElse(workerCollie.clusters
      .map { clusters =>
        clusters.size match {
          case 0 =>
            throw new IllegalArgumentException(
              s"we can't choose default worker cluster since there is no worker cluster")
          case 1 => clusters.keys.head
          case _ =>
            throw new IllegalArgumentException(
              s"we can't choose default worker cluster since there are too many worker cluster:${clusters.keys.map(_.name).mkString(",")}")
        }
      }
      .map(c => (c, workerCollie.workerClient(c))))

  private[route] def both[T](wkClusterName: Option[String])(
    implicit brokerCollie: BrokerCollie,
    cleaner: AdminCleaner,
    workerCollie: WorkerCollie,
    executionContext: ExecutionContext): Future[(BrokerClusterInfo, TopicAdmin, WorkerClusterInfo, WorkerClient)] =
    workerClient(wkClusterName).flatMap {
      case (wkInfo, wkClient) =>
        brokerCollie.topicAdmin(wkInfo.brokerClusterName).map {
          case (bkInfo, topicAdmin) => (bkInfo, cleaner.add(topicAdmin), wkInfo, wkClient)
        }
    }

  private[route] def as[T <: ClusterInfo: ClassTag](clusters: Seq[ClusterInfo]): Seq[T] =
    clusters.filter(classTag[T].runtimeClass.isInstance).map(_.asInstanceOf[T])
}
