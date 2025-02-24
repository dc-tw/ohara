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

import com.island.ohara.client.configurator.v0._
import com.island.ohara.common.rule.SmallTest
import com.island.ohara.common.util.{CommonUtils, Releasable}
import com.island.ohara.configurator.Configurator
import org.junit.{After, Test}
import org.scalatest.Matchers

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
class TestLogRoute extends SmallTest with Matchers {
  private[this] val configurator = Configurator.builder().fake().build()

  private[this] val logApi = LogApi.access().hostname(configurator.hostname).port(configurator.port)

  private[this] val zkApi = ZookeeperApi.access.hostname(configurator.hostname).port(configurator.port)

  private[this] val bkApi = BrokerApi.access().hostname(configurator.hostname).port(configurator.port)

  private[this] val wkApi = WorkerApi.access().hostname(configurator.hostname).port(configurator.port)

  private[this] def result[T](f: Future[T]): T = Await.result(f, 10 seconds)

  @Test
  def fetchLogFromZookeeper(): Unit = {
    val cluster = result(zkApi.list).head
    val clusterLogs = result(logApi.log4ZookeeperCluster(cluster.name))
    clusterLogs.name shouldBe cluster.name
    clusterLogs.logs.isEmpty shouldBe false
  }

  @Test
  def fetchLogFromBroker(): Unit = {
    val cluster = result(bkApi.list).head
    val clusterLogs = result(logApi.log4BrokerCluster(cluster.name))
    clusterLogs.name shouldBe cluster.name
    clusterLogs.logs.isEmpty shouldBe false
  }

  @Test
  def fetchLogFromWorker(): Unit = {
    val cluster = result(wkApi.list).head
    val clusterLogs = result(logApi.log4WorkerCluster(cluster.name))
    clusterLogs.name shouldBe cluster.name
    clusterLogs.logs.isEmpty shouldBe false
  }

  @Test
  def fetchLogFromUnknown(): Unit = {
    an[IllegalArgumentException] should be thrownBy result(logApi.log4ZookeeperCluster(CommonUtils.randomString(10)))
    an[IllegalArgumentException] should be thrownBy result(logApi.log4BrokerCluster(CommonUtils.randomString(10)))
    an[IllegalArgumentException] should be thrownBy result(logApi.log4WorkerCluster(CommonUtils.randomString(10)))
  }

  @After
  def tearDown(): Unit = Releasable.close(configurator)
}
