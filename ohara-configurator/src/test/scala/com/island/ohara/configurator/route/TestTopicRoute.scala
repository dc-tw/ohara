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

import com.island.ohara.client.configurator.v0.TopicApi.{Request, TopicInfo}
import com.island.ohara.client.configurator.v0.{BrokerApi, TopicApi, ZookeeperApi}
import com.island.ohara.common.rule.SmallTest
import com.island.ohara.common.util.{CommonUtils, Releasable}
import com.island.ohara.configurator.Configurator
import org.junit.{After, Test}
import org.scalatest.Matchers

import scala.concurrent.ExecutionContext.Implicits.global
class TestTopicRoute extends SmallTest with Matchers {

  private[this] val configurator = Configurator.builder().fake(1, 0).build()

  private[this] val topicApi = TopicApi.access.hostname(configurator.hostname).port(configurator.port)

  @Test
  def test(): Unit = {
    // test add
    result(topicApi.list).size shouldBe 0
    val name = CommonUtils.randomString(10)
    val numberOfPartitions: Int = 3
    val numberOfReplications: Short = 3
    val response = result(
      topicApi.request
        .name(name)
        .numberOfPartitions(numberOfPartitions)
        .numberOfReplications(numberOfReplications)
        .create())
    response.name shouldBe name
    response.numberOfPartitions shouldBe numberOfPartitions
    response.numberOfReplications shouldBe numberOfReplications

    // test get
    val response2 = result(topicApi.get(response.name))
    response.name shouldBe response2.name
    response.brokerClusterName shouldBe response2.brokerClusterName
    response.numberOfPartitions shouldBe response2.numberOfPartitions
    response.numberOfReplications shouldBe response2.numberOfReplications

    // test update
    val numberOfPartitions3: Int = 5
    val response3 = result(topicApi.request.name(name).numberOfPartitions(numberOfPartitions3).update())
    response3.numberOfPartitions shouldBe numberOfPartitions3

    // test delete
    result(topicApi.list).size shouldBe 1
    result(topicApi.delete(response.name))
    result(topicApi.list).size shouldBe 0

    // test nonexistent data
    an[IllegalArgumentException] should be thrownBy result(topicApi.get("123"))
  }

  @Test
  def removeTopicFromNonexistentBrokerCluster(): Unit = {
    val name = methodName()
    result(
      topicApi.request
        .name(name)
        .create()
        .flatMap { topicInfo =>
          BrokerApi
            .access()
            .hostname(configurator.hostname)
            .port(configurator.port)
            .delete(topicInfo.brokerClusterName)
            .flatMap(_ => topicApi.delete(topicInfo.name))
        }
        .flatMap(_ => topicApi.list)
        .map(topics => topics.exists(_.name == name))) shouldBe false
  }

  @Test
  def createTopicOnNonexistentCluster(): Unit = {
    an[IllegalArgumentException] should be thrownBy result(
      topicApi.request.name(CommonUtils.randomString(10)).brokerClusterName(CommonUtils.randomString()).create())
  }

  @Test
  def createTopicWithoutBrokerClusterName(): Unit = {
    val zk = result(ZookeeperApi.access.hostname(configurator.hostname).port(configurator.port).list).head

    val zk2 = result(
      ZookeeperApi.access
        .hostname(configurator.hostname)
        .port(configurator.port)
        .request
        .name(CommonUtils.randomString(10))
        .nodeNames(zk.nodeNames)
        .create()
    )

    val bk2 = result(
      BrokerApi
        .access()
        .hostname(configurator.hostname)
        .port(configurator.port)
        .request()
        .name(CommonUtils.randomString(10))
        .zookeeperClusterName(zk2.name)
        .nodeNames(zk2.nodeNames)
        .create())

    an[IllegalArgumentException] should be thrownBy result(topicApi.request.name(CommonUtils.randomString(10)).create())

    result(topicApi.request.name(CommonUtils.randomString(10)).brokerClusterName(bk2.name).create())
  }

  @Test
  def testUpdateBrokerClusterName(): Unit = {
    val zk = result(ZookeeperApi.access.hostname(configurator.hostname).port(configurator.port).list).head

    val zk2 = result(
      ZookeeperApi.access
        .hostname(configurator.hostname)
        .port(configurator.port)
        .request
        .name(CommonUtils.randomString(10))
        .nodeNames(zk.nodeNames)
        .create()
    )

    result(
      BrokerApi
        .access()
        .hostname(configurator.hostname)
        .port(configurator.port)
        .request()
        .name(CommonUtils.randomString(10))
        .zookeeperClusterName(zk2.name)
        .nodeNames(zk2.nodeNames)
        .create())

    val bks = result(BrokerApi.access().hostname(configurator.hostname).port(configurator.port).list)
    bks.size shouldBe 2

    val topicInfo = result(
      topicApi.request.name(CommonUtils.randomString(10)).brokerClusterName(bks.head.name).create())

    an[IllegalArgumentException] should be thrownBy result(
      topicApi.request.name(topicInfo.name).brokerClusterName(bks.last.name).update())
  }

  @Test
  def testPartitions(): Unit = {
    val topic0 = result(topicApi.request.name(CommonUtils.randomString(10)).create())

    // we can't reduce number of partitions
    an[IllegalArgumentException] should be thrownBy result(
      topicApi.request.name(topic0.name).numberOfPartitions(topic0.numberOfPartitions - 1).update())

    val topic1 = result(topicApi.request.name(topic0.name).numberOfPartitions(topic0.numberOfPartitions + 1).update())

    topic0.name shouldBe topic1.name
    topic0.name shouldBe topic1.name
    topic0.numberOfPartitions + 1 shouldBe topic1.numberOfPartitions
    topic0.numberOfReplications shouldBe topic1.numberOfReplications
  }

  @Test
  def testReplications(): Unit = {
    val topicInfo = result(topicApi.request.name(CommonUtils.randomString(10)).create())

    // we can't reduce number of replications
    an[IllegalArgumentException] should be thrownBy result(
      topicApi.request
        .name(topicInfo.name)
        .numberOfReplications((topicInfo.numberOfReplications - 1).asInstanceOf[Short])
        .update())

    // we can't add number of replications
    an[IllegalArgumentException] should be thrownBy result(
      topicApi.request
        .name(topicInfo.name)
        .numberOfReplications((topicInfo.numberOfReplications + 1).asInstanceOf[Short])
        .update())

    // pass since we don't make changes on number of replications
    result(
      topicApi.request.name(CommonUtils.randomString(10)).numberOfReplications(topicInfo.numberOfReplications).create())
  }

  @Test
  def duplicateDelete(): Unit =
    (0 to 10).foreach(_ => result(topicApi.delete(CommonUtils.randomString(5))))

  @Test
  def duplicateUpdate(): Unit =
    (0 to 10).foreach(_ => result(topicApi.request.name(CommonUtils.randomString()).update()))

  @Test
  def testBrokerClusterName(): Unit = {
    val topicInfo = result(topicApi.request.name(CommonUtils.randomString(10)).create())
    an[IllegalArgumentException] should be thrownBy result(
      topicApi.request.name(topicInfo.name).brokerClusterName(CommonUtils.randomString()).update())
  }

  @Test
  def testUpdateNumberOfPartitions(): Unit = {
    val numberOfPartitions = 2
    updatePartOfField(_.numberOfPartitions(numberOfPartitions), _.copy(numberOfPartitions = numberOfPartitions))
  }

  private[this] def updatePartOfField(req: Request => Request, _expected: TopicInfo => TopicInfo): Unit = {
    val previous = result(
      topicApi.request.name(CommonUtils.randomString()).numberOfReplications(1).numberOfPartitions(1).update())
    val updated = result(req(topicApi.request.name(previous.name)).update())
    val expected = _expected(previous)
    updated.name shouldBe expected.name
    updated.brokerClusterName shouldBe expected.brokerClusterName
    updated.numberOfReplications shouldBe expected.numberOfReplications
    updated.numberOfPartitions shouldBe expected.numberOfPartitions
  }

  @Test
  def deleteAnTopicRemovedFromKafka(): Unit = {
    val topicName = methodName

    val topic = result(topicApi.request.name(topicName).create())

    val topicAdmin = configurator.clusterCollie
      .brokerCollie()
      .topicAdmin(result(configurator.clusterCollie.brokerCollie().clusters).head._1)
    try {
      topicAdmin.delete(topic.name)
      // the topic is removed but we don't throw exception.
      result(topicApi.delete(topic.name))
    } finally topicAdmin.close()
  }

  @After
  def tearDown(): Unit = Releasable.close(configurator)
}
