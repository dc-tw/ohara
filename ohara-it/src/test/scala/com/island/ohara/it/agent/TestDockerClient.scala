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

package com.island.ohara.it.agent

import java.util.concurrent.TimeUnit

import com.island.ohara.agent.docker.{ContainerState, DockerClient}
import com.island.ohara.client.configurator.v0.ContainerApi.PortPair
import com.island.ohara.common.util.{CommonUtils, Releasable}
import com.island.ohara.it.IntegrationTest
import org.junit.{After, Before, Test}
import org.scalatest.Matchers

/**
  * all test cases here are executed on remote node. If no remote node is defined, all tests are skipped.
  * You can run following command to pass the information of remote node.
  * $ gradle clean ohara-it:test --tests *TestDockerClient -PskipManager -Pohara.it.docker=$user:$password@$hostname:$port
  */
class TestDockerClient extends IntegrationTest with Matchers {

  private[this] var client: DockerClient = _

  private[this] val webHost = "www.google.com.tw"

  private[this] var remoteHostname: String = _

  private[this] val imageName = "centos:7"

  @Before
  def setup(): Unit =
    CollieTestUtils.nodeCache().headOption.foreach { node =>
      client = DockerClient.builder.hostname(node.name).port(node.port).user(node.user).password(node.password).build
      remoteHostname = node.name
    }

  /**
    * make sure all test cases here are executed only if we have defined the docker server.
    * @param f test case
    */
  private[this] def runTest(f: DockerClient => Unit): Unit = if (client == null)
    skipTest(s"no available nodes are passed from env variables")
  else f(client)

  @Test
  def testLog(): Unit = runTest { client =>
    val name = CommonUtils.randomString(10)
    client
      .containerCreator()
      .name(name)
      .imageName(imageName)
      .removeContainerOnExit()
      .command(s"""/bin/bash -c \"ping $webHost\"""")
      .execute()
    try client.log(name).contains(webHost) shouldBe true
    finally client.forceRemove(name)
  }

  @Test
  def testList(): Unit = runTest { client =>
    val name = CommonUtils.randomString(10)
    client.containerNames().contains(name) shouldBe false
    client
      .containerCreator()
      .name(name)
      .imageName(imageName)
      .removeContainerOnExit()
      .command(s"""/bin/bash -c \"ping $webHost\"""")
      .execute()
    val container = client.container(name)
    try client.containerNames().contains(container.name) shouldBe true
    finally client.forceRemove(container.name)
    client.containerNames().contains(container.name) shouldBe false
  }

  @Test
  def testCleanup(): Unit = runTest { client =>
    val name = CommonUtils.randomString(5)
    // ping google 3 times
    client
      .containerCreator()
      .name(name)
      .imageName(imageName)
      .removeContainerOnExit()
      .command(s"""/bin/bash -c \"ping $webHost -c 3\"""")
      .execute()
    TimeUnit.SECONDS.sleep(2)
    await(() => client.nonExist(name))
  }

  @Test
  def testNonCleanup(): Unit = runTest { client =>
    val name = CommonUtils.randomString(5)
    // ping google 3 times
    client
      .containerCreator()
      .name(name)
      .imageName(imageName)
      .command(s"""/bin/bash -c \"ping $webHost -c 3\"""")
      .execute()
    try {
      client.containerNames().contains(name) shouldBe true
      TimeUnit.SECONDS.sleep(3)
      client.container(name).state shouldBe ContainerState.EXITED.name
    } finally client.forceRemove(name)
  }

  @Test
  def testVerify(): Unit = runTest(_.verify() shouldBe true)

  @Test
  def testRoute(): Unit = runTest { client =>
    val name = CommonUtils.randomString(5)
    client
      .containerCreator()
      .name(name)
      .route(Map("ABC" -> "192.168.123.123"))
      .imageName(imageName)
      .removeContainerOnExit()
      .command(s"""/bin/bash -c \"ping $webHost\"""")
      .execute()
    try {
      val hostFile = client.containerInspector(name).cat("/etc/hosts").get
      hostFile.contains("192.168.123.123") shouldBe true
      hostFile.contains("ABC") shouldBe true
    } finally client.forceRemove(name)
  }

  @Test
  def testPortMapping(): Unit = runTest { client =>
    val availablePort = CommonUtils.availablePort()
    val name = CommonUtils.randomString(5)
    client
      .containerCreator()
      .name(name)
      .imageName(imageName)
      .portMappings(Map(availablePort -> availablePort))
      .removeContainerOnExit()
      .command(s"""/bin/bash -c \"ping $webHost\"""")
      .execute()
    try {
      val container = client.container(name)
      container.portMappings.size shouldBe 1
      container.portMappings.head.portPairs.size shouldBe 1
      container.portMappings.head.portPairs.head shouldBe PortPair(availablePort, availablePort)
    } finally client.forceRemove(name)
  }

  @Test
  def testSetEnv(): Unit = runTest { client =>
    val name = CommonUtils.randomString(5)
    client
      .containerCreator()
      .name(name)
      .imageName(imageName)
      .envs(Map("abc" -> "123", "ccc" -> "ttt"))
      .removeContainerOnExit()
      .command(s"""/bin/bash -c \"ping $webHost\"""")
      .execute()
    try {
      val container = client.container(name)
      container.environments("abc") shouldBe "123"
      container.environments("ccc") shouldBe "ttt"
    } finally client.forceRemove(name)
  }

  @Test
  def testHostname(): Unit = runTest { client =>
    val name = CommonUtils.randomString(5)
    val hostname = CommonUtils.randomString(5)
    client
      .containerCreator()
      .name(name)
      .imageName(imageName)
      .hostname(hostname)
      .removeContainerOnExit()
      .command(s"""/bin/bash -c \"ping $webHost\"""")
      .execute()
    try client.container(name).hostname shouldBe hostname
    finally client.forceRemove(name)
  }

  @Test
  def testNodeName(): Unit = runTest { client =>
    val name = CommonUtils.randomString(5)
    client
      .containerCreator()
      .name(name)
      .imageName(imageName)
      .removeContainerOnExit()
      .command(s"""/bin/bash -c \"ping $webHost\"""")
      .execute()
    try client.container(name).nodeName shouldBe remoteHostname
    finally client.forceRemove(name)
  }

  @Test
  def testAppend(): Unit = runTest { client =>
    val name = CommonUtils.randomString(5)
    client
      .containerCreator()
      .name(name)
      .imageName(imageName)
      .removeContainerOnExit()
      .command(s"""/bin/bash -c \"ping $webHost\"""")
      .execute()
    try {
      val container = client.container(name)
      client.containerInspector(container.name).append("/tmp/ttt", "abc") shouldBe "abc\n"
      client.containerInspector(container.name).append("/tmp/ttt", "abc") shouldBe "abc\nabc\n"
      client.containerInspector(container.name).append("/tmp/ttt", Seq("t", "z")) shouldBe "abc\nabc\nt\nz\n"
    } finally client.forceRemove(name)
  }

  @Test
  def nullHostname(): Unit = an[NullPointerException] should be thrownBy DockerClient.builder.hostname(null)

  @Test
  def emptyHostname(): Unit = an[IllegalArgumentException] should be thrownBy DockerClient.builder.hostname("")

  @Test
  def negativePort(): Unit = {
    an[IllegalArgumentException] should be thrownBy DockerClient.builder.port(0)
    an[IllegalArgumentException] should be thrownBy DockerClient.builder.port(-1)
  }

  @Test
  def nullUser(): Unit = an[NullPointerException] should be thrownBy DockerClient.builder.user(null)

  @Test
  def emptyUser(): Unit = an[IllegalArgumentException] should be thrownBy DockerClient.builder.user("")

  @Test
  def nullPassword(): Unit = an[NullPointerException] should be thrownBy DockerClient.builder.password(null)

  @Test
  def emptyPassword(): Unit = an[IllegalArgumentException] should be thrownBy DockerClient.builder.password("")

  @After
  def tearDown(): Unit = Releasable.close(client)
}
