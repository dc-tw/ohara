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

package com.island.ohara.connector.ftp

import java.io.{BufferedWriter, OutputStreamWriter}
import java.time.Duration

import com.island.ohara.client.ftp.FtpClient
import com.island.ohara.client.kafka.WorkerClient
import com.island.ohara.common.data.{Cell, Column, DataType, Row}
import com.island.ohara.common.util.{CommonUtils, Releasable}
import com.island.ohara.testing.With3Brokers3Workers
import org.junit.{After, Before, Test}
import org.scalatest.Matchers

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * ftp csv -> topic -> ftp csv
  */
class TestFtp2Ftp extends With3Brokers3Workers with Matchers {
  private[this] val workerClient = WorkerClient(testUtil.workersConnProps)

  private[this] val schema: Seq[Column] = Seq(
    Column.builder().name("name").dataType(DataType.STRING).order(1).build(),
    Column.builder().name("ranking").dataType(DataType.INT).order(2).build(),
    Column.builder().name("single").dataType(DataType.BOOLEAN).order(3).build()
  )
  private[this] val rows: Seq[Row] = Seq(
    Row.of(Cell.of("name", "chia"), Cell.of("ranking", 1), Cell.of("single", false)),
    Row.of(Cell.of("name", "jack"), Cell.of("ranking", 99), Cell.of("single", true))
  )
  private[this] val header: String = rows.head.cells().asScala.map(_.name).mkString(",")
  private[this] val data: Seq[String] = rows.map(row => {
    row.cells().asScala.map(_.value.toString).mkString(",")
  })
  private[this] val ftpClient = FtpClient
    .builder()
    .hostname(testUtil.ftpServer.hostname)
    .port(testUtil.ftpServer.port)
    .user(testUtil.ftpServer.user)
    .password(testUtil.ftpServer.password)
    .build()

  private[this] val sourceProps = FtpSourceProps(
    inputFolder = "/input",
    completedFolder = Some("/backup"),
    errorFolder = "/error",
    user = testUtil.ftpServer.user,
    password = testUtil.ftpServer.password,
    hostname = testUtil.ftpServer.hostname,
    port = testUtil.ftpServer.port,
    encode = "UTF-8"
  )

  private[this] val sinkProps = FtpSinkProps(
    outputFolder = "/output",
    needHeader = true,
    user = testUtil.ftpServer.user,
    password = testUtil.ftpServer.password,
    hostname = testUtil.ftpServer.hostname,
    port = testUtil.ftpServer.port,
    encode = "UTF-8"
  )

  @Before
  def setup(): Unit = {
    TestFtp2Ftp.rebuild(ftpClient, sourceProps.inputFolder)
    TestFtp2Ftp.rebuild(ftpClient, sourceProps.completedFolder.get)
    TestFtp2Ftp.rebuild(ftpClient, sourceProps.errorFolder)
    TestFtp2Ftp.rebuild(ftpClient, sinkProps.outputFolder)
    TestFtp2Ftp.setupInput(ftpClient, sourceProps, header, data)
  }

  @Test
  def testNormalCase(): Unit = {
    val topicName = methodName
    val sinkName = methodName + "-sink"
    val sourceName = methodName + "-source"
    // start sink
    Await.result(
      workerClient
        .connectorCreator()
        .topicName(topicName)
        .connectorClass(classOf[FtpSink])
        .numberOfTasks(1)
        .name(sinkName)
        .columns(schema)
        .settings(sinkProps.toMap)
        .create,
      10 seconds
    )

    try {
      try {
        Await.result(
          workerClient
            .connectorCreator()
            .topicName(topicName)
            .connectorClass(classOf[FtpSource])
            .numberOfTasks(1)
            .name(sourceName)
            .columns(schema)
            .settings(sourceProps.toMap)
            .create,
          10 seconds
        )
        CommonUtils.await(() => ftpClient.listFileNames(sourceProps.inputFolder).isEmpty, Duration.ofSeconds(30))
        CommonUtils
          .await(() => ftpClient.listFileNames(sourceProps.completedFolder.get).size == 1, Duration.ofSeconds(30))
        CommonUtils.await(() => ftpClient.listFileNames(sinkProps.outputFolder).size == 1, Duration.ofSeconds(30))
        val lines =
          ftpClient.readLines(
            com.island.ohara.common.util.CommonUtils
              .path(sinkProps.outputFolder, ftpClient.listFileNames(sinkProps.outputFolder).head))
        lines.length shouldBe rows.length + 1 // header
        lines(0) shouldBe header
        lines(1) shouldBe data.head
        lines(2) shouldBe data(1)
      } finally workerClient.delete(sourceName)
    } finally workerClient.delete(sinkName)
  }

  @After
  def tearDown(): Unit = {
    Releasable.close(ftpClient)
  }
}

object TestFtp2Ftp extends Matchers {

  /**
    * delete all stuffs in the path and then recreate it as a folder
    * @param ftpClient ftp client
    * @param path path on ftp server
    */
  def rebuild(ftpClient: FtpClient, path: String): Unit = {
    if (ftpClient.exist(path)) {
      ftpClient
        .listFileNames(path)
        .map(com.island.ohara.common.util.CommonUtils.path(path, _))
        .foreach(ftpClient.delete)
      ftpClient.listFileNames(path).size shouldBe 0
      ftpClient.delete(path)
    }
    ftpClient.mkdir(path)
  }

  def setupInput(ftpClient: FtpClient, props: FtpSourceProps, header: String, data: Seq[String]): Unit = {
    val writer = new BufferedWriter(
      new OutputStreamWriter(ftpClient.create(com.island.ohara.common.util.CommonUtils.path(props.inputFolder, "abc"))))
    try {
      writer.append(header)
      writer.newLine()
      data.foreach(line => {
        writer.append(line)
        writer.newLine()
      })
    } finally writer.close()
  }
}
