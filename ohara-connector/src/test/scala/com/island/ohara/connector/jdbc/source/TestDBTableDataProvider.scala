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

package com.island.ohara.connector.jdbc.source

import java.sql.{Statement, Timestamp}

import com.island.ohara.client.configurator.v0.QueryApi.RdbColumn
import com.island.ohara.client.database.DatabaseClient
import com.island.ohara.common.rule.MediumTest
import com.island.ohara.common.util.{CommonUtils, Releasable}
import com.island.ohara.connector.jdbc.util.{ColumnInfo, DateTimeUtils}
import com.island.ohara.testing.service.Database
import org.junit.{After, Before, Test}
import org.scalatest.Matchers

import scala.collection.mutable.ListBuffer

class TestDBTableDataProvider extends MediumTest with Matchers {

  private[this] val db = Database.local()
  private[this] val client = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName = "table1"

  @Before
  def setup(): Unit = {
    val column1 = RdbColumn("column1", "TIMESTAMP", true)
    val column2 = RdbColumn("column2", "varchar(45)", false)
    val column3 = RdbColumn("column3", "VARCHAR(45)", false)
    val column4 = RdbColumn("column4", "integer", false)

    client.createTable(tableName, Seq(column1, column2, column3, column4))
    val statement: Statement = db.connection.createStatement()

    statement.executeUpdate(
      s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES('2018-09-01 00:00:00', 'a11', 'a12', 1)")
    statement.executeUpdate(
      s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES('2018-09-01 00:00:01', 'a21', 'a22', 2)")
    statement.executeUpdate(
      s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES('2018-09-01 00:00:02', 'a31', 'a32', 3)")
    statement.executeUpdate(
      s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES(NOW() + INTERVAL 3 MINUTE, 'a41', 'a42', 4)")
    statement.executeUpdate(
      s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES(NOW() + INTERVAL 1 DAY, 'a51', 'a52', 5)")
  }

  @Test
  def testRowListResultSet(): Unit = {
    val dbTableDataProvider = new DBTableDataProvider(db.url, db.user, db.password)
    val results: QueryResultIterator = dbTableDataProvider.executeQuery(tableName, "column1", new Timestamp(0)) //0 is 1970-01-01 00:00:00

    var count = 0
    val resultList: ListBuffer[Seq[ColumnInfo[_]]] = new ListBuffer[Seq[ColumnInfo[_]]]
    while (results.hasNext) {
      val listBuffer: Seq[ColumnInfo[_]] = results.next()
      resultList += listBuffer
      count = count + 1
    }
    count shouldBe 3
    resultList.head(3).columnName shouldBe "column4"
    resultList.head(3).columnType shouldBe "INT"
    resultList.head(3).value shouldBe 1
  }

  @Test
  def testDbCurrentTime(): Unit = {
    val dbTableDataProvider = new DBTableDataProvider(db.url, db.user, db.password)
    val dbCurrentTime = dbTableDataProvider.dbCurrentTime(DateTimeUtils.CALENDAR)
    val dbCurrentTimestamp = dbCurrentTime.getTime
    val systemCurrentTimestamp = CommonUtils.current()
    ((systemCurrentTimestamp - dbCurrentTimestamp) < 5000) shouldBe true
  }

  @Test
  def testColumnList(): Unit = {
    val dbTableDataProvider = new DBTableDataProvider(db.url, db.user, db.password)
    val columns: Seq[RdbColumn] = dbTableDataProvider.columns(tableName)
    columns.head.name shouldBe "column1"
    columns(1).name shouldBe "column2"
    columns(2).name shouldBe "column3"
    columns(3).name shouldBe "column4"
  }

  @Test
  def testTableISNotExists(): Unit = {
    val dbTableDataProvider = new DBTableDataProvider(db.url, db.user, db.password)
    dbTableDataProvider.isTableExists("table100") shouldBe false
  }

  @Test
  def testColumnHaveTable(): Unit = {
    val dbTableDataProvider = new DBTableDataProvider(db.url, db.user, db.password)
    dbTableDataProvider.isTableExists(tableName) shouldBe true
  }
  @After
  def tearDown(): Unit = {
    Releasable.close(client)
    Releasable.close(db)
  }
}
