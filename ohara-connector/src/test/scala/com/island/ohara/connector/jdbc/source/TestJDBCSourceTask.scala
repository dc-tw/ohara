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
import com.island.ohara.common.data.{Column, DataType, Row}
import com.island.ohara.common.rule.MediumTest
import com.island.ohara.connector.jdbc.util.ColumnInfo
import com.island.ohara.kafka.connector.{RowSourceRecord, TaskSetting}
import com.island.ohara.testing.service.Database
import org.apache.kafka.connect.source.SourceTaskContext
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.junit.{Before, Test}
import org.mockito.Mockito._
import org.scalatest.Matchers
import org.scalatest.mockito.MockitoSugar

import scala.collection.JavaConverters._

class TestJDBCSourceTask extends MediumTest with Matchers with MockitoSugar {
  private[this] val db = Database.local()
  private[this] val client = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName = "TABLE1"
  private[this] val timestampColumnName = "COLUMN1"

  @Before
  def setup(): Unit = {
    val column1 = RdbColumn("COLUMN1", "TIMESTAMP", true)
    val column2 = RdbColumn("COLUMN2", "varchar(45)", false)
    val column3 = RdbColumn("COLUMN3", "VARCHAR(45)", false)
    val column4 = RdbColumn("COLUMN4", "integer", false)

    client.createTable(tableName, Seq(column1, column2, column3, column4))
    val statement: Statement = db.connection.createStatement()

    statement.executeUpdate(
      s"INSERT INTO $tableName(COLUMN1,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:00', 'a11', 'a12', 1)")
    statement.executeUpdate(
      s"INSERT INTO $tableName(COLUMN1,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:01', 'a21', 'a22', 2)")
    statement.executeUpdate(
      s"INSERT INTO $tableName(COLUMN1,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:02', 'a31', 'a32', 3)")
    statement.executeUpdate(
      s"INSERT INTO $tableName(COLUMN1,COLUMN2,COLUMN3,COLUMN4) VALUES(NOW() + INTERVAL 3 MINUTE, 'a41', 'a42', 4)")
    statement.executeUpdate(
      s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES(NOW() + INTERVAL 1 DAY, 'a51', 'a52', 5)")
  }

  @Test
  def testPoll(): Unit = {
    val jdbcSourceTask: JDBCSourceTask = new JDBCSourceTask()
    val taskContext: SourceTaskContext = mock[SourceTaskContext]
    val offsetStorageReader: OffsetStorageReader = mock[OffsetStorageReader]
    when(taskContext.offsetStorageReader()).thenReturn(offsetStorageReader)
    jdbcSourceTask.initialize(taskContext.asInstanceOf[SourceTaskContext])

    val taskSetting: TaskSetting = mock[TaskSetting]
    when(taskSetting.stringValue(DB_URL)).thenReturn(db.url)
    when(taskSetting.stringValue(DB_USERNAME)).thenReturn(db.user)
    when(taskSetting.stringValue(DB_PASSWORD)).thenReturn(db.password)
    when(taskSetting.stringValue(DB_TABLENAME)).thenReturn(tableName)
    when(taskSetting.stringOption(DB_SCHEMA_PATTERN)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringOption(DB_CATALOG_PATTERN)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringOption(MODE)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringValue(TIMESTAMP_COLUMN_NAME)).thenReturn(timestampColumnName)

    val columns: Seq[Column] = Seq(
      Column.builder().name("COLUMN1").dataType(DataType.OBJECT).order(0).build(),
      Column.builder().name("COLUMN2").dataType(DataType.STRING).order(1).build(),
      Column.builder().name("COLUMN4").dataType(DataType.INT).order(3).build()
    )

    when(taskSetting.columns).thenReturn(columns.asJava)
    when(taskSetting.topicNames()).thenReturn(Seq("topic1").asJava)
    jdbcSourceTask._start(taskSetting)

    val rows: Seq[RowSourceRecord] = jdbcSourceTask._poll().asScala
    rows.head.row.cell(0).value.toString shouldBe "2018-09-01 00:00:00.0"
    rows.head.row.cell(1).value shouldBe "a11"
    rows.head.row.cell(2).value shouldBe 1

    rows.head.row.cell(0).name shouldBe "COLUMN1"
    rows(1).row.cell(1).name shouldBe "COLUMN2"
    rows(2).row.cell(2).name shouldBe "COLUMN4"

    //Test row 1 offset
    rows.head.sourceOffset.asScala.foreach(x => {
      x._1 shouldBe JDBCSourceTask.DB_TABLE_OFFSET_KEY
      x._2 shouldBe 1535731200000L
    })
    //Test row 2 offset
    rows(1).sourceOffset.asScala.foreach(x => {
      x._1 shouldBe JDBCSourceTask.DB_TABLE_OFFSET_KEY
      x._2 shouldBe 1535731201000L
    })
  }

  @Test
  def testRowTimestamp(): Unit = {
    val jdbcSourceTask: JDBCSourceTask = new JDBCSourceTask()
    val schema: Seq[Column] = Seq(Column.builder().name("COLUMN1").dataType(DataType.OBJECT).order(0).build())
    val columnInfo: Seq[ColumnInfo[Timestamp]] = Seq(ColumnInfo("COLUMN1", "timestamp", new Timestamp(0)))
    val row0: Row = jdbcSourceTask.row(schema, columnInfo)
    row0.cell("COLUMN1").value.toString shouldBe "1970-01-01 08:00:00.0"
  }

  @Test
  def testRowInt(): Unit = {
    val jdbcSourceTask: JDBCSourceTask = new JDBCSourceTask()
    val schema: Seq[Column] = Seq(Column.builder().name("COLUMN1").dataType(DataType.INT).order(0).build())
    val columnInfo: Seq[ColumnInfo[Int]] = Seq(ColumnInfo("COLUMN1", "int", new Integer(100)))
    val row0: Row = jdbcSourceTask.row(schema, columnInfo)
    row0.cell("COLUMN1").value shouldBe 100
  }

  @Test
  def testCellOrder(): Unit = {
    val jdbcSourceTask: JDBCSourceTask = new JDBCSourceTask()
    val schema: Seq[Column] = Seq(Column.builder().name("c1").dataType(DataType.INT).order(1).build(),
                                  Column.builder().name("c0").dataType(DataType.INT).order(0).build())
    val columnInfo: Seq[ColumnInfo[Int]] =
      Seq(ColumnInfo("c1", "int", new Integer(100)), ColumnInfo("c0", "int", new Integer(50)))
    val cells = jdbcSourceTask.row(schema, columnInfo).cells().asScala
    cells.head.name shouldBe "c0"
    cells.head.value shouldBe 50
    cells(1).name shouldBe "c1"
    cells(1).value shouldBe 100
  }

  @Test
  def testRowNewName(): Unit = {
    val jdbcSourceTask: JDBCSourceTask = new JDBCSourceTask()
    val schema: Seq[Column] = Seq(
      Column.builder().name("COLUMN1").newName("COLUMN100").dataType(DataType.INT).order(0).build())
    val columnInfo: Seq[ColumnInfo[Int]] = Seq(ColumnInfo("COLUMN1", "int", new Integer(100)))
    val row0: Row = jdbcSourceTask.row(schema, columnInfo)
    row0.cell("COLUMN100").value shouldBe 100
  }

  @Test
  def testPollNewName(): Unit = {
    val jdbcSourceTask: JDBCSourceTask = new JDBCSourceTask()
    val taskContext: SourceTaskContext = mock[SourceTaskContext]
    val offsetStorageReader: OffsetStorageReader = mock[OffsetStorageReader]
    when(taskContext.offsetStorageReader()).thenReturn(offsetStorageReader)
    jdbcSourceTask.initialize(taskContext.asInstanceOf[SourceTaskContext])

    val taskSetting: TaskSetting = mock[TaskSetting]
    when(taskSetting.stringValue(DB_URL)).thenReturn(db.url)
    when(taskSetting.stringValue(DB_USERNAME)).thenReturn(db.user)
    when(taskSetting.stringValue(DB_PASSWORD)).thenReturn(db.password)
    when(taskSetting.stringValue(DB_TABLENAME)).thenReturn(tableName)
    when(taskSetting.stringOption(DB_SCHEMA_PATTERN)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringOption(DB_CATALOG_PATTERN)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringOption(MODE)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringValue(TIMESTAMP_COLUMN_NAME)).thenReturn(timestampColumnName)

    val columns: Seq[Column] = Seq(
      Column.builder().name("COLUMN1").newName("COLUMN100").dataType(DataType.OBJECT).order(0).build(),
      Column.builder().name("COLUMN2").newName("COLUMN200").dataType(DataType.STRING).order(1).build(),
      Column.builder().name("COLUMN4").newName("COLUMN400").dataType(DataType.INT).order(3).build()
    )

    when(taskSetting.columns).thenReturn(columns.asJava)
    when(taskSetting.topicNames()).thenReturn(Seq("topic1").asJava)

    jdbcSourceTask._start(taskSetting)

    val rows: Seq[RowSourceRecord] = jdbcSourceTask._poll().asScala
    rows.head.row.cell(0).value.toString shouldBe "2018-09-01 00:00:00.0"
    rows.head.row.cell(1).value shouldBe "a11"
    rows.head.row.cell(2).value shouldBe 1

    rows.head.row.cell(0).name shouldBe "COLUMN100"
    rows(1).row.cell(1).name shouldBe "COLUMN200"
    rows(2).row.cell(2).name shouldBe "COLUMN400"
  }

  @Test
  def testDbTimestampColumnValue(): Unit = {
    val jdbcSourceTask: JDBCSourceTask = new JDBCSourceTask()
    val dbColumnInfo: Seq[ColumnInfo[_]] = Seq(ColumnInfo("column1", "string", "value1"),
                                               ColumnInfo("column2", "timestamp", new Timestamp(1537510900000L)),
                                               ColumnInfo("column3", "string", "value3"))
    val timestamp: Long = jdbcSourceTask.dbTimestampColumnValue(dbColumnInfo, "column2")
    timestamp shouldBe 1537510900000L
  }
}
