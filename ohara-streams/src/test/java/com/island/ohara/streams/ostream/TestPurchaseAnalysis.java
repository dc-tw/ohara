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

package com.island.ohara.streams.ostream;

import com.island.ohara.common.data.Cell;
import com.island.ohara.common.data.Pair;
import com.island.ohara.common.data.Row;
import com.island.ohara.common.data.Serializer;
import com.island.ohara.common.util.CommonUtils;
import com.island.ohara.kafka.BrokerClient;
import com.island.ohara.kafka.Consumer;
import com.island.ohara.kafka.Producer;
import com.island.ohara.streams.OStream;
import com.island.ohara.streams.StreamApp;
import com.island.ohara.testing.With3Brokers;
import java.lang.reflect.Field;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"rawtypes"})
public class TestPurchaseAnalysis extends With3Brokers {
  private static final Logger LOG = LoggerFactory.getLogger(TestPurchaseAnalysis.class);
  private static final String appid = "test-purchase-analysis";
  private static final String resultTopic = "gender-amount";
  private static final String itemTopic = "items";
  private static final String orderTopic = "orders";
  private static final String userTopic = "users";
  private final BrokerClient client = BrokerClient.of(testUtil().brokersConnProps());
  private final Producer<Row, byte[]> producer =
      Producer.<Row, byte[]>builder()
          .connectionProps(client.connectionProps())
          .keySerializer(Serializer.ROW)
          .valueSerializer(Serializer.BYTES)
          .build();

  @Before
  public void setup() {
    int partitions = 3;
    short replications = 1;
    try {
      client
          .topicCreator()
          .numberOfPartitions(partitions)
          .numberOfReplications(replications)
          .topicName(orderTopic)
          .create();
      client
          .topicCreator()
          .numberOfPartitions(partitions)
          .numberOfReplications(replications)
          .topicName(itemTopic)
          .create();
      client
          .topicCreator()
          .numberOfPartitions(partitions)
          .numberOfReplications(replications)
          .topicName(userTopic)
          .create();
      client
          .topicCreator()
          .numberOfPartitions(partitions)
          .numberOfReplications(replications)
          .topicName(resultTopic)
          .create();
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }

  @Test
  public void testStreamApp() throws InterruptedException {
    // write items.csv to kafka broker
    produceData("items.csv", itemTopic);

    // write users.csv to kafka broker
    produceData("users.csv", userTopic);

    // we make sure the join topic has data already
    assertResult(client, itemTopic, 4);
    assertResult(client, userTopic, 4);
    TimeUnit.SECONDS.sleep(1);
    // write orders.csv to kafka broker
    produceData("orders.csv", orderTopic);
    assertResult(client, orderTopic, 16);

    RunStreamApp app = new RunStreamApp(client.connectionProps());
    StreamApp.runStreamApp(app.getClass(), client.connectionProps());
  }

  @After
  public void cleanUp() {
    producer.close();
    client.close();
  }

  /** StreamApp Main Entry */
  public static class RunStreamApp extends StreamApp {

    final String brokers;

    public RunStreamApp(String brokers) {
      this.brokers = brokers;
    }

    @Override
    public void start() {
      OStream<Row> ostream =
          OStream.builder()
              .appid(appid)
              .bootstrapServers(brokers)
              .fromTopicWith(orderTopic, Serdes.ROW, Serdes.BYTES)
              .toTopicWith(resultTopic, Serdes.ROW, Serdes.BYTES)
              .cleanStart()
              .timestampExtractor(MyExtractor.class)
              .enableExactlyOnce()
              .build();

      ostream
          .leftJoin(
              userTopic,
              Conditions.add(Collections.singletonList(Pair.of("userName", "name"))),
              (row1, row2) ->
                  Row.of(
                      row1.cell("userName"),
                      row1.cell("itemName"),
                      row1.cell("quantity"),
                      row2 == null ? Cell.of("address", "") : row2.cell("address"),
                      row2 == null ? Cell.of("gender", "") : row2.cell("gender")))
          .filter(row -> row.cell("address").value() != null)
          .leftJoin(
              itemTopic,
              Conditions.add(Collections.singletonList(Pair.of("itemName", "itemName"))),
              (row1, row2) ->
                  Row.of(
                      row1.cell("userName"),
                      row1.cell("itemName"),
                      row1.cell("quantity"),
                      Cell.of("useraddress", row1.cell("address").value()),
                      row1.cell("gender"),
                      row2 == null
                          ? Cell.of("itemaddress", "")
                          : Cell.of("itemaddress", row2.cell("address").value()),
                      row2 == null ? Cell.of("type", "") : row2.cell("type"),
                      row2 == null ? Cell.of("price", "") : row2.cell("price")))
          .filter(
              row ->
                  row.cell("useraddress")
                      .value()
                      .toString()
                      .equals(row.cell("itemaddress").value().toString()))
          .map(
              row ->
                  Row.of(
                      row.cell("gender"),
                      Cell.of(
                          "amount",
                          Double.valueOf(row.cell("quantity").value().toString())
                              * Double.valueOf(row.cell("price").value().toString()))))
          .groupByKey(Collections.singletonList("gender"))
          .reduce((Double r1, Double r2) -> r1 + r2, "amount")
          .start();

      Consumer<Row, byte[]> consumer =
          Consumer.<Row, byte[]>builder()
              .topicName(resultTopic)
              .connectionProps(brokers)
              .groupId("group-" + resultTopic)
              .offsetFromBegin()
              .keySerializer(Serializer.ROW)
              .valueSerializer(Serializer.BYTES)
              .build();

      List<Consumer.Record<Row, byte[]>> records = consumer.poll(Duration.ofSeconds(30), 4);
      records.forEach(
          row ->
              LOG.debug(
                  "final result : " + (row.key().isPresent() ? row.key().get().toString() : null)));
      Assert.assertEquals(
          "the result will get \"accumulation\" ; hence we will get 4 records.", 4, records.size());

      Map<String, Double[]> actualResultMap = new HashMap<>();
      actualResultMap.put("male", new Double[] {9000D, 60000D, 69000D});
      actualResultMap.put("female", new Double[] {15000D, 30000D, 45000D});
      final double THRESHOLD = 0.0001;

      records.forEach(
          record -> {
            if (record.key().isPresent()) {
              Optional<Double> amount =
                  record.key().get().cells().stream()
                      .filter(cell -> cell.name().equals("amount"))
                      .map(cell -> Double.valueOf(cell.value().toString()))
                      .findFirst();
              Assert.assertTrue(
                  "the result should be contain in actualResultMap",
                  actualResultMap.containsKey(record.key().get().cell("gender").value().toString())
                      && actualResultMap.values().stream()
                          .flatMap(Arrays::stream)
                          .anyMatch(d -> Math.abs(d - amount.orElse(-999.0)) < THRESHOLD));
            }
          });

      consumer.close();
      ostream.stop();
    }
  }

  public static class MyExtractor implements TimestampExtractor {

    @Override
    public long extract(
        org.apache.kafka.clients.consumer.ConsumerRecord<Object, Object> record,
        long previousTimestamp) {
      LOG.debug(
          String.format(
              "timeExtract : topic[%s], value[%s], partition[%s], time[%s]",
              record.topic(), record.key().toString(), record.partition(), record.timestamp()));
      Object value = record.key();
      if (value instanceof Row) {
        Row row = (Row) value;
        // orders
        if (row.names().contains("transactionDate"))
          return LocalDateTime.of(2019, 2, 2, 2, 2, 2).toEpochSecond(ZoneOffset.UTC) * 1000;
        // items
        else if (row.names().contains("price"))
          return LocalDateTime.of(2019, 1, 1, 1, 1, 1).toEpochSecond(ZoneOffset.UTC) * 1000;
        // users
        else if (row.names().contains("gender"))
          return LocalDateTime.of(2019, 1, 1, 1, 1, 1).toEpochSecond(ZoneOffset.UTC) * 1000;
        // other
        else
          throw new RuntimeException(
              "the headers of this row are not expected :" + String.join(",", row.names()));
      } else {
        throw new RuntimeException("who are you? :" + value.getClass().getName());
      }
    }
  }

  private void produceData(String filename, String topicName) {
    try {
      List<?> dataList = DataUtils.readData(filename);
      dataList.stream()
          .map(
              object -> {
                try {
                  List<Cell> cells = new ArrayList<>();
                  LOG.debug("Class Name : " + object.getClass().getName());
                  for (Field f : object.getClass().getDeclaredFields()) {
                    f.setAccessible(true);
                    Cell cell = Cell.of(f.getName(), f.get(object));
                    cells.add(cell);
                    LOG.debug("--" + f.getName() + ":" + f.get(object));
                  }
                  return new AbstractMap.SimpleEntry<>(
                      Row.of(cells.toArray(new Cell[0])), new byte[0]);
                } catch (Exception e) {
                  LOG.debug(e.getMessage());
                  return new AbstractMap.SimpleEntry<>(Row.EMPTY, new byte[0]);
                }
              })
          .forEach(
              entry ->
                  producer
                      .sender()
                      .key(entry.getKey())
                      .value(entry.getValue())
                      .topicName(topicName)
                      .send());
    } catch (Exception e) {
      LOG.debug(e.getMessage());
    }
  }

  private void assertResult(BrokerClient client, String topic, int expectedSize) {
    Consumer<Row, byte[]> consumer =
        Consumer.<Row, byte[]>builder()
            .topicName(topic)
            .connectionProps(client.connectionProps())
            .groupId("group-" + CommonUtils.randomString(5))
            .offsetFromBegin()
            .keySerializer(Serializer.ROW)
            .valueSerializer(Serializer.BYTES)
            .build();

    List<Consumer.Record<Row, byte[]>> records =
        consumer.poll(Duration.ofSeconds(30), expectedSize);
    Assert.assertEquals(expectedSize, records.size());
    consumer.close();
  }
}
