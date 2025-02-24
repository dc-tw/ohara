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

package com.island.ohara.streams;

import com.island.ohara.common.annotations.VisibleForTesting;
import com.island.ohara.common.data.Row;
import com.island.ohara.common.data.Serializer;
import com.island.ohara.common.util.CommonUtils;
import com.island.ohara.kafka.BrokerClient;
import com.island.ohara.kafka.Consumer;
import com.island.ohara.kafka.Producer;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;
import org.junit.Assert;

@VisibleForTesting
class StreamTestUtils {
  private static final Logger log = Logger.getLogger(StreamTestUtils.class);

  static void createTopic(BrokerClient client, String topic, int partitions, short replications) {
    client
        .topicCreator()
        .numberOfPartitions(partitions)
        .numberOfReplications(replications)
        .topicName(topic)
        .create();
  }

  static void setOharaEnv(String brokerProps, String appid, String from, String to) {
    try {
      Map<String, String> map = new HashMap<>();
      map.put("STREAMAPP_SERVERS", brokerProps);
      map.put("STREAMAPP_APPID", appid);
      map.put("STREAMAPP_FROMTOPIC", from);
      map.put("STREAMAPP_TOTOPIC", to);
      setEnv(map);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  static void produceData(Producer<Row, byte[]> producer, List<Row> rows, String topic) {
    rows.forEach(
        row -> {
          try {
            producer.sender().key(row).value(new byte[0]).topicName(topic).send().get();
          } catch (InterruptedException | ExecutionException e) {
            Assert.fail(e.getMessage());
          }
        });
  }

  static void assertResult(
      BrokerClient client, String toTopic, List<Row> expectedContainedRows, int expectedSize) {
    Consumer<Row, byte[]> consumer =
        Consumer.<Row, byte[]>builder()
            .topicName(toTopic)
            .connectionProps(client.connectionProps())
            .groupId("group-" + CommonUtils.randomString(5))
            .offsetFromBegin()
            .keySerializer(Serializer.ROW)
            .valueSerializer(Serializer.BYTES)
            .build();

    List<Consumer.Record<Row, byte[]>> records =
        consumer.poll(Duration.ofSeconds(30), expectedSize);
    records.forEach(
        record -> log.info(String.format("record: %s", record.key().orElse(Row.EMPTY).toString())));
    Assert.assertTrue(
        records.stream()
            .map(
                record -> {
                  Assert.assertTrue(record.key().isPresent());
                  return record.key().get();
                })
            .collect(Collectors.toList())
            .containsAll(expectedContainedRows));

    consumer.close();
  }

  /**
   * A dirty hacks to set environments Reference: https://stackoverflow.com/a/7201825
   *
   * @param newenv the environment variables that will be used
   * @throws Exception exception
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void setEnv(Map<String, String> newenv) throws Exception {
    try {
      Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
      Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
      theEnvironmentField.setAccessible(true);
      Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
      env.putAll(newenv);
      Field theCaseInsensitiveEnvironmentField =
          processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
      theCaseInsensitiveEnvironmentField.setAccessible(true);
      Map<String, String> cienv =
          (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
      cienv.putAll(newenv);
    } catch (NoSuchFieldException e) {
      Class[] classes = Collections.class.getDeclaredClasses();
      Map<String, String> env = System.getenv();
      for (Class cl : classes) {
        if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
          Field field = cl.getDeclaredField("m");
          field.setAccessible(true);
          Object obj = field.get(env);
          Map<String, String> map = (Map<String, String>) obj;
          map.clear();
          map.putAll(newenv);
        }
      }
    }
  }
}
