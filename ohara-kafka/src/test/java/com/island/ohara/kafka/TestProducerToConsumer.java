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

package com.island.ohara.kafka;

import com.island.ohara.common.data.Serializer;
import com.island.ohara.common.util.CommonUtils;
import com.island.ohara.testing.WithBroker;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestProducerToConsumer extends WithBroker {

  private final String topicName = CommonUtils.randomString();

  @Before
  public void setup() {
    try (BrokerClient client = BrokerClient.of(testUtil().brokersConnProps())) {
      client
          .topicCreator()
          .numberOfPartitions(1)
          .numberOfReplications((short) 1)
          .topicName(topicName)
          .create();
    }
  }

  @Test
  public void testTimestamp() {
    long timestamp = CommonUtils.current();
    try (Producer<String, String> producer =
        Producer.<String, String>builder()
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .connectionProps(testUtil().brokersConnProps())
            .build()) {
      producer.sender().key("a").value("b").topicName(topicName).timestamp(timestamp).send();
    }
    try (Consumer<String, String> consumer =
        Consumer.<String, String>builder()
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .offsetFromBegin()
            .topicName(topicName)
            .connectionProps(testUtil().brokersConnProps())
            .build()) {
      List<Consumer.Record<String, String>> records = consumer.poll(Duration.ofSeconds(30), 1);
      Assert.assertEquals(1, records.size());
      Assert.assertEquals(timestamp, records.get(0).timestamp());
      Assert.assertEquals(TimestampType.CREATE_TIME, records.get(0).timestampType());
    }
  }

  @Test
  public void normalCase() throws ExecutionException, InterruptedException {
    try (Producer<String, String> producer =
        Producer.<String, String>builder()
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .connectionProps(testUtil().brokersConnProps())
            .build()) {
      Producer.RecordMetadata metadata =
          producer.sender().key("a").value("b").topicName(topicName).send().get();
      Assert.assertEquals(metadata.topicName(), topicName);
      try (Consumer<String, String> consumer =
          Consumer.<String, String>builder()
              .keySerializer(Serializer.STRING)
              .valueSerializer(Serializer.STRING)
              .offsetFromBegin()
              .topicName(topicName)
              .connectionProps(testUtil().brokersConnProps())
              .build()) {
        List<Consumer.Record<String, String>> records = consumer.poll(Duration.ofSeconds(30), 1);
        Assert.assertEquals(1, records.size());
        Assert.assertEquals("a", records.get(0).key().get());
        Assert.assertEquals("b", records.get(0).value().get());
      }
    }
  }

  @Test
  public void withIdleTime() throws ExecutionException, InterruptedException {
    long timeout = 5000;
    try (Producer<String, String> producer =
        Producer.<String, String>builder()
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .connectionProps(testUtil().brokersConnProps())
            .options(
                Collections.singletonMap(
                    CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, String.valueOf(timeout)))
            .build()) {
      Assert.assertEquals(
          producer.sender().key("a").value("b").topicName(topicName).send().get().topicName(),
          topicName);
      try (Consumer<String, String> consumer =
          Consumer.<String, String>builder()
              .keySerializer(Serializer.STRING)
              .valueSerializer(Serializer.STRING)
              .offsetFromBegin()
              .topicName(topicName)
              .connectionProps(testUtil().brokersConnProps())
              .options(
                  Collections.singletonMap(
                      CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, String.valueOf(timeout)))
              .build()) {
        List<Consumer.Record<String, String>> records = consumer.poll(Duration.ofSeconds(30), 1);
        Assert.assertEquals(1, records.size());
        Assert.assertEquals("a", records.get(0).key().get());
        Assert.assertEquals("b", records.get(0).value().get());

        TimeUnit.MILLISECONDS.sleep(timeout * 2);
        Assert.assertEquals(
            producer.sender().key("c").value("d").topicName(topicName).send().get().topicName(),
            topicName);
        List<Consumer.Record<String, String>> records2 = consumer.poll(Duration.ofSeconds(30), 1);
        Assert.assertEquals(1, records2.size());
        Assert.assertEquals("c", records2.get(0).key().get());
        Assert.assertEquals("d", records2.get(0).value().get());
      }
    }
  }

  @After
  public void tearDown() {
    try (BrokerClient client = BrokerClient.of(testUtil().brokersConnProps())) {
      client.deleteTopic(topicName);
    }
  }
}
