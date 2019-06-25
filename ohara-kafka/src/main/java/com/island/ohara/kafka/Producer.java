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

import com.island.ohara.common.annotations.Optional;
import com.island.ohara.common.annotations.VisibleForTesting;
import com.island.ohara.common.data.Serializer;
import com.island.ohara.common.util.CommonUtils;
import com.island.ohara.common.util.Releasable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * a simple wrap from kafka producer.
 *
 * @param <Key> key type
 * @param <Value> value type
 */
public interface Producer<Key, Value> extends Releasable {

  /**
   * create a sender used to send a record to brokers
   *
   * @return a sender
   */
  Sender<Key, Value> sender();

  /** flush all on-the-flight data. */
  void flush();

  static <Key, Value> Builder<Key, Value> builder() {
    return new Builder<>();
  }

  class Builder<Key, Value> implements com.island.ohara.common.Builder<Producer<Key, Value>> {
    private Map<String, String> options = Collections.emptyMap();
    private String connectionProps;
    // default noAcks
    private short numberOfAcks = 1;
    private Serializer<Key> keySerializer = null;
    private Serializer<Value> valueSerializer = null;

    private Builder() {
      // no nothing
    }

    @Optional("default is empty")
    public Builder<Key, Value> options(Map<String, String> options) {
      this.options = CommonUtils.requireNonEmpty(options);
      return this;
    }

    public Builder<Key, Value> connectionProps(String connectionProps) {
      this.connectionProps = CommonUtils.requireNonEmpty(connectionProps);
      return this;
    }

    @com.island.ohara.common.annotations.Optional("default is no ack")
    public Builder<Key, Value> noAcks() {
      this.numberOfAcks = 0;
      return this;
    }

    @com.island.ohara.common.annotations.Optional("default is no ack")
    public Builder<Key, Value> allAcks() {
      this.numberOfAcks = -1;
      return this;
    }

    public Builder<Key, Value> keySerializer(Serializer<Key> keySerializer) {
      this.keySerializer = Objects.requireNonNull(keySerializer);
      return this;
    }

    public Builder<Key, Value> valueSerializer(Serializer<Value> valueSerializer) {
      this.valueSerializer = Objects.requireNonNull(valueSerializer);
      return this;
    }

    /**
     * Used to convert ohara row to byte array. It is a private class since ohara producer will
     * instantiate one and pass it to kafka producer. Hence, no dynamical call will happen in kafka
     * producer. The access exception won't be caused.
     *
     * @param serializer ohara serializer
     * @param <T> object type
     * @return a wrapper from kafka serializer
     */
    private static <T> org.apache.kafka.common.serialization.Serializer<T> wrap(
        Serializer<T> serializer) {
      return new org.apache.kafka.common.serialization.Serializer<T>() {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
          // do nothing
        }

        @Override
        public byte[] serialize(String topic, T data) {
          return data == null ? null : serializer.to(data);
        }

        @Override
        public void close() {
          // do nothing
        }
      };
    }

    private void checkArguments() {
      CommonUtils.requireNonEmpty(connectionProps);
      Objects.requireNonNull(keySerializer);
      Objects.requireNonNull(valueSerializer);
    }

    @Override
    public Producer<Key, Value> build() {
      checkArguments();
      return new Producer<Key, Value>() {

        private Properties getProducerConfig() {
          Properties props = new Properties();
          options.forEach(props::setProperty);
          props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, connectionProps);
          props.setProperty(ProducerConfig.ACKS_CONFIG, String.valueOf(numberOfAcks));
          return props;
        }

        private final KafkaProducer<Key, Value> producer =
            new KafkaProducer<>(getProducerConfig(), wrap(keySerializer), wrap(valueSerializer));

        @Override
        public final Sender<Key, Value> sender() {
          return new Sender<Key, Value>() {
            @Override
            public Future<RecordMetadata> doSend() {
              CompletableFuture<RecordMetadata> completableFuture = new CompletableFuture<>();
              ProducerRecord<Key, Value> record =
                  new ProducerRecord<>(
                      topicName,
                      partition,
                      timestamp,
                      key,
                      value,
                      headers.stream()
                          .map(Builder.this::toKafkaHeader)
                          .collect(Collectors.toList()));

              producer.send(
                  record,
                  (metadata, exception) -> {
                    if (metadata == null && exception == null)
                      completableFuture.completeExceptionally(
                          new IllegalStateException(
                              "no meta and exception from kafka producer...It should be impossible"));
                    if (metadata != null && exception != null)
                      completableFuture.completeExceptionally(
                          new IllegalStateException(
                              "Both meta and exception from kafka producer...It should be impossible"));
                    if (metadata != null)
                      completableFuture.complete(
                          new RecordMetadata(
                              metadata.topic(),
                              metadata.partition(),
                              metadata.offset(),
                              metadata.timestamp(),
                              metadata.serializedKeySize(),
                              metadata.serializedValueSize()));
                    if (exception != null) completableFuture.completeExceptionally(exception);
                  });
              return completableFuture;
            }
          };
        }

        @Override
        public void flush() {
          producer.flush();
        }

        @Override
        public void close() {
          producer.close();
        }
      };
    }

    private org.apache.kafka.common.header.Header toKafkaHeader(Header header) {
      return new org.apache.kafka.common.header.Header() {
        @Override
        public String key() {
          return header.key();
        }

        @Override
        public byte[] value() {
          return header.value();
        }
      };
    }
  }

  /**
   * a fluent-style sender. kafak.ProducerRecord has many fields and most from them are nullable. It
   * makes kafak.ProducerRecord's constructor complicated. This class has fluent-style methods
   * helping user to fill the fields they have.
   */
  abstract class Sender<Key, Value> {
    protected Integer partition = null;
    protected List<Header> headers = Collections.emptyList();
    protected Key key = null;
    protected Value value = null;
    protected Long timestamp = null;
    protected String topicName = null;

    @VisibleForTesting
    Sender() {
      // do nothing
    }

    @Optional("default is hash of key")
    public Sender<Key, Value> partition(int partition) {
      this.partition = partition;
      return this;
    }

    @Optional("default is empty")
    public Sender<Key, Value> header(Header header) {
      return headers(Collections.singletonList(Objects.requireNonNull(header)));
    }

    @Optional("default is empty")
    public Sender<Key, Value> headers(List<Header> headers) {
      this.headers = CommonUtils.requireNonEmpty(headers);
      return this;
    }

    @Optional("default is null")
    public Sender<Key, Value> key(Key key) {
      this.key = Objects.requireNonNull(key);
      return this;
    }

    @Optional("default is null")
    public Sender<Key, Value> value(Value value) {
      this.value = Objects.requireNonNull(value);
      return this;
    }

    @Optional("default is null")
    public Sender<Key, Value> timestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public Sender<Key, Value> topicName(String topicName) {
      this.topicName = CommonUtils.requireNonEmpty(topicName);
      return this;
    }

    public Sender<Key, Value> handler(String topicName) {
      this.topicName = CommonUtils.requireNonEmpty(topicName);
      return this;
    }

    private void checkArguments() {
      Objects.requireNonNull(headers);
      Objects.requireNonNull(topicName);
    }

    /**
     * start to send the data in background. Noted: you should check the returned future to handle
     * the exception or result
     *
     * @return an async thread processing the request
     */
    public Future<RecordMetadata> send() {
      checkArguments();
      return doSend();
    }

    protected abstract Future<RecordMetadata> doSend();
  }

  /** wrap from kafka RecordMetadata; */
  class RecordMetadata {
    private final String topicName;
    private final int partition;
    private final long offset;
    private final long timestamp;
    private final int serializedKeySize;
    private final int serializedValueSize;

    private RecordMetadata(
        String topicName,
        int partition,
        long offset,
        long timestamp,
        int serializedKeySize,
        int serializedValueSize) {
      this.topicName = topicName;
      this.partition = partition;
      this.offset = offset;
      this.timestamp = timestamp;
      this.serializedKeySize = serializedKeySize;
      this.serializedValueSize = serializedValueSize;
    }

    public String topicName() {
      return topicName;
    }

    public int partition() {
      return partition;
    }

    public long offset() {
      return offset;
    }

    public long timestamp() {
      return timestamp;
    }

    public int serializedKeySize() {
      return serializedKeySize;
    }

    public int serializedValueSize() {
      return serializedValueSize;
    }
  }
}
