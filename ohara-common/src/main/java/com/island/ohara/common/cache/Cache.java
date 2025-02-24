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

package com.island.ohara.common.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.island.ohara.common.annotations.Optional;
import com.island.ohara.common.util.CommonUtils;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * A wrap of google guava.Caching. Guava offers a powerful local cache, which is good to ohara to
 * speed up some slow data access. However, using guava cache in whole ohara is overkill so we wrap
 * it to offer a more simple version to other modules. In this wrap, we offer two kind of behavior
 * of getting data from cache. The first is **blockingOnGet** which will block all get when the
 * associated key-value is timeout. Another is non-blocking on get which only blocks the first call
 * when the associated key-value is timeout, and the other call will get out-of-date value.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface Cache<K, V> {

  /**
   * return the value associated to the input key. The loading will happen if there is no value.
   * Noted that the call will be blocked when the value is timeout if you enable the {@link
   * Builder#blockingOnGet}
   *
   * @param key key
   * @return value
   */
  V get(K key);

  /**
   * snapshot all cached key-value pairs
   *
   * @return a unmodified map
   */
  Map<K, V> snapshot();

  /**
   * update the key-value stored in this cache. the previous value will be replaced.
   *
   * @param key key
   * @param value new value
   */
  default void put(K key, V value) {
    put(Collections.singletonMap(key, value));
  }

  /**
   * update the key-values stored in this cache. the previous values will be replaced.
   *
   * @param map keys-newValues
   */
  void put(Map<? extends K, ? extends V> map);

  /** @return the approximate number of this cache. */
  long size();

  /** Remove all entries in this cache. */
  void clear();

  static <K, V> Builder<K, V> builder() {
    return new Builder<>();
  }

  class Builder<K, V> implements com.island.ohara.common.Builder<Cache<K, V>> {
    private int maxSize = 1000;
    private Duration timeout = Duration.ofSeconds(5);
    private boolean blockingOnGet = false;
    private Function<K, V> fetcher = null;

    private Builder() {}

    @Optional("Default value is 1000")
    public Builder<K, V> maxSize(int maxSize) {
      this.maxSize = CommonUtils.requirePositiveInt(maxSize);
      return this;
    }

    @Optional("Default value is 5 seconds")
    public Builder<K, V> timeout(Duration timeout) {
      this.timeout = Objects.requireNonNull(timeout);
      return this;
    }

    @Optional("Default value is false")
    public Builder<K, V> blockingOnGet() {
      this.blockingOnGet = true;
      return this;
    }

    /**
     * Some callers prefer to wrap this builder in their custom fluent pattern, so we provide this
     * method to accept value to help them to complete their pattern.
     *
     * @param blockingOnGet blockingOnGet
     * @return this builder
     */
    @Optional("Default value is false")
    public Builder<K, V> blockingOnGet(boolean blockingOnGet) {
      this.blockingOnGet = blockingOnGet;
      return this;
    }

    public Builder<K, V> fetcher(Function<K, V> fetcher) {
      this.fetcher = Objects.requireNonNull(fetcher);
      return this;
    }

    @Override
    public Cache<K, V> build() {
      Objects.requireNonNull(fetcher);
      return new Cache<K, V>() {
        private LoadingCache<K, V> cache =
            blockingOnGet
                ? CacheBuilder.newBuilder()
                    .maximumSize(maxSize)
                    .expireAfterWrite(timeout.toMillis(), TimeUnit.MILLISECONDS)
                    .build(
                        new CacheLoader<K, V>() {
                          @Override
                          public V load(K key) {
                            return fetcher.apply(key);
                          }
                        })
                : CacheBuilder.newBuilder()
                    .maximumSize(maxSize)
                    .refreshAfterWrite(timeout.toMillis(), TimeUnit.MILLISECONDS)
                    .build(
                        new CacheLoader<K, V>() {
                          @Override
                          public V load(K key) {
                            return fetcher.apply(key);
                          }
                        });

        @Override
        public V get(K key) {
          try {
            return cache.get(Objects.requireNonNull(key));
          } catch (ExecutionException e) {
            if (e.getCause() != null) throw new IllegalStateException(e.getCause());
            else throw new IllegalStateException(e);
          }
        }

        @Override
        public Map<K, V> snapshot() {
          return Collections.unmodifiableMap(new HashMap<>(cache.asMap()));
        }

        @Override
        public void put(Map<? extends K, ? extends V> map) {
          cache.putAll(map);
        }

        @Override
        public long size() {
          return cache.size();
        }

        @Override
        public void clear() {
          cache.invalidateAll();
        }
      };
    }
  }
}
