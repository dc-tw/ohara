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

package com.island.ohara.kafka.connector.json;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import com.island.ohara.common.util.CommonUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class Validation implements JsonObject {
  private final Map<String, String> settings;

  public static Validation ofJson(String json) {
    return of(JsonUtils.toObject(json, new TypeReference<Map<String, String>>() {}));
  }

  /**
   * Construct a Validation with basic required arguments only.
   *
   * @param className class name
   * @param topicsName topic names
   * @return validation
   */
  public static Validation of(String className, List<String> topicsName) {
    return new Validation(
        ImmutableMap.of(
            SettingDefinition.CONNECTOR_CLASS_DEFINITION.key(),
                CommonUtils.requireNonEmpty(className),
            SettingDefinition.TOPIC_NAMES_DEFINITION.key(), StringList.toKafkaString(topicsName)));
  }

  public static Validation of(Map<String, String> settings) {
    return new Validation(settings);
  }

  private Validation(Map<String, String> settings) {
    CommonUtils.requireNonEmpty(settings)
        .forEach(
            (k, v) -> {
              CommonUtils.requireNonEmpty(k);
              CommonUtils.requireNonEmpty(v);
            });
    this.settings = Collections.unmodifiableMap(new HashMap<>(settings));
  }

  public Map<String, String> settings() {
    return settings;
  }

  @Override
  public String toJsonString() {
    return JsonUtils.toString(settings);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Validation) return toJsonString().equals(((Validation) obj).toJsonString());
    return false;
  }

  @Override
  public int hashCode() {
    return toJsonString().hashCode();
  }

  // ------------------------------------------[helper
  // methods]------------------------------------------//
  private String value(SettingDefinition settingDefinition) {
    if (settings.containsKey(settingDefinition.key())) return settings.get(settingDefinition.key());
    else throw new NoSuchElementException(settingDefinition.key() + " doesn't exist");
  }

  /**
   * Kafka validation requires the class name so this class offers this helper method.
   *
   * @return class name
   */
  public String className() {
    return value(SettingDefinition.CONNECTOR_CLASS_DEFINITION);
  }

  /**
   * Kafka-2.x validation requires the topics so this class offers this helper method.
   *
   * @return topics name
   */
  public List<String> topicNames() {
    return StringList.ofKafkaList(value(SettingDefinition.TOPIC_NAMES_DEFINITION));
  }
}
