// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.catalog.events;

import com.google.common.base.Preconditions;
import org.apache.impala.catalog.events.ConfigValidator.ValidationResult;
import org.apache.impala.compat.MetastoreShim;

/**
 * Metastore configurations and their expected values for event processing.
 * These configs are validated before creating an instance of the event processor.
 * This is a super set of all the event processor configurations needed to be validated
 * and MetastoreShim version is used to get the actual sub-set of configurations to
 * validate depending on the hive version.
 */
public enum MetastoreEventProcessorConfig {
  FIRE_EVENTS_FOR_DML("hive.metastore.dml.events", "true"),
  METASTORE_DEFAULT_CATALOG_NAME("metastore.catalog.default",
      MetastoreShim.getDefaultCatalogName());

  private final ConfigValidator validator_;

  MetastoreEventProcessorConfig(String configKey, String expectedValue) {
    Preconditions.checkNotNull(configKey);
    this.validator_ = new DefaultConfigValidator(configKey, expectedValue);
  }

  MetastoreEventProcessorConfig(ConfigValidator validator) {
    this.validator_ = Preconditions.checkNotNull(validator);
  }

  public ConfigValidator getValidator() {
    return validator_;
  }

  @Override
  public String toString() {
    return validator_.toString();
  }

  /**
   * Validates the given value for this Configuration
   * @param value
   * @return ValidationResult which indicates whether the validation is successful or
   * not. If the validation is not successful, provides a reason as to why the config
   * validation failed
   */
  public ValidationResult validate(String value) {
    return validator_.validate(value);
  }
}
