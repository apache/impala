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
import org.apache.commons.lang3.StringUtils;

/**
 * This class implements a simple ConfigValidator which validates a given
 * configuration value against a expected value. Used by MetastoreEventsProcessor to
 * validate if metastore is configured correctly for using NotificationEvents
 */
public class DefaultConfigValidator implements ConfigValidator {

  private final String CONFIG_VALIDATION_ERROR_MSG = "Unexpected configuration value "
      + "for %s in Hive Metastore. Expected: %s Found: %s";
  private final String CONFIG_UNAVAILABLE_ERROR_MSG = "Configuration key %s unavailable"
      + " in Hive Metastore. Expected value: %s";

  private final String expectedValue_;
  private final String configKey_;

  public DefaultConfigValidator(String configKey, String expectedConfigValue) {
    this.expectedValue_ = Preconditions.checkNotNull(expectedConfigValue);
    this.configKey_ = Preconditions.checkNotNull(configKey);
  }

  /**
   * Validates the given value against the expected value
   */
  @Override
  public ValidationResult validate(String configValue) {
    if (expectedValue_.equalsIgnoreCase(configValue)) {
      return new ValidationResult(true, null);
    }

    if (StringUtils.isBlank(configValue)) {
      return new ValidationResult(false,
          String.format(CONFIG_UNAVAILABLE_ERROR_MSG, configKey_, expectedValue_));
    } else {
      return new ValidationResult(false, String
          .format(CONFIG_VALIDATION_ERROR_MSG, configKey_, expectedValue_, configValue));
    }
  }

  @Override
  public String getConfigKey() {
    return configKey_;
  }

  @Override
  public String toString() {
    return String.format("Config : %s, Expected Value : %s", configKey_, expectedValue_);
  }
}
