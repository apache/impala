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
import com.google.errorprone.annotations.Immutable;

/**
 * Interface which is used by EventProcessor to implement the configuration
 * validation logic for the configurations relevant to Event Processor. Provides a
 * helper class to capture the ValidationResult
 */
public interface ConfigValidator {
  ValidationResult validate(String configValue);
  String getConfigKey();

  /**
   * This class represents the result object for EventProcessor validation. If result is
   * success, valid should be set to true and reason is optional(ignored). If result is
   * failure, valid should be set to false and reason of failure should be set
   * accordingly.
   */
  @Immutable
  public class ValidationResult {

    /**
     * Boolean indicating whether the validation was success or failure.
     */
    private final boolean valid_;

    /**
     * Reason indicating the failure of validation. Only relevant when
     * the validation fails.
     */
    private final String reason_;

    public ValidationResult(boolean valid, String reason) {
      Preconditions.checkArgument(valid || reason != null, "Must include a reason if "
          + "the validation result is false");
      this.valid_ = valid;
      this.reason_ = reason;
    }

    public boolean isValid() {
      return valid_;
    }

    /**
     * Reason which gives details as to why the configuration validation failed.
     */
    public String getReason() {
      Preconditions.checkState(!valid_);
      return reason_;
    }
  }
}