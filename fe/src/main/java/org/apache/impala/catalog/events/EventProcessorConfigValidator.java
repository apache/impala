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

import static java.util.regex.Pattern.compile;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.hadoop.util.StringUtils;
import org.apache.impala.catalog.events.EventProcessorConfigValidator.ValidationResult;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventPropertyKey;
import org.apache.thrift.TException;
import java.util.List;

/**
 * This interface contains methods to validate the event processor during its
 * initialization. For any validation on the MetastoreEventsProcessor, it should return a
 * ValidationResult object. If the validation is successful, a ValidationResult object
 * with valid set as true should be returned. If the validation is unsuccessful, a
 * ValidationResult object is returned with valid = false and also the reason for the
 * validation failure.
 */
public interface EventProcessorConfigValidator extends Function<MetastoreEventsProcessor,
    ValidationResult> {

  // Metastore configurations and their expected values for event processing.
  // These configs are validated before creating an instance of the event processor.
  enum MetastoreEventConfigsToValidate {
    ADD_THRIFT_OBJECTS("hive.metastore.notifications.add.thrift.objects", "true"),
    ALTER_NOTIFICATIONS_BASIC("hive.metastore.alter.notifications.basic", "false"),
    FIRE_EVENTS_FOR_DML("hive.metastore.dml.events", "true");

    private final String conf_, expectedValue_;

    MetastoreEventConfigsToValidate(String conf, String expectedValue) {
      this.conf_ = conf;
      this.expectedValue_ = expectedValue;
    }

    /**
     * Returns the value expected to be set for a Metastore config which is required to
     * run event processor successfully
     */
    public String getExpectedValue() { return expectedValue_; }

    /**
     * Returns the configuration name
     */
    public String getConf() { return conf_; }

    @Override
    public String toString() {
      return "Config : " + conf_ + ", Expected Value : " + expectedValue_;
    }
  }

  // Hive config name that can have regular expressions to filter out parameters.
  String METASTORE_PARAMETER_EXCLUDE_PATTERNS =
      "hive.metastore.notification.parameters.exclude.patterns";

  // Default config value (currently just an empty String) to be returned from Metastore
  // when the given config is not set.
  String DEFAULT_METASTORE_CONFIG_VALUE = "";

  static EventProcessorConfigValidator hasValidMetastoreConfigs() {
    return EventProcessorConfigValidator::validateMetastoreConfigs;
  }

  static EventProcessorConfigValidator verifyParametersNotFiltered() {
    return EventProcessorConfigValidator::validateMetastoreEventParameters;
  }

  default EventProcessorConfigValidator and(EventProcessorConfigValidator other) {
    return metastoreEventsProcessor -> {
      final ValidationResult result = this.apply(metastoreEventsProcessor);
      return result.isValid() ? other.apply(metastoreEventsProcessor) : result;
    };
  }

  /**
   * Validates the Impala parameters required for event processing. Hive has a config
   * METASTORE_PARAMETER_EXCLUDE_PATTERNS that filters out parameters from Metastore
   * entities, so we need to make sure that the relevant parameters for event processing
   * are not filtered. We do this by getting the regex from this Hive config and testing
   * against each key in MetatsoreEvents.MetastoreEventPropertyKey. If the regex does not
   * filter any parameter, the method returns a ValidationResult indicating success. If
   * any parameter matches the regex, it returns a ValidationResult indicating failure,
   * the regex and the parameter filtered.
   */
  @VisibleForTesting
  static ValidationResult validateMetastoreEventParameters(
      MetastoreEventsProcessor eventsProcessor) {
    ValidationResult result = new ValidationResult();
    try {
      String filterRegex = eventsProcessor.getConfigValueFromMetastore(
          METASTORE_PARAMETER_EXCLUDE_PATTERNS, DEFAULT_METASTORE_CONFIG_VALUE);
      List<String> excludePatterns =
          Arrays.asList(StringUtils.getTrimmedStrings(filterRegex));
      // Combine all Predicates to a single Predicate (as done in Hive side).
      Predicate<String> paramsFilter =
          compilePatternsToPredicates(excludePatterns).stream().reduce(Predicate::or)
              .orElse(x -> false);
      for (MetastoreEventPropertyKey param : MetastoreEventPropertyKey.values()) {
        if (paramsFilter.test(param.getKey())) {
          result = new ValidationResult(false, String.format("Failed config validation. "
                  + "Required Impala parameter %s is"
                  + " filtered out using the Hive configuration %s=%s", param.getKey(),
              METASTORE_PARAMETER_EXCLUDE_PATTERNS, filterRegex));
          break;
        }
      }
      return result;
    } catch (TException e) {
      return new ValidationResult(false,
          "Unable to get the configuration from Metastore. Check if the Metastore "
              + "service is accessible.");
    }
  }

  static List<Predicate<String>> compilePatternsToPredicates(List<String> patterns) {
    return patterns.stream().map(pattern -> compile(pattern).asPredicate()).collect(
        Collectors.toList());
  }

  /**
   * Validates the metastore configurations required for event processing. This method
   * verifies that each config provided in MetastoreEventConfigsToValidate has the
   * expected value set in Metastore. If the expected value is set for all the configs,
   * the method returns a ValidationResult indicating success. Otherwise, it returns a
   * ValidationResult indicating invalid config and also will include reason for failure.
   */
  @VisibleForTesting
  static ValidationResult validateMetastoreConfigs(MetastoreEventsProcessor processor) {
    ValidationResult result = new ValidationResult();
    try {
      for (MetastoreEventConfigsToValidate metaConf : MetastoreEventConfigsToValidate
          .values()) {
        String configValueFromMetastore = processor
            .getConfigValueFromMetastore(metaConf.getConf(),
                DEFAULT_METASTORE_CONFIG_VALUE);
        if (!configValueFromMetastore.equals(metaConf.getExpectedValue())) {
          result = new ValidationResult(false, String.format("Incorrect configuration "
              + "for %s. Found : %s", metaConf.toString(), configValueFromMetastore));
          break;
        }
      }
      return result;
    } catch (TException e) {
      return new ValidationResult(false,
          "Unable to get the configuration from Metastore. Check if the Metastore "
              + "service is accessible.");
    }
  }


  /**
   * This class represents the result object for EventProcessor validation. If result is
   * success, valid should be set to true and reason is optional(ignored). If result is
   * failure, valid should be set to false and reason of failure should be set
   * accordingly.
   */
  class ValidationResult {

    // Boolean indicating whether the validation was success or failure.
    private boolean valid_;
    // Reason indicating the failure of validation. Only relevant when
    // the validation fails.
    private String reason_;

    ValidationResult(Boolean valid, String reason) {
      Preconditions.checkNotNull(valid);
      this.valid_ = valid;
      this.reason_ = reason;
    }

    ValidationResult() {
      this(true, null);
    }

    boolean isValid() {
      return valid_;
    }

    /**
     * When reason is null, the method returns an Optional.empty. Returns an Optional with
     * present value if the reason is not null.
     */
    Optional<String> getReason() {
      return Optional.ofNullable(reason_);
    }
  }

}