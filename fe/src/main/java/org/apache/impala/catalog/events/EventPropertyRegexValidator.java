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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.regex.Pattern;
import org.apache.hadoop.util.StringUtils;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventPropertyKey;
import java.util.function.Predicate;

/**
 * ConfigValidator implementation for validating regex type configuration. In case of
 * hive.metastore.notification.parameters.exclude.patterns configuration, in HMS-2 it
 * excludes certain properties from the thrift objects in the event message. This
 * Validator is used to make sure that the properties which MetastoreEventProcessor
 * depends on are not getting excluded by this HMS configuration.
 */
public class EventPropertyRegexValidator implements ConfigValidator {

  private final String configKey_;

  public EventPropertyRegexValidator(String configKey) {
    this.configKey_ = Preconditions.checkNotNull(configKey);
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
  @Override
  public ValidationResult validate(String filterRegex) {
    List<String> excludePatterns =
        Arrays.asList(StringUtils.getTrimmedStrings(filterRegex));
    // Combine all Predicates to a single Predicate (as done in Hive side).
    Predicate<String> paramsFilter =
        compilePatternsToPredicates(excludePatterns).stream().reduce(Predicate::or)
            .orElse(x -> false);
    for (MetastoreEventPropertyKey param : MetastoreEventPropertyKey.values()) {
      if (paramsFilter.test(param.getKey())) {
        return new ValidationResult(false, String.format(
            "Failed config validation. Required Impala parameter %s is"
                + " filtered out using the Hive configuration %s=%s", param.getKey(),
            configKey_, filterRegex));
      }
    }
    return new ValidationResult(true, null);
  }

  @Override
  public String getConfigKey() {
    return configKey_;
  }

  private static List<Predicate<String>> compilePatternsToPredicates(
      List<String> patterns) {
    return patterns.stream().map(pattern -> Pattern.compile(pattern).asPredicate())
        .collect(Collectors.toList());
  }
}
