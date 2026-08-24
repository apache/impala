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

package org.apache.impala.catalog.iceberg.translate;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A declarative engine that translates one configuration map into another by applying an
 * ordered list of {@link TranslationRule}s. It is the single translation mechanism shared
 * by the catalog-property translation in {@code RESTCatalogProperties} and the
 * credential-to-Hadoop translation in {@code Credential}.
 *
 * Each rule consumes (drains) the source keys it recognizes and writes translated entries
 * into the output. After all rules have run, any keys still left in the source are either
 * copied through unchanged (when 'passThroughRemaining' is true) or dropped (when false).
 * This single flag is the only behavioral difference between the two callers: catalog
 * properties pass unknown keys through to the Iceberg client, while credentials emit only
 * the keys they explicitly translate.
 *
 * {@link TranslationRule.ProgrammaticRule}s always see the original, unmodified source
 * snapshot so provider-selection logic can inspect keys that earlier rules already
 * drained.
 */
public class ConfigTranslator {
  private final ImmutableList<TranslationRule> rules_;
  private final boolean passThroughRemaining_;

  public ConfigTranslator(List<TranslationRule> rules, boolean passThroughRemaining) {
    rules_ = ImmutableList.copyOf(Preconditions.checkNotNull(rules));
    passThroughRemaining_ = passThroughRemaining;
  }

  /**
   * Translates 'source' into a new map. 'source' is not modified. May throw
   * IllegalStateException if a rule detects a configuration error (ambiguity, a missing
   * required key, or an unexpected verified value).
   */
  public Map<String, String> translate(Map<String, String> source) {
    Preconditions.checkNotNull(source);
    // 'working' is drained as rules match; 'original' is a stable snapshot for
    // ProgrammaticRules that need to inspect already-consumed keys.
    Map<String, String> original = new HashMap<>(source);
    Map<String, String> working = new HashMap<>(source);
    Map<String, String> output = new HashMap<>();

    for (TranslationRule rule : rules_) {
      if (rule instanceof TranslationRule.ProgrammaticRule) {
        rule.apply(original, output);
      } else {
        rule.apply(working, output);
      }
    }

    if (passThroughRemaining_) {
      for (Map.Entry<String, String> entry : working.entrySet()) {
        TranslationRule.putChecked(output, entry.getKey(), entry.getValue());
      }
    }
    return output;
  }
}
