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
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * A single declarative step of a {@link ConfigTranslator}. A rule inspects the mutable
 * 'source' map, consumes (removes) the keys it recognizes, and writes translated entries
 * into 'output'. Draining from 'source' lets a translator know which keys remain
 * unhandled after all rules have run.
 */
public interface TranslationRule {
  /**
   * Applies this rule. Implementations MUST remove any 'source' keys they consume and
   * MUST write results into 'output'. Returns true if the rule matched (consumed at
   * least one source key), false otherwise. May throw IllegalStateException on a
   * configuration error (ambiguity, missing required key, unexpected value).
   */
  boolean apply(Map<String, String> source, Map<String, String> output);

  /**
   * Puts 'value' under 'key' in 'output', throwing if 'key' is already present. Shared
   * duplicate-detection primitive so all rules report the same error for a doubly-defined
   * output key.
   */
  static void putChecked(Map<String, String> output, String key, String value) {
    String existing = output.get(key);
    if (existing != null) {
      throw new IllegalStateException(String.format(
          "Property is defined multiple times: %s%nCurrent value: %s", key, existing));
    }
    output.put(key, value);
  }

  /**
   * Moves a canonical key (and optional alternative spellings of it) from 'source' to
   * 'output' under the canonical key. Detects ambiguity when more than one spelling is
   * present.
   */
  class RenameRule implements TranslationRule {
    protected final String canonicalKey;
    protected final List<String> aliases;

    public RenameRule(String canonicalKey) {
      this(canonicalKey, List.of());
    }

    public RenameRule(String canonicalKey, List<String> aliases) {
      this.canonicalKey = Preconditions.checkNotNull(canonicalKey);
      this.aliases = Preconditions.checkNotNull(aliases);
    }

    @Override
    public boolean apply(Map<String, String> source, Map<String, String> output) {
      boolean applied = false;
      String value = source.get(canonicalKey);
      if (value != null) {
        applied = true;
        source.remove(canonicalKey);
        putChecked(output, canonicalKey, value);
      }
      for (String alias : aliases) {
        value = source.get(alias);
        if (value != null) {
          if (applied) {
            throw new IllegalStateException(String.format(
                "Alternative key '%s' sets the same configuration as '%s' which is "
                    + "already defined with value '%s'",
                alias, canonicalKey, value));
          }
          applied = true;
          source.remove(alias);
          putChecked(output, canonicalKey, value);
        }
      }
      return applied;
    }
  }

  /** A {@link RenameRule} that must match; throws if the key is absent. */
  class RequiredRule extends RenameRule {
    public RequiredRule(String canonicalKey) {
      super(canonicalKey);
    }

    public RequiredRule(String canonicalKey, List<String> aliases) {
      super(canonicalKey, aliases);
    }

    @Override
    public boolean apply(Map<String, String> source, Map<String, String> output) {
      if (super.apply(source, output)) return true;
      throw new IllegalStateException(
          String.format("Missing required property: %s", canonicalKey));
    }
  }

  /** Consumes a key from 'source' and drops it (produces no output). */
  class IgnoreRule implements TranslationRule {
    private final String key;

    public IgnoreRule(String key) {
      this.key = Preconditions.checkNotNull(key);
    }

    @Override
    public boolean apply(Map<String, String> source, Map<String, String> output) {
      return source.remove(key) != null;
    }
  }

  /**
   * Consumes a key from 'source', verifies its value equals 'expectedValue'
   * (case-insensitive), and passes it through to 'output' unchanged.
   */
  class VerifyRule implements TranslationRule {
    private final String key;
    private final String expectedValue;

    public VerifyRule(String key, String expectedValue) {
      this.key = Preconditions.checkNotNull(key);
      this.expectedValue = Preconditions.checkNotNull(expectedValue);
    }

    @Override
    public boolean apply(Map<String, String> source, Map<String, String> output) {
      String value = source.get(key);
      if (value == null) return false;
      // Keys are case sensitive, but verified values typically are not
      // (false/FALSE, none/NONE).
      if (!expectedValue.equalsIgnoreCase(value)) {
        throw new IllegalStateException(String.format(
            "The only allowed value for property '%s' is '%s'.\n"
                + "Value in configuration is '%s'",
            key, expectedValue, value));
      }
      source.remove(key);
      putChecked(output, key, value);
      return true;
    }
  }

  /**
   * Bulk key rename: for each entry of 'keyMap' (sourceKey -> outputKey) present in
   * 'source', moves the value verbatim to 'output' under outputKey.
   */
  class MapRenameRule implements TranslationRule {
    private final ImmutableMap<String, String> keyMap;

    public MapRenameRule(ImmutableMap<String, String> keyMap) {
      this.keyMap = Preconditions.checkNotNull(keyMap);
    }

    @Override
    public boolean apply(Map<String, String> source, Map<String, String> output) {
      boolean applied = false;
      for (Map.Entry<String, String> rename : keyMap.entrySet()) {
        String value = source.get(rename.getKey());
        if (value != null) {
          source.remove(rename.getKey());
          putChecked(output, rename.getValue(), value);
          applied = true;
        }
      }
      return applied;
    }
  }

  /**
   * Arbitrary post-processing that cannot be expressed as a key rename, e.g. selecting a
   * credential provider based on which keys are present. Receives the ORIGINAL,
   * unmodified source snapshot and the current 'output' to mutate. Does not drain
   * 'source' (returns false), so it never suppresses pass-through of inspected keys.
   */
  class ProgrammaticRule implements TranslationRule {
    private final BiConsumer<Map<String, String>, Map<String, String>> action;

    public ProgrammaticRule(
        BiConsumer<Map<String, String>, Map<String, String>> action) {
      this.action = Preconditions.checkNotNull(action);
    }

    @Override
    public boolean apply(Map<String, String> source, Map<String, String> output) {
      action.accept(source, output);
      return false;
    }
  }
}
