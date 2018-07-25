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

package org.apache.impala.util;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

/**
 * Utility class to handle pattern-matching for different types of patterns (
 * e.g. hive SHOW patterns, JDBC patterns).
 * It maps those patterns onto the java regex pattern objects.
 */
public class PatternMatcher implements Predicate<String> {
  // Patterns to match against. A string is considered to match if it matches
  // any of the patterns.
  private List<Pattern> patterns_;

  // Returns true if patterns_ is null or the candidate matches.
  // Returns false if patterns_ is empty or the candidate mismatches.
  public boolean matches(String candidate) {
    if (patterns_ == null) return true;
    if (patterns_.isEmpty()) return false;
    for (Pattern pattern: patterns_) {
      if (pattern.matcher(candidate).matches()) return true;
    }
    return false;
  }

  @Override // Implementation of Predicate interface.
  public boolean apply(String input) {
    return matches(input);
  }

  // Immutable pattern matcher that matches all
  private final static class MatchAllPatternMatcher extends PatternMatcher {
    MatchAllPatternMatcher() {}
    public boolean matches(String candidate) { return true; }
  }

  // Immutable pattern matcher that matches none
  private final static class MatchNonePatternMatcher extends PatternMatcher {
    MatchNonePatternMatcher() {}
    public boolean matches(String candidate) { return false; }
  }

  public static final PatternMatcher MATCHER_MATCH_ALL = new MatchAllPatternMatcher();
  public static final PatternMatcher MATCHER_MATCH_NONE = new MatchNonePatternMatcher();

  /**
   * Creates a pattern matcher for hive patterns.
   * The only metacharacters are '*' which matches any string of characters, and '|'
   * which denotes choice.
   * If hivePattern is null, all strings are considered to match. If it is the
   * empty string, no strings match.
   */
  public static PatternMatcher createHivePatternMatcher(String hivePattern) {
    if (hivePattern == null) return MATCHER_MATCH_ALL;
    if (hivePattern.isEmpty()) return MATCHER_MATCH_NONE;
    PatternMatcher result = new PatternMatcher();
    result.patterns_ = Lists.newArrayList();
    // Hive ignores pretty much all metacharacters, so we have to escape them.
    final String metaCharacters = "+?.^()]\\/{}";
    final Pattern regex = Pattern.compile("([" + Pattern.quote(metaCharacters) + "])");

    for (String pattern: Arrays.asList(hivePattern.split("\\|"))) {
      Matcher matcher = regex.matcher(pattern);
      pattern = matcher.replaceAll("\\\\$1").replace("*", ".*");
      result.patterns_.add(Pattern.compile(pattern));
    }
    return result;
  }

  /**
   * Creates a matcher object for JDBC match strings.
   */
  public static PatternMatcher createJdbcPatternMatcher(String pattern) {
    if (pattern == null || pattern.isEmpty()) {
      return MATCHER_MATCH_ALL;
    }
    String wildcardPattern = ".*";
    String result = pattern
        .replaceAll("([^\\\\])%", "$1" + wildcardPattern)
        .replaceAll("\\\\%", "%")
        .replaceAll("^%", wildcardPattern)
        .replaceAll("([^\\\\])_", "$1.")
        .replaceAll("\\\\_", "_")
        .replaceAll("^_", ".");
    PatternMatcher matcher = new PatternMatcher();
    matcher.patterns_ = Lists.newArrayList();
    matcher.patterns_.add(Pattern.compile(result));
    return matcher;
  }
}
