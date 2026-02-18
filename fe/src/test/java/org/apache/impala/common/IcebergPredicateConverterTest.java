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

package org.apache.impala.common;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Unit tests for IcebergPredicateConverter helper methods.
 */
public class IcebergPredicateConverterTest {

  @Test
  public void testIsWildcardEscaped() {
    // Test case 1: No backslashes - not escaped
    assertEquals(false, IcebergPredicateConverter.isWildcardEscaped("test%", 4));
    assertEquals(false, IcebergPredicateConverter.isWildcardEscaped("test_value", 4));
    assertEquals(false, IcebergPredicateConverter.isWildcardEscaped("%test", 0));

    // Test case 2: Single backslash - escaped (odd count)
    assertEquals(true, IcebergPredicateConverter.isWildcardEscaped("test\\%", 5));
    assertEquals(true, IcebergPredicateConverter.isWildcardEscaped("test\\_value", 5));

    // Test case 3: Double backslash - not escaped (even count)
    assertEquals(false, IcebergPredicateConverter.isWildcardEscaped("test\\\\%", 6));
    assertEquals(false, IcebergPredicateConverter.isWildcardEscaped("test\\\\_value", 6));

    // Test case 4: Triple backslash - escaped (odd count)
    assertEquals(true, IcebergPredicateConverter.isWildcardEscaped("test\\\\\\%", 7));

    // Test case 5: Multiple wildcards with mixed escaping
    // Pattern: "test\%_" - first wildcard escaped, second not
    assertEquals(true, IcebergPredicateConverter.isWildcardEscaped("test\\%_", 5));
    assertEquals(false, IcebergPredicateConverter.isWildcardEscaped("test\\%_", 6));

    // Test case 6: Edge case - wildcard at position 0
    assertEquals(false, IcebergPredicateConverter.isWildcardEscaped("%test", 0));
    assertEquals(false, IcebergPredicateConverter.isWildcardEscaped("_test", 0));
  }

  @Test
  public void testHasLiteralContentAfterWildcard() {
    // Test case 1: No content after first wildcard (only unescaped wildcards or nothing)
    assertEquals(false,
        IcebergPredicateConverter.hasLiteralContentAfterWildcard("test%", 4));
    assertEquals(false,
        IcebergPredicateConverter.hasLiteralContentAfterWildcard("test%%", 4));
    assertEquals(false,
        IcebergPredicateConverter.hasLiteralContentAfterWildcard("test%_%", 4));
    assertEquals(false,
        IcebergPredicateConverter.hasLiteralContentAfterWildcard("test_", 4));
    assertEquals(false,
        IcebergPredicateConverter.hasLiteralContentAfterWildcard("prefix%%%", 6));

    // Test case 2: Literal content after first wildcard
    assertEquals(true,
        IcebergPredicateConverter.hasLiteralContentAfterWildcard("d%d", 1));
    assertEquals(true,
        IcebergPredicateConverter.hasLiteralContentAfterWildcard("test%suffix", 4));
    assertEquals(true,
        IcebergPredicateConverter.hasLiteralContentAfterWildcard("a%b%c", 1));
    assertEquals(true,
        IcebergPredicateConverter.hasLiteralContentAfterWildcard("pre_%fix", 3));

    // Test case 3: Escaped wildcards after first wildcard are literal content
    // "test%\\%" has escaped % after the unescaped %, which is literal content
    assertEquals(true,
        IcebergPredicateConverter.hasLiteralContentAfterWildcard("test%\\%", 4));
    // "test%\\_" has escaped _ after the first %, which is literal content
    assertEquals(true,
        IcebergPredicateConverter.hasLiteralContentAfterWildcard("test%\\_", 4));

    // Test case 4: Mix of wildcards and escaped content
    // "pre%_%\\%" has unescaped _, then escaped %, so has literal content
    assertEquals(true,
        IcebergPredicateConverter.hasLiteralContentAfterWildcard("pre%_%\\%", 3));
    // Obviously has literal 'x' at the end
    assertEquals(true,
        IcebergPredicateConverter.hasLiteralContentAfterWildcard("pre%_%\\%x", 3));
  }

  @Test
  public void testFindFirstUnescapedWildcard() {
    // Test case 1: No wildcards
    assertEquals(-1, IcebergPredicateConverter.findFirstUnescapedWildcard("exact"));
    assertEquals(-1, IcebergPredicateConverter.findFirstUnescapedWildcard("test"));

    // Test case 2: Unescaped wildcards
    assertEquals(0, IcebergPredicateConverter.findFirstUnescapedWildcard("%test"));
    assertEquals(0, IcebergPredicateConverter.findFirstUnescapedWildcard("_test"));
    assertEquals(4, IcebergPredicateConverter.findFirstUnescapedWildcard("test%"));
    assertEquals(4, IcebergPredicateConverter.findFirstUnescapedWildcard("test_suffix"));
    assertEquals(3, IcebergPredicateConverter.findFirstUnescapedWildcard("abc%def"));

    // Test case 3: Escaped wildcards (what getUnescapedValue() returns for SQL \%)
    // SQL 'test\%' → getUnescapedValue() returns "test\\%"
    assertEquals(-1, IcebergPredicateConverter.findFirstUnescapedWildcard("test\\%"));
    assertEquals(-1,
        IcebergPredicateConverter.findFirstUnescapedWildcard("test\\_value"));
    assertEquals(-1, IcebergPredicateConverter.findFirstUnescapedWildcard("abc\\%def"));

    // Test case 4: Mixed escaped and unescaped wildcards
    // "wild\\%card%" in memory: w,i,l,d,\,%,c,a,r,d,%
    // First % at 5 is escaped, second % at 10 is unescaped
    assertEquals(10,
        IcebergPredicateConverter.findFirstUnescapedWildcard("wild\\%card%"));
    assertEquals(10,
        IcebergPredicateConverter.findFirstUnescapedWildcard("wild\\_card_mix"));
    // "test\\%%more" in memory: t,e,s,t,\,%,%,m,o,r,e
    // First % at 5 is escaped, second % at 6 is unescaped
    assertEquals(6, IcebergPredicateConverter.findFirstUnescapedWildcard("test\\%%more"));

    // Test case 5: Double backslash (escaped backslash + unescaped wildcard)
    // "test\\\\%" in Java source = "test\\%" in memory
    // % at index 6 has even (2) backslashes, so it's unescaped
    assertEquals(6, IcebergPredicateConverter.findFirstUnescapedWildcard("test\\\\%"));
    assertEquals(6, IcebergPredicateConverter.findFirstUnescapedWildcard("test\\\\_"));

    // Test case 6: Triple backslash (escaped backslash + escaped wildcard)
    // "test\\\\\\%" in Java source = "test\\\\%" in memory
    // % at index 7 has odd (3) backslashes, so it's escaped
    assertEquals(-1, IcebergPredicateConverter.findFirstUnescapedWildcard("test\\\\\\%"));
  }

  @Test
  public void testUnescapeLikePattern() {
    // Test case 1: No escapes
    assertEquals("exact", IcebergPredicateConverter.unescapeLikePattern("exact", -1));
    assertEquals("prefix", IcebergPredicateConverter.unescapeLikePattern("prefix", 6));

    // Test case 2: Escaped % (what getUnescapedValue() returns for SQL 'test\%value')
    // SQL: 'test\%value' → getUnescapedValue() returns "test\\%value" (backslash + %)
    // After unescaping → "test%value" (literal %)
    assertEquals("test%value",
        IcebergPredicateConverter.unescapeLikePattern("test\\%value", -1));
    assertEquals("abc%def",
        IcebergPredicateConverter.unescapeLikePattern("abc\\%def", -1));

    // Test case 3: Escaped _
    assertEquals("test_value",
        IcebergPredicateConverter.unescapeLikePattern("test\\_value", -1));
    assertEquals("abc_def",
        IcebergPredicateConverter.unescapeLikePattern("abc\\_def", -1));

    // Test case 4: Mixed escaped wildcards
    assertEquals("wild%card_mix",
        IcebergPredicateConverter.unescapeLikePattern("wild\\%card\\_mix", -1));

    // Test case 5: Prefix extraction (endPos specified)
    // "wild\\%card%suffix" in memory: w,i,l,d,\,%,c,a,r,d,%,s,u,f,f,i,x
    // Unescaped % is at index 10, extract up to (not including) that position
    assertEquals("wild%card",
        IcebergPredicateConverter.unescapeLikePattern("wild\\%card%suffix", 10));
    // "test\\%%more" in memory: t,e,s,t,\,%,%,m,o,r,e
    // Unescaped % is at index 6, extract up to that position
    assertEquals("test%",
        IcebergPredicateConverter.unescapeLikePattern("test\\%%more", 6));

    // Test case 6: Escaped backslash (\\)
    // SQL: 'test\\value' → getUnescapedValue() returns "test\\\\value" (2 backslashes)
    // After unescaping \\ → \ (single backslash)
    assertEquals("test\\value",
        IcebergPredicateConverter.unescapeLikePattern("test\\\\value", -1));
    assertEquals("path\\to\\file",
        IcebergPredicateConverter.unescapeLikePattern("path\\\\to\\\\file", -1));

    // Test case 7: Multiple consecutive escapes
    // "test\\\\\\%" in Java = "test\\\\%" in memory
    // First pair of backslashes (4-5) unescapes to single \
    // Third backslash (6) escapes the %, so result is test\ followed by literal %
    assertEquals("test\\%",
        IcebergPredicateConverter.unescapeLikePattern("test\\\\\\%", -1));
    assertEquals("a%b_c", IcebergPredicateConverter.unescapeLikePattern("a\\%b\\_c", -1));

    // Test case 8: Empty and edge cases
    assertEquals("", IcebergPredicateConverter.unescapeLikePattern("", -1));
    assertEquals("\\", IcebergPredicateConverter.unescapeLikePattern("\\\\", -1));
    assertEquals("%", IcebergPredicateConverter.unescapeLikePattern("\\%", -1));
    assertEquals("_", IcebergPredicateConverter.unescapeLikePattern("\\_", -1));
  }
}
