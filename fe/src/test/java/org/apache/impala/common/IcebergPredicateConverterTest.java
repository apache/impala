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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.expressions.UnboundTransform;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LikePredicate;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.StringLiteral;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.IcebergColumn;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.IcebergPredicateConverter.ConversionStatus;
import org.apache.impala.common.IcebergPredicateConverter.ConverterResult;
import org.apache.impala.common.IcebergPredicateConverter.Term;
import org.junit.Test;

/**
 * Unit tests for IcebergPredicateConverter.
 *
 * LIKE patterns in this test are the strings that the backend receives, i.e. after
 * SQL string literal unescaping. E.g. the Java literal "a\\b%" is the pattern a\b%,
 * written as LIKE 'a\\b%' in SQL.
 */
public class IcebergPredicateConverterTest {
  private static final IcebergColumn STRING_COL =
      new IcebergColumn("s", Type.STRING, null, 0, 1, -1, -1, true);

  private static class TestConverter extends IcebergPredicateConverter {
    TestConverter() { super(null, null); }

    @Override
    protected Column getColumnFromSlotRef(SlotRef slotRef) { return STRING_COL; }
  }

  private final TestConverter converter_ = new TestConverter();

  private static SlotRef col() {
    return new SlotRef(Collections.singletonList(STRING_COL.getName())) {
      { type_ = STRING_COL.getType(); }
    };
  }

  /** Returns a string literal whose getUnescapedValue() is 'value'. */
  private static StringLiteral strLit(String value) {
    StringLiteral lit = new StringLiteral(
        value.replace("\\", "\\\\").replace("'", "\\'"));
    assertEquals(value, lit.getUnescapedValue());
    return lit;
  }

  /** Returns a string literal like the ones created by constant folding. */
  private static StringLiteral foldedLit(String value) {
    return new StringLiteral(value, Type.STRING, false);
  }

  private static StringLiteral nonUtf8Lit() {
    return new StringLiteral(new byte[] {'a', (byte) 0xFF, '%'}, Type.STRING);
  }

  private static Expr like(String pattern) { return like(strLit(pattern)); }

  private static Expr like(StringLiteral pattern) {
    return new LikePredicate(LikePredicate.Operator.LIKE, col(), pattern);
  }

  private static Expr eq(String value) { return eq(strLit(value)); }

  private static Expr eq(StringLiteral value) {
    return new BinaryPredicate(BinaryPredicate.Operator.EQ, col(), value);
  }

  private static Expr not(Expr e) {
    return new CompoundPredicate(CompoundPredicate.Operator.NOT, e, null);
  }

  private static Expr and(Expr l, Expr r) {
    return new CompoundPredicate(CompoundPredicate.Operator.AND, l, r);
  }

  private static Expr or(Expr l, Expr r) {
    return new CompoundPredicate(CompoundPredicate.Operator.OR, l, r);
  }

  private ConversionStatus status(Expr e) {
    return converter_.convert(e).getStatus();
  }

  /**
   * Asserts that LIKE 'pattern' converts to '<op>(s, value)' with 'status', and that
   * NOT LIKE 'pattern' is not pushed down.
   */
  private void assertLike(String pattern, ConversionStatus status, Operation op,
      String value) {
    ConverterResult result = converter_.convert(like(pattern));
    assertEquals(pattern, status, result.getStatus());
    UnboundPredicate<?> pred = (UnboundPredicate<?>) result.getIcebergExpression();
    assertEquals(pattern, op, pred.op());
    assertEquals(pattern, STRING_COL.getName(), pred.ref().name());
    assertEquals(pattern, value, pred.literal().value().toString());
    assertEquals(pattern, ConversionStatus.FAILED, status(not(like(pattern))));
  }

  private void assertFull(String pattern, Operation op, String value) {
    assertLike(pattern, ConversionStatus.FULLY_CONVERTED, op, value);
  }

  private void assertPartial(String pattern, String prefix) {
    assertLike(pattern, ConversionStatus.PARTIALLY_CONVERTED, Operation.STARTS_WITH,
        prefix);
  }

  private void assertFailed(String pattern) {
    assertEquals(pattern, ConversionStatus.FAILED, status(like(pattern)));
    assertEquals(pattern, ConversionStatus.FAILED, status(not(like(pattern))));
  }

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
  public void testGetVerbatimStartsWithPrefix() {
    assertEquals("ab", IcebergPredicateConverter.getVerbatimStartsWithPrefix("ab%"));
    assertEquals("ab", IcebergPredicateConverter.getVerbatimStartsWithPrefix("ab%%%"));
    assertEquals("", IcebergPredicateConverter.getVerbatimStartsWithPrefix("%"));
    // Backslashes are literal.
    assertEquals("a\\b", IcebergPredicateConverter.getVerbatimStartsWithPrefix("a\\b%"));
    assertEquals("a\\", IcebergPredicateConverter.getVerbatimStartsWithPrefix("a\\%%"));
    // A newline makes the backend's ends-with-'\%' check fail.
    assertEquals("a\nb\\",
        IcebergPredicateConverter.getVerbatimStartsWithPrefix("a\nb\\%"));

    assertNull(IcebergPredicateConverter.getVerbatimStartsWithPrefix("ab"));
    assertNull(IcebergPredicateConverter.getVerbatimStartsWithPrefix("ab_"));
    assertNull(IcebergPredicateConverter.getVerbatimStartsWithPrefix("ab%_"));
    assertNull(IcebergPredicateConverter.getVerbatimStartsWithPrefix("a%b"));
    assertNull(IcebergPredicateConverter.getVerbatimStartsWithPrefix("ab%\\"));
    assertNull(IcebergPredicateConverter.getVerbatimStartsWithPrefix("a\\%b%"));
    // Ends in '\%'.
    assertNull(IcebergPredicateConverter.getVerbatimStartsWithPrefix("ab\\%"));
    assertNull(IcebergPredicateConverter.getVerbatimStartsWithPrefix("ab\\\\%"));
  }

  @Test
  public void testMatchesAnySuffixFrom() {
    assertEquals(true, IcebergPredicateConverter.matchesAnySuffixFrom("ab%", 2));
    assertEquals(true, IcebergPredicateConverter.matchesAnySuffixFrom("ab%%%", 2));
    assertEquals(true, IcebergPredicateConverter.matchesAnySuffixFrom("ab%\\", 2));

    assertEquals(false, IcebergPredicateConverter.matchesAnySuffixFrom("ab_", 2));
    assertEquals(false, IcebergPredicateConverter.matchesAnySuffixFrom("ab%_", 2));
    assertEquals(false, IcebergPredicateConverter.matchesAnySuffixFrom("ab%c", 2));
    assertEquals(false, IcebergPredicateConverter.matchesAnySuffixFrom("ab%\\%", 2));
    assertEquals(false, IcebergPredicateConverter.matchesAnySuffixFrom("ab%\\\\", 2));
  }

  @Test
  public void testGetRegexLiteral() {
    // No escapes
    assertEquals("exact", IcebergPredicateConverter.getRegexLiteral("exact", -1));
    assertEquals("prefix", IcebergPredicateConverter.getRegexLiteral("prefix%", 6));

    // Escaped wildcards
    assertEquals("test%value",
        IcebergPredicateConverter.getRegexLiteral("test\\%value", -1));
    assertEquals("test_value",
        IcebergPredicateConverter.getRegexLiteral("test\\_value", -1));
    assertEquals("wild%card_mix",
        IcebergPredicateConverter.getRegexLiteral("wild\\%card\\_mix", -1));

    // Prefix before the unescaped wildcard at endPos
    assertEquals("wild%card",
        IcebergPredicateConverter.getRegexLiteral("wild\\%card%suffix", 10));
    assertEquals("test%",
        IcebergPredicateConverter.getRegexLiteral("test\\%%more", 6));

    // Escaped backslash
    assertEquals("test\\value",
        IcebergPredicateConverter.getRegexLiteral("test\\\\value", -1));
    assertEquals("test\\%",
        IcebergPredicateConverter.getRegexLiteral("test\\\\\\%", -1));
    assertEquals("a\\b", IcebergPredicateConverter.getRegexLiteral("a\\\\b%c", 4));

    // A backslash escapes any character, not only wildcards
    assertEquals("ab", IcebergPredicateConverter.getRegexLiteral("a\\b", -1));

    // The backend drops a trailing lone backslash
    assertEquals("a%b", IcebergPredicateConverter.getRegexLiteral("a\\%b\\", -1));

    assertEquals("", IcebergPredicateConverter.getRegexLiteral("", -1));
    assertEquals("\\", IcebergPredicateConverter.getRegexLiteral("\\\\", -1));
    assertEquals("%", IcebergPredicateConverter.getRegexLiteral("\\%", -1));
    assertEquals("_", IcebergPredicateConverter.getRegexLiteral("\\_", -1));
  }

  /**
   * Places one token at the beginning, in the middle and at the end of 'ab'. The token
   * is an unescaped, escaped '%' or '_', a normal character, an escaped normal character,
   * an escaped backslash or a lone backslash.
   */
  @Test
  public void testLikeTokenPositions() {
    // Beginning
    assertFailed("%ab");
    assertFailed("_ab");
    assertFull("\\%ab", Operation.EQ, "%ab");
    assertFull("\\_ab", Operation.EQ, "_ab");
    assertFull("xab", Operation.EQ, "xab");
    // Patterns without '%' and '_' are matched verbatim
    assertFull("\\xab", Operation.EQ, "\\xab");
    assertFull("\\\\ab", Operation.EQ, "\\\\ab");
    assertFull("\\ab", Operation.EQ, "\\ab");

    // Middle
    assertPartial("a%b", "a");
    assertPartial("a_b", "a");
    assertFull("a\\%b", Operation.EQ, "a%b");
    assertFull("a\\_b", Operation.EQ, "a_b");
    assertFull("axb", Operation.EQ, "axb");
    assertFull("a\\xb", Operation.EQ, "a\\xb");
    assertFull("a\\\\b", Operation.EQ, "a\\\\b");
    assertFull("a\\b", Operation.EQ, "a\\b");

    // End
    assertFull("ab%", Operation.STARTS_WITH, "ab");
    assertPartial("ab_", "ab");
    assertFull("ab\\%", Operation.EQ, "ab%");
    assertFull("ab\\_", Operation.EQ, "ab_");
    assertFull("abx", Operation.EQ, "abx");
    assertFull("ab\\x", Operation.EQ, "ab\\x");
    assertFull("ab\\\\", Operation.EQ, "ab\\\\");
    assertFull("ab\\", Operation.EQ, "ab\\");
  }

  /**
   * Combines prefixes that contain escapes with different wildcard suffixes. 'prefix%'
   * patterns are matched verbatim by the backend, all other patterns with an unescaped
   * wildcard go through its regex conversion.
   */
  @Test
  public void testLikePrefixAndWildcardSuffix() {
    assertFull("ab%", Operation.STARTS_WITH, "ab");
    assertFull("ab%%", Operation.STARTS_WITH, "ab");
    assertFull("ab%\\", Operation.STARTS_WITH, "ab");
    assertPartial("ab_", "ab");
    assertPartial("ab_%", "ab");
    assertPartial("ab%_", "ab");
    assertPartial("ab%c", "ab");
    assertPartial("ab%\\%", "ab");
    assertPartial("ab%\\\\", "ab");

    // Escaped normal character
    assertFull("a\\b%", Operation.STARTS_WITH, "a\\b");
    assertFull("a\\b%%", Operation.STARTS_WITH, "a\\b");
    assertFull("a\\b%\\", Operation.STARTS_WITH, "ab");
    assertPartial("a\\b_", "ab");
    assertPartial("a\\b_%", "ab");
    assertPartial("a\\b%c", "ab");

    // Escaped backslash
    assertFull("a\\\\b%", Operation.STARTS_WITH, "a\\\\b");
    assertFull("a\\\\b%%", Operation.STARTS_WITH, "a\\\\b");
    assertFull("a\\\\b%\\", Operation.STARTS_WITH, "a\\b");
    assertPartial("a\\\\b_", "a\\b");
    assertPartial("a\\\\b_%", "a\\b");
    assertPartial("a\\\\b%c", "a\\b");

    // Escaped '%'
    assertFull("a\\%b%", Operation.STARTS_WITH, "a%b");
    assertFull("a\\%b%%", Operation.STARTS_WITH, "a%b");
    assertFull("a\\%b%\\", Operation.STARTS_WITH, "a%b");
    assertPartial("a\\%b_", "a%b");
    assertPartial("a\\%b_%", "a%b");
    assertPartial("a\\%b%c", "a%b");

    // Escaped '_'
    assertFull("a\\_b%", Operation.STARTS_WITH, "a_b");
    assertFull("a\\_b%%", Operation.STARTS_WITH, "a_b");
    assertFull("a\\_b%\\", Operation.STARTS_WITH, "a_b");
    assertPartial("a\\_b_", "a_b");
    assertPartial("a\\_b_%", "a_b");
    assertPartial("a\\_b%c", "a_b");
  }

  /** Patterns that hit special cases of the backend's LIKE implementation. */
  @Test
  public void testLikeBackendEdgeCases() {
    // Only wildcards
    assertFailed("%");
    assertFailed("%%");
    assertFailed("_");
    assertFailed("%ab%");
    // Matched verbatim: 'a\' followed by '%%'.
    assertFull("a\\%%", Operation.STARTS_WITH, "a\\");
    // Ends in '\%', so it goes through the regex conversion where '\\' is a backslash.
    assertFull("a\\\\%", Operation.STARTS_WITH, "a\\");
    assertFull("a\\\\\\%", Operation.EQ, "a\\%");
    // The newline makes the backend's ends-with-'\%' check fail, so it is verbatim.
    assertFull("a\nb\\%", Operation.STARTS_WITH, "a\nb\\");
    // The trailing lone backslash is dropped.
    assertFull("a\\%b\\", Operation.EQ, "a%b");
    assertPartial("a\\%b_\\", "a%b");
    assertFull("a\\%b\\_", Operation.EQ, "a%b_");
    // UTF-8
    assertFull("árvíz%", Operation.STARTS_WITH, "árvíz");
    assertPartial("árvíz%p", "árvíz");
  }

  @Test
  public void testCompoundPredicates() {
    // NOT and OR of other predicates are still pushed down.
    assertEquals(ConversionStatus.FULLY_CONVERTED, status(not(eq("ab"))));
    assertEquals(ConversionStatus.FULLY_CONVERTED, status(not(not(eq("ab")))));
    assertEquals(ConversionStatus.FULLY_CONVERTED, status(or(eq("ab"), eq("x"))));

    // A LIKE anywhere below a NOT or an OR is not pushed down.
    assertEquals(ConversionStatus.FAILED, status(not(not(like("ab%")))));
    assertEquals(ConversionStatus.FAILED, status(not(and(like("ab%"), eq("x")))));
    assertEquals(ConversionStatus.FAILED, status(not(or(eq("x"), like("ab")))));
    assertEquals(ConversionStatus.FAILED, status(not(and(like("a%b"), eq("x")))));
    assertEquals(ConversionStatus.FAILED, status(and(not(like("ab%")), eq("x"))));
    assertEquals(ConversionStatus.FAILED, status(or(like("ab%"), eq("x"))));
    assertEquals(ConversionStatus.FAILED, status(or(eq("x"), like("a%c"))));
    assertEquals(ConversionStatus.FAILED, status(or(like("ab%"), like("ab"))));
    assertEquals(ConversionStatus.FAILED,
        status(or(and(like("ab%"), eq("x")), eq("y"))));
    assertEquals(ConversionStatus.FAILED,
        status(and(or(like("ab%"), eq("x")), eq("y"))));

    // AND propagates the status of its children.
    assertEquals(ConversionStatus.FULLY_CONVERTED,
        status(and(like("ab%"), not(eq("x")))));
    assertEquals(ConversionStatus.FULLY_CONVERTED,
        status(and(like("ab%"), or(eq("x"), eq("y")))));
    assertEquals(ConversionStatus.PARTIALLY_CONVERTED,
        status(and(like("a%b"), not(eq("x")))));
    assertEquals(ConversionStatus.PARTIALLY_CONVERTED,
        status(and(like("ab%"), like("ab_"))));
  }

  /** In partition statements LIKE must apply to the partition transform. */
  @Test
  public void testLikeOnTransformTerm() {
    IcebergPredicateConverter converter = new TestConverter() {
      @Override
      protected Term getTerm(Expr expr) {
        return new Term(Expressions.truncate(STRING_COL.getName(), 3), STRING_COL);
      }
    };
    for (String pattern : new String[] {"abc", "a\\%b", "ab%", "a%c"}) {
      ConverterResult result = converter.convert(like(pattern));
      UnboundPredicate<?> pred = (UnboundPredicate<?>) result.getIcebergExpression();
      assertTrue(pattern, pred.term() instanceof UnboundTransform);
    }
  }

  /**
   * Constant folded literals are sent to the backend without unescaping, so their
   * backslashes are not escapes. Non UTF-8 literals can't be converted.
   */
  @Test
  public void testFoldedAndNonUtf8Literals() {
    ConverterResult result = converter_.convert(like(foldedLit("a\\b%")));
    assertEquals(ConversionStatus.FULLY_CONVERTED, result.getStatus());
    UnboundPredicate<?> pred = (UnboundPredicate<?>) result.getIcebergExpression();
    assertEquals(Operation.STARTS_WITH, pred.op());
    assertEquals("a\\b", pred.literal().value().toString());

    result = converter_.convert(like(foldedLit("a\\b%c")));
    assertEquals(ConversionStatus.PARTIALLY_CONVERTED, result.getStatus());
    pred = (UnboundPredicate<?>) result.getIcebergExpression();
    assertEquals("ab", pred.literal().value().toString());

    result = converter_.convert(eq(foldedLit("a\\bc")));
    assertEquals(ConversionStatus.FULLY_CONVERTED, result.getStatus());
    pred = (UnboundPredicate<?>) result.getIcebergExpression();
    assertEquals(Operation.EQ, pred.op());
    assertEquals("a\\bc", pred.literal().value().toString());

    assertEquals(ConversionStatus.FAILED, status(like(nonUtf8Lit())));
    assertEquals(ConversionStatus.FAILED, status(eq(nonUtf8Lit())));
  }
}
