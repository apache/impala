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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.impala.common.PrintUtils;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * Unit tests for PrintUtils functions.
 */
public class PrintUtilsTest {

  @Test
  public void testPrintBytes() {
    assertEquals("0B", PrintUtils.printBytes(0L));
    assertEquals("55B", PrintUtils.printBytes(55L));
    assertEquals("1023B", PrintUtils.printBytes(1023L));
    assertEquals("1.00KB", PrintUtils.printBytes(1024L));
    assertEquals("54.16KB", PrintUtils.printBytes(55463L));
    assertEquals("54.16KB", PrintUtils.printBytes(55463L));
    // The exact threshold before rounding is used to choose the unit.
    // TODO: this behaviour seems fairly harmless but wrong
    assertEquals("1024.00KB", PrintUtils.printBytes(1024L * 1024L - 1L));
    assertEquals("1.00MB", PrintUtils.printBytes(1024L * 1024L));
    assertEquals("1.00MB", PrintUtils.printBytes(1024L * 1024L + 10L));
    assertEquals("1.00GB", PrintUtils.printBytes(1024L * 1024L * 1024L));
    assertEquals("1.50GB", PrintUtils.printBytes((long)(1024L * 1024L * 1024L * 1.5)));
    assertEquals("4.00TB", PrintUtils.printBytes(1024L * 1024L * 1024L * 1024L * 4L));
    assertEquals("8.42PB",
        PrintUtils.printBytes((long)(1024L * 1024L * 1024L * 1024L * 1024L * 8.42)));

    // Negative values always get bytes as unit.
    // TODO: fix this behaviour if needed.
    assertEquals("-10B", PrintUtils.printBytes(-10L));
    assertEquals("-123456789B", PrintUtils.printBytes(-123456789L));
  }

  @Test
  public void testPrintBytesRoundedToMb() {
    assertEquals("0B", PrintUtils.printBytesRoundedToMb(0L));
    assertEquals("55B", PrintUtils.printBytesRoundedToMb(55L));
    assertEquals("1023B", PrintUtils.printBytesRoundedToMb(1023L));
    assertEquals("1KB", PrintUtils.printBytesRoundedToMb(1024L));
    assertEquals("54KB", PrintUtils.printBytesRoundedToMb(55463L));
    assertEquals("54KB", PrintUtils.printBytesRoundedToMb(55463L));
    // The exact threshold before rounding is used to choose the unit.
    // TODO: this behaviour seems fairly harmless but wrong
    assertEquals("1024KB", PrintUtils.printBytesRoundedToMb(1024L * 1024L - 1L));
    assertEquals("1MB", PrintUtils.printBytesRoundedToMb(1024L * 1024L));
    assertEquals("1MB", PrintUtils.printBytesRoundedToMb(1024L * 1024L + 10L));
    assertEquals("1.00GB", PrintUtils.printBytesRoundedToMb(1024L * 1024L * 1024L));
    assertEquals("1.50GB",
        PrintUtils.printBytesRoundedToMb((long)(1024L * 1024L * 1024L * 1.5)));
    assertEquals("4.00TB",
        PrintUtils.printBytesRoundedToMb(1024L * 1024L * 1024L * 1024L * 4L));
    assertEquals("8.42PB", PrintUtils.printBytesRoundedToMb(
        (long)(1024L * 1024L * 1024L * 1024L * 1024L * 8.42)));

    // Negative values always get bytes as unit.
    // TODO: fix this behaviour if needed.
    assertEquals("-10B", PrintUtils.printBytesRoundedToMb(-10L));
    assertEquals("-123456789B", PrintUtils.printBytesRoundedToMb(-123456789L));
  }

  /**
   * Wrap length for testWrapText() - less than 80 to make test layout nicer.
   */
  private static final int WRAP_LENGTH = 60;

  /**
   * Test for PrintUtils.wrapString().
   */
  @Test
  public void testWrapText() {
    // Simple query wrapping.
    assertWrap(
        "Analyzed query: SELECT * FROM functional_kudu.alltypestiny WHERE CAST(bigint_col"
            + " AS DOUBLE) < CAST(10 AS DOUBLE)",
        "Analyzed query: SELECT * FROM functional_kudu.alltypestiny\n"
            + "WHERE CAST(bigint_col AS DOUBLE) < CAST(10 AS DOUBLE)");
    // test that a long string of blanks prints OK, some may be lost for clarity
    assertWrap("insert into foo values ('                                      "
            + "                                                                          "
            + "                                ')",
        "insert into foo values ('                                   \n"
            + "')");
    // test that long words are broken up for clarity
    assertWrap("select xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        "select\n"
            + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n"
            + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n"
            + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n"
            + "xxxxxxxxxxxxxxxxxxxxxxxxx");
  }

  /**
   * Check that code that has been wrapped is correctly formatted.
   * @param expected what it should be
   */
  private void assertWrap(String input, String expected) {
    String actual = PrintUtils.wrapString(input, WRAP_LENGTH);
    assertEquals(expected, actual);
    assertNoBlankLines(actual);
    assertNoTerminatingNewline(actual);
    assertNoLongLines(actual);
  }

  /**
   * Assert that all lines of wrapped output are 80 chars or less.
   */
  private void assertNoLongLines(String s) {
    for (String line : s.split("\n")) {
      assertTrue("line too long: " + line, line.length() <= WRAP_LENGTH);
    }
  }

  /**
   * Assert that the wrapped output does not end in a newline.
   */
  private void assertNoTerminatingNewline(String s) {
    assertFalse("wrapped string ends in newline: " + s, s.endsWith("\n"));
  }

  /**
   * Assert that there are no blank lines embedded in the wrapped output.
   */
  private void assertNoBlankLines(String s) {
    assertFalse("output contains blank line " + s, s.contains("\n\n"));
  }

  @Test
  public void testJoinQuoted() {
    assertEquals("", PrintUtils.joinQuoted(ImmutableList.of()));
    assertEquals("'a'", PrintUtils.joinQuoted(ImmutableList.of("a")));
    assertEquals("'a', 'b'", PrintUtils.joinQuoted(ImmutableList.of("a", "b")));
  }

  /**
   * Test for PrintUtils.printTimeNs && PrintUtils.printTimeMs
   */
  @Test
  public void testPrintTime() {
    assertEquals("", PrintUtils.printTimeNs(-1));
    assertEquals("0ns", PrintUtils.printTimeNs(0));
    assertEquals("1ns", PrintUtils.printTimeNs(1));
    assertEquals("10ns", PrintUtils.printTimeNs(10));
    assertEquals("100ns", PrintUtils.printTimeNs(100));
    assertEquals("1.000us", PrintUtils.printTimeNs(1000));
    assertEquals("10.000us", PrintUtils.printTimeNs(10000));
    assertEquals("100.000us", PrintUtils.printTimeNs(100000));
    assertEquals("1.000ms", PrintUtils.printTimeNs(1000000));
    assertEquals("10.000ms", PrintUtils.printTimeNs(10000000));
    assertEquals("100.000ms", PrintUtils.printTimeNs(100000000));
    assertEquals("1s", PrintUtils.printTimeNs(1000000000));
    assertEquals("10s", PrintUtils.printTimeNs(10000000000L));
    assertEquals("1m40s", PrintUtils.printTimeNs(100000000000L));
    assertEquals("16m40s", PrintUtils.printTimeNs(1000000000000L));
    assertEquals("2h46m", PrintUtils.printTimeNs(10000000000000L));
    assertEquals("27h46m", PrintUtils.printTimeNs(100000000000000L));
  }
}
