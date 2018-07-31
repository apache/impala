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

import static org.junit.Assert.*;

import org.apache.impala.common.PrintUtils;
import org.junit.Test;

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
}
