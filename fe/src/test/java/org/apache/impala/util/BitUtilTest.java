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

import org.junit.Test;

/**
 * Unit tests for BitUtil functions.
 */
public class BitUtilTest {

  // Test the log2/PowerOf2 functions
  @Test
  public void testPowersOf2() {
    assertEquals(8, BitUtil.log2Ceiling(256)); // Power-of-two
    assertEquals(256, BitUtil.roundUpToPowerOf2(256)); // Power-of-two
    assertEquals(4, BitUtil.log2Ceiling(10)); // Rounds up
    assertEquals(16, BitUtil.roundUpToPowerOf2(10)); // Rounds up
    assertEquals(33, BitUtil.log2Ceiling(8L * 1000L * 1000L * 1000L)); // > 32bit number
    assertEquals(8L * 1024L * 1024L * 1024L,
        BitUtil.roundUpToPowerOf2(8L * 1000L * 1000L * 1000L)); // > 32bit number

    assertEquals(0, BitUtil.log2Ceiling(1)); // Minimum valid input
    assertEquals(1, BitUtil.roundUpToPowerOf2(1)); // Minimum valid input
    assertEquals(63, BitUtil.log2Ceiling(Long.MAX_VALUE)); // Maximum valid input
    // Overflow to -2^62.
    assertEquals(-0x8000000000000000L, BitUtil.roundUpToPowerOf2(Long.MAX_VALUE));
    // Maximum non-overflow output: 2^62.
    assertEquals(0x8000000000000000L, BitUtil.roundUpToPowerOf2(0x8000000000000000L - 1));
  }

  @Test
  public void testPowerOf2Factor() {
    assertEquals(BitUtil.roundUpToPowerOf2Factor(7, 8), 8);
    assertEquals(BitUtil.roundUpToPowerOf2Factor(8, 8), 8);
    assertEquals(BitUtil.roundUpToPowerOf2Factor(9, 8), 16);
  }
}
