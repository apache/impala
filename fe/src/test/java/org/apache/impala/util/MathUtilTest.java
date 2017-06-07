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
 * Unit tests for MathUtil functions.
 */
public class MathUtilTest {

  @Test
  public void testSaturatingMultiply() {
    // Positive * positive
    assertEquals(10, MathUtil.saturatingMultiply(2, 5));
    assertEquals(Long.MAX_VALUE, MathUtil.saturatingMultiply(2, Long.MAX_VALUE));
    assertEquals(Long.MAX_VALUE, MathUtil.saturatingMultiply(3, Long.MAX_VALUE / 2));

    // Positive * negative
    assertEquals(-10, MathUtil.saturatingMultiply(2, -5));
    assertEquals(Long.MIN_VALUE, MathUtil.saturatingMultiply(2, Long.MIN_VALUE));
    assertEquals(Long.MIN_VALUE, MathUtil.saturatingMultiply(-3, Long.MAX_VALUE / 2));

    // Negative * negative
    assertEquals(10, MathUtil.saturatingMultiply(-2, -5));
    assertEquals(Long.MAX_VALUE, MathUtil.saturatingMultiply(-1, Long.MIN_VALUE));
    assertEquals(Long.MAX_VALUE, MathUtil.saturatingMultiply(Long.MIN_VALUE / 10, -100));
  }

  @Test
  public void testSaturatingAdd() {
    // No overflow
    assertEquals(1234, MathUtil.saturatingAdd(1200, 34));
    assertEquals(-1, MathUtil.saturatingAdd(Long.MAX_VALUE, Long.MIN_VALUE));

    // Underflow
    assertEquals(Long.MIN_VALUE, MathUtil.saturatingAdd(Long.MIN_VALUE, -1));
    assertEquals(Long.MIN_VALUE, MathUtil.saturatingAdd(Long.MIN_VALUE, Long.MIN_VALUE / 2));

    // Overflow
    assertEquals(Long.MAX_VALUE, MathUtil.saturatingAdd(Long.MAX_VALUE - 10, 11));
    assertEquals(Long.MAX_VALUE, MathUtil.saturatingAdd(Long.MAX_VALUE, Long.MAX_VALUE / 2));
  }
}
