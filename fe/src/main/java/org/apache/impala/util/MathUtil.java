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

import com.google.common.base.Preconditions;
import com.google.common.math.LongMath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MathUtil {
  private final static Logger LOG = LoggerFactory.getLogger(MathUtil.class);

  // Multiply two numbers. If the multiply would overflow, return either Long.MIN_VALUE
  // (if a xor b is negative) or Long.MAX_VALUE otherwise.
  public static long saturatingMultiply(long a, long b) {
    return LongMath.saturatedMultiply(a, b);
  }

  // Add two numbers. If the add would overflow, return either Long.MAX_VALUE if both are
  // positive or Long.MIN_VALUE if both are negative.
  public static long saturatingAdd(long a, long b) {
    return LongMath.saturatedAdd(a, b);
  }

  /**
   * Computes and returns the sum of two cardinality numbers. If an overflow occurs,
   * the maximum Long value is returned (Long.MAX_VALUE).
   * Both number should be a valid cardinality number (>= -1).
   * Return -1 if any argument is -1.
   */
  public static long addCardinalities(long cardinality1, long cardinality2) {
    Preconditions.checkArgument(
        cardinality1 >= -1, "cardinality1 is invalid: %s", cardinality1);
    Preconditions.checkArgument(
        cardinality2 >= -1, "cardinality2 is invalid: %s", cardinality2);
    if (cardinality1 == -1 || cardinality2 == -1) return -1;
    try {
      return LongMath.checkedAdd(cardinality1, cardinality2);
    } catch (ArithmeticException e) {
      LOG.warn("overflow when adding longs: " + cardinality1 + ", " + cardinality2);
      return Long.MAX_VALUE;
    }
  }

  /**
   * Computes and returns the product of two cardinality numbers. If an overflow
   * occurs, the maximum Long value is returned (Long.MAX_VALUE).
   * Both number should be a valid cardinality number (>= -1).
   * Return -1 if any argument is -1.
   */
  public static long multiplyCardinalities(long cardinality1, long cardinality2) {
    Preconditions.checkArgument(
        cardinality1 >= -1, "cardinality1 is invalid: %s", cardinality1);
    Preconditions.checkArgument(
        cardinality2 >= -1, "cardinality2 is invalid: %s", cardinality2);
    if (cardinality1 == -1 || cardinality2 == -1) return -1;
    try {
      return LongMath.checkedMultiply(cardinality1, cardinality2);
    } catch (ArithmeticException e) {
      LOG.warn("overflow when multiplying longs: " + cardinality1 + ", " + cardinality2);
      return Long.MAX_VALUE;
    }
  }

  /**
   * Return the least between 'cardinality1' and 'cardinality2'
   * that is not a negative number (unknown).
   * Can return -1 if both number is less than 0.
   * Both argument should not be < -1.
   */
  public static long smallestValidCardinality(long cardinality1, long cardinality2) {
    Preconditions.checkArgument(
        cardinality1 >= -1, "cardinality1 is invalid: %s", cardinality1);
    Preconditions.checkArgument(
        cardinality2 >= -1, "cardinality2 is invalid: %s", cardinality2);
    if (cardinality1 >= 0) {
      if (cardinality2 >= 0) return Math.min(cardinality1, cardinality2);
      return cardinality1;
    }
    return Math.max(-1, cardinality2);
  }
}
