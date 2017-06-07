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

import com.google.common.math.LongMath;

public class MathUtil {

  // Multiply two numbers. If the multiply would overflow, return either Long.MIN_VALUE
  // (if a xor b is negative) or Long.MAX_VALUE otherwise. The overflow path is not
  // optimised at all and may be somewhat slow.
  public static long saturatingMultiply(long a, long b) {
    try {
      return LongMath.checkedMultiply(a, b);
    } catch (ArithmeticException e) {
      return a < 0 != b < 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
    }
  }

  // Add two numbers. If the add would overflow, return either Long.MAX_VALUE if both are
  // positive or Long.MIN_VALUE if both are negative. The overflow path is not optimised
  // at all and may be somewhat slow.
  public static long saturatingAdd(long a, long b) {
    try {
      return LongMath.checkedAdd(a, b);
    } catch (ArithmeticException e) {
      return a < 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
    }
  }
}
