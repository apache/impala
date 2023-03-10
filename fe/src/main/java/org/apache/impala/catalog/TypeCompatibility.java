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

package org.apache.impala.catalog;

import com.google.common.base.Preconditions;
/**
 Enumeration to decide the mode of the compatibility lookup for type comparisons.
 */
public enum TypeCompatibility {
  /**
   * Allow implicit casts between string types (STRING, VARCHAR, CHAR) and between numeric
   * types and string types. From STRING to [VAR]CHAR types truncation may happen.
   */
  UNSAFE,
  /**
   * Baseline compatibility between types
   */
  DEFAULT,
  /**
   * Only consider casts that result in no loss of information when casting between
   * decimal types.
   */
  STRICT_DECIMAL,
  /**
   * Only consider casts that result in no loss of information when casting between any
   * two types other than both decimals
   */
  STRICT,
  /**
   * only consider casts that result in no loss of information when casting between any
   * types
   */
  ALL_STRICT;

  public static TypeCompatibility applyStrictDecimal(TypeCompatibility compatibility) {
    switch (compatibility) {
      case DEFAULT:
      case STRICT_DECIMAL: return STRICT_DECIMAL;
      case STRICT:
      case ALL_STRICT: return ALL_STRICT;
      case UNSAFE: return UNSAFE;
      default:
        // Unreachable state
        Preconditions.checkState(false);
        return compatibility;
    }
  }

  public boolean isDefault() { return this.equals(DEFAULT); }

  public boolean isUnsafe() { return this.equals(UNSAFE); }

  public boolean isStrict() { return this.equals(STRICT) || this.equals(ALL_STRICT); }

  public boolean isStrictDecimal() {
    return this.equals(STRICT_DECIMAL) || this.equals(ALL_STRICT);
  }
}
