// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.analysis;

import java.math.BigDecimal;

import com.cloudera.impala.catalog.ColumnType;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Preconditions;

// Utility class for handling types.
public class TypesUtil {
  // The sql standard specifies that the scale after division is incremented
  // by a system wide constant. Hive picked 4 so we will as well.
  // TODO: how did they pick this?
  static final int DECIMAL_DIVISION_SCALE_INCREMENT = 4;

  /**
   * [1-9] precision -> 4 bytes
   * [10-18] precision -> 8 bytes
   * [19-38] precision -> 16 bytes
   * TODO: Support 12 byte decimal?
   * For precision [20-28], we could support a 12 byte decimal but currently a 12
   * byte decimal in the BE is not implemented.
   */
  public static int getDecimalSlotSize(ColumnType type) {
    Preconditions.checkState(type.isFullySpecifiedDecimal());
    if (type.decimalPrecision() <= 9) return 4;
    if (type.decimalPrecision() <= 18) return 8;
    return 16;
  }

  /**
   * Returns the smallest integer type that can store decType without loss
   * of precision. decType must have scale == 0.
   * In the case where the decimal can be bigger than BIGINT, we return
   * BIGINT (and the execution will report it as overflows).
   */
  public static ColumnType getContainingIntType(ColumnType decType) {
    Preconditions.checkState(decType.isFullySpecifiedDecimal());
    Preconditions.checkState(decType.decimalScale() == 0);
    // TINYINT_MAX = 128
    if (decType.decimalPrecision() <= 2) return ColumnType.TINYINT;
    // SMALLINT_MAX = 32768
    if (decType.decimalPrecision() <= 4) return ColumnType.SMALLINT;
    // INT_MAX = 2147483648
    if (decType.decimalPrecision() <= 9) return ColumnType.INT;
    return ColumnType.BIGINT;
  }

  /**
   * Returns the decimal type that can hold t1 and t2 without loss of precision.
   * decimal(10, 2) && decimal(12, 2) -> decimal(12, 2)
   * decimal (10, 5) && decimal(12, 3) -> decimal(14, 5)
   * Either t1 or t2 can be a wildcard decimal (but not both).
   */
  public static ColumnType getDecimalAssignmentCompatibleType(
      ColumnType t1, ColumnType t2) {
    Preconditions.checkState(t1.isDecimal());
    Preconditions.checkState(t2.isDecimal());
    Preconditions.checkState(!(t1.isWildcardDecimal() && t2.isWildcardDecimal()));
    if (t1.isWildcardDecimal()) return t2;
    if (t2.isWildcardDecimal()) return t1;

    Preconditions.checkState(t1.isFullySpecifiedDecimal());
    Preconditions.checkState(t2.isFullySpecifiedDecimal());
    if (t1.equals(t2)) return t1;
    int s1 = t1.decimalScale();
    int s2 = t2.decimalScale();
    int p1 = t1.decimalPrecision();
    int p2 = t2.decimalPrecision();
    int digitsBefore = Math.max(p1 - s1, p2 - s2);
    int digitsAfter = Math.max(s1, s2);
    return ColumnType.createDecimalTypeInternal(digitsBefore + digitsAfter, digitsAfter);
  }

  /**
   * Returns the necessary result type for t1 op t2. Throws an analysis exception
   * if the operation does not make sense for the types.
   */
  public static ColumnType getArithmeticResultType(ColumnType t1, ColumnType t2,
      ArithmeticExpr.Operator op) throws AnalysisException {
    Preconditions.checkState(t1.isNumericType() || t1.isNull());
    Preconditions.checkState(t2.isNumericType() || t2.isNull());

    if (t1.isNull() && t2.isNull()) return ColumnType.NULL;

    if (t1.isDecimal() || t2.isDecimal()) {
      if (t1.isNull()) return t2;
      if (t2.isNull()) return t1;

      t1 = t1.getMinResolutionDecimal();
      t2 = t2.getMinResolutionDecimal();
      Preconditions.checkState(t1.isDecimal());
      Preconditions.checkState(t2.isDecimal());
      return getDecimalArithmeticResultType(t1, t2, op);
    }

    ColumnType type = null;
    switch (op) {
      case MULTIPLY:
      case ADD:
      case SUBTRACT:
        // If one of the types is null, use the compatible type without promotion.
        // Otherwise, promote the compatible type to the next higher resolution type,
        // to ensure that that a <op> b won't overflow/underflow.
        type = ColumnType.getAssignmentCompatibleType(t1, t2).getNextResolutionType();
        break;
      case MOD:
        type = ColumnType.getAssignmentCompatibleType(t1, t2);
        break;
      case DIVIDE:
        type = ColumnType.DOUBLE;
        break;
      default:
        throw new AnalysisException("Invalid op: " + op);
    }
    Preconditions.checkState(type.isValid());
    return type;
  }

  /**
   * Returns the resulting typical type from (t1 op t2)
   * These rules are identical to the hive/sql server rules.
   * http://blogs.msdn.com/b/sqlprogrammability/archive/2006/03/29/564110.aspx
   */
  public static ColumnType getDecimalArithmeticResultType(ColumnType t1, ColumnType t2,
      ArithmeticExpr.Operator op) throws AnalysisException {
    Preconditions.checkState(t1.isFullySpecifiedDecimal());
    Preconditions.checkState(t2.isFullySpecifiedDecimal());
    int s1 = t1.decimalScale();
    int s2 = t2.decimalScale();
    int p1 = t1.decimalPrecision();
    int p2 = t2.decimalPrecision();
    int sMax = Math.max(s1, s2);

    switch (op) {
      case ADD:
      case SUBTRACT:
        return ColumnType.createDecimalTypeInternal(
            sMax + Math.max(p1 - s1, p2 - s2) + 1, sMax);
      case MULTIPLY:
        return ColumnType.createDecimalTypeInternal(p1 + p2 + 1, s1 + s2);
      case DIVIDE:
        int resultScale = Math.max(DECIMAL_DIVISION_SCALE_INCREMENT, s1 + p2 + 1);
        int resultPrecision = p1 - s1 + s2 + resultScale;
        if (resultPrecision > ColumnType.MAX_PRECISION) {
          // In this case, the desired resulting precision exceeds the maximum and
          // we need to truncate some way. We can either remove digits before or
          // after the decimal and there is no right answer. This is an implementation
          // detail and different databases will handle this differently.
          // For simplicity, we will set the resulting scale to be the max of the input
          // scales and use the maximum precision.
          resultScale = Math.max(s1, s2);
          resultPrecision = ColumnType.MAX_PRECISION;
        }
        return ColumnType.createDecimalTypeInternal(resultPrecision, resultScale);
      case MOD:
        return ColumnType.createDecimalTypeInternal(
            Math.min(p1 - s1, p2 - s2) + sMax, sMax);
      default:
        throw new AnalysisException(
            "Operation '" + op + "' is not allowed for decimal types.");
    }
  }

  /**
   * Computes the ColumnType that can represent 'v' with no loss of resolution.
   * The scale/precision in BigDecimal is not compatible with SQL decimal semantics
   * (much more like significant figures and exponent).
   * Returns null if the value cannot be represented.
   */
  public static ColumnType computeDecimalType(BigDecimal v) {
    // PlainString returns the string with no exponent. We walk it to compute
    // the digits before and after.
    // TODO: better way?
    String str = v.toPlainString();
    int digitsBefore = 0;
    int digitsAfter = 0;
    boolean decimalFound = false;
    boolean leadingZeros = true;
    for (int i = 0; i < str.length(); ++i) {
      char c = str.charAt(i);
      if (c == '-') continue;
      if (c == '.') {
        decimalFound = true;
        continue;
      }
      if (decimalFound) {
        ++digitsAfter;
      } else {
        // Strip out leading 0 before the decimal point. We want "0.1" to
        // be parsed as ".1" (1 digit instead of 2).
        if (c == '0' && leadingZeros) continue;
        leadingZeros = false;
        ++digitsBefore;
      }
    }
    if (digitsAfter > ColumnType.MAX_SCALE) return null;
    if (digitsBefore + digitsAfter > ColumnType.MAX_PRECISION) return null;
    if (digitsBefore == 0 && digitsAfter == 0) digitsBefore = 1;
    return ColumnType.createDecimalType(digitsBefore + digitsAfter, digitsAfter);
  }
}
