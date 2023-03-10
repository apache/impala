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

package org.apache.impala.analysis;

import java.math.BigDecimal;

import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.TypeCompatibility;
import org.apache.impala.common.AnalysisException;
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
  public static int getDecimalSlotSize(ScalarType type) {
    Preconditions.checkState(type.isDecimal() && !type.isWildcardDecimal());
    if (type.decimalPrecision() <= 9) return 4;
    if (type.decimalPrecision() <= 18) return 8;
    return 16;
  }

  /**
   * Returns the decimal type that can hold t1 and t2 without loss of precision.
   * decimal(10, 2) && decimal(12, 2) -> decimal(12, 2)
   * decimal (10, 5) && decimal(12, 3) -> decimal(14, 5)
   * Either t1 or t2 can be a wildcard decimal (but not both).
   *
   * If strictDecimal is true, returns a type that results in no loss of information. If
   * this is not possible, returns INVLAID. For example,
   * decimal(38,0) && decimal(38,38) -> INVALID.
   */
  public static ScalarType getDecimalAssignmentCompatibleType(
      ScalarType t1, ScalarType t2, boolean strictDecimal) {
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
    if (strictDecimal) {
      if (digitsBefore + digitsAfter > 38) return Type.INVALID;
      return ScalarType.createDecimalType(digitsBefore + digitsAfter, digitsAfter);
    }
    return ScalarType.createClippedDecimalType(digitsBefore + digitsAfter, digitsAfter);
  }

  /**
   * Returns the necessary result type for t1 op t2. Throws an analysis exception
   * if the operation does not make sense for the types.
   */
  public static Type getArithmeticResultType(Type t1, Type t2,
      ArithmeticExpr.Operator op, boolean decimalV2) throws AnalysisException {
    Preconditions.checkState(t1.isNumericType() || t1.isNull());
    Preconditions.checkState(t2.isNumericType() || t2.isNull());

    if (t1.isNull() && t2.isNull()) return Type.NULL;

    if (t1.isDecimal() || t2.isDecimal()) {
      if (t1.isNull()) return t2;
      if (t2.isNull()) return t1;

      // For multiplications involving at least one floating point type we cast decimal to
      // double in order to prevent decimals from overflowing.
      if (op == ArithmeticExpr.Operator.MULTIPLY &&
          (t1.isFloatingPointType() || t2.isFloatingPointType())) {
        return Type.DOUBLE;
      }

      t1 = ((ScalarType) t1).getMinResolutionDecimal();
      t2 = ((ScalarType) t2).getMinResolutionDecimal();
      Preconditions.checkState(t1.isDecimal());
      Preconditions.checkState(t2.isDecimal());
      return getDecimalArithmeticResultType(t1, t2, op, decimalV2);
    }

    Type type = null;
    switch (op) {
      case MULTIPLY:
      case ADD:
      case SUBTRACT:
        // If one of the types is null, use the compatible type without promotion.
        // Otherwise, promote the compatible type to the next higher resolution type,
        // to ensure that a <op> b won't overflow/underflow.
        Type compatibleType =
            ScalarType.getAssignmentCompatibleType(t1, t2, TypeCompatibility.DEFAULT);
        Preconditions.checkState(compatibleType.isScalarType());
        type = ((ScalarType) compatibleType).getNextResolutionType();
        break;
      case MOD:
        type = ScalarType.getAssignmentCompatibleType(t1, t2, TypeCompatibility.DEFAULT);
        break;
      case DIVIDE:
        type = Type.DOUBLE;
        break;
      default:
        throw new AnalysisException("Invalid op: " + op);
    }
    Preconditions.checkState(type.isValid());
    return type;
  }

  /**
   * Returns the result type for (t1 op t2) where t1 and t2 are both DECIMAL, depending
   * on whether DECIMAL version 1 or DECIMAL version 2 is enabled.
   *
   * TODO: IMPALA-4924: remove DECIMAL V1 code.
   */
  private static ScalarType getDecimalArithmeticResultType(Type t1, Type t2,
      ArithmeticExpr.Operator op, boolean decimalV2) throws AnalysisException {
    if (decimalV2) return getDecimalArithmeticResultTypeV2(t1, t2, op);
    return getDecimalArithmeticResultTypeV1(t1, t2, op);
  }

  /**
   * Returns the result type for (t1 op t2) where t1 and t2 are both DECIMAL, used when
   * DECIMAL version 1 is enabled.
   *
   * These rules were mostly taken from the (pre Dec 2016) Hive / sql server rules with
   * some changes.
   * http://blogs.msdn.com/b/sqlprogrammability/archive/2006/03/29/564110.aspx
   * https://msdn.microsoft.com/en-us/library/ms190476.aspx
   *
   * Changes:
   *  - Multiply does not need +1 for the result precision.
   *  - Divide scale truncation is different. When the maximum precision is exceeded,
   *    unlike SQL server, we do not try to preserve at least a scale of 6.
   *  - For other operators, when maximum precision is exceeded, we clip the precision
   *    and scale at maximum precision rather tha reducing scale. Therefore, we
   *    sacrifice digits to the left of the decimal point before sacrificing digits to
   *    the right.
   */
  private static ScalarType getDecimalArithmeticResultTypeV1(Type t1, Type t2,
      ArithmeticExpr.Operator op) throws AnalysisException {
    Preconditions.checkState(t1.isFullySpecifiedDecimal());
    Preconditions.checkState(t2.isFullySpecifiedDecimal());
    ScalarType st1 = (ScalarType) t1;
    ScalarType st2 = (ScalarType) t2;
    int s1 = st1.decimalScale();
    int s2 = st2.decimalScale();
    int p1 = st1.decimalPrecision();
    int p2 = st2.decimalPrecision();
    int sMax = Math.max(s1, s2);

    switch (op) {
      case ADD:
      case SUBTRACT:
        return ScalarType.createClippedDecimalType(
            sMax + Math.max(p1 - s1, p2 - s2) + 1, sMax);
      case MULTIPLY:
        return ScalarType.createClippedDecimalType(p1 + p2, s1 + s2);
      case DIVIDE:
        int resultScale = Math.max(DECIMAL_DIVISION_SCALE_INCREMENT, s1 + p2 + 1);
        int resultPrecision = p1 - s1 + s2 + resultScale;
        if (resultPrecision > ScalarType.MAX_PRECISION) {
          // In this case, the desired resulting precision exceeds the maximum and
          // we need to truncate some way. We can either remove digits before or
          // after the decimal and there is no right answer. This is an implementation
          // detail and different databases will handle this differently.
          // For simplicity, we will set the resulting scale to be the max of the input
          // scales and use the maximum precision.
          resultScale = Math.max(s1, s2);
          resultPrecision = ScalarType.MAX_PRECISION;
        }
        return ScalarType.createClippedDecimalType(resultPrecision, resultScale);
      case MOD:
        return ScalarType.createClippedDecimalType(
            Math.min(p1 - s1, p2 - s2) + sMax, sMax);
      default:
        throw new AnalysisException(
            "Operation '" + op + "' is not allowed for decimal types.");
    }
  }

  /**
   * Returns the result type for (t1 op t2) where t1 and t2 are both DECIMAL, used when
   * DECIMAL version 2 is enabled.
   *
   * These rules are similar to (post Dec 2016) Hive / sql server rules.
   * http://blogs.msdn.com/b/sqlprogrammability/archive/2006/03/29/564110.aspx
   * https://msdn.microsoft.com/en-us/library/ms190476.aspx
   *
   * Changes:
   *  - There are slight difference with how precision/scale reduction occurs compared
   *    to SQL server when the desired precision is more than the maximum supported
   *    precision.  But an algorithm of reducing scale to a minimum of 6 is used.
   */
  private static ScalarType getDecimalArithmeticResultTypeV2(Type t1, Type t2,
      ArithmeticExpr.Operator op) throws AnalysisException {
    Preconditions.checkState(t1.isFullySpecifiedDecimal());
    Preconditions.checkState(t2.isFullySpecifiedDecimal());
    ScalarType st1 = (ScalarType) t1;
    ScalarType st2 = (ScalarType) t2;
    int s1 = st1.decimalScale();
    int s2 = st2.decimalScale();
    int p1 = st1.decimalPrecision();
    int p2 = st2.decimalPrecision();
    int resultScale;
    int resultPrecision;

    switch (op) {
      case DIVIDE:
        // Divide result always gets at least MIN_ADJUSTED_SCALE decimal places.
        resultScale = Math.max(ScalarType.MIN_ADJUSTED_SCALE, s1 + p2 + 1);
        resultPrecision = p1 - s1 + s2 + resultScale;
        break;
      case MOD:
        resultScale = Math.max(s1, s2);
        resultPrecision = Math.min(p1 - s1, p2 - s2) + resultScale;
        break;
      case MULTIPLY:
        resultScale = s1 + s2;
        resultPrecision = p1 + p2 + 1;
        break;
      case ADD:
      case SUBTRACT:
        resultScale = Math.max(s1, s2);
        resultPrecision = Math.max(p1 - s1, p2 - s2) + resultScale + 1;
        break;
      default:
        Preconditions.checkState(false);
        return null;
    }
    // Use the scale reduction technique when resultPrecision is too large.
    return ScalarType.createAdjustedDecimalType(resultPrecision, resultScale);
  }

  /**
   * Computes the ColumnType that can represent 'v' with no loss of resolution.
   * The scale/precision in BigDecimal is not compatible with SQL decimal semantics
   * (much more like significant figures and exponent).
   * Returns null if the value cannot be represented.
   */
  public static Type computeDecimalType(BigDecimal v) {
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
    if (digitsAfter > ScalarType.MAX_SCALE) return null;
    if (digitsBefore + digitsAfter > ScalarType.MAX_PRECISION) return null;
    if (digitsBefore == 0 && digitsAfter == 0) digitsBefore = 1;
    return ScalarType.createDecimalType(digitsBefore + digitsAfter, digitsAfter);
  }
}
