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

import org.apache.commons.lang3.StringUtils;
import org.apache.impala.analysis.TypesUtil;
import org.apache.impala.thrift.TColumnType;
import org.apache.impala.thrift.TScalarType;
import org.apache.impala.thrift.TTypeNode;
import org.apache.impala.thrift.TTypeNodeType;

import com.google.common.base.Preconditions;

/**
 * Describes a scalar type. For most types this class just wraps a PrimitiveType enum,
 * but for types like CHAR and DECIMAL, this class contain additional information.
 *
 * Scalar types have a few ways they can be compared to other scalar types. They can be:
 *   1. completely identical,
 *   2. implicitly castable (convertible without loss of precision)
 *   3. subtype. For example, in the case of decimal, a type can be decimal(*, *)
 *   indicating that any decimal type is a subtype of the decimal type.
 */
public class ScalarType extends Type {
  private final PrimitiveType type_;

  // Used for fixed-length types parameterized by size, i.e. CHAR, VARCHAR and
  // FIXED_UDA_INTERMEDIATE.
  private int len_;

  // Only used if type is DECIMAL. -1 (for both) is used to represent a
  // decimal with any precision and scale.
  // It is invalid to have one by -1 and not the other.
  // TODO: we could use that to store DECIMAL(8,*), indicating a decimal
  // with 8 digits of precision and any valid ([0-8]) scale.
  private int precision_;
  private int scale_;

  // SQL allows the engine to pick the default precision. We pick the largest
  // precision that is supported by the smallest decimal type in the BE (4 bytes).
  public static final int DEFAULT_PRECISION = 9;
  public static final int DEFAULT_SCALE = 0; // SQL standard

  // Longest supported VARCHAR and CHAR, chosen to match Hive.
  public static final int MAX_VARCHAR_LENGTH = (1 << 16) - 1; // 65535
  public static final int MAX_CHAR_LENGTH = (1 << 8) - 1; // 255

  // Hive, mysql, sql server standard.
  public static final int MAX_PRECISION = 38;
  public static final int MAX_SCALE = MAX_PRECISION;
  public static final int MIN_ADJUSTED_SCALE = 6;

  protected ScalarType(PrimitiveType type) {
    type_ = type;
  }

  public static ScalarType createType(PrimitiveType type) {
    switch (type) {
      case INVALID_TYPE: return INVALID;
      case NULL_TYPE: return NULL;
      case BOOLEAN: return BOOLEAN;
      case SMALLINT: return SMALLINT;
      case TINYINT: return TINYINT;
      case INT: return INT;
      case BIGINT: return BIGINT;
      case FLOAT: return FLOAT;
      case DOUBLE: return DOUBLE;
      case STRING: return STRING;
      case VARCHAR: return createVarcharType();
      case BINARY: return BINARY;
      case TIMESTAMP: return TIMESTAMP;
      case DATE: return DATE;
      case DATETIME: return DATETIME;
      case DECIMAL: return (ScalarType) createDecimalType();
      default:
        Preconditions.checkState(false);
        return NULL;
    }
  }

  public static ScalarType createCharType(int len) {
    ScalarType type = new ScalarType(PrimitiveType.CHAR);
    type.len_ = len;
    return type;
  }

  public static ScalarType createFixedUdaIntermediateType(int len) {
    ScalarType type = new ScalarType(PrimitiveType.FIXED_UDA_INTERMEDIATE);
    type.len_ = len;
    return type;
  }

  public static ScalarType createDecimalType() { return DEFAULT_DECIMAL; }

  public static ScalarType createDecimalType(int precision) {
    return createDecimalType(precision, DEFAULT_SCALE);
  }

  /**
   * Returns a DECIMAL type with the specified precision and scale.
   */
  public static ScalarType createDecimalType(int precision, int scale) {
    Preconditions.checkState(precision >= 0); // Enforced by parser
    Preconditions.checkState(scale >= 0); // Enforced by parser.
    ScalarType type = new ScalarType(PrimitiveType.DECIMAL);
    type.precision_ = precision;
    type.scale_ = scale;
    return type;
  }

  /**
   * Returns a DECIMAL wildcard type (i.e. precision and scale hasn't yet been resolved).
   */
  public static ScalarType createWildCardDecimalType() {
    ScalarType type = new ScalarType(PrimitiveType.DECIMAL);
    type.precision_ = -1;
    type.scale_ = -1;
    return type;
  }

  /**
   * Returns a DECIMAL type with the specified precision and scale, but truncating the
   * precision to the max storable precision (i.e. removes digits from before the
   * decimal point).
   */
  public static ScalarType createClippedDecimalType(int precision, int scale) {
    Preconditions.checkState(precision >= 0);
    Preconditions.checkState(scale >= 0);
    ScalarType type = new ScalarType(PrimitiveType.DECIMAL);
    type.precision_ = Math.min(precision, MAX_PRECISION);
    type.scale_ = Math.min(type.precision_, scale);
    return type;
  }

  /**
   * Returns a DECIMAL type with the specified precision and scale. When the given
   * precision exceeds the max storable precision, reduce both precision and scale but
   * preserve at least MIN_ADJUSTED_SCALE for scale (unless the desired scale was less).
   */
  public static ScalarType createAdjustedDecimalType(int precision, int scale) {
    Preconditions.checkState(precision >= 0);
    Preconditions.checkState(scale >= 0);
    if (precision > MAX_PRECISION) {
      int minScale = Math.min(scale, MIN_ADJUSTED_SCALE);
      int delta = precision - MAX_PRECISION;
      precision = MAX_PRECISION;
      scale = Math.max(scale - delta, minScale);
    }
    ScalarType type = new ScalarType(PrimitiveType.DECIMAL);
    type.precision_ = precision;
    type.scale_ = scale;
    return type;
  }

  public static ScalarType createVarcharType(int len) {
    // length checked in analysis
    ScalarType type = new ScalarType(PrimitiveType.VARCHAR);
    type.len_ = len;
    return type;
  }

  public static ScalarType createVarcharType() {
    return DEFAULT_VARCHAR;
  }

  @Override
  public String toString() {
    if (type_ == PrimitiveType.CHAR) {
      if (isWildcardChar()) return "CHAR(*)";
      return "CHAR(" + len_ + ")";
    } else  if (type_ == PrimitiveType.DECIMAL) {
      if (isWildcardDecimal()) return "DECIMAL(*,*)";
      return "DECIMAL(" + precision_ + "," + scale_ + ")";
    } else if (type_ == PrimitiveType.VARCHAR) {
      if (isWildcardVarchar()) return "VARCHAR(*)";
      return "VARCHAR(" + len_ + ")";
    } else if (type_ == PrimitiveType.FIXED_UDA_INTERMEDIATE) {
      return "FIXED_UDA_INTERMEDIATE(" + len_ + ")";
    }
    return type_.toString();
  }

  @Override
  public String toSql(int depth) {
    if (depth >= MAX_NESTING_DEPTH) return "...";
    switch(type_) {
      case BINARY: return type_.toString();
      case VARCHAR:
      case CHAR:
      case FIXED_UDA_INTERMEDIATE:
         return type_.toString() + "(" + len_ + ")";
      case DECIMAL:
        return String.format("%s(%s,%s)", type_.toString(), precision_, scale_);
      default: return type_.toString();
    }
  }

  @Override
  protected String prettyPrint(int lpad) {
    return StringUtils.repeat(' ', lpad) + toSql();
  }

  @Override
  public void toThrift(TColumnType container) {
    TTypeNode node = new TTypeNode();
    container.types.add(node);
    switch(type_) {
      case VARCHAR:
      case CHAR:
      case FIXED_UDA_INTERMEDIATE: {
        node.setType(TTypeNodeType.SCALAR);
        TScalarType scalarType = new TScalarType();
        scalarType.setType(type_.toThrift());
        scalarType.setLen(len_);
        node.setScalar_type(scalarType);
        break;
      }
      case DECIMAL: {
        node.setType(TTypeNodeType.SCALAR);
        TScalarType scalarType = new TScalarType();
        scalarType.setType(type_.toThrift());
        scalarType.setScale(scale_);
        scalarType.setPrecision(precision_);
        node.setScalar_type(scalarType);
        break;
      }
      default: {
        node.setType(TTypeNodeType.SCALAR);
        TScalarType scalarType = new TScalarType();
        scalarType.setType(type_.toThrift());
        node.setScalar_type(scalarType);
        break;
      }
    }
  }

  public int decimalPrecision() {
    Preconditions.checkState(type_ == PrimitiveType.DECIMAL);
    return precision_;
  }

  public int decimalScale() {
    Preconditions.checkState(type_ == PrimitiveType.DECIMAL);
    return scale_;
  }

  @Override
  public PrimitiveType getPrimitiveType() { return type_; }
  public int ordinal() { return type_.ordinal(); }
  public int getLength() { return len_; }

  @Override
  public boolean isWildcardDecimal() {
    return type_ == PrimitiveType.DECIMAL && precision_ == -1 && scale_ == -1;
  }

  @Override
  public boolean isWildcardVarchar() {
    return type_ == PrimitiveType.VARCHAR && len_ == -1;
  }

  @Override
  public boolean isWildcardChar() {
    return type_ == PrimitiveType.CHAR && len_ == -1;
  }

  /**
   *  Returns true if this type is a fully specified (not wild card) decimal.
   */
  @Override
  public boolean isFullySpecifiedDecimal() {
    if (!isDecimal()) return false;
    if (isWildcardDecimal()) return false;
    if (precision_ <= 0 || precision_ > MAX_PRECISION) return false;
    if (scale_ < 0 || scale_ > precision_) return false;
    return true;
  }

  @Override
  public boolean isFixedLengthType() {
    return type_ == PrimitiveType.BOOLEAN || type_ == PrimitiveType.TINYINT
        || type_ == PrimitiveType.SMALLINT || type_ == PrimitiveType.INT
        || type_ == PrimitiveType.BIGINT || type_ == PrimitiveType.FLOAT
        || type_ == PrimitiveType.DOUBLE || type_ == PrimitiveType.DATE
        || type_ == PrimitiveType.DATETIME || type_ == PrimitiveType.TIMESTAMP
        || type_ == PrimitiveType.CHAR || type_ == PrimitiveType.DECIMAL
        || type_ == PrimitiveType.FIXED_UDA_INTERMEDIATE;
  }

  @Override
  public boolean isSupported() {
    return isValid() && !getUnsupportedTypes().contains(this);
  }

  @Override
  public boolean supportsTablePartitioning() {
    if (!isSupported() || isComplexType() || type_ == PrimitiveType.TIMESTAMP) {
      return false;
    }
    return true;
  }

  @Override
  public int getSlotSize() {
    switch (type_) {
      case CHAR:
      case FIXED_UDA_INTERMEDIATE:
        return len_;
      case DECIMAL: return TypesUtil.getDecimalSlotSize(this);
      default:
        return type_.getSlotSize();
    }
  }

  /**
   * Returns true if this object is of type t.
   * Handles wildcard types. That is, if t is the wildcard type variant
   * of 'this', returns true.
   */
  @Override
  public boolean matchesType(Type t) {
    if (equals(t)) return true;
    if (!t.isScalarType()) return false;
    ScalarType scalarType = (ScalarType) t;
    if (type_ == PrimitiveType.VARCHAR && scalarType.isWildcardVarchar()) {
      Preconditions.checkState(!isWildcardVarchar());
      return true;
    }
    if (type_ == PrimitiveType.CHAR && scalarType.isWildcardChar()) {
      Preconditions.checkState(!isWildcardChar());
      return true;
    }
    if (isDecimal() && scalarType.isWildcardDecimal()) {
      Preconditions.checkState(!isWildcardDecimal());
      return true;
    }
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ScalarType)) return false;
    ScalarType other = (ScalarType)o;
    if (type_ != other.type_) return false;
    if (type_ == PrimitiveType.CHAR || type_ == PrimitiveType.FIXED_UDA_INTERMEDIATE) {
      return len_ == other.len_;
    }
    if (type_ == PrimitiveType.VARCHAR) return len_ == other.len_;
    if (type_ == PrimitiveType.DECIMAL) {
      return precision_ == other.precision_ && scale_ == other.scale_;
    }
    return true;
  }

  public Type getMaxResolutionType() {
    if (isIntegerType()) {
      return ScalarType.BIGINT;
    // Timestamps get summed as DOUBLE for AVG.
    } else if (isFloatingPointType() || type_ == PrimitiveType.TIMESTAMP) {
      return ScalarType.DOUBLE;
    } else if (isNull()) {
      return ScalarType.NULL;
    } else if (isDecimal()) {
      return createClippedDecimalType(MAX_PRECISION, scale_);
    } else {
      return ScalarType.INVALID;
    }
  }

  public ScalarType getNextResolutionType() {
    Preconditions.checkState(isNumericType() || isNull());
    if (type_ == PrimitiveType.DOUBLE || type_ == PrimitiveType.BIGINT || isNull()) {
      return this;
    } else if (type_ == PrimitiveType.DECIMAL) {
      return createClippedDecimalType(MAX_PRECISION, scale_);
    }
    return createType(PrimitiveType.values()[type_.ordinal() + 1]);
  }

  /**
   * Returns the smallest decimal type that can safely store this type. Returns
   * INVALID if this type cannot be stored as a decimal.
   */
  public ScalarType getMinResolutionDecimal() {
    switch (type_) {
      case NULL_TYPE: return Type.NULL;
      case DECIMAL: return this;
      case TINYINT: return createDecimalType(3);
      case SMALLINT: return createDecimalType(5);
      case INT: return createDecimalType(10);
      case BIGINT: return createDecimalType(19);
      case FLOAT: return createDecimalType(MAX_PRECISION, 9);
      case DOUBLE: return createDecimalType(MAX_PRECISION, 17);
      default: return ScalarType.INVALID;
    }
  }

  /**
   * Returns true if this decimal type is a supertype of the other decimal type.
   * e.g. (10,3) is a supertype of (3,3) but (5,4) is not a supertype of (3,0).
   * To be a super type of another decimal, the number of digits before and after
   * the decimal point must be greater or equal.
   */
  public boolean isSupertypeOf(ScalarType o) {
    Preconditions.checkState(isDecimal());
    Preconditions.checkState(o.isDecimal());
    if (isWildcardDecimal()) return true;
    if (o.isWildcardDecimal()) return false;
    return scale_ >= o.scale_ && precision_ - scale_ >= o.precision_ - o.scale_;
  }

  /**
   * Return type t such that values from both t1 and t2 can be assigned to t.
   * If strict, only return types when there will be no loss of precision.
   * Returns INVALID_TYPE if there is no such type or if any of t1 and t2
   * is INVALID_TYPE.
   */
  public static ScalarType getAssignmentCompatibleType(ScalarType t1,
      ScalarType t2, boolean strict) {
    if (!t1.isValid() || !t2.isValid()) return INVALID;
    if (t1.equals(t2)) return t1;
    if (t1.isNull()) return t2;
    if (t2.isNull()) return t1;

    if (t1.type_ == PrimitiveType.VARCHAR || t2.type_ == PrimitiveType.VARCHAR) {
      if (t1.type_ == PrimitiveType.STRING || t2.type_ == PrimitiveType.STRING) {
        return STRING;
      }
      if (t1.isStringType() && t2.isStringType()) {
        return createVarcharType(Math.max(t1.len_, t2.len_));
      }
      return INVALID;
    }

    if (t1.type_ == PrimitiveType.CHAR || t2.type_ == PrimitiveType.CHAR) {
      Preconditions.checkState(t1.type_ != PrimitiveType.VARCHAR);
      Preconditions.checkState(t2.type_ != PrimitiveType.VARCHAR);
      if (t1.type_ == PrimitiveType.STRING || t2.type_ == PrimitiveType.STRING) {
        return STRING;
      }
      if (t1.type_ == PrimitiveType.CHAR && t2.type_ == PrimitiveType.CHAR) {
        return createCharType(Math.max(t1.len_, t2.len_));
      }
      return INVALID;
    }

    if (t1.isDecimal() || t2.isDecimal()) {
      // The case of decimal and float/double must be handled carefully. There are two
      // modes: strict and non-strict. In non-strict mode, we convert to the floating
      // point type, since it can contain a larger range of values than any decimal (but
      // has lower precision in some parts of its range), so it is generally better.
      // In strict mode, we avoid conversion in either direction because there are also
      // decimal values (e.g. 0.1) that cannot be exactly represented in binary
      // floating point.
      // TODO: it might make sense to promote to double in many cases, but this would
      // require more work elsewhere to avoid breaking things, e.g. inserting decimal
      // literals into float columns.
      if (t1.isFloatingPointType()) return strict ? INVALID : t1;
      if (t2.isFloatingPointType()) return strict ? INVALID : t2;

      // Allow casts between decimal and numeric types by converting
      // numeric types to the containing decimal type.
      ScalarType t1Decimal = t1.getMinResolutionDecimal();
      ScalarType t2Decimal = t2.getMinResolutionDecimal();
      if (t1Decimal.isInvalid() || t2Decimal.isInvalid()) return Type.INVALID;
      Preconditions.checkState(t1Decimal.isDecimal());
      Preconditions.checkState(t2Decimal.isDecimal());

      if (t1Decimal.equals(t2Decimal)) {
        Preconditions.checkState(!(t1.isDecimal() && t2.isDecimal()));
        // The containing decimal type for a non-decimal type is always an exclusive
        // upper bound, therefore the decimal has higher precision.
        return t1Decimal;
      }
      if (t1Decimal.isSupertypeOf(t2Decimal)) return t1;
      if (t2Decimal.isSupertypeOf(t1Decimal)) return t2;
      return TypesUtil.getDecimalAssignmentCompatibleType(t1Decimal, t2Decimal);
    }

    PrimitiveType smallerType =
        (t1.type_.ordinal() < t2.type_.ordinal() ? t1.type_ : t2.type_);
    PrimitiveType largerType =
        (t1.type_.ordinal() > t2.type_.ordinal() ? t1.type_ : t2.type_);
    PrimitiveType result = null;
    if (strict) {
      result = strictCompatibilityMatrix[smallerType.ordinal()][largerType.ordinal()];
    }
    if (result == null) {
      result = compatibilityMatrix[smallerType.ordinal()][largerType.ordinal()];
    }
    Preconditions.checkNotNull(result);
    return createType(result);
  }

  /**
   * Returns true t1 can be implicitly cast to t2, false otherwise.
   * If strict is true, only consider casts that result in no loss of precision.
   */
  public static boolean isImplicitlyCastable(ScalarType t1, ScalarType t2,
      boolean strict) {
    return getAssignmentCompatibleType(t1, t2, strict).matchesType(t2);
  }
}
