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

import java.util.ArrayList;
import java.util.List;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TColumnType;
import com.cloudera.impala.thrift.TPrimitiveType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Class wrapping a type of a column. For most types, this will just be a wrapper
 * around an enum but for types like CHAR(n) and decimal, this will contain additional
 * information.
 */
public class ColumnType {
  private final PrimitiveType type_;

  // Unused if type_ is always the same length.
  private int len_;

  // Only used if type is DECIMAL
  private int precision_;
  private int scale_;

  static final int DEFAULT_PRECISION = 10; // Hive and mysql standard
  static final int DEFAULT_SCALE = 0; // SQL standard

  // Hive, mysql, sql server standard.
  static final int MAX_PRECISION = 38;

  // Static constant types for simple types that don't require additional information.
  public static final ColumnType INVALID = new ColumnType(PrimitiveType.INVALID_TYPE);
  public static final ColumnType NULL = new ColumnType(PrimitiveType.NULL_TYPE);
  public static final ColumnType BOOLEAN = new ColumnType(PrimitiveType.BOOLEAN);
  public static final ColumnType TINYINT = new ColumnType(PrimitiveType.TINYINT);
  public static final ColumnType SMALLINT = new ColumnType(PrimitiveType.SMALLINT);
  public static final ColumnType INT = new ColumnType(PrimitiveType.INT);
  public static final ColumnType BIGINT = new ColumnType(PrimitiveType.BIGINT);
  public static final ColumnType FLOAT = new ColumnType(PrimitiveType.FLOAT);
  public static final ColumnType DOUBLE = new ColumnType(PrimitiveType.DOUBLE);
  public static final ColumnType STRING = new ColumnType(PrimitiveType.STRING);
  public static final ColumnType BINARY = new ColumnType(PrimitiveType.BINARY);
  public static final ColumnType TIMESTAMP = new ColumnType(PrimitiveType.TIMESTAMP);
  public static final ColumnType DATE = new ColumnType(PrimitiveType.DATE);
  public static final ColumnType DATETIME = new ColumnType(PrimitiveType.DATETIME);

  private static ArrayList<ColumnType> fixedSizeNumericTypes;
  private static ArrayList<ColumnType> fixedPointTypes;
  private static ArrayList<ColumnType> numericTypes;
  private static ArrayList<ColumnType> nativeTypes;
  private static ArrayList<ColumnType> supportedTypes;

  static {
    fixedSizeNumericTypes = Lists.newArrayList();
    fixedSizeNumericTypes.add(TINYINT);
    fixedSizeNumericTypes.add(SMALLINT);
    fixedSizeNumericTypes.add(INT);
    fixedSizeNumericTypes.add(BIGINT);
    fixedSizeNumericTypes.add(FLOAT);
    fixedSizeNumericTypes.add(DOUBLE);

    fixedPointTypes = Lists.newArrayList();
    fixedPointTypes.add(TINYINT);
    fixedPointTypes.add(SMALLINT);
    fixedPointTypes.add(INT);
    fixedPointTypes.add(BIGINT);

    numericTypes = Lists.newArrayList();
    numericTypes.add(TINYINT);
    numericTypes.add(SMALLINT);
    numericTypes.add(INT);
    numericTypes.add(BIGINT);
    numericTypes.add(FLOAT);
    numericTypes.add(DOUBLE);

    nativeTypes = Lists.newArrayList();
    nativeTypes.add(BOOLEAN);
    nativeTypes.add(TINYINT);
    nativeTypes.add(SMALLINT);
    nativeTypes.add(INT);
    nativeTypes.add(BIGINT);
    nativeTypes.add(FLOAT);
    nativeTypes.add(DOUBLE);

    supportedTypes = Lists.newArrayList();
    supportedTypes.add(NULL);
    supportedTypes.add(BOOLEAN);
    supportedTypes.add(TINYINT);
    supportedTypes.add(SMALLINT);
    supportedTypes.add(INT);
    supportedTypes.add(BIGINT);
    supportedTypes.add(FLOAT);
    supportedTypes.add(DOUBLE);
    supportedTypes.add(STRING);
    supportedTypes.add(TIMESTAMP);
  }

  private ColumnType(PrimitiveType type) {
    type_ = type;
  }

  public static ColumnType createType(PrimitiveType type) {
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
      case BINARY: return BINARY;
      case TIMESTAMP: return TIMESTAMP;
      case DATE: return DATE;
      case DATETIME: return DATETIME;
      case DECIMAL: return createDecimalType();
      default:
        Preconditions.checkState(false);
        return NULL;
    }
  }

  public static ColumnType createCharType(int len) {
    ColumnType type = new ColumnType(PrimitiveType.CHAR);
    type.len_ = len;
    return type;
  }

  public static ColumnType createDecimalType() {
    return createDecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
  }

  public static ColumnType createDecimalType(int precision) {
    return createDecimalType(precision, DEFAULT_SCALE);
  }

  public static ColumnType createDecimalType(int precision, int scale) {
    ColumnType type = new ColumnType(PrimitiveType.DECIMAL);
    type.precision_ = precision;
    type.scale_ = scale;
    return type;
  }

  public static ArrayList<ColumnType> getFixedSizeNumericTypes() {
    return fixedSizeNumericTypes;
  }
  public static ArrayList<ColumnType> getFixedPointTypes() {
    return fixedPointTypes;
  }
  public static ArrayList<ColumnType> getNumericTypes() {
    return numericTypes;
  }
  public static ArrayList<ColumnType> getNativeTypes() {
    return nativeTypes;
  }
  public static ArrayList<ColumnType> getSupportedTypes() {
    return supportedTypes;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ColumnType)) return false;
    ColumnType other = (ColumnType)o;
    if (type_ != other.type_) return false;
    if (type_ == PrimitiveType.CHAR) return len_ == other.len_;
    if (type_ == PrimitiveType.DECIMAL) {
      return precision_ == other.precision_ && scale_ == other.scale_;
    }
    return true;
  }

  public final PrimitiveType getPrimitiveType() { return type_; }
  public boolean isNull() { return getPrimitiveType() == PrimitiveType.NULL_TYPE; }
  public boolean isValid() { return getPrimitiveType() != PrimitiveType.INVALID_TYPE; }
  public int ordinal() { return type_.ordinal(); }

  public boolean isBoolean() { return type_ == PrimitiveType.BOOLEAN; }

  public boolean isFixedPointType() {
    return type_ == PrimitiveType.TINYINT || type_ == PrimitiveType.SMALLINT ||
           type_ == PrimitiveType.INT || type_ == PrimitiveType.BIGINT ||
           type_ == PrimitiveType.DECIMAL;
  }

  public boolean isFloatingPointType() {
    return type_ == PrimitiveType.FLOAT || type_ == PrimitiveType.DOUBLE;
  }

  public boolean isIntegerType() {
    return type_ == PrimitiveType.TINYINT || type_ == PrimitiveType.SMALLINT
      || type_ == PrimitiveType.INT || type_ == PrimitiveType.BIGINT;
  }

  public boolean isFixedLengthType() {
    return type_ == PrimitiveType.BOOLEAN || type_ == PrimitiveType.TINYINT
        || type_ == PrimitiveType.SMALLINT || type_ == PrimitiveType.INT
        || type_ == PrimitiveType.BIGINT || type_ == PrimitiveType.FLOAT
        || type_ == PrimitiveType.DOUBLE || type_ == PrimitiveType.DATE
        || type_ == PrimitiveType.DATETIME || type_ == PrimitiveType.TIMESTAMP
        || type_ == PrimitiveType.CHAR || type_ == PrimitiveType.DECIMAL;
  }

  public boolean isNumericType() {
    return isFixedPointType() || isFloatingPointType() || type_ == PrimitiveType.DECIMAL;
  }

  public boolean isDateType() {
    return type_ == PrimitiveType.DATE || type_ == PrimitiveType.DATETIME
        || type_ == PrimitiveType.TIMESTAMP;
  }

  public boolean isStringType() { return type_ == PrimitiveType.STRING; }

  public ColumnType getMaxResolutionType() {
    if (isIntegerType()) {
      return ColumnType.BIGINT;
    // Timestamps get summed as DOUBLE for AVG.
    } else if (isFloatingPointType() || type_ == PrimitiveType.TIMESTAMP) {
      return ColumnType.DOUBLE;
    } else if (isNull()) {
      return ColumnType.NULL;
    } else {
      return ColumnType.INVALID;
    }
  }

  public ColumnType getNextResolutionType() {
    Preconditions.checkState(isNumericType() || isNull());
    if (type_ == PrimitiveType.DOUBLE || type_ == PrimitiveType.BIGINT || isNull()) {
      return this;
    }
    return createType(PrimitiveType.values()[type_.ordinal() + 1]);
  }

  /**
   * Return type t such that values from both t1 and t2 can be assigned to t
   * without loss of precision. Returns INVALID_TYPE if there is no such type
   * or if any of t1 and t2 is INVALID_TYPE.
   */
  public static ColumnType getAssignmentCompatibleType(ColumnType t1,
      ColumnType t2) {
    if (!t1.isValid() || !t2.isValid()) return ColumnType.INVALID;

    PrimitiveType smallerType =
        (t1.type_.ordinal() < t2.type_.ordinal() ? t1.type_ : t2.type_);
    PrimitiveType largerType =
        (t1.type_.ordinal() > t2.type_.ordinal() ? t1.type_ : t2.type_);
    PrimitiveType result =
        compatibilityMatrix[smallerType.ordinal()][largerType.ordinal()];
    Preconditions.checkNotNull(result);
    return createType(result);
  }

  /**
   * Returns if it is compatible to implicitly cast from t1 to t2 (casting from
   * t1 to t2 results in no loss of precision).
   */
  public static boolean isImplicitlyCastable(ColumnType t1, ColumnType t2) {
    return getAssignmentCompatibleType(t1, t2).equals(t2);
  }

  public boolean isSupported() {
    switch (type_) {
      case DATE:
      case DATETIME:
      case BINARY:
      case DECIMAL:
        return false;
      default:
        return true;
    }
  }

  /**
   * Indicates whether we support partitioning tables on columns of this type.
   */
  public boolean supportsTablePartitioning() {
    if (!isSupported() || type_ == PrimitiveType.TIMESTAMP || type_ == PrimitiveType.CHAR) {
      return false;
    }
    return true;
  }

  public int getSlotSize() {
    switch (type_) {
    case CHAR:
      return len_;
    default:
      return type_.getSlotSize();
    }
  }

  public void analyze() throws AnalysisException {
    Preconditions.checkState(type_ != PrimitiveType.INVALID_TYPE);
    if (type_ == PrimitiveType.CHAR) {
      if (len_ <= 0) {
        throw new AnalysisException("Array size must be > 0. Size was set to: " +
            len_ + ".");
      }
    } else if (type_ == PrimitiveType.DECIMAL) {
      if (precision_ <= 0) {
        throw new AnalysisException("Decimal precision must be > 0.");
      }
      if (precision_ > MAX_PRECISION) {
        throw new AnalysisException("Decimal precision must be <= " + MAX_PRECISION + ".");
      }
      if (scale_ <= 0) {
        throw new AnalysisException("Decimal scale must be > 0.");
      }
      if (scale_ >= precision_) {
        throw new AnalysisException("Decimal scale(" + scale_+ ") must be less than " +
            " precision(" + precision_ + ").");
      }
    }
  }

  @Override
  public String toString() {
    if (type_ == PrimitiveType.CHAR) return "CHAR(" + len_ + ")";
    return type_.toString();
  }

  public TColumnType toThrift() {
    TColumnType thrift = new TColumnType();
    thrift.type = type_.toThrift();
    if (type_ == PrimitiveType.CHAR) thrift.setLen(len_);
    return thrift;
  }

  public static List<TColumnType> toThrift(ColumnType[] types) {
    return toThrift(Lists.newArrayList(types));
  }

  public static List<TColumnType> toThrift(ArrayList<ColumnType> types) {
    ArrayList<TColumnType> result = Lists.newArrayList();
    for (ColumnType t: types) {
      result.add(t.toThrift());
    }
    return result;
  }

  public static ArrayList<TPrimitiveType> toTPrimitiveTypes(ColumnType[] types) {
    ArrayList<TPrimitiveType> result = Lists.newArrayList();
    for (ColumnType t: types) {
      result.add(t.getPrimitiveType().toThrift());
    }
    return result;
  }

  public static ColumnType[] toColumnType(PrimitiveType[] types) {
    ColumnType result[] = new ColumnType[types.length];
    for (int i = 0; i < types.length; ++i) {
      result[i] = createType(types[i]);
    }
    return result;
  }

  public static ColumnType fromThrift(TColumnType thrift) {
    PrimitiveType type = PrimitiveType.fromThrift(thrift.type);
    if (type == PrimitiveType.CHAR) {
      Preconditions.checkState(thrift.isSetLen());
      return createCharType(thrift.len);
    } else {
      return createType(type);
    }
  }


  /**
   * JDBC data type description
   * Returns the column size for this type.
   * For numeric data this is the maximum precision.
   * For character data this is the length in characters.
   * For datetime types this is the length in characters of the String representation
   * (assuming the maximum allowed precision of the fractional seconds component).
   * For binary data this is the length in bytes.
   * Null is returned for for data types where the column size is not applicable.
   */
  public Integer getColumnSize() {
    if (isNumericType()) return getPrecision();
    switch (type_) {
      case STRING:
        return Integer.MAX_VALUE;
      case TIMESTAMP:
        return 30;
      default:
        return null;
    }
  }

  /**
   * JDBC data type description
   * For numeric types, returns the maximum precision for this type.
   * For non-numeric types, returns null.
   */
  public Integer getPrecision() {
    switch (type_) {
      case TINYINT:
        return 3;
      case SMALLINT:
        return 5;
      case INT:
        return 10;
      case BIGINT:
        return 19;
      case FLOAT:
        return 7;
      case DOUBLE:
        return 15;
      default:
        return null;
    }
  }

  /**
   * JDBC data type description
   * Returns the number of fractional digits for this type, or null if not applicable.
   */
  public Integer getDecimalDigits() {
    switch (type_) {
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case INT:
      case BIGINT:
        return 0;
      case FLOAT:
        return 7;
      case DOUBLE:
        return 15;
      default:
        return null;
    }
  }

  /**
   * JDBC data type description
   * Returns the radix for this type (typically either 2 or 10) or null if not applicable.
   */
  public Integer getNumPrecRadix() {
    switch (type_) {
      case TINYINT:
      case SMALLINT:
      case INT:
      case BIGINT:
        return 10;
      case FLOAT:
      case DOUBLE:
        return 2;
      default:
        // everything else (including boolean and string) is null
        return null;
    }
  }

  /**
   * JDBC data type description
   * Returns the java SQL type enum
   */
  public int getJavaSQLType() {
    switch (type_) {
      case NULL_TYPE: return java.sql.Types.NULL;
      case BOOLEAN: return java.sql.Types.BOOLEAN;
      case TINYINT: return java.sql.Types.TINYINT;
      case SMALLINT: return java.sql.Types.SMALLINT;
      case INT: return java.sql.Types.INTEGER;
      case BIGINT: return java.sql.Types.BIGINT;
      case FLOAT: return java.sql.Types.FLOAT;
      case DOUBLE: return java.sql.Types.DOUBLE;
      case TIMESTAMP: return java.sql.Types.TIMESTAMP;
      case STRING: return java.sql.Types.VARCHAR;
      case BINARY: return java.sql.Types.BINARY;
      case DECIMAL: return java.sql.Types.DECIMAL;
      default:
        Preconditions.checkArgument(false, "Invalid type " + type_.name());
        return 0;
    }
  }

  /**
   * Matrix that records "smallest" assignment-compatible type of two types
   * (INVALID_TYPE if no such type exists, ie, if the input types are fundamentally
   * incompatible). A value of any of the two types could be assigned to a slot
   * of the assignment-compatible type without loss of precision.
   *
   * We chose not to follow MySQL's type casting behavior as described here:
   * http://dev.mysql.com/doc/refman/5.0/en/type-conversion.html
   * for the following reasons:
   * conservative casting in arithmetic exprs: TINYINT + TINYINT -> BIGINT
   * comparison of many types as double: INT < FLOAT -> comparison as DOUBLE
   * special cases when dealing with dates and timestamps
   */
  private static PrimitiveType[][] compatibilityMatrix;
  static {
    compatibilityMatrix = new
        PrimitiveType[STRING.ordinal() + 1][STRING.ordinal() + 1];

    // NULL_TYPE is compatible with any type and results in the non-null type.
    compatibilityMatrix[NULL.ordinal()][NULL.ordinal()] = PrimitiveType.NULL_TYPE;
    compatibilityMatrix[NULL.ordinal()][BOOLEAN.ordinal()] = PrimitiveType.BOOLEAN;
    compatibilityMatrix[NULL.ordinal()][TINYINT.ordinal()] = PrimitiveType.TINYINT;
    compatibilityMatrix[NULL.ordinal()][SMALLINT.ordinal()] = PrimitiveType.SMALLINT;
    compatibilityMatrix[NULL.ordinal()][INT.ordinal()] = PrimitiveType.INT;
    compatibilityMatrix[NULL.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
    compatibilityMatrix[NULL.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
    compatibilityMatrix[NULL.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
    compatibilityMatrix[NULL.ordinal()][DATE.ordinal()] = PrimitiveType.DATE;
    compatibilityMatrix[NULL.ordinal()][DATETIME.ordinal()] = PrimitiveType.DATETIME;
    compatibilityMatrix[NULL.ordinal()][TIMESTAMP.ordinal()] = PrimitiveType.TIMESTAMP;
    compatibilityMatrix[NULL.ordinal()][STRING.ordinal()] = PrimitiveType.STRING;

    compatibilityMatrix[BOOLEAN.ordinal()][BOOLEAN.ordinal()] = PrimitiveType.BOOLEAN;
    compatibilityMatrix[BOOLEAN.ordinal()][TINYINT.ordinal()] = PrimitiveType.TINYINT;
    compatibilityMatrix[BOOLEAN.ordinal()][SMALLINT.ordinal()] = PrimitiveType.SMALLINT;
    compatibilityMatrix[BOOLEAN.ordinal()][INT.ordinal()] = PrimitiveType.INT;
    compatibilityMatrix[BOOLEAN.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
    compatibilityMatrix[BOOLEAN.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
    compatibilityMatrix[BOOLEAN.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
    compatibilityMatrix[BOOLEAN.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[BOOLEAN.ordinal()][DATETIME.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[BOOLEAN.ordinal()][TIMESTAMP.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[BOOLEAN.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[TINYINT.ordinal()][TINYINT.ordinal()] = PrimitiveType.TINYINT;
    compatibilityMatrix[TINYINT.ordinal()][SMALLINT.ordinal()] = PrimitiveType.SMALLINT;
    compatibilityMatrix[TINYINT.ordinal()][INT.ordinal()] = PrimitiveType.INT;
    compatibilityMatrix[TINYINT.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
    compatibilityMatrix[TINYINT.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
    compatibilityMatrix[TINYINT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
    compatibilityMatrix[TINYINT.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[TINYINT.ordinal()][DATETIME.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[TINYINT.ordinal()][TIMESTAMP.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[TINYINT.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[SMALLINT.ordinal()][SMALLINT.ordinal()] = PrimitiveType.SMALLINT;
    compatibilityMatrix[SMALLINT.ordinal()][INT.ordinal()] = PrimitiveType.INT;
    compatibilityMatrix[SMALLINT.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
    compatibilityMatrix[SMALLINT.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
    compatibilityMatrix[SMALLINT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
    compatibilityMatrix[SMALLINT.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[SMALLINT.ordinal()][DATETIME.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[SMALLINT.ordinal()][TIMESTAMP.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[SMALLINT.ordinal()][STRING.ordinal()] =
        PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[INT.ordinal()][INT.ordinal()] = PrimitiveType.INT;
    compatibilityMatrix[INT.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
    compatibilityMatrix[INT.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
    compatibilityMatrix[INT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
    compatibilityMatrix[INT.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[INT.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[INT.ordinal()][TIMESTAMP.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[INT.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[BIGINT.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
    compatibilityMatrix[BIGINT.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
    compatibilityMatrix[BIGINT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
    compatibilityMatrix[BIGINT.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[BIGINT.ordinal()][DATETIME.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[BIGINT.ordinal()][TIMESTAMP.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[BIGINT.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[FLOAT.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
    compatibilityMatrix[FLOAT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
    compatibilityMatrix[FLOAT.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[FLOAT.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[FLOAT.ordinal()][TIMESTAMP.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[FLOAT.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[DOUBLE.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
    compatibilityMatrix[DOUBLE.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[DOUBLE.ordinal()][DATETIME.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[DOUBLE.ordinal()][TIMESTAMP.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[DOUBLE.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[DATE.ordinal()][DATE.ordinal()] = PrimitiveType.DATE;
    compatibilityMatrix[DATE.ordinal()][DATETIME.ordinal()] = PrimitiveType.DATETIME;
    compatibilityMatrix[DATE.ordinal()][TIMESTAMP.ordinal()] = PrimitiveType.TIMESTAMP;
    compatibilityMatrix[DATE.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[DATETIME.ordinal()][DATETIME.ordinal()] = PrimitiveType.DATETIME;
    compatibilityMatrix[DATETIME.ordinal()][TIMESTAMP.ordinal()] =
        PrimitiveType.TIMESTAMP;
    compatibilityMatrix[DATETIME.ordinal()][STRING.ordinal()] =
        PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[TIMESTAMP.ordinal()][TIMESTAMP.ordinal()] =
        PrimitiveType.TIMESTAMP;
    compatibilityMatrix[TIMESTAMP.ordinal()][STRING.ordinal()] =
        PrimitiveType.TIMESTAMP;

    compatibilityMatrix[STRING.ordinal()][STRING.ordinal()] = PrimitiveType.STRING;
  }
}
