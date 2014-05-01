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

package com.cloudera.impala.catalog;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import com.cloudera.impala.analysis.CreateTableStmt;
import com.cloudera.impala.analysis.SqlParser;
import com.cloudera.impala.analysis.SqlScanner;
import com.cloudera.impala.analysis.TypesUtil;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TColumnType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Class wrapping a type of a column. For most types, this will just be a wrapper
 * around an enum but for types like CHAR(n) and decimal, this will contain additional
 * information.
 *
 * Types have a few ways they can be compared to other types. Types can be:
 *   1. completely identical,
 *   2. implicitly castable (convertible without loss of precision)
 *   3. subtype. For example, in the case of decimal, a type can be decimal(*, *)
 *   indicating that any decimal type is a subtype of the decimal type.
 */
public class ColumnType {
  // If true, this type has been analyzed.
  private boolean isAnalyzed_;

  private final PrimitiveType type_;

  // Unused if type_ is always the same length.
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
  static final int DEFAULT_PRECISION = 9;
  static final int DEFAULT_SCALE = 0; // SQL standard

  // Hive, mysql, sql server standard.
  public static final int MAX_PRECISION = 38;
  public static final int MAX_SCALE = MAX_PRECISION;

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
  public static final ColumnType DEFAULT_DECIMAL =
      createDecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
  public static final ColumnType DECIMAL = createDecimalTypeInternal(-1, -1);

  private static ArrayList<ColumnType> fixedSizeNumericTypes;
  private static ArrayList<ColumnType> integerTypes;
  private static ArrayList<ColumnType> numericTypes;
  private static ArrayList<ColumnType> supportedTypes;

  static {
    fixedSizeNumericTypes = Lists.newArrayListWithExpectedSize(6);
    fixedSizeNumericTypes.add(TINYINT);
    fixedSizeNumericTypes.add(SMALLINT);
    fixedSizeNumericTypes.add(INT);
    fixedSizeNumericTypes.add(BIGINT);
    fixedSizeNumericTypes.add(FLOAT);
    fixedSizeNumericTypes.add(DOUBLE);

    integerTypes = Lists.newArrayListWithExpectedSize(4);
    integerTypes.add(TINYINT);
    integerTypes.add(SMALLINT);
    integerTypes.add(INT);
    integerTypes.add(BIGINT);

    numericTypes = Lists.newArrayListWithExpectedSize(7);
    numericTypes.add(TINYINT);
    numericTypes.add(SMALLINT);
    numericTypes.add(INT);
    numericTypes.add(BIGINT);
    numericTypes.add(FLOAT);
    numericTypes.add(DOUBLE);
    numericTypes.add(DECIMAL);

    supportedTypes = Lists.newArrayListWithExpectedSize(11);
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
    supportedTypes.add(DECIMAL);
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

  public static ColumnType createDecimalType() { return DEFAULT_DECIMAL; }

  public static ColumnType createDecimalType(int precision) {
    return createDecimalType(precision, DEFAULT_SCALE);
  }

  public static ColumnType createDecimalType(int precision, int scale) {
    Preconditions.checkState(precision >= 0); // Enforced by parser
    Preconditions.checkState(scale >= 0); // Enforced by parser.
    ColumnType type = new ColumnType(PrimitiveType.DECIMAL);
    type.precision_ = precision;
    type.scale_ = scale;
    return type;
  }

  // Identical to createDecimalType except that higher precisions are truncated
  // to the max storable precision. The BE will report overflow in these cases
  // (think of this as adding ints to BIGINT but BIGINT can still overflow).
  public static ColumnType createDecimalTypeInternal(int precision, int scale) {
    ColumnType type = new ColumnType(PrimitiveType.DECIMAL);
    type.precision_ = Math.min(precision, MAX_PRECISION);
    type.scale_ = Math.min(type.precision_, scale);
    type.isAnalyzed_ = true;
    return type;
  }

  public static ArrayList<ColumnType> getFixedSizeNumericTypes() {
    return fixedSizeNumericTypes;
  }
  public static ArrayList<ColumnType> getIntegerTypes() {
    return integerTypes;
  }
  public static ArrayList<ColumnType> getNumericTypes() {
    return numericTypes;
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

  /**
   * Returns true if this object is of type t.
   * Handles wildcard types. That is, if t is the wildcard type variant
   * of 'this', returns true.
   */
  public boolean matchesType(ColumnType t) {
    if (equals(t)) return true;
    if (isDecimal() && t.isWildcardDecimal()) {
      Preconditions.checkState(!isWildcardDecimal());
      return true;
    }
    return false;
  }

  public final PrimitiveType getPrimitiveType() { return type_; }
  public boolean isNull() { return getPrimitiveType() == PrimitiveType.NULL_TYPE; }
  public boolean isValid() { return getPrimitiveType() != PrimitiveType.INVALID_TYPE; }
  public int ordinal() { return type_.ordinal(); }

  public int decimalPrecision() {
    Preconditions.checkState(type_ == PrimitiveType.DECIMAL);
    return precision_;
  }

  public int decimalScale() {
    Preconditions.checkState(type_ == PrimitiveType.DECIMAL);
    return scale_;
  }

  public boolean isInvalid() { return type_ == PrimitiveType.INVALID_TYPE; }
  public boolean isBoolean() { return type_ == PrimitiveType.BOOLEAN; }
  public boolean isDecimal() { return type_ == PrimitiveType.DECIMAL; }
  public boolean isDecimalOrNull() { return isDecimal() || isNull(); }

  public boolean isWildcardDecimal() {
    return type_ == PrimitiveType.DECIMAL && precision_ == -1 && scale_ == -1;
  }

  /**
   *  Returns true if this type is a fully specified (not wild card) decimal.
   */
  public boolean isFullySpecifiedDecimal() {
    if (!isDecimal()) return false;
    if (isWildcardDecimal()) return false;
    if (precision_ <= 0 || precision_ > MAX_PRECISION) return false;
    if (scale_ < 0 || scale_ > precision_) return false;
    return true;
  }

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
    } else if (isDecimal()) {
      return createDecimalTypeInternal(MAX_PRECISION, scale_);
    } else {
      return ColumnType.INVALID;
    }
  }

  public ColumnType getNextResolutionType() {
    Preconditions.checkState(isNumericType() || isNull());
    if (type_ == PrimitiveType.DOUBLE || type_ == PrimitiveType.BIGINT || isNull()) {
      return this;
    } else if (type_ == PrimitiveType.DECIMAL) {
      return createDecimalTypeInternal(MAX_PRECISION, scale_);
    }
    return createType(PrimitiveType.values()[type_.ordinal() + 1]);
  }

  /**
   * Returns the smallest decimal type that can safely store this type. Returns
   * INVALID if this type cannot be stored as a decimal.
   */
  public ColumnType getMinResolutionDecimal() {
    switch (type_) {
      case NULL_TYPE: return ColumnType.NULL;
      case DECIMAL: return this;
      case TINYINT: return createDecimalType(3);
      case SMALLINT: return createDecimalType(5);
      case INT: return createDecimalType(10);
      case BIGINT: return createDecimalType(20);
      case FLOAT: return createDecimalTypeInternal(MAX_PRECISION, 9);
      case DOUBLE: return createDecimalTypeInternal(MAX_PRECISION, 17);
    }
    return ColumnType.INVALID;
  }

  /**
   * Return type t such that values from both t1 and t2 can be assigned to t
   * without loss of precision. Returns INVALID_TYPE if there is no such type
   * or if any of t1 and t2 is INVALID_TYPE.
   */
  public static ColumnType getAssignmentCompatibleType(ColumnType t1,
      ColumnType t2) {
    if (!t1.isValid() || !t2.isValid()) return INVALID;
    if (t1.equals(t2)) return t1;


    if (t1.isDecimal() || t2.isDecimal()) {
      if (t1.isNull()) return t2;
      if (t2.isNull()) return t1;

      // Allow casts between decimal and numeric types by converting
      // numeric types to the containing decimal type.
      t1 = t1.getMinResolutionDecimal();
      t2 = t2.getMinResolutionDecimal();
      if (t1.isInvalid() || t2.isInvalid()) return ColumnType.INVALID;
      Preconditions.checkState(t1.isDecimal());
      Preconditions.checkState(t2.isDecimal());
      return TypesUtil.getDecimalAssignmentCompatibleType(t1, t2);
    }

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
    return getAssignmentCompatibleType(t1, t2).matchesType(t2);
  }

  /**
   * Returns true if Impala supports this type in the metdata. It does not mean we
   * can manipulate data of this type. For tables that contain columns with these
   * types, we can safely skip over them.
   */
  public boolean isSupported() {
    switch (type_) {
      case DATE:
      case DATETIME:
      case BINARY:
        return false;
      default:
        return true;
    }
  }

  /**
   * Indicates whether we support partitioning tables on columns of this type.
   */
  public boolean supportsTablePartitioning() {
    if (!isSupported() || type_ == PrimitiveType.TIMESTAMP ||
        type_ == PrimitiveType.CHAR) {
      return false;
    }
    return true;
  }

  public int getSlotSize() {
    switch (type_) {
      case CHAR: return len_;
      case DECIMAL: return TypesUtil.getDecimalSlotSize(this);
      default:
        return type_.getSlotSize();
    }
  }

  public void analyze() throws AnalysisException {
    Preconditions.checkState(type_ != PrimitiveType.INVALID_TYPE);

    if (isAnalyzed_) return;
    if (type_ == PrimitiveType.CHAR) {
      if (len_ <= 0) {
        throw new AnalysisException("Char size must be > 0. Size was set to: " +
            len_ + ".");
      }
    } else if (type_ == PrimitiveType.DECIMAL) {
      if (precision_ > MAX_PRECISION) {
        throw new AnalysisException(
            "Decimal precision must be <= " + MAX_PRECISION + ".");
      }
      if (precision_ == 0) {
        throw new AnalysisException("Decimal precision must be greater than 0.");
      }
      if (scale_ > precision_) {
        throw new AnalysisException("Decimal scale (" + scale_+ ") must be <= " +
            "precision (" + precision_ + ").");
      }
    }
    isAnalyzed_ = true;
  }

  @Override
  // The output of this is stored directly in the hive metastore as the column type.
  // The string must match exactly.
  public String toString() {
    if (type_ == PrimitiveType.CHAR) {
      return "CHAR(" + len_ + ")";
    } else  if (type_ == PrimitiveType.DECIMAL) {
      return "DECIMAL(" + precision_ + "," + scale_ + ")";
    }
    return type_.toString();
  }

  public TColumnType toThrift() {
    TColumnType thrift = new TColumnType();
    thrift.type = type_.toThrift();
    if (type_ == PrimitiveType.CHAR) {
      thrift.setLen(len_);
    } else  if (type_ == PrimitiveType.DECIMAL) {
      thrift.setPrecision(precision_);
      thrift.setScale(scale_);
    }
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
    } if (type == PrimitiveType.DECIMAL) {
      Preconditions.checkState(thrift.isSetPrecision());
      Preconditions.checkState(thrift.isSetScale());
      return createDecimalType(thrift.precision, thrift.scale);
    } else {
      return createType(type);
    }
  }

  /*
   * Gets the ColumnType from the given FieldSchema by using Impala's SqlParser.
   * Returns null if the FieldSchema could not be parsed.
   * The type can either be:
   *   - Supported by Impala, in which case the type is returned.
   *   - A type Impala understands but is not yet implemented (e.g. date), the type is
   *     returned but type.IsSupported() returns false.
   *   - A type Impala can't understand at all in which case null is returned.
   */
  public static ColumnType parseColumnType(FieldSchema fs) {
    // Wrap the type string in a CREATE TABLE stmt and use Impala's Parser
    // to get the ColumnType.
    // Pick a table name that can't be used.
    String stmt = String.format("CREATE TABLE $DUMMY ($DUMMY %s)", fs.getType());
    SqlScanner input = new SqlScanner(new StringReader(stmt));
    SqlParser parser = new SqlParser(input);
    CreateTableStmt createTableStmt;
    try {
      Object o = parser.parse().value;
      if (!(o instanceof CreateTableStmt)) {
        // Should never get here.
        throw new IllegalStateException("Couldn't parse create table stmt.");
      }
      createTableStmt = (CreateTableStmt) o;
      if (createTableStmt.getColumnDescs().isEmpty()) {
        // Should never get here.
        throw new IllegalStateException("Invalid create table stmt.");
      }
    } catch (Exception e) {
      return null;
    }
    return createTableStmt.getColumnDescs().get(0).getColType();
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
