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

import java.util.ArrayList;

import com.cloudera.impala.analysis.SqlParserSymbols;
import com.cloudera.impala.thrift.TPrimitiveType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public enum PrimitiveType {
  INVALID_TYPE("INVALID_TYPE", -1, TPrimitiveType.INVALID_TYPE),
  // NULL_TYPE - used only in LiteralPredicate and NullLiteral to make NULLs compatible
  // with all other types.
  NULL_TYPE("NULL_TYPE", 1, TPrimitiveType.NULL_TYPE),
  BOOLEAN("BOOLEAN", 1, TPrimitiveType.BOOLEAN),
  TINYINT("TINYINT", 1, TPrimitiveType.TINYINT),
  SMALLINT("SMALLINT", 2, TPrimitiveType.SMALLINT),
  INT("INT", 4, TPrimitiveType.INT),
  BIGINT("BIGINT", 8, TPrimitiveType.BIGINT),
  FLOAT("FLOAT", 4, TPrimitiveType.FLOAT),
  DOUBLE("DOUBLE", 8, TPrimitiveType.DOUBLE),
  DATE("DATE", 4, TPrimitiveType.DATE),
  DATETIME("DATETIME", 8, TPrimitiveType.DATETIME),
  // The timestamp structure is 12 bytes, Aligning to 8 bytes makes it 16.
  TIMESTAMP("TIMESTAMP", 16, TPrimitiveType.TIMESTAMP),
  // 8-byte pointer and 4-byte length indicator (12 bytes total).
  // Aligning to 8 bytes so 16 total.
  STRING("STRING", 16, TPrimitiveType.STRING),
  // Unsupported scalar types.
  BINARY("BINARY", -1, TPrimitiveType.BINARY),
  DECIMAL("DECIMAL", -1, TPrimitiveType.DECIMAL);

  private final String description;
  private final int slotSize;  // size of tuple slot for this type
  private final TPrimitiveType thriftType;

  private PrimitiveType(String description, int slotSize, TPrimitiveType thriftType) {
    this.description = description;
    this.slotSize = slotSize;
    this.thriftType = thriftType;
  }

  @Override
  public String toString() {
    return description;
  }

  public TPrimitiveType toThrift() {
    return thriftType;
  }

  public int getSlotSize() {
    return slotSize;
  }

  public static int getMaxSlotSize() {
    return STRING.slotSize;
  }

  public boolean isFixedPointType() {
    return this == TINYINT || this == SMALLINT || this == INT || this == BIGINT;
  }

  public boolean isFloatingPointType() {
    return this == FLOAT || this == DOUBLE;
  }

  public PrimitiveType getMaxResolutionType() {
    if (isFixedPointType()) {
      return BIGINT;
    // Timestamps get summed as DOUBLE for AVG.
    } else if (isFloatingPointType() || this == TIMESTAMP) {
      return DOUBLE;
    } else if (isNull()) {
      return NULL_TYPE;
    } else {
      return INVALID_TYPE;
    }
  }

  public boolean isNumericType() {
    return isFixedPointType() || isFloatingPointType();
  }

  public boolean isValid() {
    return this != INVALID_TYPE;
  }

  public boolean isNull() {
    return this == NULL_TYPE;
  }

  public boolean isDateType() {
    return (this == DATE || this == DATETIME || this == TIMESTAMP);
  }

  public boolean isStringType() {
    return (this == STRING);
  }

  public boolean isSupported() {
    switch (this) {
      case DATE:
      case DATETIME:
      case BINARY:
      case DECIMAL:
        return false;
      default:
        return true;
    }
  }

  public boolean isFixedLengthType() {
    return this == BOOLEAN || this == TINYINT || this == SMALLINT || this == INT
        || this == BIGINT || this == FLOAT || this == DOUBLE || this == DATE
        || this == DATETIME || this == TIMESTAMP;
  }

  private static ArrayList<PrimitiveType> numericTypes;
  static {
    numericTypes = Lists.newArrayList();
    numericTypes.add(TINYINT);
    numericTypes.add(SMALLINT);
    numericTypes.add(INT);
    numericTypes.add(BIGINT);
    numericTypes.add(FLOAT);
    numericTypes.add(DOUBLE);
  }

  public static ArrayList<PrimitiveType> getNumericTypes() {
    return numericTypes;
  }

  private static ArrayList<PrimitiveType> fixedPointTypes;
  static {
    fixedPointTypes = Lists.newArrayList();
    fixedPointTypes.add(TINYINT);
    fixedPointTypes.add(SMALLINT);
    fixedPointTypes.add(INT);
    fixedPointTypes.add(BIGINT);
  }

  public static ArrayList<PrimitiveType> getFixedPointTypes() {
    return fixedPointTypes;
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
    compatibilityMatrix = new PrimitiveType[STRING.ordinal() + 1][STRING.ordinal() + 1];

    // NULL_TYPE is compatible with any type and results in the non-null type.
    compatibilityMatrix[NULL_TYPE.ordinal()][NULL_TYPE.ordinal()] = NULL_TYPE;
    compatibilityMatrix[NULL_TYPE.ordinal()][BOOLEAN.ordinal()] = BOOLEAN;
    compatibilityMatrix[NULL_TYPE.ordinal()][TINYINT.ordinal()] = TINYINT;
    compatibilityMatrix[NULL_TYPE.ordinal()][SMALLINT.ordinal()] = SMALLINT;
    compatibilityMatrix[NULL_TYPE.ordinal()][INT.ordinal()] = INT;
    compatibilityMatrix[NULL_TYPE.ordinal()][BIGINT.ordinal()] = BIGINT;
    compatibilityMatrix[NULL_TYPE.ordinal()][FLOAT.ordinal()] = FLOAT;
    compatibilityMatrix[NULL_TYPE.ordinal()][DOUBLE.ordinal()] = DOUBLE;
    compatibilityMatrix[NULL_TYPE.ordinal()][DATE.ordinal()] = DATE;
    compatibilityMatrix[NULL_TYPE.ordinal()][DATETIME.ordinal()] = DATETIME;
    compatibilityMatrix[NULL_TYPE.ordinal()][TIMESTAMP.ordinal()] = TIMESTAMP;
    compatibilityMatrix[NULL_TYPE.ordinal()][STRING.ordinal()] = STRING;

    compatibilityMatrix[BOOLEAN.ordinal()][BOOLEAN.ordinal()] = BOOLEAN;
    compatibilityMatrix[BOOLEAN.ordinal()][TINYINT.ordinal()] = TINYINT;
    compatibilityMatrix[BOOLEAN.ordinal()][SMALLINT.ordinal()] = SMALLINT;
    compatibilityMatrix[BOOLEAN.ordinal()][INT.ordinal()] = INT;
    compatibilityMatrix[BOOLEAN.ordinal()][BIGINT.ordinal()] = BIGINT;
    compatibilityMatrix[BOOLEAN.ordinal()][FLOAT.ordinal()] = FLOAT;
    compatibilityMatrix[BOOLEAN.ordinal()][DOUBLE.ordinal()] = DOUBLE;
    compatibilityMatrix[BOOLEAN.ordinal()][DATE.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[BOOLEAN.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[BOOLEAN.ordinal()][TIMESTAMP.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[BOOLEAN.ordinal()][STRING.ordinal()] = INVALID_TYPE;

    compatibilityMatrix[TINYINT.ordinal()][TINYINT.ordinal()] = TINYINT;
    compatibilityMatrix[TINYINT.ordinal()][SMALLINT.ordinal()] = SMALLINT;
    compatibilityMatrix[TINYINT.ordinal()][INT.ordinal()] = INT;
    compatibilityMatrix[TINYINT.ordinal()][BIGINT.ordinal()] = BIGINT;
    compatibilityMatrix[TINYINT.ordinal()][FLOAT.ordinal()] = FLOAT;
    compatibilityMatrix[TINYINT.ordinal()][DOUBLE.ordinal()] = DOUBLE;
    compatibilityMatrix[TINYINT.ordinal()][DATE.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[TINYINT.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[TINYINT.ordinal()][TIMESTAMP.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[TINYINT.ordinal()][STRING.ordinal()] = INVALID_TYPE;

    compatibilityMatrix[SMALLINT.ordinal()][SMALLINT.ordinal()] = SMALLINT;
    compatibilityMatrix[SMALLINT.ordinal()][INT.ordinal()] = INT;
    compatibilityMatrix[SMALLINT.ordinal()][BIGINT.ordinal()] = BIGINT;
    compatibilityMatrix[SMALLINT.ordinal()][FLOAT.ordinal()] = FLOAT;
    compatibilityMatrix[SMALLINT.ordinal()][DOUBLE.ordinal()] = DOUBLE;
    compatibilityMatrix[SMALLINT.ordinal()][DATE.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[SMALLINT.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[SMALLINT.ordinal()][TIMESTAMP.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[SMALLINT.ordinal()][STRING.ordinal()] = INVALID_TYPE;

    compatibilityMatrix[INT.ordinal()][INT.ordinal()] = INT;
    compatibilityMatrix[INT.ordinal()][BIGINT.ordinal()] = BIGINT;
    compatibilityMatrix[INT.ordinal()][FLOAT.ordinal()] = FLOAT;
    compatibilityMatrix[INT.ordinal()][DOUBLE.ordinal()] = DOUBLE;
    compatibilityMatrix[INT.ordinal()][DATE.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[INT.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[INT.ordinal()][TIMESTAMP.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[INT.ordinal()][STRING.ordinal()] = INVALID_TYPE;

    compatibilityMatrix[BIGINT.ordinal()][BIGINT.ordinal()] = BIGINT;
    compatibilityMatrix[BIGINT.ordinal()][FLOAT.ordinal()] = DOUBLE;
    compatibilityMatrix[BIGINT.ordinal()][DOUBLE.ordinal()] = DOUBLE;
    compatibilityMatrix[BIGINT.ordinal()][DATE.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[BIGINT.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[BIGINT.ordinal()][TIMESTAMP.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[BIGINT.ordinal()][STRING.ordinal()] = INVALID_TYPE;

    compatibilityMatrix[FLOAT.ordinal()][FLOAT.ordinal()] = FLOAT;
    compatibilityMatrix[FLOAT.ordinal()][DOUBLE.ordinal()] = DOUBLE;
    compatibilityMatrix[FLOAT.ordinal()][DATE.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[FLOAT.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[FLOAT.ordinal()][TIMESTAMP.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[FLOAT.ordinal()][STRING.ordinal()] = INVALID_TYPE;

    compatibilityMatrix[DOUBLE.ordinal()][DOUBLE.ordinal()] = DOUBLE;
    compatibilityMatrix[DOUBLE.ordinal()][DATE.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[DOUBLE.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[DOUBLE.ordinal()][TIMESTAMP.ordinal()] = INVALID_TYPE;
    compatibilityMatrix[DOUBLE.ordinal()][STRING.ordinal()] = INVALID_TYPE;

    compatibilityMatrix[DATE.ordinal()][DATE.ordinal()] = DATE;
    compatibilityMatrix[DATE.ordinal()][DATETIME.ordinal()] = DATETIME;
    compatibilityMatrix[DATE.ordinal()][TIMESTAMP.ordinal()] = TIMESTAMP;
    compatibilityMatrix[DATE.ordinal()][STRING.ordinal()] = INVALID_TYPE;

    compatibilityMatrix[DATETIME.ordinal()][DATETIME.ordinal()] = DATETIME;
    compatibilityMatrix[DATETIME.ordinal()][TIMESTAMP.ordinal()] = TIMESTAMP;
    compatibilityMatrix[DATETIME.ordinal()][STRING.ordinal()] = INVALID_TYPE;

    compatibilityMatrix[TIMESTAMP.ordinal()][TIMESTAMP.ordinal()] = TIMESTAMP;
    compatibilityMatrix[TIMESTAMP.ordinal()][STRING.ordinal()] = TIMESTAMP;

    compatibilityMatrix[STRING.ordinal()][STRING.ordinal()] = STRING;
  }

  /**
   * Return type t such that values from both t1 and t2 can be assigned to t
   * without loss of precision. Returns INVALID_TYPE if there is no such type
   * or if any of t1 and t2 is INVALID_TYPE.
   */
  public static PrimitiveType getAssignmentCompatibleType(PrimitiveType t1,
      PrimitiveType t2) {
    if (!t1.isValid() || !t2.isValid()) {
      return INVALID_TYPE;
    }

    PrimitiveType smallerType = (t1.ordinal() < t2.ordinal() ? t1 : t2);
    PrimitiveType largerType = (t1.ordinal() > t2.ordinal() ? t1 : t2);
    PrimitiveType result =
        compatibilityMatrix[smallerType.ordinal()][largerType.ordinal()];
    Preconditions.checkNotNull(result);
    return result;
  }

  /**
   * Returns if it is compatible to implicitly cast from t1 to t2 (casting from
   * t1 to t2 results in no loss of precision.
   */
  public static boolean isImplicitlyCastable(PrimitiveType t1, PrimitiveType t2) {
    return getAssignmentCompatibleType(t1, t2) == t2;
  }

  // Returns the highest resolution type
  // corresponding to the lexer symbol of numeric literals.
  // Currently used to determine whether the literal is fixed or floating point.
  public static PrimitiveType literalSymbolIdToType(int symbolId) {
    switch (symbolId) {
      case SqlParserSymbols.INTEGER_LITERAL:
        return BIGINT;
      case SqlParserSymbols.FLOATINGPOINT_LITERAL:
        return DOUBLE;
      default:
        return INVALID_TYPE;
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
    if (isNumericType()) {
      return getPrecision();
    }
    switch (this) {
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
    switch (this) {
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
    switch (this) {
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
    switch (this) {
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
    switch (this) {
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
        Preconditions.checkArgument(false, "Invalid type " + name());
        return 0;
    }
  }
}

