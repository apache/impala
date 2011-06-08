// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import com.google.common.base.Preconditions;

public enum PrimitiveType {
  INVALID_TYPE,
  BOOLEAN,
  TINYINT,
  SMALLINT,
  INT,
  BIGINT,
  FLOAT,
  DOUBLE,
  DATE,
  DATETIME,
  TIMESTAMP,
  STRING;

  public boolean isFixedPointType() {
    return this == TINYINT || this == SMALLINT || this == INT || this == BIGINT;
  }

  public boolean isFloatingPointType() {
    return this == FLOAT || this == DOUBLE;
  }

  public boolean isNumericType() {
    return isFixedPointType() || isFloatingPointType();
  }

  public boolean isValid() {
    return this != INVALID_TYPE;
  }

  /**
   * matrix that records "smallest" assignment-compatible type of two types
   * (INVALID_TYPE if no such type exists, ie, if the input types are fundamentally
   * incompatible)
   * TODO: implement mysql's matrix, as described here:
   * http://dev.mysql.com/doc/refman/5.0/en/type-conversion.html
   */
  private static PrimitiveType[][] compatibilityMatrix;
  static {
    compatibilityMatrix = new PrimitiveType[STRING.ordinal() + 1][STRING.ordinal() + 1];

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
    compatibilityMatrix[INT.ordinal()][FLOAT.ordinal()] = FLOAT; // DOUBLE?
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
    compatibilityMatrix[TIMESTAMP.ordinal()][STRING.ordinal()] = INVALID_TYPE;

    compatibilityMatrix[STRING.ordinal()][STRING.ordinal()] = STRING;
  }

  /**
   * Return type t such that values from both t1 and t2 can be assigned to t
   * without loss of precision. Returns INVALID_TYPE if there is no such type
   * or if any of t1 and t2 is INVALID_TYPE.
   */
  public static PrimitiveType getAssignmentCompatibleType(PrimitiveType t1, PrimitiveType t2) {
    if (!t1.isValid() || !t2.isValid()) {
      return INVALID_TYPE;
    }

    PrimitiveType smallerType = (t1.ordinal() < t2.ordinal() ? t1 : t2);
    PrimitiveType largerType = (t1.ordinal() > t2.ordinal() ? t1 : t2);
    PrimitiveType result =  compatibilityMatrix[smallerType.ordinal()][largerType.ordinal()];
    Preconditions.checkNotNull(result);
    return result;
  }
}

