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

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.analysis.CreateTableStmt;
import org.apache.impala.analysis.Parser;
import org.apache.impala.analysis.StatementBase;
import org.apache.impala.analysis.TypeDef;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TColumnType;
import org.apache.impala.thrift.TPrimitiveType;
import org.apache.impala.thrift.TScalarType;
import org.apache.impala.thrift.TStructField;
import org.apache.impala.thrift.TTypeNode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Abstract class describing an Impala data type (scalar/complex type).
 * Mostly contains static type instances and helper methods for convenience, as well
 * as abstract methods that subclasses must implement.
 */
public abstract class Type {
  // Maximum nesting depth of a type. This limit was determined experimentally by
  // generating and scanning deeply nested Parquet and Avro files. In those experiments,
  // we exceeded the stack space in the scanner (which uses recursion for dealing with
  // nested types) at a nesting depth between 200 and 300 (200 worked, 300 crashed).
  public static int MAX_NESTING_DEPTH = 100;

  // Static constant types for scalar types that don't require additional information.
  public static final ScalarType INVALID = new ScalarType(PrimitiveType.INVALID_TYPE);
  public static final ScalarType NULL = new ScalarType(PrimitiveType.NULL_TYPE);
  public static final ScalarType BOOLEAN = new ScalarType(PrimitiveType.BOOLEAN);
  public static final ScalarType TINYINT = new ScalarType(PrimitiveType.TINYINT);
  public static final ScalarType SMALLINT = new ScalarType(PrimitiveType.SMALLINT);
  public static final ScalarType INT = new ScalarType(PrimitiveType.INT);
  public static final ScalarType BIGINT = new ScalarType(PrimitiveType.BIGINT);
  public static final ScalarType FLOAT = new ScalarType(PrimitiveType.FLOAT);
  public static final ScalarType DOUBLE = new ScalarType(PrimitiveType.DOUBLE);
  public static final ScalarType STRING = new ScalarType(PrimitiveType.STRING);
  public static final ScalarType BINARY = new ScalarType(PrimitiveType.BINARY);
  public static final ScalarType TIMESTAMP = new ScalarType(PrimitiveType.TIMESTAMP);
  public static final ScalarType DATE = new ScalarType(PrimitiveType.DATE);
  public static final ScalarType DATETIME = new ScalarType(PrimitiveType.DATETIME);
  public static final ScalarType DEFAULT_DECIMAL =
      ScalarType.createDecimalType(ScalarType.DEFAULT_PRECISION,
          ScalarType.DEFAULT_SCALE);
  public static final ScalarType DECIMAL = ScalarType.createWildCardDecimalType();
  public static final ScalarType DEFAULT_VARCHAR = ScalarType.createVarcharType(-1);
  public static final ScalarType VARCHAR = ScalarType.createVarcharType(-1);
  public static final ScalarType CHAR = ScalarType.createCharType(-1);
  public static final ScalarType FIXED_UDA_INTERMEDIATE =
      ScalarType.createFixedUdaIntermediateType(-1);

  private static List<ScalarType> integerTypes;
  private static List<ScalarType> numericTypes;
  private static List<ScalarType> supportedTypes;
  private static List<ScalarType> unsupportedTypes;

  static {
    integerTypes = new ArrayList<>();
    integerTypes.add(TINYINT);
    integerTypes.add(SMALLINT);
    integerTypes.add(INT);
    integerTypes.add(BIGINT);

    numericTypes = new ArrayList<>();
    numericTypes.add(TINYINT);
    numericTypes.add(SMALLINT);
    numericTypes.add(INT);
    numericTypes.add(BIGINT);
    numericTypes.add(FLOAT);
    numericTypes.add(DOUBLE);
    numericTypes.add(DECIMAL);

    supportedTypes = new ArrayList<>();
    supportedTypes.add(NULL);
    supportedTypes.add(BOOLEAN);
    supportedTypes.add(TINYINT);
    supportedTypes.add(SMALLINT);
    supportedTypes.add(INT);
    supportedTypes.add(BIGINT);
    supportedTypes.add(FLOAT);
    supportedTypes.add(DOUBLE);
    supportedTypes.add(STRING);
    supportedTypes.add(VARCHAR);
    supportedTypes.add(CHAR);
    supportedTypes.add(TIMESTAMP);
    supportedTypes.add(DECIMAL);
    supportedTypes.add(DATE);

    unsupportedTypes = new ArrayList<>();
    unsupportedTypes.add(BINARY);
    unsupportedTypes.add(DATETIME);
  }

  public static List<ScalarType> getIntegerTypes() {
    return integerTypes;
  }
  public static List<ScalarType> getNumericTypes() {
    return numericTypes;
  }
  public static List<ScalarType> getSupportedTypes() {
    return supportedTypes;
  }
  public static List<ScalarType> getUnsupportedTypes() {
    return unsupportedTypes;
  }

  /**
   * Returns the static ScalarType members. The ScalarTypes which require additional
   * metadata (such as CHAR, DECIMAL, FIXED_UDA_INTERMEDIATE) must be created using
   * proper create functions defined in ScalarType class.
   */
  public static ScalarType getDefaultScalarType(PrimitiveType ptype) {
    switch (ptype) {
      case INVALID_TYPE: return INVALID;
      case NULL_TYPE: return NULL;
      case BOOLEAN: return BOOLEAN;
      case TINYINT: return TINYINT;
      case SMALLINT: return SMALLINT;
      case INT: return INT;
      case BIGINT: return BIGINT;
      case FLOAT: return FLOAT;
      case DOUBLE: return DOUBLE;
      case DATE: return DATE;
      case DATETIME: return DATETIME;
      case TIMESTAMP: return TIMESTAMP;
      case STRING: return STRING;
      case VARCHAR: return VARCHAR;
      case BINARY: return BINARY;
      case DECIMAL: return DECIMAL;
      case CHAR: return CHAR;
      case FIXED_UDA_INTERMEDIATE: return FIXED_UDA_INTERMEDIATE;
      default: return INVALID;
    }
  }

  /**
   * The output of this is stored directly in the hive metastore as the column type.
   * The string must match exactly.
   */
  public final String toSql() { return toSql(0); }

  /**
   * Recursive helper for toSql() to be implemented by subclasses. Keeps track of the
   * nesting depth and terminates the recursion if MAX_NESTING_DEPTH is reached.
   */
  protected abstract String toSql(int depth);

  /**
   * Same as toSql() but adds newlines and spaces for better readability of nested types.
   */
  public String prettyPrint() { return prettyPrint(0); }

  /**
   * Pretty prints this type with lpad number of leading spaces. Used to implement
   * prettyPrint() with space-indented nested types.
   */
  protected abstract String prettyPrint(int lpad);

  public boolean isInvalid() { return isScalarType(PrimitiveType.INVALID_TYPE); }
  public boolean isValid() { return !isInvalid(); }
  public boolean isNull() { return isScalarType(PrimitiveType.NULL_TYPE); }
  public boolean isBoolean() { return isScalarType(PrimitiveType.BOOLEAN); }
  public boolean isTimestamp() { return isScalarType(PrimitiveType.TIMESTAMP); }
  public boolean isDate() { return isScalarType(PrimitiveType.DATE); }
  public boolean isDecimal() { return isScalarType(PrimitiveType.DECIMAL); }
  public boolean isFullySpecifiedDecimal() { return false; }
  public boolean isWildcardDecimal() { return false; }
  public boolean isWildcardVarchar() { return false; }
  public boolean isWildcardChar() { return false; }

  public boolean isStringType() {
    return isScalarType(PrimitiveType.STRING) || isScalarType(PrimitiveType.VARCHAR) ||
        isScalarType(PrimitiveType.CHAR);
  }

  public boolean isScalarType() { return this instanceof ScalarType; }
  public boolean isScalarType(PrimitiveType t) {
    return isScalarType() && ((ScalarType) this).getPrimitiveType() == t;
  }

  public boolean isFixedPointType() {
    return isScalarType(PrimitiveType.TINYINT) || isScalarType(PrimitiveType.SMALLINT) ||
        isScalarType(PrimitiveType.INT) || isScalarType(PrimitiveType.BIGINT) ||
        isScalarType(PrimitiveType.DECIMAL);
  }

  public boolean isFloatingPointType() {
    return isScalarType(PrimitiveType.FLOAT) || isScalarType(PrimitiveType.DOUBLE);
  }

  public boolean isIntegerType() {
    return isScalarType(PrimitiveType.TINYINT) || isScalarType(PrimitiveType.SMALLINT)
        || isScalarType(PrimitiveType.INT) || isScalarType(PrimitiveType.BIGINT);
  }

  // TODO: Handle complex types properly. Some instances may be fixed length.
  public boolean isFixedLengthType() { return false; }

  public boolean isNumericType() {
    return isFixedPointType() || isFloatingPointType() || isDecimal();
  }

  public boolean isDateOrTimeType() {
    return isScalarType(PrimitiveType.DATE) || isScalarType(PrimitiveType.DATETIME)
        || isScalarType(PrimitiveType.TIMESTAMP);
  }

  public boolean isComplexType() { return isStructType() || isCollectionType(); }
  public boolean isCollectionType() { return isMapType() || isArrayType(); }
  public boolean isMapType() { return this instanceof MapType; }
  public boolean isArrayType() { return this instanceof ArrayType; }
  public boolean isStructType() { return this instanceof StructType; }

  /**
   * Returns true if Impala supports this type in the metdata. It does not mean we
   * can manipulate data of this type. For tables that contain columns with these
   * types, we can safely skip over them.
   */
  public boolean isSupported() { return true; }

  /**
   * Indicates whether we support partitioning tables on columns of this type.
   */
  public boolean supportsTablePartitioning() { return false; }

  public PrimitiveType getPrimitiveType() { return PrimitiveType.INVALID_TYPE; }

  /**
   * Returns the size in bytes of the fixed-length portion that a slot of this type
   * occupies in a tuple.
   */
  public int getSlotSize() {
    // 8-byte pointer and 4-byte length indicator (12 bytes total).
    if (isCollectionType()) return 12;
    throw new IllegalStateException("getSlotSize() not implemented for type " + toSql());
  }

  public TColumnType toThrift() {
    TColumnType container = new TColumnType();
    container.setTypes(new ArrayList<>());
    toThrift(container);
    return container;
  }

  /**
   * Subclasses should override this method to add themselves to the thrift container.
   */
  public abstract void toThrift(TColumnType container);

  /**
   * Returns true if this type is equal to t, or if t is a wildcard variant of this
   * type. Subclasses should override this as appropriate. The default implementation
   * here is to avoid special-casing logic in callers for concrete types.
   */
  public boolean matchesType(Type t) { return false; }

  /**
   * Gets the ColumnType from the given FieldSchema by using Impala's SqlParser.
   * Returns null if the FieldSchema could not be parsed.
   * The type can either be:
   *   - Supported by Impala, in which case the type is returned.
   *   - A type Impala understands but is not yet implemented (e.g. date), the type is
   *     returned but type.IsSupported() returns false.
   *   - A type Impala can't understand at all in which case null is returned.
   */
  public static Type parseColumnType(String typeStr) {
    // Wrap the type string in a CREATE TABLE stmt and use Impala's Parser
    // to get the ColumnType.
    // Pick a table name that can't be used.
    String stmt = String.format("CREATE TABLE $DUMMY ($DUMMY %s)", typeStr);
    CreateTableStmt createTableStmt;
    try {
      StatementBase o = Parser.parse(stmt);
      if (!(o instanceof CreateTableStmt)) {
        // Should never get here.
        throw new IllegalStateException("Couldn't parse create table stmt.");
      }
      createTableStmt = (CreateTableStmt) o;
      if (createTableStmt.getColumnDefs().isEmpty()) {
        // Should never get here.
        throw new IllegalStateException("Invalid create table stmt.");
      }
    } catch (AnalysisException e) {
      return null;
    }
    TypeDef typeDef = createTableStmt.getColumnDefs().get(0).getTypeDef();
    return typeDef.getType();
  }

  /**
   * Returns true if t1 can be implicitly cast to t2 according to Impala's casting rules.
   * Implicit casts are always allowed when no loss of information would result (i.e.
   * every value of t1 can be represented exactly by a value of t2). Implicit casts are
   * allowed in certain other cases such as casting numeric types to floating point types
   * and converting strings to timestamps.
   *
   * If strictDecimal is true, only consider casts that result in no loss of information
   * when casting between decimal types.
   * If strict is true, only consider casts that result in no loss of information when
   * casting between any two types other than both decimals.
   *
   * TODO: Support casting of non-scalar types.
   */
  public static boolean isImplicitlyCastable(
      Type t1, Type t2, boolean strict, boolean strictDecimal) {
    if (t1.isScalarType() && t2.isScalarType()) {
      return ScalarType.isImplicitlyCastable(
          (ScalarType) t1, (ScalarType) t2, strict, strictDecimal);
    }
    return false;
  }

  /**
   * Return type t such that values from both t1 and t2 can be assigned to t without an
   * explicit cast. If strict, does not consider conversions that would result in loss
   * of precision (e.g. converting decimal to float). Returns INVALID_TYPE if there is
   * no such type or if any of t1 and t2 is INVALID_TYPE.
   *
   * If strictDecimal is true, only consider casts that result in no loss of information
   * when casting between decimal types.
   * If strict is true, only consider casts that result in no loss of information when
   * casting between any two types other than both decimals.
   *
   *
   * TODO: Support non-scalar types.
   */
  public static Type getAssignmentCompatibleType(
      Type t1, Type t2, boolean strict, boolean strictDecimal) {
    if (t1.isScalarType() && t2.isScalarType()) {
      return ScalarType.getAssignmentCompatibleType(
          (ScalarType) t1, (ScalarType) t2, strict, strictDecimal);
    }
    return ScalarType.INVALID;
  }

  /**
   * Returns true if this type exceeds the MAX_NESTING_DEPTH, false otherwise.
   */
  public boolean exceedsMaxNestingDepth() { return exceedsMaxNestingDepth(0); }

  /**
   * Helper for exceedsMaxNestingDepth(). Recursively computes the max nesting depth,
   * terminating early if MAX_NESTING_DEPTH is reached. Returns true if this type
   * exceeds the MAX_NESTING_DEPTH, false otherwise.
   *
   * Examples of types and their nesting depth:
   * INT --> 1
   * STRUCT<f1:INT> --> 2
   * STRUCT<f1:STRUCT<f2:INT>> --> 3
   * ARRAY<INT> --> 2
   * ARRAY<STRUCT<f1:INT>> --> 3
   * MAP<STRING,INT> --> 2
   * MAP<STRING,STRUCT<f1:INT>> --> 3
   */
  private boolean exceedsMaxNestingDepth(int d) {
    if (d >= MAX_NESTING_DEPTH) return true;
    if (isStructType()) {
      StructType structType = (StructType) this;
      for (StructField f: structType.getFields()) {
        if (f.getType().exceedsMaxNestingDepth(d + 1)) return true;
      }
    } else if (isArrayType()) {
      ArrayType arrayType = (ArrayType) this;
      if (arrayType.getItemType().exceedsMaxNestingDepth(d + 1)) return true;
    } else if (isMapType()) {
      MapType mapType = (MapType) this;
      if (mapType.getValueType().exceedsMaxNestingDepth(d + 1)) return true;
    } else {
      Preconditions.checkState(isScalarType());
    }
    return false;
  }

  public static List<TColumnType> toThrift(Type[] types) {
    return toThrift(Lists.newArrayList(types));
  }

  public static List<TColumnType> toThrift(List<Type> types) {
    List<TColumnType> result = new ArrayList<>();
    for (Type t: types) {
      result.add(t.toThrift());
    }
    return result;
  }

  public static Type fromThrift(TColumnType thrift) {
    Preconditions.checkState(thrift.types.size() > 0);
    Pair<Type, Integer> t = fromThrift(thrift, 0);
    Preconditions.checkState(t.second.equals(thrift.getTypesSize()));
    return t.first;
  }

  /**
   * Constructs a ColumnType rooted at the TTypeNode at nodeIdx in TColumnType.
   * Returned pair: The resulting ColumnType and the next nodeIdx that is not a child
   * type of the result.
   */
  protected static Pair<Type, Integer> fromThrift(TColumnType col, int nodeIdx) {
    TTypeNode node = col.getTypes().get(nodeIdx);
    Type type = null;
    switch (node.getType()) {
      case SCALAR: {
        Preconditions.checkState(node.isSetScalar_type());
        TScalarType scalarType = node.getScalar_type();
        if (scalarType.getType() == TPrimitiveType.CHAR) {
          Preconditions.checkState(scalarType.isSetLen());
          type = ScalarType.createCharType(scalarType.getLen());
        } else if (scalarType.getType() == TPrimitiveType.VARCHAR) {
          Preconditions.checkState(scalarType.isSetLen());
          type = ScalarType.createVarcharType(scalarType.getLen());
        } else if (scalarType.getType() == TPrimitiveType.DECIMAL) {
          Preconditions.checkState(scalarType.isSetPrecision()
              && scalarType.isSetScale());
          type = ScalarType.createDecimalType(scalarType.getPrecision(),
              scalarType.getScale());
        } else {
          type = ScalarType.createType(
              PrimitiveType.fromThrift(scalarType.getType()));
        }
        ++nodeIdx;
        break;
      }
      case ARRAY: {
        Preconditions.checkState(nodeIdx + 1 < col.getTypesSize());
        Pair<Type, Integer> childType = fromThrift(col, nodeIdx + 1);
        type = new ArrayType(childType.first);
        nodeIdx = childType.second;
        break;
      }
      case MAP: {
        Preconditions.checkState(nodeIdx + 2 < col.getTypesSize());
        Pair<Type, Integer> keyType = fromThrift(col, nodeIdx + 1);
        Pair<Type, Integer> valueType = fromThrift(col, keyType.second);
        type = new MapType(keyType.first, valueType.first);
        nodeIdx = valueType.second;
        break;
      }
      case STRUCT: {
        Preconditions.checkState(nodeIdx + node.getStruct_fieldsSize() < col.getTypesSize());
        List<StructField> structFields = new ArrayList<>();
        ++nodeIdx;
        for (int i = 0; i < node.getStruct_fieldsSize(); ++i) {
          TStructField thriftField = node.getStruct_fields().get(i);
          String name = thriftField.getName();
          String comment = null;
          if (thriftField.isSetComment()) comment = thriftField.getComment();
          Pair<Type, Integer> res = fromThrift(col, nodeIdx);
          nodeIdx = res.second.intValue();
          structFields.add(new StructField(name, res.first, comment));
        }
        type = new StructType(structFields);
        break;
      }
    }
    return new Pair<Type, Integer>(type, nodeIdx);
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
    if (!isScalarType()) return null;
    if (isNumericType()) return getPrecision();
    ScalarType t = (ScalarType) this;
    switch (t.getPrimitiveType()) {
      case STRING:
        return Integer.MAX_VALUE;
      case TIMESTAMP:
        return 29;
      case DATE:
        return 10;
      case CHAR:
      case VARCHAR:
      case FIXED_UDA_INTERMEDIATE:
        return t.getLength();
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
    if (!isScalarType()) return null;
    ScalarType t = (ScalarType) this;
    switch (t.getPrimitiveType()) {
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
      case DECIMAL:
        return t.decimalPrecision();
      default:
        return null;
    }
  }

  /**
   * JDBC data type description
   * Returns the number of fractional digits for this type, or null if not applicable.
   * For timestamp/time types, returns the number of digits in the fractional seconds
   * component.
   */
  public Integer getDecimalDigits() {
    if (!isScalarType()) return null;
    ScalarType t = (ScalarType) this;
    switch (t.getPrimitiveType()) {
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case INT:
      case BIGINT:
      case DATE:
        return 0;
      case FLOAT:
        return 7;
      case DOUBLE:
        return 15;
      case TIMESTAMP:
        return 9;
      case DECIMAL:
        return t.decimalScale();
      default:
        return null;
    }
  }

  /**
   * JDBC data type description
   * For numeric data types, either 10 or 2. If it is 10, the values in COLUMN_SIZE
   * and DECIMAL_DIGITS give the number of decimal digits allowed for the column.
   * For example, a DECIMAL(12,5) column would return a NUM_PREC_RADIX of 10,
   * a COLUMN_SIZE of 12, and a DECIMAL_DIGITS of 5; a FLOAT column could return
   * a NUM_PREC_RADIX of 10, a COLUMN_SIZE of 15, and a DECIMAL_DIGITS of NULL.
   * If it is 2, the values in COLUMN_SIZE and DECIMAL_DIGITS give the number of bits
   * allowed in the column. For example, a FLOAT column could return a RADIX of 2,
   * a COLUMN_SIZE of 53, and a DECIMAL_DIGITS of NULL. NULL is returned for data
   * types where NUM_PREC_RADIX is not applicable.
   */
  public Integer getNumPrecRadix() {
    if (!isScalarType()) return null;
    ScalarType t = (ScalarType) this;
    switch (t.getPrimitiveType()) {
      case TINYINT:
      case SMALLINT:
      case INT:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
        return 10;
      default:
        // everything else (including boolean and string) is null
        return null;
    }
  }

  /**
   * JDBC data type description
   * Returns the java SQL type enum
   */
  public int getJavaSqlType() {
    if (isStructType()) return java.sql.Types.STRUCT;
    // Both MAP and ARRAY are reported as ARRAY, since there is no better matching
    // Java SQL type. This behavior is consistent with Hive.
    if (isCollectionType()) return java.sql.Types.ARRAY;

    Preconditions.checkState(isScalarType(), "Invalid non-scalar type: " + toSql());
    ScalarType t = (ScalarType) this;
    switch (t.getPrimitiveType()) {
      case NULL_TYPE: return java.sql.Types.NULL;
      case BOOLEAN: return java.sql.Types.BOOLEAN;
      case TINYINT: return java.sql.Types.TINYINT;
      case SMALLINT: return java.sql.Types.SMALLINT;
      case INT: return java.sql.Types.INTEGER;
      case BIGINT: return java.sql.Types.BIGINT;
      case FLOAT: return java.sql.Types.FLOAT;
      case DOUBLE: return java.sql.Types.DOUBLE;
      case TIMESTAMP: return java.sql.Types.TIMESTAMP;
      case DATE: return java.sql.Types.DATE;
      case STRING: return java.sql.Types.VARCHAR;
      case CHAR: return java.sql.Types.CHAR;
      case VARCHAR: return java.sql.Types.VARCHAR;
      case BINARY: return java.sql.Types.BINARY;
      case DECIMAL: return java.sql.Types.DECIMAL;
      case FIXED_UDA_INTERMEDIATE: return java.sql.Types.BINARY;
      default:
        Preconditions.checkArgument(false, "Invalid primitive type " +
            t.getPrimitiveType().name());
        return 0;
    }
  }

  /**
   * Matrix that records "smallest" assignment-compatible type of two types
   * (INVALID_TYPE if no such type exists, ie, if the input types are fundamentally
   * incompatible). A value of any of the two types could be assigned to a slot
   * of the assignment-compatible type. For strict compatibility, this can be done
   * without any loss of precision. For non-strict compatibility, there may be loss of
   * precision, e.g. if converting from BIGINT to FLOAT.
   *
   * We chose not to follow MySQL's type casting behavior as described here:
   * http://dev.mysql.com/doc/refman/5.0/en/type-conversion.html
   * for the following reasons:
   * conservative casting in arithmetic exprs: TINYINT + TINYINT -> BIGINT
   * comparison of many types as double: INT < FLOAT -> comparison as DOUBLE
   * special cases when dealing with dates and timestamps.
   */
  protected static PrimitiveType[][] compatibilityMatrix;

  /**
   * If we are checking in strict mode, any non-null entry in this matrix overrides
   * compatibilityMatrix. If the entry is null, the entry in compatibility matrix
   * is valid.
   */
  protected static PrimitiveType[][] strictCompatibilityMatrix;

  static {
    compatibilityMatrix = new
        PrimitiveType[PrimitiveType.values().length][PrimitiveType.values().length];
    strictCompatibilityMatrix = new
        PrimitiveType[PrimitiveType.values().length][PrimitiveType.values().length];

    for (int i = 0; i < PrimitiveType.values().length; ++i) {
      // Each type is compatible with itself.
      compatibilityMatrix[i][i] = PrimitiveType.values()[i];
      // BINARY is not supported.
      compatibilityMatrix[BINARY.ordinal()][i] = PrimitiveType.INVALID_TYPE;
      compatibilityMatrix[i][BINARY.ordinal()] = PrimitiveType.INVALID_TYPE;

      // FIXED_UDA_INTERMEDIATE cannot be cast to/from another type
      if (i != FIXED_UDA_INTERMEDIATE.ordinal()) {
        compatibilityMatrix[FIXED_UDA_INTERMEDIATE.ordinal()][i] =
            PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[i][FIXED_UDA_INTERMEDIATE.ordinal()] =
            PrimitiveType.INVALID_TYPE;
      }
    }

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
    compatibilityMatrix[BOOLEAN.ordinal()][STRING.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[BOOLEAN.ordinal()][VARCHAR.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[BOOLEAN.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[TINYINT.ordinal()][SMALLINT.ordinal()] = PrimitiveType.SMALLINT;
    compatibilityMatrix[TINYINT.ordinal()][INT.ordinal()] = PrimitiveType.INT;
    compatibilityMatrix[TINYINT.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
    // 8 bit integer fits in mantissa of both float and double.
    compatibilityMatrix[TINYINT.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
    compatibilityMatrix[TINYINT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
    compatibilityMatrix[TINYINT.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[TINYINT.ordinal()][DATETIME.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[TINYINT.ordinal()][TIMESTAMP.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[TINYINT.ordinal()][STRING.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[TINYINT.ordinal()][VARCHAR.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[TINYINT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[SMALLINT.ordinal()][INT.ordinal()] = PrimitiveType.INT;
    compatibilityMatrix[SMALLINT.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
    // 16 bit integer fits in mantissa of both float and double.
    compatibilityMatrix[SMALLINT.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
    compatibilityMatrix[SMALLINT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
    compatibilityMatrix[SMALLINT.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[SMALLINT.ordinal()][DATETIME.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[SMALLINT.ordinal()][TIMESTAMP.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[SMALLINT.ordinal()][STRING.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[SMALLINT.ordinal()][VARCHAR.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[SMALLINT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[INT.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
    // 32 bit integer fits only mantissa of double.
    // TODO: arguably we should promote INT + FLOAT to DOUBLE to avoid loss of precision,
    // but we depend on it remaining FLOAT for some use cases, e.g.
    // "insert into tbl (float_col) select int_col + float_col from ..."
    compatibilityMatrix[INT.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
    strictCompatibilityMatrix[INT.ordinal()][FLOAT.ordinal()] = PrimitiveType.DOUBLE;
    compatibilityMatrix[INT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
    compatibilityMatrix[INT.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[INT.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[INT.ordinal()][TIMESTAMP.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[INT.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[INT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[INT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;

    // 64 bit integer does not fit in mantissa of double or float.
    // TODO: arguably we should always promote BIGINT + FLOAT to double here to keep as
    // much precision as possible, but we depend on this implicit cast for some use
    // cases, similarly to INT + FLOAT.
    compatibilityMatrix[BIGINT.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
    strictCompatibilityMatrix[BIGINT.ordinal()][FLOAT.ordinal()] = PrimitiveType.DOUBLE;
    // TODO: we're breaking the definition of strict compatibility for BIGINT + DOUBLE,
    // but this forces function overloading to consider the DOUBLE overload first.
    compatibilityMatrix[BIGINT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
    compatibilityMatrix[BIGINT.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[BIGINT.ordinal()][DATETIME.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[BIGINT.ordinal()][TIMESTAMP.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[BIGINT.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[BIGINT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[BIGINT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[FLOAT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
    compatibilityMatrix[FLOAT.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[FLOAT.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[FLOAT.ordinal()][TIMESTAMP.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[FLOAT.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[FLOAT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[FLOAT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[DOUBLE.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[DOUBLE.ordinal()][DATETIME.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[DOUBLE.ordinal()][TIMESTAMP.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[DOUBLE.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[DOUBLE.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[DOUBLE.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;

    // We can convert some but not all string values to date.
    compatibilityMatrix[DATE.ordinal()][STRING.ordinal()] = PrimitiveType.DATE;
    strictCompatibilityMatrix[DATE.ordinal()][STRING.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[DATE.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[DATE.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[DATETIME.ordinal()][TIMESTAMP.ordinal()] =
        PrimitiveType.TIMESTAMP;
    compatibilityMatrix[DATETIME.ordinal()][DATE.ordinal()] = PrimitiveType.DATETIME;
    compatibilityMatrix[DATETIME.ordinal()][STRING.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[DATETIME.ordinal()][VARCHAR.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[DATETIME.ordinal()][CHAR.ordinal()] =
        PrimitiveType.INVALID_TYPE;

    // We can convert some but not all date values to timestamps.
    compatibilityMatrix[TIMESTAMP.ordinal()][DATE.ordinal()] = PrimitiveType.TIMESTAMP;
    strictCompatibilityMatrix[TIMESTAMP.ordinal()][DATE.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    // We can convert some but not all string values to timestamps.
    compatibilityMatrix[TIMESTAMP.ordinal()][STRING.ordinal()] =
        PrimitiveType.TIMESTAMP;
    strictCompatibilityMatrix[TIMESTAMP.ordinal()][STRING.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[TIMESTAMP.ordinal()][VARCHAR.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[TIMESTAMP.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[STRING.ordinal()][VARCHAR.ordinal()] = PrimitiveType.STRING;
    compatibilityMatrix[STRING.ordinal()][CHAR.ordinal()] = PrimitiveType.STRING;

    compatibilityMatrix[VARCHAR.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;

    // Check all of the necessary entries that should be filled.
    for (int i = 0; i < PrimitiveType.values().length; ++i) {
      for (int j = i; j < PrimitiveType.values().length; ++j) {
        PrimitiveType t1 = PrimitiveType.values()[i];
        PrimitiveType t2 = PrimitiveType.values()[j];
        // DECIMAL, NULL, and INVALID_TYPE  are handled separately.
        if (t1 == PrimitiveType.INVALID_TYPE ||
            t2 == PrimitiveType.INVALID_TYPE) continue;
        if (t1 == PrimitiveType.NULL_TYPE || t2 == PrimitiveType.NULL_TYPE) continue;
        if (t1 == PrimitiveType.DECIMAL || t2 == PrimitiveType.DECIMAL) continue;
        Preconditions.checkNotNull(compatibilityMatrix[i][j]);
      }
    }
  }
}
