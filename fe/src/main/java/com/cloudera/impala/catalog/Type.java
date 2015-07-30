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

import com.cloudera.impala.analysis.CreateTableStmt;
import com.cloudera.impala.analysis.SqlParser;
import com.cloudera.impala.analysis.SqlScanner;
import com.cloudera.impala.analysis.TypeDef;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.thrift.TColumnType;
import com.cloudera.impala.thrift.TPrimitiveType;
import com.cloudera.impala.thrift.TScalarType;
import com.cloudera.impala.thrift.TStructField;
import com.cloudera.impala.thrift.TTypeNode;
import com.cloudera.impala.thrift.TTypeNodeType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Abstract class describing an Impala data type (scalar/complex type).
 * Mostly contains static type instances and helper methods for convenience, as well
 * as abstract methods that subclasses must implement.
 */
public abstract class Type {
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
  public static final ScalarType DEFAULT_DECIMAL = (ScalarType)
      ScalarType.createDecimalType(ScalarType.DEFAULT_PRECISION,
          ScalarType.DEFAULT_SCALE);
  public static final ScalarType DECIMAL =
      (ScalarType) ScalarType.createDecimalTypeInternal(-1, -1);
  public static final ScalarType DEFAULT_VARCHAR = ScalarType.createVarcharType(-1);
  public static final ScalarType VARCHAR = ScalarType.createVarcharType(-1);
  public static final ScalarType CHAR = (ScalarType) ScalarType.createCharType(-1);

  private static ArrayList<ScalarType> integerTypes;
  private static ArrayList<ScalarType> numericTypes;
  private static ArrayList<ScalarType> supportedTypes;

  static {
    integerTypes = Lists.newArrayList();
    integerTypes.add(TINYINT);
    integerTypes.add(SMALLINT);
    integerTypes.add(INT);
    integerTypes.add(BIGINT);

    numericTypes = Lists.newArrayList();
    numericTypes.add(TINYINT);
    numericTypes.add(SMALLINT);
    numericTypes.add(INT);
    numericTypes.add(BIGINT);
    numericTypes.add(FLOAT);
    numericTypes.add(DOUBLE);
    numericTypes.add(DECIMAL);

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
    supportedTypes.add(VARCHAR);
    supportedTypes.add(CHAR);
    supportedTypes.add(TIMESTAMP);
    supportedTypes.add(DECIMAL);
  }

  public static ArrayList<ScalarType> getIntegerTypes() {
    return integerTypes;
  }
  public static ArrayList<ScalarType> getNumericTypes() {
    return numericTypes;
  }
  public static ArrayList<ScalarType> getSupportedTypes() {
    return supportedTypes;
  }

  /**
   * The output of this is stored directly in the hive metastore as the column type.
   * The string must match exactly.
   */
  public abstract String toSql();

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
  public boolean isDecimal() { return isScalarType(PrimitiveType.DECIMAL); }
  public boolean isDecimalOrNull() { return isDecimal() || isNull(); }
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

  public boolean isDateType() {
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
    // Per struct alignment rules, there is an extra 4 bytes of padding to align to 8
    // bytes so 16 bytes total.
    if (isCollectionType()) return 16;
    throw new IllegalStateException("getSlotSize() not implemented for type " + toSql());
  }

  public TColumnType toThrift() {
    TColumnType container = new TColumnType();
    container.setTypes(new ArrayList<TTypeNode>());
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
      if (createTableStmt.getColumnDefs().isEmpty()) {
        // Should never get here.
        throw new IllegalStateException("Invalid create table stmt.");
      }
    } catch (Exception e) {
      return null;
    }
    TypeDef typeDef = createTableStmt.getColumnDefs().get(0).getTypeDef();
    return typeDef.getType();
  }

  /**
   * Returns true if casting t1 to t2 results in no loss of precision, false otherwise.
   * TODO: Support casting of non-scalar types.
   */
  public static boolean isImplicitlyCastable(Type t1, Type t2) {
    if (t1.isScalarType() && t1.isScalarType()) {
      return ScalarType.isImplicitlyCastable(
          (ScalarType) t1, (ScalarType) t2);
    }
    return false;
  }

  /**
   * Return type t such that values from both t1 and t2 can be assigned to t
   * without loss of precision. Returns INVALID_TYPE if there is no such type
   * or if any of t1 and t2 is INVALID_TYPE.
   * TODO: Support non-scalar types.
   */
  public static Type getAssignmentCompatibleType(Type t1, Type t2) {
    if (t1.isScalarType() && t2.isScalarType()) {
      return ScalarType.getAssignmentCompatibleType(
          (ScalarType) t1, (ScalarType) t2);
    }
    return ScalarType.INVALID;
  }

  public static List<TColumnType> toThrift(Type[] types) {
    return toThrift(Lists.newArrayList(types));
  }

  public static List<TColumnType> toThrift(ArrayList<Type> types) {
    ArrayList<TColumnType> result = Lists.newArrayList();
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
              && scalarType.isSetPrecision());
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
        ArrayList<StructField> structFields = Lists.newArrayList();
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
   * Utility function to get the primitive type of a thrift type that is known
   * to be scalar.
   */
  public TPrimitiveType getTPrimitiveType(TColumnType ttype) {
    Preconditions.checkState(ttype.getTypesSize() == 1);
    Preconditions.checkState(ttype.types.get(0).getType() == TTypeNodeType.SCALAR);
    return ttype.types.get(0).scalar_type.getType();
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
      case CHAR:
      case VARCHAR:
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
      case STRING: return java.sql.Types.VARCHAR;
      case CHAR: return java.sql.Types.CHAR;
      case VARCHAR: return java.sql.Types.VARCHAR;
      case BINARY: return java.sql.Types.BINARY;
      case DECIMAL: return java.sql.Types.DECIMAL;
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
   * of the assignment-compatible type without loss of precision.
   *
   * We chose not to follow MySQL's type casting behavior as described here:
   * http://dev.mysql.com/doc/refman/5.0/en/type-conversion.html
   * for the following reasons:
   * conservative casting in arithmetic exprs: TINYINT + TINYINT -> BIGINT
   * comparison of many types as double: INT < FLOAT -> comparison as DOUBLE
   * special cases when dealing with dates and timestamps
   */
  protected static PrimitiveType[][] compatibilityMatrix;
  static {
    compatibilityMatrix = new
        PrimitiveType[CHAR.ordinal() + 1][CHAR.ordinal() + 1];

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
    compatibilityMatrix[NULL.ordinal()][VARCHAR.ordinal()] = PrimitiveType.VARCHAR;
    compatibilityMatrix[NULL.ordinal()][CHAR.ordinal()] = PrimitiveType.CHAR;

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
    compatibilityMatrix[BOOLEAN.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[BOOLEAN.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;

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
    compatibilityMatrix[TINYINT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[TINYINT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;

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
    compatibilityMatrix[SMALLINT.ordinal()][VARCHAR.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[SMALLINT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[INT.ordinal()][INT.ordinal()] = PrimitiveType.INT;
    compatibilityMatrix[INT.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
    compatibilityMatrix[INT.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
    compatibilityMatrix[INT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
    compatibilityMatrix[INT.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[INT.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[INT.ordinal()][TIMESTAMP.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[INT.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[INT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[INT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[BIGINT.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
    compatibilityMatrix[BIGINT.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
    compatibilityMatrix[BIGINT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
    compatibilityMatrix[BIGINT.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[BIGINT.ordinal()][DATETIME.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[BIGINT.ordinal()][TIMESTAMP.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[BIGINT.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[BIGINT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[BIGINT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[FLOAT.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
    compatibilityMatrix[FLOAT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
    compatibilityMatrix[FLOAT.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[FLOAT.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[FLOAT.ordinal()][TIMESTAMP.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[FLOAT.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[FLOAT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[FLOAT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[DOUBLE.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
    compatibilityMatrix[DOUBLE.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[DOUBLE.ordinal()][DATETIME.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[DOUBLE.ordinal()][TIMESTAMP.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[DOUBLE.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[DOUBLE.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[DOUBLE.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[DATE.ordinal()][DATE.ordinal()] = PrimitiveType.DATE;
    compatibilityMatrix[DATE.ordinal()][DATETIME.ordinal()] = PrimitiveType.DATETIME;
    compatibilityMatrix[DATE.ordinal()][TIMESTAMP.ordinal()] = PrimitiveType.TIMESTAMP;
    compatibilityMatrix[DATE.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[DATE.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[DATE.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[DATETIME.ordinal()][DATETIME.ordinal()] = PrimitiveType.DATETIME;
    compatibilityMatrix[DATETIME.ordinal()][TIMESTAMP.ordinal()] =
        PrimitiveType.TIMESTAMP;
    compatibilityMatrix[DATETIME.ordinal()][STRING.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[DATETIME.ordinal()][VARCHAR.ordinal()] =
        PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[TIMESTAMP.ordinal()][TIMESTAMP.ordinal()] =
        PrimitiveType.TIMESTAMP;
    compatibilityMatrix[TIMESTAMP.ordinal()][STRING.ordinal()] =
        PrimitiveType.TIMESTAMP;
    compatibilityMatrix[TIMESTAMP.ordinal()][VARCHAR.ordinal()] =
        PrimitiveType.INVALID_TYPE;
    compatibilityMatrix[TIMESTAMP.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[STRING.ordinal()][STRING.ordinal()] = PrimitiveType.STRING;
    compatibilityMatrix[STRING.ordinal()][VARCHAR.ordinal()] = PrimitiveType.STRING;
    compatibilityMatrix[STRING.ordinal()][CHAR.ordinal()] = PrimitiveType.STRING;

    compatibilityMatrix[VARCHAR.ordinal()][VARCHAR.ordinal()] = PrimitiveType.VARCHAR;
    compatibilityMatrix[VARCHAR.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;

    compatibilityMatrix[CHAR.ordinal()][CHAR.ordinal()] = PrimitiveType.CHAR;
  }
}
