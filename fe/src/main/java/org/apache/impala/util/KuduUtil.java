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

package org.apache.impala.util;

import java.util.HashSet;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TExpr;
import org.apache.impala.thrift.TExprNode;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RangePartitionBound;

import static java.lang.String.format;

public class KuduUtil {

  private static final String KUDU_TABLE_NAME_PREFIX = "impala::";

  /**
   * Creates a PartialRow from a list of range partition boundary values.
   */
  private static PartialRow parseRangePartitionBoundaryValues(Schema schema,
      List<String> rangePartitionColumns, List<TExpr> boundaryValues)
      throws ImpalaRuntimeException {
    Preconditions.checkState(rangePartitionColumns.size() == boundaryValues.size());
    PartialRow bound = new PartialRow(schema);
    for (int i = 0; i < boundaryValues.size(); ++i) {
      String colName = rangePartitionColumns.get(i);
      ColumnSchema col = schema.getColumn(colName);
      Preconditions.checkNotNull(col);
      setKey(col.getType(), boundaryValues.get(i), schema.getColumnIndex(colName),
          colName, bound);
    }
    return bound;
  }

  /**
   * Builds and returns a range-partition bound used in the creation of a Kudu
   * table. The range-partition bound consists of a PartialRow with the boundary
   * values and a RangePartitionBound indicating if the bound is inclusive or exclusive.
   * Throws an ImpalaRuntimeException if an error occurs while parsing the boundary
   * values.
   */
  public static Pair<PartialRow, RangePartitionBound> buildRangePartitionBound(
      Schema schema, List<String> rangePartitionColumns, List<TExpr> boundaryValues,
      boolean isInclusiveBound) throws ImpalaRuntimeException {
    if (boundaryValues == null || boundaryValues.isEmpty()) {
      // TODO: Do we need to set the bound type?
      return new Pair<PartialRow, RangePartitionBound>(new PartialRow(schema),
          RangePartitionBound.INCLUSIVE_BOUND);
    }
    PartialRow bound =
        parseRangePartitionBoundaryValues(schema, rangePartitionColumns, boundaryValues);
    RangePartitionBound boundType = null;
    if (isInclusiveBound) {
      boundType = RangePartitionBound.INCLUSIVE_BOUND;
    } else {
      boundType = RangePartitionBound.EXCLUSIVE_BOUND;
    }
    return new Pair<PartialRow, RangePartitionBound>(bound, boundType);
  }

  /**
   * Sets the value 'boundaryVal' in 'key' at 'pos'. Checks if 'boundaryVal' has the
   * expected data type.
   */
  private static void setKey(org.apache.kudu.Type type, TExpr boundaryVal, int pos,
      String colName, PartialRow key) throws ImpalaRuntimeException {
    Preconditions.checkState(boundaryVal.getNodes().size() == 1);
    TExprNode literal = boundaryVal.getNodes().get(0);
    switch (type) {
      case INT8:
        checkCorrectType(literal.isSetInt_literal(), type, colName, literal);
        key.addByte(pos, (byte) literal.getInt_literal().getValue());
        break;
      case INT16:
        checkCorrectType(literal.isSetInt_literal(), type, colName, literal);
        key.addShort(pos, (short) literal.getInt_literal().getValue());
        break;
      case INT32:
        checkCorrectType(literal.isSetInt_literal(), type, colName, literal);
        key.addInt(pos, (int) literal.getInt_literal().getValue());
        break;
      case INT64:
        checkCorrectType(literal.isSetInt_literal(), type, colName, literal);
        key.addLong(pos, literal.getInt_literal().getValue());
        break;
      case STRING:
        checkCorrectType(literal.isSetString_literal(), type, colName, literal);
        key.addString(pos, literal.getString_literal().getValue());
        break;
      default:
        throw new ImpalaRuntimeException("Key columns not supported for type: "
            + type.toString());
    }
  }

  /**
   * If correctType is true, returns. Otherwise throws a formatted error message
   * indicating problems with the type of the literal of the range literal.
   */
  private static void checkCorrectType(boolean correctType, org.apache.kudu.Type t,
      String colName, TExprNode boundaryVal) throws ImpalaRuntimeException {
    if (correctType) return;
    throw new ImpalaRuntimeException(
        format("Expected '%s' literal for column '%s' got '%s'", t.getName(), colName,
            Type.fromThrift(boundaryVal.getType()).toSql()));
  }

  /**
   * Parses a string of the form "a, b, c" and returns a set of values split by ',' and
   * stripped of the whitespace.
   */
  public static HashSet<String> parseKeyColumns(String cols) {
    return Sets.newHashSet(Splitter.on(",").trimResults().split(cols.toLowerCase()));
  }

  public static List<String> parseKeyColumnsAsList(String cols) {
    return Lists.newArrayList(Splitter.on(",").trimResults().split(cols.toLowerCase()));
  }

  public static boolean isSupportedKeyType(org.apache.impala.catalog.Type type) {
    return type.isIntegerType() || type.isStringType();
  }

  /**
   * Return the name that should be used in Kudu when creating a table, assuming a custom
   * name was not provided.
   */
  public static String getDefaultCreateKuduTableName(String metastoreDbName,
      String metastoreTableName) {
    return KUDU_TABLE_NAME_PREFIX + metastoreDbName + "." + metastoreTableName;
  }

  /**
   * Converts a given Impala catalog type to the Kudu type. Throws an exception if the
   * type cannot be converted.
   */
  public static org.apache.kudu.Type fromImpalaType(Type t)
      throws ImpalaRuntimeException {
    if (!t.isScalarType()) {
      throw new ImpalaRuntimeException(format(
          "Type %s is not supported in Kudu", t.toSql()));
    }
    ScalarType s = (ScalarType) t;
    switch (s.getPrimitiveType()) {
      case TINYINT: return org.apache.kudu.Type.INT8;
      case SMALLINT: return org.apache.kudu.Type.INT16;
      case INT: return org.apache.kudu.Type.INT32;
      case BIGINT: return org.apache.kudu.Type.INT64;
      case BOOLEAN: return org.apache.kudu.Type.BOOL;
      case STRING: return org.apache.kudu.Type.STRING;
      case DOUBLE: return org.apache.kudu.Type.DOUBLE;
      case FLOAT: return org.apache.kudu.Type.FLOAT;
        /* Fall through below */
      case INVALID_TYPE:
      case NULL_TYPE:
      case TIMESTAMP:
      case BINARY:
      case DATE:
      case DATETIME:
      case DECIMAL:
      case CHAR:
      case VARCHAR:
      default:
        throw new ImpalaRuntimeException(format(
            "Type %s is not supported in Kudu", s.toSql()));
    }
  }

  public static Type toImpalaType(org.apache.kudu.Type t)
      throws ImpalaRuntimeException {
    switch (t) {
      case BOOL: return Type.BOOLEAN;
      case DOUBLE: return Type.DOUBLE;
      case FLOAT: return Type.FLOAT;
      case INT8: return Type.TINYINT;
      case INT16: return Type.SMALLINT;
      case INT32: return Type.INT;
      case INT64: return Type.BIGINT;
      case STRING: return Type.STRING;
      default:
        throw new ImpalaRuntimeException(String.format(
            "Kudu type '%s' is not supported in Impala", t.getName()));
    }
  }
}
