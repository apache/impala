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

import static java.lang.String.format;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.DescriptorTable;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.InsertStmt;
import org.apache.impala.analysis.KuduPartitionExpr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.Pair;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TColumnEncoding;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TExpr;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TExprNodeType;
import org.apache.impala.thrift.THdfsCompression;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnSchema.CompressionAlgorithm;
import org.apache.kudu.ColumnSchema.Encoding;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduClient.KuduClientBuilder;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RangePartitionBound;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class KuduUtil {

  private static final String KUDU_TABLE_NAME_PREFIX = "impala::";

  // Number of worker threads created by each KuduClient, regardless of whether or not
  // they're needed. Impala does not share KuduClients between operations, so the number
  // of threads created can get very large under concurrent workloads. This number should
  // be sufficient for the Frontend/Catalog use, and has been tested in stress tests.
  private static int KUDU_CLIENT_WORKER_THREAD_COUNT = 5;

  // Maps lists of master addresses to KuduClients, for sharing clients across the FE.
  private static Map<String, KuduClient> kuduClients_ = Maps.newHashMap();

  /**
   * Gets a KuduClient for the specified Kudu master addresses (as a comma-separated
   * list of host:port pairs). It will look up and share an existing KuduClient, if
   * possible, or it will create a new one to return.
   * The 'admin operation timeout' and the 'operation timeout' are set to
   * BackendConfig.getKuduClientTimeoutMs(). The 'admin operations timeout' is used for
   * operations like creating/deleting tables. The 'operation timeout' is used when
   * fetching tablet metadata.
   */
  public static KuduClient getKuduClient(String kuduMasters) {
    if (!kuduClients_.containsKey(kuduMasters)) {
      KuduClientBuilder b = new KuduClient.KuduClientBuilder(kuduMasters);
      b.defaultAdminOperationTimeoutMs(BackendConfig.INSTANCE.getKuduClientTimeoutMs());
      b.defaultOperationTimeoutMs(BackendConfig.INSTANCE.getKuduClientTimeoutMs());
      b.workerCount(KUDU_CLIENT_WORKER_THREAD_COUNT);
      kuduClients_.put(kuduMasters, b.build());
    }
    return kuduClients_.get(kuduMasters);
  }

  /**
   * Creates a PartialRow from a list of range partition boundary values.
   * 'rangePartitionColumns' must be specified in Kudu case.
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
      setKey(col, boundaryValues.get(i), schema.getColumnIndex(colName), bound);
    }
    return bound;
  }

  /**
   * Builds and returns a range-partition bound used in the creation of a Kudu
   * table. The range-partition bound consists of a PartialRow with the boundary
   * values and a RangePartitionBound indicating if the bound is inclusive or exclusive.
   * Throws an ImpalaRuntimeException if an error occurs while parsing the boundary
   * values. 'rangePartitionColumns' must be specified in Kudu case.
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
  private static void setKey(ColumnSchema col, TExpr boundaryVal, int pos, PartialRow key)
      throws ImpalaRuntimeException {
    Preconditions.checkState(boundaryVal.getNodes().size() == 1);
    TExprNode literal = boundaryVal.getNodes().get(0);
    String colName = col.getName();
    org.apache.kudu.Type type = col.getType();
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
      case UNIXTIME_MICROS:
        checkCorrectType(literal.isSetInt_literal(), type, colName, literal);
        key.addLong(pos, literal.getInt_literal().getValue());
        break;
      case DECIMAL:
        checkCorrectType(literal.isSetDecimal_literal(), type, colName, literal);
        BigInteger unscaledVal = new BigInteger(literal.getDecimal_literal().getValue());
        int scale = col.getTypeAttributes().getScale();
        key.addDecimal(pos, new BigDecimal(unscaledVal, scale));
        break;
      default:
        throw new ImpalaRuntimeException("Key columns not supported for type: "
            + type.toString());
    }
  }

  /**
   * Returns the actual value of the specified defaultValue literal. The returned type is
   * the value type stored by Kudu for the column. For example, The `impalaType` is
   * translated to a Kudu Type 'type' and if 'type' is 'INT8', the returned
   * value is a Java byte, or if 'type' is 'UNIXTIME_MICROS', the returned value is
   * a Java long.
   */
  public static Object getKuduDefaultValue(
      TExpr defaultValue, Type impalaType, String colName) throws ImpalaRuntimeException {
    Preconditions.checkState(defaultValue.getNodes().size() == 1);
    TExprNode literal = defaultValue.getNodes().get(0);
    if (literal.getNode_type() == TExprNodeType.NULL_LITERAL) return null;
    org.apache.kudu.Type type = KuduUtil.fromImpalaType(impalaType);
    switch (type) {
      case INT8:
        checkCorrectType(literal.isSetInt_literal(), type, colName, literal);
        return (byte) literal.getInt_literal().getValue();
      case INT16:
        checkCorrectType(literal.isSetInt_literal(), type, colName, literal);
        return (short) literal.getInt_literal().getValue();
      case INT32:
        checkCorrectType(literal.isSetInt_literal(), type, colName, literal);
        return (int) literal.getInt_literal().getValue();
      case INT64:
        checkCorrectType(literal.isSetInt_literal(), type, colName, literal);
        return (long) literal.getInt_literal().getValue();
      case FLOAT:
        checkCorrectType(literal.isSetFloat_literal(), type, colName, literal);
        return (float) literal.getFloat_literal().getValue();
      case DOUBLE:
        checkCorrectType(literal.isSetFloat_literal(), type, colName, literal);
        return (double) literal.getFloat_literal().getValue();
      case STRING:
        checkCorrectType(literal.isSetString_literal(), type, colName, literal);
        return literal.getString_literal().getValue();
      case BOOL:
        checkCorrectType(literal.isSetBool_literal(), type, colName, literal);
        return literal.getBool_literal().isValue();
      case UNIXTIME_MICROS:
        checkCorrectType(literal.isSetInt_literal(), type, colName, literal);
        return literal.getInt_literal().getValue();
      case DECIMAL:
        checkCorrectType(literal.isSetDecimal_literal(), type, colName, literal);
        BigInteger unscaledVal = new BigInteger(literal.getDecimal_literal().getValue());
        return new BigDecimal(unscaledVal, impalaType.getDecimalDigits());
      default:
        throw new ImpalaRuntimeException("Unsupported value for column type: " +
            type.toString());
    }
  }

  public static long timestampToUnixTimeMicros(Analyzer analyzer, Expr timestampExpr)
      throws AnalysisException, InternalException {
    Preconditions.checkArgument(timestampExpr.isAnalyzed());
    Preconditions.checkArgument(timestampExpr.isConstant());
    Preconditions.checkArgument(timestampExpr.getType() == Type.TIMESTAMP);
    Expr toUnixTimeExpr = new FunctionCallExpr("utc_to_unix_micros",
        Lists.newArrayList(timestampExpr));
    toUnixTimeExpr.analyze(analyzer);
    TColumnValue result = FeSupport.EvalExprWithoutRow(toUnixTimeExpr,
        analyzer.getQueryCtx());
    if (!result.isSetLong_val()) {
      throw new InternalException("Error converting timestamp expression: " +
          timestampExpr.debugString());
    }
    return result.getLong_val();
  }

  public static Encoding fromThrift(TColumnEncoding encoding)
      throws ImpalaRuntimeException {
    switch (encoding) {
      case AUTO:
        return Encoding.AUTO_ENCODING;
      case PLAIN:
        return Encoding.PLAIN_ENCODING;
      case PREFIX:
        return Encoding.PREFIX_ENCODING;
      case GROUP_VARINT:
        return Encoding.GROUP_VARINT;
      case RLE:
        return Encoding.RLE;
      case DICTIONARY:
        return Encoding.DICT_ENCODING;
      case BIT_SHUFFLE:
        return Encoding.BIT_SHUFFLE;
      default:
        throw new ImpalaRuntimeException("Unsupported encoding: " +
            encoding.toString());
    }
  }

  public static TColumnEncoding toThrift(Encoding encoding)
      throws ImpalaRuntimeException {
    switch (encoding) {
      case AUTO_ENCODING:
        return TColumnEncoding.AUTO;
      case PLAIN_ENCODING:
        return TColumnEncoding.PLAIN;
      case PREFIX_ENCODING:
        return TColumnEncoding.PREFIX;
      case GROUP_VARINT:
        return TColumnEncoding.GROUP_VARINT;
      case RLE:
        return TColumnEncoding.RLE;
      case DICT_ENCODING:
        return TColumnEncoding.DICTIONARY;
      case BIT_SHUFFLE:
        return TColumnEncoding.BIT_SHUFFLE;
      default:
        throw new ImpalaRuntimeException("Unsupported encoding: " +
            encoding.toString());
    }
  }

  public static CompressionAlgorithm fromThrift(THdfsCompression compression)
      throws ImpalaRuntimeException {
    switch (compression) {
      case DEFAULT:
        return CompressionAlgorithm.DEFAULT_COMPRESSION;
      case NONE:
        return CompressionAlgorithm.NO_COMPRESSION;
      case SNAPPY:
        return CompressionAlgorithm.SNAPPY;
      case LZ4:
        return CompressionAlgorithm.LZ4;
      case ZLIB:
        return CompressionAlgorithm.ZLIB;
      default:
        throw new ImpalaRuntimeException("Unsupported compression algorithm: " +
            compression.toString());
    }
  }

  public static THdfsCompression toThrift(CompressionAlgorithm compression)
      throws ImpalaRuntimeException {
    switch (compression) {
      case NO_COMPRESSION:
        return THdfsCompression.NONE;
      case DEFAULT_COMPRESSION:
        return THdfsCompression.DEFAULT;
      case SNAPPY:
        return THdfsCompression.SNAPPY;
      case LZ4:
        return THdfsCompression.LZ4;
      case ZLIB:
        return THdfsCompression.ZLIB;
      default:
        throw new ImpalaRuntimeException("Unsupported compression algorithm: " +
            compression.toString());
    }
  }

  public static TColumn setColumnOptions(TColumn column, boolean isKey,
      Boolean isNullable, Encoding encoding, CompressionAlgorithm compression,
      Expr defaultValue, Integer blockSize, String kuduName) {
    column.setIs_key(isKey);
    if (isNullable != null) column.setIs_nullable(isNullable);
    try {
      if (encoding != null) column.setEncoding(toThrift(encoding));
      if (compression != null) column.setCompression(toThrift(compression));
    } catch (ImpalaRuntimeException e) {
      // This shouldn't happen
      throw new IllegalStateException(String.format("Error parsing " +
          "encoding/compression values for Kudu column '%s': %s", column.getColumnName(),
          e.getMessage()));
    }

    if (defaultValue != null) {
      Preconditions.checkState(defaultValue instanceof LiteralExpr);
      column.setDefault_value(defaultValue.treeToThrift());
    }
    if (blockSize != null) column.setBlock_size(blockSize);
    Preconditions.checkNotNull(kuduName);
    column.setKudu_column_name(kuduName);
    return column;
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

  public static boolean isSupportedKeyType(org.apache.impala.catalog.Type type) {
    return type.isIntegerType() || type.isStringType() || type.isTimestamp();
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
      case TIMESTAMP: return org.apache.kudu.Type.UNIXTIME_MICROS;
      case DECIMAL: return org.apache.kudu.Type.DECIMAL;
      /* Fall through below */
      case INVALID_TYPE:
      case NULL_TYPE:
      case BINARY:
      case DATE:
      case DATETIME:
      case CHAR:
      case VARCHAR:
      default:
        throw new ImpalaRuntimeException(format(
            "Type %s is not supported in Kudu", s.toSql()));
    }
  }

  public static Type toImpalaType(org.apache.kudu.Type t,
      ColumnTypeAttributes typeAttributes) throws ImpalaRuntimeException {
    switch (t) {
      case BOOL: return Type.BOOLEAN;
      case DOUBLE: return Type.DOUBLE;
      case FLOAT: return Type.FLOAT;
      case INT8: return Type.TINYINT;
      case INT16: return Type.SMALLINT;
      case INT32: return Type.INT;
      case INT64: return Type.BIGINT;
      case STRING: return Type.STRING;
      case UNIXTIME_MICROS: return Type.TIMESTAMP;
      case DECIMAL:
        return ScalarType.createDecimalType(
            typeAttributes.getPrecision(), typeAttributes.getScale());
      default:
        throw new ImpalaRuntimeException(String.format(
            "Kudu type '%s' is not supported in Impala", t.getName()));
    }
  }

  /**
   * Creates and returns an Expr that takes rows being inserted by 'insertStmt' and
   * returns the partition number for each row.
   */
  public static Expr createPartitionExpr(InsertStmt insertStmt, Analyzer analyzer)
      throws AnalysisException {
    Preconditions.checkState(insertStmt.getTargetTable() instanceof KuduTable);
    Expr kuduPartitionExpr = new KuduPartitionExpr(DescriptorTable.TABLE_SINK_ID,
        (KuduTable) insertStmt.getTargetTable(),
        Lists.newArrayList(insertStmt.getPartitionKeyExprs()),
        insertStmt.getPartitionColPos());
    kuduPartitionExpr.analyze(analyzer);
    return kuduPartitionExpr;
  }
}
