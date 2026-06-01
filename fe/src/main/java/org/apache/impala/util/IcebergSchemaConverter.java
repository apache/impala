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

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.BoolLiteral;
import org.apache.impala.analysis.DateLiteral;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.analysis.StringLiteral;
import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.IcebergColumn;
import org.apache.impala.catalog.IcebergStructField;
import org.apache.impala.catalog.MapType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TColumnType;

/**
 * Utility class for converting between Iceberg and Impala schemas and types.
 */
public class IcebergSchemaConverter {
  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");

  // The methods in this class are public and static, hence it's possible to invoke
  // them from multiple threads. Hence we use this thread-local integer to generate
  // unique field ids for each schema element. Please note that Iceberg only care about
  // the uniqueness of the field ids, but they will be reassigned by Iceberg.
  private static ThreadLocal<Integer> iThreadLocal = new ThreadLocal<Integer>() {
    @Override
    public Integer initialValue() {
        return 0;
    }
  };

  /**
   * Transform iceberg type to impala type
   */
  public static Type toImpalaType(org.apache.iceberg.types.Type t)
      throws ImpalaRuntimeException {
    switch (t.typeId()) {
      case BOOLEAN:
        return Type.BOOLEAN;
      case INTEGER:
        return Type.INT;
      case LONG:
        return Type.BIGINT;
      case FLOAT:
        return Type.FLOAT;
      case DOUBLE:
        return Type.DOUBLE;
      case STRING:
        return Type.STRING;
      case DATE:
        return Type.DATE;
      case BINARY:
        return Type.BINARY;
      case TIMESTAMP:
        return Type.TIMESTAMP;
      case DECIMAL:
        Types.DecimalType decimal = (Types.DecimalType) t;
        return ScalarType.createDecimalType(decimal.precision(), decimal.scale());
      case LIST: {
        Types.ListType listType = (Types.ListType) t;
        return new ArrayType(toImpalaType(listType.elementType()));
      }
      case MAP: {
        Types.MapType mapType = (Types.MapType) t;
        return new MapType(toImpalaType(mapType.keyType()),
            toImpalaType(mapType.valueType()));
      }
      case STRUCT: {
        Types.StructType structType = (Types.StructType) t;
        List<StructField> structFields = new ArrayList<>();
        List<Types.NestedField> nestedFields = structType.fields();
        for (Types.NestedField nestedField : nestedFields) {
          // Get field id from 'NestedField'.
          structFields.add(new IcebergStructField(nestedField.name(),
              toImpalaType(nestedField.type()), nestedField.doc(),
              nestedField.fieldId()));
        }
        return new StructType(structFields);
      }
      default:
        throw new ImpalaRuntimeException(String.format(
            "Iceberg type '%s' is not supported in Impala", t.typeId()));
    }
  }

  /**
   * Converts Iceberg schema to a Hive schema.
   */
  public static List<FieldSchema> convertToHiveSchema(Schema schema)
      throws ImpalaRuntimeException {
    List<FieldSchema> ret = new ArrayList<>();
    for (Types.NestedField column : schema.columns()) {
      Type colType = toImpalaType(column.type());
      // Update sd cols by iceberg NestedField
      ret.add(new FieldSchema(column.name().toLowerCase(), colType.toSql().toLowerCase(),
          column.doc()));
    }
    return ret;
  }

  /**
   * Converts Iceberg schema to an Impala schema.
   */
  public static List<Column> convertToImpalaSchema(Schema schema)
      throws ImpalaRuntimeException {
    List<Column> ret = new ArrayList<>();
    int pos = 0;
    for (Types.NestedField column : schema.columns()) {
      Type colType = toImpalaType(column.type());
      int keyId = -1, valueId = -1;
      if (colType.isMapType()) {
        // Get real map key and value field id if this column is Map type.
        Types.MapType mapType = (Types.MapType) column.type();
        keyId = mapType.keyId();
        valueId = mapType.valueId();
      }
      String initialDefault = column.initialDefaultLiteral() != null ?
          convertIcebergLiteralToString(column.initialDefaultLiteral(),
          column.type()) : null;
      String writeDefault = column.writeDefaultLiteral() != null ?
          convertIcebergLiteralToString(column.writeDefaultLiteral(),
          column.type()) : null;
      ret.add(new IcebergColumn(column.name(), colType, column.doc(), pos++,
          column.fieldId(), keyId, valueId, column.isOptional(), false,
          initialDefault, writeDefault));
    }
    return ret;
  }

  public static Schema convertToIcebergSchema(Table table) {
    List<FieldSchema> columns = Lists.newArrayList(table.getSd().getCols());
    columns.addAll(table.getPartitionKeys());
    return HiveSchemaUtil.convert(columns, false);
  }

  public static PartitionSpec createIcebergPartitionSpec(Table table,
      Schema schema) {
    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
    for (FieldSchema partitionKey : table.getPartitionKeys()) {
      specBuilder.identity(partitionKey.getName());
    }
    return specBuilder.build();
  }

  /**
   * Generates Iceberg schema from given columns. It also assigns a unique 'field id' for
   * each schema element, although Iceberg will reassign the ids. 'primaryKeyColumnNames'
   * are used for populating 'identifier-field-ids' in the Schema.
   */
  public static Schema genIcebergSchema(List<TColumn> columns,
      List<String> primaryKeyColumnNames) throws ImpalaRuntimeException {
    iThreadLocal.set(1);
    List<Types.NestedField> fields = new ArrayList<Types.NestedField>();
    Map<String, Integer> colNameToFieldId = new HashMap<>();
    for (TColumn column : columns) {
      Types.NestedField icebergField = createIcebergField(column);
      fields.add(icebergField);
      colNameToFieldId.put(icebergField.name(), icebergField.fieldId());
    }

    if (primaryKeyColumnNames == null || primaryKeyColumnNames.isEmpty()) {
      return new Schema(fields);
    }

    Set<Integer> identifierFieldIds = new HashSet<>();
    for (String pkColName : primaryKeyColumnNames) {
      if (!colNameToFieldId.containsKey(pkColName)) {
        throw new ImpalaRuntimeException("Invalid primary key column name: " + pkColName);
      }
      identifierFieldIds.add(colNameToFieldId.get(pkColName));
    }
    return new Schema(fields, identifierFieldIds);
  }

  /**
   * Validates that a column type supports Iceberg V3 DDL default values.
   */
  public static void validateIcebergDdlDefaultType(Type impalaType)
      throws AnalysisException {
    if (impalaType.isComplexType()) {
      throw new AnalysisException(String.format(
          "Default values are not supported for type: %s", impalaType.toSql()));
    }
    if (impalaType.isTimestamp()) {
      throw new AnalysisException(
          "Iceberg DDL default values for TIMESTAMP type are not supported; "
          + "only DATE is supported among temporal types.");
    }
    if (impalaType.isBinary()) {
      throw new AnalysisException(String.format(
          "Default values are not supported for type: %s", impalaType.toSql()));
    }
  }

  /**
   * Converts an analyzed Impala literal to an Iceberg schema default literal.
   */
  public static Literal<?> literalExprToIcebergLiteral(LiteralExpr expr,
      org.apache.iceberg.types.Type icebergType) throws ImpalaRuntimeException {
    try {
      switch (icebergType.typeId()) {
        case BOOLEAN:
          return Literal.of(((BoolLiteral) expr).getValue());
        case INTEGER:
          return Literal.of(((NumericLiteral) expr).getIntValue());
        case LONG:
          return Literal.of(((NumericLiteral) expr).getLongValue());
        case FLOAT:
          return Literal.of((float) ((NumericLiteral) expr).getDoubleValue());
        case DOUBLE:
          return Literal.of(((NumericLiteral) expr).getDoubleValue());
        case STRING:
          return Literal.of(((StringLiteral) expr).getUnescapedValue());
        case DECIMAL:
          return Literal.of(((NumericLiteral) expr).getValue());
        case DATE:
          return Literal.of(((DateLiteral) expr).getValue());
        case TIME:
        case TIMESTAMP:
          throw new ImpalaRuntimeException(
              "Iceberg DDL default values for " + icebergType.typeId()
              + " type are not supported; only DATE is supported among temporal types.");
        case BINARY:
          throw new ImpalaRuntimeException(
              "Default values are not supported for type: " + icebergType.typeId());
        default:
          throw new ImpalaRuntimeException(
              "Default values are not supported for type: " + icebergType.typeId());
      }
    } catch (ClassCastException e) {
      throw new ImpalaRuntimeException(String.format("Cannot convert default literal '%s'"
          + " to Iceberg type %s", expr.toSql(), icebergType.typeId()));
    }
  }

  /**
   * Parses a default value string (from analyzed {@link LiteralExpr} serialization) into
   * an Iceberg literal, using {@link LiteralExpr#createFromUnescapedStr}.
   */
  public static Literal<?> parseStringToIcebergLiteral(String defaultStr,
      org.apache.iceberg.types.Type type) throws ImpalaRuntimeException {
    if (defaultStr == null) {
      return null;
    }
    if (defaultStr.isEmpty()) {
      if (type.typeId() == org.apache.iceberg.types.Type.TypeID.STRING)
        return org.apache.iceberg.expressions.Literal.of("");
      throw new ImpalaRuntimeException(String.format("Invalid empty literal for " +
          "non-STRING type: %s", type.typeId()));
    }
    try {
      Type impalaType = toImpalaType(type);
      LiteralExpr lit =
          LiteralExpr.createFromUnescapedStr(defaultStr.trim(), impalaType);
      return literalExprToIcebergLiteral(lit, type);
    } catch (AnalysisException e) {
      throw new ImpalaRuntimeException(String.format("Cannot parse default value '%s' as "
          + "%s: %s", defaultStr, type.typeId(), e.getMessage()));
    }
  }

  /**
   * Create iceberg field from TColumn
   */
  private static Types.NestedField createIcebergField(TColumn column)
      throws ImpalaRuntimeException {
    org.apache.iceberg.types.Type icebergType = fromImpalaColumnType(
        column.getColumnType());
    Types.NestedField.Builder fieldBuilder = column.isIs_nullable()
        ? Types.NestedField.optional(column.getColumnName())
        : Types.NestedField.required(column.getColumnName());
    fieldBuilder.withId(nextId()).ofType(icebergType);
    if (column.getComment() != null && !column.getComment().isEmpty()) {
      fieldBuilder.withDoc(column.getComment());
    }

    boolean hasInit = column.isSetIceberg_initial_default();
    boolean hasWrite = column.isSetIceberg_write_default();
    String initStr = hasInit ? column.getIceberg_initial_default() : null;
    String writeStr = hasWrite ? column.getIceberg_write_default() : null;
    if (hasInit && hasWrite && !initStr.equals(writeStr)) {
      throw new ImpalaRuntimeException(String.format(
          "Iceberg ADD/CREATE column '%s' cannot set different initial-default and "
              + "write-default literals in a single operation.",
          column.getColumnName()));
    }
    String defStr = hasInit ? initStr : writeStr;
    if (defStr != null) {
      Literal<?> lit = parseStringToIcebergLiteral(defStr, icebergType);
      fieldBuilder.withInitialDefault(lit).withWriteDefault(lit);
    }
    return fieldBuilder.build();
  }

  public static org.apache.iceberg.types.Type fromImpalaColumnType(TColumnType colType)
      throws ImpalaRuntimeException {
    return fromImpalaType(Type.fromThrift(colType));
  }

  /**
   * Transform impala type to iceberg type.
   */
  public static org.apache.iceberg.types.Type fromImpalaType(Type t)
      throws ImpalaRuntimeException {
    if (t.isScalarType()) {
      ScalarType st = (ScalarType) t;
      switch (st.getPrimitiveType()) {
        case BOOLEAN:
          return Types.BooleanType.get();
        case INT:
          return Types.IntegerType.get();
        case BIGINT:
          return Types.LongType.get();
        case FLOAT:
          return Types.FloatType.get();
        case DOUBLE:
          return Types.DoubleType.get();
        case STRING:
          return Types.StringType.get();
        case DATE:
          return Types.DateType.get();
        case BINARY:
          return Types.BinaryType.get();
        case TIMESTAMP:
          // Impala TIMESTAMP has timestamp without time zone semantics.
          return Types.TimestampType.withoutZone();
        case DECIMAL:
          return Types.DecimalType.of(st.decimalPrecision(), st.decimalScale());
        default:
          throw new ImpalaRuntimeException(String.format(
              "Type %s is not supported in Iceberg", t.toSql()));
      }
    } else if (t.isArrayType()) {
      ArrayType at = (ArrayType) t;
      return Types.ListType.ofRequired(nextId(), fromImpalaType(at.getItemType()));
    } else if (t.isMapType()) {
      MapType mt = (MapType) t;
      return Types.MapType.ofRequired(nextId(), nextId(),
          fromImpalaType(mt.getKeyType()), fromImpalaType(mt.getValueType()));
    } else if (t.isStructType()) {
      StructType st = (StructType) t;
      List<Types.NestedField> icebergFields = new ArrayList<>();
      for (StructField field : st.getFields()) {
        icebergFields.add(Types.NestedField.required(nextId(), field.getName(),
            fromImpalaType(field.getType()), field.getComment()));
      }
      return Types.StructType.of(icebergFields);
    } else {
      throw new ImpalaRuntimeException(String.format(
          "Type %s is not supported in Iceberg", t.toSql()));
    }
  }

  private static int nextId() {
    int nextId = iThreadLocal.get();
    iThreadLocal.set(nextId+1);
    return nextId;
  }

  /**
   * Converts an Iceberg Literal to a string that Impala's TextConverter can parse.
   * Iceberg stores defaults in metadata.json but the Literal API returns internal
   * representations.
   */
  private static String convertIcebergLiteralToString(
      org.apache.iceberg.expressions.Literal<?> literal,
      org.apache.iceberg.types.Type icebergType) {
    if (literal == null) return null;

    switch (icebergType.typeId()) {
      case DATE:
        // Iceberg Literal.value() returns Integer (days since Unix epoch 1970-01-01)
        // TextConverter expects "yyyy-MM-dd" format
        Integer days = (Integer) literal.value();
        if (days == null) return null;
        return LocalDate.ofEpochDay(days).toString();

      case TIMESTAMP:
        // Iceberg Literal.value() returns Long (microseconds since Unix epoch)
        // TextConverter expects "yyyy-MM-dd'T'HH:mm:ss.SSSSSS" format (ISO-8601)
        Long micros = (Long) literal.value();
        if (micros == null) return null;

        // Use floor division/mod for handling pre-epoch timestamps.
        long seconds = Math.floorDiv(micros, 1_000_000L);
        long nanos = Math.floorMod(micros, 1_000_000L) * 1000L;
        Instant instant = Instant.ofEpochSecond(seconds, nanos);

        return instant.atZone(ZoneOffset.UTC).format(TIMESTAMP_FORMATTER);

      case STRING:
        Object val = literal.value();
        return val != null ? val.toString() : null;

      default:
        return literal.toString();
    }
  }
}
