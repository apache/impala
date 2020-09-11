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

package org.apache.impala.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.MapType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TCreateTableParams;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.util.IcebergUtil;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * This is a helper for the CatalogOpExecutor to provide Iceberg related DDL functionality
 * such as creating and dropping tables from Iceberg.
 */
public class IcebergCatalogOpExecutor {
  public static final Logger LOG = Logger.getLogger(IcebergCatalogOpExecutor.class);

  // Keep id increase for each thread
  private static ThreadLocal<Integer> iThreadLocal = new ThreadLocal<>();

  /**
   * Create Iceberg table by Iceberg api
   * Return value is table location from Iceberg
   */
  public static String createTable(TIcebergCatalog catalog, String identifier,
      String location, TCreateTableParams params) throws ImpalaRuntimeException {
    // Each table id increase from zero
    iThreadLocal.set(0);
    Schema schema = createIcebergSchema(params);
    PartitionSpec spec = IcebergUtil.createIcebergPartition(schema, params);
    String tableLoc = null;
    if (catalog == TIcebergCatalog.HADOOP_CATALOG) {
      tableLoc = createTableByHadoopCatalog(location, schema, spec, identifier);
    } else {
      Preconditions.checkArgument(catalog == TIcebergCatalog.HADOOP_TABLES);
      tableLoc = createTableByHadoopTables(location, schema, spec);
    }
    LOG.info("Create iceberg table successful.");
    return tableLoc;
  }

  // Create Iceberg table by HadoopTables
  private static String createTableByHadoopTables(String metadataLoc, Schema schema,
      PartitionSpec spec) {
    HadoopTables tables = IcebergUtil.getHadoopTables();
    BaseTable table = (BaseTable) tables.create(schema, spec, null, metadataLoc);
    return table.location();
  }

  // Create Iceberg table by HadoopCatalog
  private static String createTableByHadoopCatalog(String catalogLoc, Schema schema,
      PartitionSpec spec, String identifier) {
    // Each table id increase from zero
    HadoopCatalog catalog = IcebergUtil.getHadoopCatalog(catalogLoc);
    BaseTable table = (BaseTable) catalog.createTable(TableIdentifier.parse(identifier),
        schema, spec, null);
    return table.location();
  }

  /**
   * Transform a StructField to Iceberg NestedField
   */
  private static Types.NestedField createIcebergNestedField(StructField structField)
      throws ImpalaRuntimeException {
    Preconditions.checkState(structField != null);
    org.apache.iceberg.types.Type icebergType = createIcebergType(structField.getType());
    Types.NestedField filed =
        Types.NestedField.required(getNextId(), structField.getName(), icebergType,
            structField.getComment());
    return filed;
  }

  /**
   * Transform a TColumn to Iceberg NestedField
   */
  private static Types.NestedField createIcebergNestedField(TColumn column)
      throws ImpalaRuntimeException {
    Type type = Type.fromThrift(column.getColumnType());
    Preconditions.checkState(type != null);
    org.apache.iceberg.types.Type icebergType = createIcebergType(type);
    Types.NestedField filed =
        Types.NestedField.required(getNextId(), column.getColumnName(), icebergType,
            column.getComment());
    return filed;
  }

  /**
   * Build iceberg schema by parameters.
   */
  private static Schema createIcebergSchema(TCreateTableParams params)
      throws ImpalaRuntimeException {
    List<Types.NestedField> fields = new ArrayList<Types.NestedField>();
    for (TColumn column : params.getColumns()) {
      fields.add(createIcebergNestedField(column));
    }
    return new Schema(fields);
  }

  /**
   * Converts a given Impala catalog type to the Iceberg type, and
   * id is necessary for each iceberg complex type
   */
  public static org.apache.iceberg.types.Type createIcebergType(Type t)
      throws ImpalaRuntimeException {
    if (t.isScalarType()) {
      ScalarType s = (ScalarType) t;
      switch (s.getPrimitiveType()) {
        case INT:
          return Types.IntegerType.get();
        case BIGINT:
          return Types.LongType.get();
        case BOOLEAN:
          return Types.BooleanType.get();
        case STRING:
          return Types.StringType.get();
        case DOUBLE:
          return Types.DoubleType.get();
        case FLOAT:
          return Types.FloatType.get();
        case TIMESTAMP:
          return Types.TimestampType.withZone();
        case DECIMAL:
          return Types.DecimalType.of(s.decimalPrecision(), s.decimalScale());
        case DATE:
          return Types.DateType.get();
        case BINARY:
          return Types.BinaryType.get();
        /* Fall through below */
        case INVALID_TYPE:
        case NULL_TYPE:
        case DATETIME:
        case CHAR:
        case TINYINT:
        case SMALLINT:
        case VARCHAR:
        default:
          throw new ImpalaRuntimeException(String.format(
              "Type %s is not supported in Iceberg", s.toSql()));
      }
    } else if (t.isArrayType()) {
      ArrayType arrayType = (ArrayType) t;
      return Types.ListType.ofRequired(getNextId(),
          createIcebergType(arrayType.getItemType()));
    } else if (t.isMapType()) {
      MapType mapType = (MapType) t;
      return Types.MapType.ofRequired(getNextId(), getNextId(),
          createIcebergType(mapType.getKeyType()),
          createIcebergType(mapType.getValueType()));
    } else if (t.isStructType()) {
      StructType structType = (StructType) t;
      List<Types.NestedField> nestedFields = new ArrayList<Types.NestedField>();
      List<StructField> structFields = structType.getFields();
      for (StructField structField : structFields) {
        nestedFields.add(createIcebergNestedField(structField));
      }
      return Types.StructType.of(nestedFields);
    } else {
      throw new ImpalaRuntimeException(String.format(
          "Type %s is not supported in Iceberg", t.toSql()));
    }
  }

  private static int getNextId() {
    int nextId = iThreadLocal.get();
    iThreadLocal.set(nextId+1);
    return nextId;
  }
}
