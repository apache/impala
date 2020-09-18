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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.MapType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.TableNotFoundException;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.iceberg.IcebergCatalog;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.fb.FbIcebergDataFile;
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
  public static Table createTable(TIcebergCatalog catalog, TableIdentifier identifier,
      String location, TCreateTableParams params) throws ImpalaRuntimeException {
    // Each table id increase from zero
    iThreadLocal.set(0);
    Schema schema = createIcebergSchema(params);
    PartitionSpec spec = IcebergUtil.createIcebergPartition(schema, params);
    IcebergCatalog icebergCatalog = IcebergUtil.getIcebergCatalog(catalog, location);
    Table iceTable = icebergCatalog.createTable(identifier, schema, spec, location, null);
    LOG.info("Create iceberg table successful.");
    return iceTable;
  }

  /**
   * Drops Iceberg table from Iceberg's catalog.
   * Throws TableNotFoundException if table is not found and 'ifExists' is false.
   */
  public static void dropTable(FeIcebergTable feTable, boolean ifExists)
      throws TableNotFoundException, ImpalaRuntimeException {
    Preconditions.checkState(
        IcebergTable.isSynchronizedTable(feTable.getMetaStoreTable()));
    IcebergCatalog iceCatalog = IcebergUtil.getIcebergCatalog(feTable);
    if (!iceCatalog.dropTable(feTable,
        IcebergTable.isSynchronizedTable(feTable.getMetaStoreTable()))) {
      // The table didn't exist.
      if (!ifExists) {
        throw new TableNotFoundException(String.format(
            "Table '%s' does not exist in Iceberg catalog.", feTable.getFullName()));
      }
    }
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
          // Impala TIMESTAMP has timestamp without time zone semantics which is
          // the TIMESTAMP type in Iceberg.
          return Types.TimestampType.withoutZone();
        case DECIMAL:
          return Types.DecimalType.of(s.decimalPrecision(), s.decimalScale());
        case DATE:
          return Types.DateType.get();
        case BINARY:
          return Types.BinaryType.get();
        case CHAR:
          return Types.FixedType.ofLength(s.getLength());
        /* Fall through below */
        case TINYINT:
        case SMALLINT:
        case INVALID_TYPE:
        case NULL_TYPE:
        case VARCHAR:
        case DATETIME:
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

  /**
   * Append the newly inserted data files to the Iceberg table using the AppendFiles
   * API.
   */
  public static void appendFiles(FeIcebergTable feIcebergTable,
      List<ByteBuffer> dataFilesFb) throws ImpalaRuntimeException, TableLoadingException {
    org.apache.iceberg.Table nativeIcebergTable =
        IcebergUtil.loadTable(feIcebergTable);
    AppendFiles append = nativeIcebergTable.newAppend();
    for (ByteBuffer buf : dataFilesFb) {
      FbIcebergDataFile dataFile = FbIcebergDataFile.getRootAsFbIcebergDataFile(buf);
      DataFiles.Builder builder = DataFiles.builder(nativeIcebergTable.spec())
          .withPath(dataFile.path())
          .withFormat(IcebergUtil.fbFileFormatToIcebergFileFormat(dataFile.format()))
          .withRecordCount(dataFile.recordCount())
          .withFileSizeInBytes(dataFile.fileSizeInBytes());
      append.appendFile(builder.build());
    }
    append.commit();
  }

  private static int getNextId() {
    int nextId = iThreadLocal.get();
    iThreadLocal.set(nextId+1);
    return nextId;
  }
}
