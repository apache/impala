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
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.TableNotFoundException;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.iceberg.IcebergCatalog;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.fb.FbIcebergDataFile;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TCreateTableParams;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.util.IcebergSchemaConverter;
import org.apache.impala.util.IcebergUtil;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * This is a helper for the CatalogOpExecutor to provide Iceberg related DDL functionality
 * such as creating and dropping tables from Iceberg.
 */
public class IcebergCatalogOpExecutor {
  public static final Logger LOG = Logger.getLogger(IcebergCatalogOpExecutor.class);

  /**
   * Create Iceberg table by Iceberg api
   * Return value is table location from Iceberg
   */
  public static Table createTable(TIcebergCatalog catalog, TableIdentifier identifier,
      String location, TCreateTableParams params) throws ImpalaRuntimeException {
    // Each table id increase from zero
    Schema schema = createIcebergSchema(params);
    PartitionSpec spec = IcebergUtil.createIcebergPartition(schema, params);
    IcebergCatalog icebergCatalog = IcebergUtil.getIcebergCatalog(catalog, location);
    Table iceTable = icebergCatalog.createTable(identifier, schema, spec, location,
        params.getTable_properties());
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
   * Adds a column to an existing Iceberg table.
   */
  public static void addColumn(FeIcebergTable feTable, List<TColumn> columns)
      throws TableLoadingException, ImpalaRuntimeException {
    UpdateSchema schema = IcebergUtil.getIcebergUpdateSchema(feTable);
    for (TColumn column : columns) {
      org.apache.iceberg.types.Type type =
          IcebergSchemaConverter.fromImpalaColumnType(column.getColumnType());
      schema.addColumn(column.getColumnName(), type, column.getComment());
    }
    schema.commit();
  }

  /**
   * Updates the column from Iceberg table.
   * Iceberg only supports these type conversions:
   *   INTEGER -> LONG
   *   FLOAT -> DOUBLE
   *   DECIMAL(s1,p1) -> DECIMAL(s1,p2), same scale, p1<=p2
   */
  public static void alterColumn(FeIcebergTable feTable, String colName, TColumn newCol)
      throws TableLoadingException, ImpalaRuntimeException {
    UpdateSchema schema = IcebergUtil.getIcebergUpdateSchema(feTable);
    org.apache.iceberg.types.Type type =
        IcebergSchemaConverter.fromImpalaColumnType(newCol.getColumnType());
    // Cannot change a column to complex type
    Preconditions.checkState(type.isPrimitiveType());
    schema.updateColumn(colName, type.asPrimitiveType());

    // Rename column if newCol name and oldCol name are different
    if (!colName.equals(newCol.getColumnName())) {
      schema.renameColumn(colName, newCol.getColumnName());
    }

    // Update column comment if not empty
    if (newCol.getComment() != null && !newCol.getComment().isEmpty()) {
      schema.updateColumnDoc(colName, newCol.getComment());
    }
    schema.commit();
  }

  /**
   * Drops a column from a Iceberg table.
   */
  public static void dropColumn(FeIcebergTable feTable, String colName)
      throws TableLoadingException, ImpalaRuntimeException {
    UpdateSchema schema = IcebergUtil.getIcebergUpdateSchema(feTable);
    schema.deleteColumn(colName);
    schema.commit();
  }

  /**
   * Rename Iceberg table
   */
  public static void renameTable(FeIcebergTable feTable, TableIdentifier tableId)
      throws ImpalaRuntimeException{
    IcebergCatalog catalog = IcebergUtil.getIcebergCatalog(feTable);
    catalog.renameTable(feTable, tableId);
  }

  /**
   * Build iceberg schema by parameters.
   */
  private static Schema createIcebergSchema(TCreateTableParams params)
      throws ImpalaRuntimeException {
    return IcebergSchemaConverter.genIcebergSchema(params.getColumns());
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
}
