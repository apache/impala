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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseReplacePartitions;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventPropertyKey;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.TableNotFoundException;
import org.apache.impala.catalog.iceberg.IcebergCatalog;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.fb.FbIcebergColumnStats;
import org.apache.impala.fb.FbIcebergDataFile;
import org.apache.impala.thrift.TAlterTableExecuteParams;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.thrift.TIcebergOperationParam;
import org.apache.impala.thrift.TIcebergPartitionSpec;
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
      String location, List<TColumn> columns, TIcebergPartitionSpec partitionSpec,
      Map<String, String> tableProperties) throws ImpalaRuntimeException {
    // Each table id increase from zero
    Schema schema = createIcebergSchema(columns);
    PartitionSpec spec = IcebergUtil.createIcebergPartition(schema, partitionSpec);
    IcebergCatalog icebergCatalog = IcebergUtil.getIcebergCatalog(catalog, location);
    Table iceTable = icebergCatalog.createTable(identifier, schema, spec, location,
        tableProperties);
    LOG.info("Create iceberg table successful.");
    return iceTable;
  }

  /**
   * Populates HMS table schema based on the Iceberg table's schema.
   */
  public static void populateExternalTableCols(
      org.apache.hadoop.hive.metastore.api.Table msTbl, Table iceTbl)
      throws TableLoadingException {
    msTbl.getSd().setCols(IcebergSchemaConverter.convertToHiveSchema(iceTbl.schema()));
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
  public static void addColumns(Transaction txn, List<TColumn> columns)
      throws TableLoadingException, ImpalaRuntimeException {
    UpdateSchema schema = txn.updateSchema();
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
   *   DECIMAL(p1,s1) -> DECIMAL(p1,s2), same scale, p1<=p2
   */
  public static void alterColumn(Transaction txn, String colName, TColumn newCol)
      throws TableLoadingException, ImpalaRuntimeException {
    UpdateSchema schema = txn.updateSchema();
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
   * Sets new default partition spec for an Iceberg table.
   */
  public static void alterTableSetPartitionSpec(FeIcebergTable feTable,
      TIcebergPartitionSpec partSpec, String catalogServiceId, long catalogVersion)
      throws TableLoadingException, ImpalaRuntimeException {
    BaseTable iceTable = (BaseTable)feTable.getIcebergApiTable();
    TableOperations tableOp = iceTable.operations();
    TableMetadata metadata = tableOp.current();
    Schema schema = metadata.schema();
    PartitionSpec newPartSpec = IcebergUtil.createIcebergPartition(schema, partSpec);
    TableMetadata newMetadata = metadata.updatePartitionSpec(newPartSpec);
    Map<String, String> properties = new HashMap<>(newMetadata.properties());
    properties.put(MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey(),
                   catalogServiceId);
    properties.put(MetastoreEventPropertyKey.CATALOG_VERSION.getKey(),
                   String.valueOf(catalogVersion));
    newMetadata = newMetadata.replaceProperties(properties);
    tableOp.commit(metadata, newMetadata);
  }

  public static String alterTableExecute(Transaction txn,
      TAlterTableExecuteParams params) {
    ExpireSnapshots expireApi = txn.expireSnapshots();
    expireApi.expireOlderThan(params.older_than_millis);
    expireApi.commit();
    return "Snapshots have been expired.";
  }

  /**
   * Drops a column from a Iceberg table.
   */
  public static void dropColumn(Transaction txn, String colName)
      throws TableLoadingException, ImpalaRuntimeException {
    UpdateSchema schema = txn.updateSchema();
    schema.deleteColumn(colName);
    schema.commit();
  }

  /**
   * Rename Iceberg table
   */
  public static void renameTable(FeIcebergTable feTable, TableIdentifier tableId)
      throws ImpalaRuntimeException {
    IcebergCatalog catalog = IcebergUtil.getIcebergCatalog(feTable);
    catalog.renameTable(feTable, tableId);
  }

  /**
   * Set TBLPROPERTIES.
   */
  public static void setTblProperties(Transaction txn, Map<String, String> properties) {
    UpdateProperties updateProps = txn.updateProperties();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      updateProps.set(entry.getKey(), entry.getValue());
    }
    updateProps.commit();
  }

  /**
   * Unset TBLPROPERTIES
   */
  public static void unsetTblProperties(Transaction txn, List<String> removeProperties) {
    UpdateProperties updateProps = txn.updateProperties();
    for (String prop : removeProperties) {
      updateProps.remove(prop);
    }
    updateProps.commit();
  }

  /**
   * Build iceberg schema by parameters.
   */
  private static Schema createIcebergSchema(List<TColumn> columns)
      throws ImpalaRuntimeException {
    return IcebergSchemaConverter.genIcebergSchema(columns);
  }

  /**
   * An auxiliary interface for the Append and Overwrite operations.
   */
  private static interface BatchWrite {
    public void addFile(DataFile file);
    public void commit();
  }

  private static class Append implements BatchWrite {
    final private AppendFiles append;
    public Append(Transaction txn) {
      append = txn.newAppend();
    }

    @Override
    public void addFile(DataFile file) {
      append.appendFile(file);
    }

    @Override
    public void commit() {
      append.commit();
    }
  }

  private static class DynamicOverwrite implements BatchWrite {
    final private ReplacePartitions replace;
    final long initialSnapshotId;
    public DynamicOverwrite(Transaction txn, long initialSnapshotId) {
      replace = txn.newReplacePartitions();
      this.initialSnapshotId = initialSnapshotId;
    }

    @Override
    public void addFile(DataFile file) {
      replace.addFile(file);
    }

    @Override
    public void commit() {
      replace.validateFromSnapshot(initialSnapshotId);
      replace.validateNoConflictingData();
      replace.validateNoConflictingDeletes();
      replace.commit();
    }
  }

  /**
   * Append the newly inserted data files to the Iceberg table using the AppendFiles
   * API.
   */
  public static void appendFiles(FeIcebergTable feIcebergTable, Transaction txn,
      TIcebergOperationParam icebergOp) throws ImpalaRuntimeException {
    org.apache.iceberg.Table nativeIcebergTable = feIcebergTable.getIcebergApiTable();
    List<ByteBuffer> dataFilesFb = icebergOp.getIceberg_data_files_fb();
    BatchWrite batchWrite;
    if (icebergOp.isIs_overwrite()) {
      batchWrite = new DynamicOverwrite(txn, icebergOp.getInitial_snapshot_id());
    } else {
      batchWrite = new Append(txn);
    }
    for (ByteBuffer buf : dataFilesFb) {
      FbIcebergDataFile dataFile = FbIcebergDataFile.getRootAsFbIcebergDataFile(buf);

      PartitionSpec partSpec = nativeIcebergTable.specs().get(icebergOp.getSpec_id());
      Metrics metrics = buildDataFileMetrics(dataFile);
      DataFiles.Builder builder =
          DataFiles.builder(partSpec)
          .withMetrics(metrics)
          .withPath(dataFile.path())
          .withFormat(IcebergUtil.fbFileFormatToIcebergFileFormat(dataFile.format()))
          .withRecordCount(dataFile.recordCount())
          .withFileSizeInBytes(dataFile.fileSizeInBytes());
      IcebergUtil.PartitionData partitionData = IcebergUtil.partitionDataFromPath(
          partSpec.partitionType(),
          feIcebergTable.getDefaultPartitionSpec(), dataFile.partitionPath());
      if (partitionData != null) builder.withPartition(partitionData);
      batchWrite.addFile(builder.build());
    }
    try {
      batchWrite.commit();
    } catch (ValidationException e) {
      throw new ImpalaRuntimeException(e.getMessage(), e);
    }
  }

  private static Metrics buildDataFileMetrics(FbIcebergDataFile dataFile) {
    Map<Integer, Long> columnSizes = new HashMap<>();
    Map<Integer, Long> valueCounts = new HashMap<>();
    Map<Integer, Long> nullValueCounts = new HashMap<>();
    Map<Integer, ByteBuffer> lowerBounds = new HashMap<>();
    Map<Integer, ByteBuffer> upperBounds = new HashMap<>();
    for (int i = 0; i < dataFile.perColumnStatsLength(); ++i) {
      FbIcebergColumnStats stats = dataFile.perColumnStats(i);
      if (stats != null) {
        int fieldId = stats.fieldId();
        if (fieldId != -1) {
          columnSizes.put(fieldId, stats.totalCompressedByteSize());
          valueCounts.put(fieldId, stats.valueCount());
          nullValueCounts.put(fieldId, stats.nullCount());
          if (stats.lowerBoundLength() > 0) {
            lowerBounds.put(fieldId, stats.lowerBoundAsByteBuffer());
          }
          if (stats.upperBoundLength() > 0) {
            upperBounds.put(fieldId, stats.upperBoundAsByteBuffer());
          }
        }
      }
    }
    return new Metrics(dataFile.recordCount(), columnSizes, valueCounts,
        nullValueCounts, null, lowerBounds, upperBounds);
  }

  /**
   * Creates new snapshot for the iceberg table by deleting all data files.
   */
  public static void truncateTable(Transaction txn)
      throws ImpalaRuntimeException {
    DeleteFiles delete = txn.newDelete();
    delete.deleteFromRowFilter(Expressions.alwaysTrue());
    delete.commit();
  }

  /**
   * Sets catalog service id and the new catalog version in table properties using 'txn'.
   * This way we can avoid reloading the table on self-events.
   */
  public static void addCatalogVersionToTxn(Transaction txn, String serviceId,
      long version) {
    UpdateProperties updateProps = txn.updateProperties();
    updateProps.set(MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey(),
                    serviceId);
    updateProps.set(MetastoreEventPropertyKey.CATALOG_VERSION.getKey(),
                    String.valueOf(version));
    updateProps.commit();
  }
}
