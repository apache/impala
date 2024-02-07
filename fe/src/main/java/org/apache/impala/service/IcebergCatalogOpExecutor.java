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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.impala.analysis.IcebergPartitionSpec;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.TableNotFoundException;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventPropertyKey;
import org.apache.impala.catalog.iceberg.GroupedContentFiles;
import org.apache.impala.catalog.iceberg.IcebergCatalog;
import org.apache.impala.catalog.iceberg.IcebergHiveCatalog;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.fb.FbIcebergColumnStats;
import org.apache.impala.fb.FbIcebergDataFile;
import org.apache.impala.thrift.TAlterTableDropPartitionParams;
import org.apache.impala.thrift.TAlterTableExecuteExpireSnapshotsParams;
import org.apache.impala.thrift.TAlterTableExecuteRollbackParams;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.thrift.TIcebergOperationParam;
import org.apache.impala.thrift.TIcebergPartitionSpec;
import org.apache.impala.thrift.TRollbackType;
import org.apache.impala.util.IcebergSchemaConverter;
import org.apache.impala.util.IcebergUtil;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a helper for the CatalogOpExecutor to provide Iceberg related DDL functionality
 * such as creating and dropping tables from Iceberg.
 */
public class IcebergCatalogOpExecutor {
  public static final Logger LOG =
      LoggerFactory.getLogger(IcebergCatalogOpExecutor.class);

  /**
   * Create Iceberg table by Iceberg api
   * Return value is table location from Iceberg
   */
  public static Table createTable(TIcebergCatalog catalog, TableIdentifier identifier,
      String location, List<TColumn> columns, TIcebergPartitionSpec partitionSpec,
      List<String> primaryKeyColumnNames, String owner,
      Map<String, String> tableProperties) throws ImpalaRuntimeException {
    // Each table id increase from zero
    Schema schema = createIcebergSchema(columns, primaryKeyColumnNames);
    PartitionSpec spec = IcebergUtil.createIcebergPartition(schema, partitionSpec);
    IcebergCatalog icebergCatalog = IcebergUtil.getIcebergCatalog(catalog, location);
    if (icebergCatalog instanceof IcebergHiveCatalog) {
      // Put table owner to table properties for HiveCatalog.
      tableProperties.put(HiveCatalog.HMS_TABLE_OWNER, owner);
    }
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
    try {
      msTbl.getSd().setCols(IcebergSchemaConverter.convertToHiveSchema(iceTbl.schema()));
    } catch (ImpalaRuntimeException e) {
      throw new TableLoadingException(e.getMessage());
    }
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
      TIcebergPartitionSpec partSpec, Transaction transaction)
      throws ImpalaRuntimeException {
    try {
      if (!feTable.getPrimaryKeyColumnNames().isEmpty()) {
        throw new ImpalaRuntimeException("Not allowed to do partition evolution on " +
            "Iceberg tables with primary keys.");
      }
    } catch (TException tEx) {
      throw new ImpalaRuntimeException(tEx.getMessage());
    }

    BaseTable iceTable = (BaseTable)feTable.getIcebergApiTable();
    UpdatePartitionSpec updatePartitionSpec = transaction.updateSpec();
    iceTable.spec().fields().forEach(partitionField -> updatePartitionSpec.removeField(
        partitionField.name()));
    List<Term> partitioningTerms = IcebergUtil.getPartitioningTerms(partSpec);
    partitioningTerms.forEach(updatePartitionSpec::addField);
    updatePartitionSpec.commit();
  }

  /**
   * Use the ExpireSnapshot API to expire snapshots by calling the
   * ExpireSnapshot.expireOlderThan(timestampMillis) method.
   * TableProperties.MIN_SNAPSHOTS_TO_KEEP table property manages how many snapshots
   * should be retained even when all snapshots are selected by expireOlderThan().
   */
  public static String alterTableExecuteExpireSnapshots(
      Transaction txn, TAlterTableExecuteExpireSnapshotsParams params) {
    ExpireSnapshots expireApi = txn.expireSnapshots();
    Preconditions.checkState(params.isSetOlder_than_millis());
    expireApi.expireOlderThan(params.older_than_millis);
    expireApi.commit();
    return "Snapshots have been expired.";
  }

  /**
   * Executes an ALTER TABLE EXECUTE ROLLBACK.
   */
  public static String alterTableExecuteRollback(
      Transaction iceTxn, FeIcebergTable tbl, TAlterTableExecuteRollbackParams params) {
    TRollbackType kind = params.getKind();
    ManageSnapshots manageSnapshots = iceTxn.manageSnapshots();
    switch (kind) {
      case TIME_ID:
        Preconditions.checkState(params.isSetTimestamp_millis());
        long timestampMillis = params.getTimestamp_millis();
        LOG.info("Rollback iceberg table to snapshot before timestamp {}",
            timestampMillis);
        manageSnapshots.rollbackToTime(timestampMillis);
        break;
      case VERSION_ID:
        Preconditions.checkState(params.isSetSnapshot_id());
        long snapshotId = params.getSnapshot_id();
        LOG.info("Rollback iceberg table to snapshot id {}", snapshotId);
        manageSnapshots.rollbackTo(snapshotId);
        break;
      default: throw new IllegalStateException("Bad kind of execute rollback " + kind);
    }
    // Commit the update.
    manageSnapshots.commit();
    return "Rollback executed.";
  }

  /**
   * Deletes files related to specific set of partitions
   */
  public static long alterTableDropPartition(
      Transaction iceTxn, TAlterTableDropPartitionParams params) {
    DeleteFiles deleteFiles = iceTxn.newDelete();
    if (params.iceberg_drop_partition_request.is_truncate) {
      deleteFiles.deleteFromRowFilter(Expressions.alwaysTrue());
    } else {
      for (String path : params.iceberg_drop_partition_request.paths) {
        deleteFiles.deleteFile(path);
      }
    }
    deleteFiles.commit();
    return params.iceberg_drop_partition_request.num_partitions;
  }

  /**
   * Drops a column from an Iceberg table.
   */
  public static void dropColumn(Transaction txn, String colName) {
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
  private static Schema createIcebergSchema(List<TColumn> columns,
      List<String> primaryKeyColumnNames) throws ImpalaRuntimeException {
    return IcebergSchemaConverter.genIcebergSchema(columns, primaryKeyColumnNames);
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

  public static void execute(FeIcebergTable feIcebergTable, Transaction txn,
      TIcebergOperationParam icebergOp) throws ImpalaRuntimeException {
    switch (icebergOp.operation) {
      case INSERT: appendFiles(feIcebergTable, txn, icebergOp); break;
      case DELETE: deleteRows(feIcebergTable, txn, icebergOp); break;
      case UPDATE: updateRows(feIcebergTable, txn, icebergOp); break;
      case OPTIMIZE: rewriteTable(feIcebergTable, txn, icebergOp); break;
      default: throw new ImpalaRuntimeException(
          "Unknown Iceberg operation: " + icebergOp.operation);
    }
  }

  private static void deleteRows(FeIcebergTable feIcebergTable, Transaction txn,
      TIcebergOperationParam icebergOp) throws ImpalaRuntimeException {
    List<ByteBuffer> deleteFilesFb = icebergOp.getIceberg_delete_files_fb();
    RowDelta rowDelta = txn.newRowDelta();
    for (ByteBuffer buf : deleteFilesFb) {
      DeleteFile deleteFile = createDeleteFile(feIcebergTable, buf);
      rowDelta.addDeletes(deleteFile);
    }
    try {
      // Validate that there are no conflicting data files, because if data files are
      // added in the meantime, they potentially contain records that should have been
      // affected by this DELETE operation.
      rowDelta.validateFromSnapshot(icebergOp.getInitial_snapshot_id());
      rowDelta.validateNoConflictingDataFiles();
      rowDelta.validateDataFilesExist(
          icebergOp.getData_files_referenced_by_position_deletes());
      rowDelta.validateDeletedFiles();
      rowDelta.commit();
    } catch (ValidationException e) {
      throw new ImpalaRuntimeException(e.getMessage(), e);
    }
  }

  private static void updateRows(FeIcebergTable feIcebergTable, Transaction txn,
      TIcebergOperationParam icebergOp) throws ImpalaRuntimeException {
    List<ByteBuffer> deleteFilesFb = icebergOp.getIceberg_delete_files_fb();
    List<ByteBuffer> dataFilesFb = icebergOp.getIceberg_data_files_fb();
    RowDelta rowDelta = txn.newRowDelta();
    for (ByteBuffer buf : deleteFilesFb) {
      DeleteFile deleteFile = createDeleteFile(feIcebergTable, buf);
      rowDelta.addDeletes(deleteFile);
    }
    for (ByteBuffer buf : dataFilesFb) {
      DataFile dataFile = createDataFile(feIcebergTable, buf);
      rowDelta.addRows(dataFile);
    }
    try {
      // Validate that there are no conflicting data files, because if data files are
      // added in the meantime, they potentially contain records that should have been
      // affected by this UPDATE operation. Also validate that there are no conflicting
      // delete files, because we don't want to revive records that have been deleted
      // in the meantime.
      rowDelta.validateFromSnapshot(icebergOp.getInitial_snapshot_id());
      rowDelta.validateNoConflictingDataFiles();
      rowDelta.validateNoConflictingDeleteFiles();
      rowDelta.validateDataFilesExist(
          icebergOp.getData_files_referenced_by_position_deletes());
      rowDelta.validateDeletedFiles();
      rowDelta.commit();
    } catch (ValidationException e) {
      throw new ImpalaRuntimeException(e.getMessage(), e);
    }
  }

  private static DataFile createDataFile(FeIcebergTable feIcebergTable, ByteBuffer buf)
      throws ImpalaRuntimeException {
    FbIcebergDataFile dataFile = FbIcebergDataFile.getRootAsFbIcebergDataFile(buf);

    PartitionSpec partSpec = feIcebergTable.getIcebergApiTable().specs().get(
        dataFile.specId());
    IcebergPartitionSpec impPartSpec =
        feIcebergTable.getPartitionSpec(dataFile.specId());
    Metrics metrics = buildDataFileMetrics(dataFile);
    DataFiles.Builder builder =
        DataFiles.builder(partSpec)
        .withMetrics(metrics)
        .withPath(dataFile.path())
        .withFormat(IcebergUtil.fbFileFormatToIcebergFileFormat(dataFile.format()))
        .withRecordCount(dataFile.recordCount())
        .withFileSizeInBytes(dataFile.fileSizeInBytes());
    IcebergUtil.PartitionData partitionData = IcebergUtil.partitionDataFromDataFile(
        partSpec.partitionType(), impPartSpec, dataFile);
    if (partitionData != null) builder.withPartition(partitionData);
    return builder.build();
  }

  private static DeleteFile createDeleteFile(FeIcebergTable feIcebergTable,
      ByteBuffer buf) throws ImpalaRuntimeException {
    FbIcebergDataFile deleteFile = FbIcebergDataFile.getRootAsFbIcebergDataFile(buf);

    PartitionSpec partSpec = feIcebergTable.getIcebergApiTable().specs().get(
        deleteFile.specId());
    IcebergPartitionSpec impPartSpec = feIcebergTable.getPartitionSpec(
        deleteFile.specId());
    Metrics metrics = buildDataFileMetrics(deleteFile);
    FileMetadata.Builder builder = FileMetadata.deleteFileBuilder(partSpec)
        .ofPositionDeletes()
        .withMetrics(metrics)
        .withPath(deleteFile.path())
        .withFormat(IcebergUtil.fbFileFormatToIcebergFileFormat(deleteFile.format()))
        .withRecordCount(deleteFile.recordCount())
        .withFileSizeInBytes(deleteFile.fileSizeInBytes());
    IcebergUtil.PartitionData partitionData = IcebergUtil.partitionDataFromDataFile(
        partSpec.partitionType(), impPartSpec, deleteFile);
    if (partitionData != null) builder.withPartition(partitionData);
    return builder.build();
  }

  /**
   * Append the newly inserted data files to the Iceberg table using the AppendFiles
   * API.
   */
  public static void appendFiles(FeIcebergTable feIcebergTable, Transaction txn,
      TIcebergOperationParam icebergOp) throws ImpalaRuntimeException {
    List<ByteBuffer> dataFilesFb = icebergOp.getIceberg_data_files_fb();
    BatchWrite batchWrite;
    if (icebergOp.isIs_overwrite()) {
      batchWrite = new DynamicOverwrite(txn, icebergOp.getInitial_snapshot_id());
    } else {
      batchWrite = new Append(txn);
    }
    for (ByteBuffer buf : dataFilesFb) {
      DataFile dataFile = createDataFile(feIcebergTable, buf);
      batchWrite.addFile(dataFile);
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

  private static void rewriteTable(FeIcebergTable feIcebergTable, Transaction txn,
      TIcebergOperationParam icebergOp) throws ImpalaRuntimeException {
    GroupedContentFiles contentFiles;
    try {
      // Get all files from the initial snapshot.
      contentFiles = IcebergUtil.getIcebergFilesFromSnapshot(
          feIcebergTable, /*predicates=*/Collections.emptyList(),
          icebergOp.getInitial_snapshot_id());
    } catch (TableLoadingException e) {
      throw new ImpalaRuntimeException(e.getMessage(), e);
    }
    RewriteFiles rewrite = txn.newRewrite();
    // Delete current data files from table.
    for (DataFile dataFile : contentFiles.dataFilesWithDeletes) {
      rewrite.deleteFile(dataFile);
    }
    for (DataFile dataFile : contentFiles.dataFilesWithoutDeletes) {
      rewrite.deleteFile(dataFile);
    }
    // Delete current delete files from table.
    for (DeleteFile deleteFile : contentFiles.positionDeleteFiles) {
      rewrite.deleteFile(deleteFile);
    }
    for (DeleteFile deleteFile : contentFiles.equalityDeleteFiles) {
      rewrite.deleteFile(deleteFile);
    }
    // Add newly written files to the table.
    List<ByteBuffer> dataFilesToAdd = icebergOp.getIceberg_data_files_fb();
    for (ByteBuffer buf : dataFilesToAdd) {
      DataFile dataFile = createDataFile(feIcebergTable, buf);
      rewrite.addFile(dataFile);
    }
    try {
      rewrite.validateFromSnapshot(icebergOp.getInitial_snapshot_id());
      rewrite.commit();
    } catch (ValidationException e) {
      throw new ImpalaRuntimeException(e.getMessage(), e);
    }
  }

  /**
   * Creates new snapshot for the iceberg table by deleting all data files.
   */
  public static void truncateTable(Transaction txn) {
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
