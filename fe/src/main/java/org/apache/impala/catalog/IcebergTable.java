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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.IcebergPartitionField;
import org.apache.impala.analysis.IcebergPartitionSpec;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.THdfsFileDesc;
import org.apache.impala.thrift.THdfsTable;
import org.apache.impala.thrift.TIcebergFileFormat;
import org.apache.impala.thrift.TIcebergPartitionField;
import org.apache.impala.thrift.TIcebergPartitionSpec;
import org.apache.impala.thrift.TIcebergTable;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.IcebergUtil;
import org.apache.thrift.TException;

import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Representation of an Iceberg table in the catalog cache.
 */
public class IcebergTable extends Table implements FeIcebergTable {

  // Alias to the string key that identifies the storage handler for Iceberg tables.
  public static final String KEY_STORAGE_HANDLER =
      hive_metastoreConstants.META_TABLE_STORAGE;

  // Iceberg specific value for the storage handler table property keyed by
  // KEY_STORAGE_HANDLER.
  public static final String ICEBERG_STORAGE_HANDLER =
      "com.expediagroup.hiveberg.IcebergStorageHandler";

  // Iceberg file format key in tblproperties
  public static final String ICEBERG_FILE_FORMAT = "iceberg_file_format";

  // Iceberg file format dependend on table properties
  private TIcebergFileFormat icebergFileFormat_;

  // The iceberg file system table location
  private String icebergTableLocation_;

  // Partitioning schemes of this Iceberg table.
  private List<IcebergPartitionSpec> partitionSpecs_;

  // Schema of the iceberg table.
  private org.apache.iceberg.Schema icebergSchema_;

  // Key is the DataFile md5, value is FileDescriptor transformed from DataFile
  private Map<String, FileDescriptor> pathMD5ToFileDescMap_;

  // Treat iceberg table as a non-partitioned hdfs table in backend
  private HdfsTable hdfsTable_;

  protected IcebergTable(org.apache.hadoop.hive.metastore.api.Table msTable,
      Db db, String name, String owner) {
    super(msTable, db, name, owner);
    icebergTableLocation_ = msTable.getSd().getLocation();
    icebergFileFormat_ = Utils.getIcebergFileFormat(msTable);
    hdfsTable_ = new HdfsTable(msTable, db, name, owner);
  }

  /**
   * If managed table or external purge table , we create table by iceberg api,
   * or we just create hms table.
   */
  public static boolean needsCreateInIceberg(
      org.apache.hadoop.hive.metastore.api.Table msTbl) {
    Preconditions.checkState(isIcebergTable(msTbl));
    return isManagedTable(msTbl) || isExternalPurgeTable(msTbl);
  }

  /**
   * Returns if this metastore table has managed table type
   */
  private static boolean isManagedTable(
      org.apache.hadoop.hive.metastore.api.Table msTbl) {
    return msTbl.getTableType().equalsIgnoreCase(TableType.MANAGED_TABLE.toString());
  }

  public HdfsTable getHdfsTable() {
    return hdfsTable_;
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.TABLE;
  }

  @Override
  public String getStorageHandlerClassName() {
    return ICEBERG_STORAGE_HANDLER;
  }

  public static boolean isIcebergStorageHandler(String handler) {
    return handler != null && handler.equals(ICEBERG_STORAGE_HANDLER);
  }

  public static boolean isIcebergTable(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    return isIcebergStorageHandler(msTbl.getParameters().get(KEY_STORAGE_HANDLER));
  }

  @Override
  public TIcebergFileFormat getIcebergFileFormat() {
    return icebergFileFormat_;
  }

  @Override
  public String getIcebergTableLocation() {
    return icebergTableLocation_;
  }

  @Override
  public FeFsTable getFeFsTable() {
    return hdfsTable_;
  }

  @Override
  public List<IcebergPartitionSpec> getPartitionSpec() {
    Preconditions.checkState(partitionSpecs_ != null);
    return ImmutableList.copyOf(partitionSpecs_);
  }

  @Override
  public Map<String, FileDescriptor> getPathMD5ToFileDescMap() {
    return pathMD5ToFileDescMap_;
  }

  @Override
  public TTable toThrift() {
    TTable table = super.toThrift();
    table.setTable_type(TTableType.ICEBERG_TABLE);
    table.setIceberg_table(Utils.getTIcebergTable(this));
    table.setHdfs_table(transfromToTHdfsTable(true));
    return table;
  }

  /**
   * Loads the metadata of a Iceberg table.
   * <p>
   * Schema and partitioning schemes are loaded directly from Iceberg whereas column stats
   * are loaded from HMS. The function also updates the table schema in HMS in order to
   * propagate alterations made to the Iceberg table to HMS.
   */
  @Override
  public void load(boolean dummy /* not used */, IMetaStoreClient msClient,
      org.apache.hadoop.hive.metastore.api.Table msTbl, String reason)
      throws TableLoadingException {
    final Timer.Context context =
        getMetrics().getTimer(Table.LOAD_DURATION_METRIC).time();
    try {
      // Copy the table to check later if anything has changed.
      msTable_ = msTbl.deepCopy();
      // Load metadata from Iceberg
      final Timer.Context ctxStorageLdTime =
          getMetrics().getTimer(Table.LOAD_DURATION_STORAGE_METADATA).time();
      try {
        loadSchemaFromIceberg();
        // Loading hdfs table after loaded schema from Iceberg,
        // in case we create external Iceberg table skipping column info in sql.
        hdfsTable_.load(false, msClient, msTable_, true, true, false, null, reason);
        pathMD5ToFileDescMap_ = Utils.loadAllPartition(msTable_.getSd().getLocation(),
            this);
        loadAllColumnStats(msClient);
      } catch (Exception e) {
        throw new TableLoadingException("Error loading metadata for Iceberg table " +
            icebergTableLocation_, e);
      } finally {
        storageMetadataLoadTime_ = ctxStorageLdTime.stop();
      }

      refreshLastUsedTime();

      // Avoid updating HMS if the schema didn't change.
      if (msTable_.equals(msTbl)) return;

      // Update the table schema in HMS.
      try {
        updateTimestampProperty(msTable_, TBL_PROP_LAST_DDL_TIME);
        msTable_.putToParameters(StatsSetupConst.DO_NOT_UPDATE_STATS,
            StatsSetupConst.TRUE);
        msClient.alter_table(msTable_.getDbName(), msTable_.getTableName(), msTable_);
      } catch (TException e) {
        throw new TableLoadingException(e.getMessage());
      }
    } finally {
      context.stop();
    }
  }

  /**
   * Load schema and partitioning schemes directly from Iceberg.
   */
  public void loadSchemaFromIceberg() throws TableLoadingException {
    TableMetadata metadata = IcebergUtil.getIcebergTableMetadata(icebergTableLocation_);
    icebergSchema_ = metadata.schema();
    loadSchema();
    partitionSpecs_ = Utils.loadPartitionSpecByIceberg(metadata);
  }

  /**
   * Loads the HMS schema by Iceberg schema
   */
  private void loadSchema() throws TableLoadingException {
    clearColumns();

    List<FieldSchema> cols = msTable_.getSd().getCols();
    cols.clear();

    int pos = 0;
    for (Types.NestedField column : icebergSchema_.columns()) {
      Preconditions.checkNotNull(column);
      Type colType = IcebergUtil.toImpalaType(column.type());
      // Update sd cols by iceberg NestedField
      cols.add(new FieldSchema(column.name(), colType.toSql().toLowerCase(),
          column.doc()));
      // Update impala Table columns by iceberg NestedField
      addColumn(new Column(column.name(), colType, column.doc(), pos++));
    }
  }

  @Override
  protected void loadFromThrift(TTable thriftTable) throws TableLoadingException {
    super.loadFromThrift(thriftTable);
    TIcebergTable ticeberg = thriftTable.getIceberg_table();
    icebergTableLocation_ = ticeberg.getTable_location();
    partitionSpecs_ = loadPartitionBySpecsFromThrift(ticeberg.getPartition_spec());
    pathMD5ToFileDescMap_ = loadFileDescFromThrift(
        ticeberg.getPath_md5_to_file_descriptor());
    hdfsTable_.loadFromThrift(thriftTable);
  }

  private List<IcebergPartitionSpec> loadPartitionBySpecsFromThrift(
      List<TIcebergPartitionSpec> params) {
    List<IcebergPartitionSpec> ret = new ArrayList<>();
    for (TIcebergPartitionSpec param : params) {
      // Non-partition iceberg table only has one PartitionSpec with an empty
      // PartitionField set and a partition id
      if (param.getPartition_fields() != null) {
        List<IcebergPartitionField> fields = new ArrayList<>();
        for (TIcebergPartitionField field : param.getPartition_fields()) {
          fields.add(new IcebergPartitionField(field.getSource_id(), field.getField_id(),
              field.getField_name(), field.getField_type()));
        }
        ret.add(new IcebergPartitionSpec(param.getPartition_id(),
            fields));
      } else {
        ret.add(new IcebergPartitionSpec(param.getPartition_id(), null));
      }
    }
    return ret;
  }

  private Map<String, FileDescriptor> loadFileDescFromThrift(
      Map<String, THdfsFileDesc> tFileDescMap) {
    Map<String, FileDescriptor> fileDescMap = new HashMap<>();
    if (tFileDescMap == null) return fileDescMap;
    for (Map.Entry<String, THdfsFileDesc> entry : tFileDescMap.entrySet()) {
      fileDescMap.put(entry.getKey(), FileDescriptor.fromThrift(entry.getValue()));
    }
    return fileDescMap;
  }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId,
      Set<Long> referencedPartitions) {
    TTableDescriptor desc = new TTableDescriptor(tableId, TTableType.ICEBERG_TABLE,
        getTColumnDescriptors(), numClusteringCols_, name_, db_.getName());
    desc.setIcebergTable(Utils.getTIcebergTable(this));
    desc.setHdfsTable(transfromToTHdfsTable(false));
    return desc;
  }

  private THdfsTable transfromToTHdfsTable(boolean updatePartitionFlag) {
    THdfsTable hdfsTable = hdfsTable_.getTHdfsTable(ThriftObjectType.FULL, null);
    if (updatePartitionFlag) {
      // Iceberg table only has one THdfsPartition, we set this partition
      // file format by iceberg file format which depend on table properties
      Utils.updateIcebergPartitionFileFormat(this, hdfsTable);
    }
    return hdfsTable;
  }
}
