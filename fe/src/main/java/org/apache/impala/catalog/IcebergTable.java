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
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.IcebergPartitionField;
import org.apache.impala.analysis.IcebergPartitionSpec;
import org.apache.impala.thrift.TCatalogObjectType;
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

  // The iceberg file system table location
  private String icebergTableLocation_;

  // Partitioning schemes of this Iceberg table.
  private List<IcebergPartitionSpec> partitionSpecs_;

  // Schema of the iceberg table.
  private org.apache.iceberg.Schema icebergSchema_;

  // PartitionSpec of the iceberg table.
  private List<PartitionSpec> icebergPartitionSpecs_;

  protected IcebergTable(org.apache.hadoop.hive.metastore.api.Table msTable,
                         Db db, String name, String owner) {
    super(msTable, db, name, owner);
    icebergTableLocation_ = msTable.getSd().getLocation();
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

  public org.apache.iceberg.Schema getIcebergSchema() {
    return icebergSchema_;
  }

  public List<PartitionSpec> getIcebergPartitionSpec() {
    return icebergPartitionSpecs_;
  }

  @Override
  public String getIcebergTableLocation() {
    return icebergTableLocation_;
  }

  @Override
  public List<IcebergPartitionSpec> getPartitionSpec() {
    Preconditions.checkState(partitionSpecs_ != null);
    return ImmutableList.copyOf(partitionSpecs_);
  }

  @Override
  public TTable toThrift() {
    TTable table = super.toThrift();
    table.setTable_type(TTableType.ICEBERG_TABLE);
    table.setIceberg_table(getTIcebergTable());
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
    icebergPartitionSpecs_ = metadata.specs();
    loadSchema();
    partitionSpecs_ = buildIcebergPartitionSpec(icebergPartitionSpecs_);
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

  /**
   * Build IcebergPartitionSpec list by iceberg PartitionSpec
   */
  private List<IcebergPartitionSpec> buildIcebergPartitionSpec(
      List<PartitionSpec> specs) throws TableLoadingException {
    List<IcebergPartitionSpec> ret = new ArrayList<>();
    for (PartitionSpec spec : specs) {
      List<IcebergPartitionField> fields = new ArrayList<>();
      for (PartitionField field : spec.fields()) {
        fields.add(new IcebergPartitionField(field.sourceId(), field.fieldId(),
            field.name(), IcebergUtil.getPartitionTransform(field)));
      }
      ret.add(new IcebergPartitionSpec(spec.specId(), fields));
    }
    return ret;
  }

  @Override
  protected void loadFromThrift(TTable thriftTable) throws TableLoadingException {
    super.loadFromThrift(thriftTable);
    TIcebergTable ticeberg = thriftTable.getIceberg_table();
    icebergTableLocation_ = ticeberg.getTable_location();
    partitionSpecs_ = loadPartitionBySpecsFromThrift(ticeberg.getPartition_spec());
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

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId,
                                             Set<Long> referencedPartitions) {
    TTableDescriptor desc = new TTableDescriptor(tableId, TTableType.ICEBERG_TABLE,
        getTColumnDescriptors(), numClusteringCols_, name_, db_.getName());
    desc.setIcebergTable(getTIcebergTable());
    return desc;
  }

  private TIcebergTable getTIcebergTable() {
    TIcebergTable tbl = new TIcebergTable();
    tbl.setTable_location(icebergTableLocation_);
    for (IcebergPartitionSpec partition : partitionSpecs_) {
      tbl.addToPartition_spec(partition.toThrift());
    }
    return tbl;
  }
}
