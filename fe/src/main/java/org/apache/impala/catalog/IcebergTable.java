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

import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.impala.analysis.IcebergPartitionField;
import org.apache.impala.analysis.IcebergPartitionSpec;
import org.apache.impala.analysis.IcebergPartitionTransform;
import org.apache.impala.catalog.iceberg.GroupedContentFiles;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TCompressionCodec;
import org.apache.impala.thrift.TGetPartialCatalogObjectRequest;
import org.apache.impala.thrift.TGetPartialCatalogObjectResponse;
import org.apache.impala.thrift.THdfsCompression;
import org.apache.impala.thrift.THdfsTable;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.thrift.TIcebergFileFormat;
import org.apache.impala.thrift.TIcebergPartitionField;
import org.apache.impala.thrift.TIcebergPartitionSpec;
import org.apache.impala.thrift.TIcebergPartitionStats;
import org.apache.impala.thrift.TIcebergTable;
import org.apache.impala.thrift.TPartialPartitionInfo;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.EventSequence;
import org.apache.impala.util.IcebergSchemaConverter;
import org.apache.impala.util.IcebergUtil;
import org.apache.thrift.TException;

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
      "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler";

  // Iceberg file format key in tblproperties
  public static final String ICEBERG_FILE_FORMAT = "write.format.default";

  // Iceberg catalog type key in tblproperties
  public static final String ICEBERG_CATALOG = "iceberg.catalog";

  // Iceberg format version numbers
  public static final int ICEBERG_FORMAT_V1 = 1;
  public static final int ICEBERG_FORMAT_V2 = 2;

  // Iceberg table catalog location key in tblproperties when using HadoopCatalog
  // This property is necessary for both managed and external Iceberg table with
  // 'hadoop.catalog'
  public static final String ICEBERG_CATALOG_LOCATION = "iceberg.catalog_location";

  // Iceberg table namespace key in tblproperties when using HadoopCatalog,
  // We use database.table instead if this property not been set in SQL
  public static final String ICEBERG_TABLE_IDENTIFIER = "iceberg.table_identifier";

  // Internal Iceberg table property that specifies the absolute path of the current
  // table metadata. This property is only valid for tables in 'hive.catalog'.
  public static final String METADATA_LOCATION = "metadata_location";

  // Internal Iceberg table property that specifies the absolute path of the previous
  // table metadata. This property is only valid for tables in 'hive.catalog'.
  public static final String PREVIOUS_METADATA_LOCATION = "previous_metadata_location";

  // Internal Iceberg table property that specifies the current schema.
  public static final String CURRENT_SCHEMA = "current-schema";

  // Internal Iceberg table property that specifies the number of snapshots.
  public static final String SNAPSHOT_COUNT = "snapshot-count";

  // Internal Iceberg table property that specifies the current snapshot id.
  public static final String CURRENT_SNAPSHOT_ID = "current-snapshot-id";

  // Internal Iceberg table property that specifies the current snapshot summary.
  public static final String CURRENT_SNAPSHOT_SUMMARY = "current-snapshot-summary";

  // Internal Iceberg table property that specifies the current snapshot timestamp in
  // milliseconds.
  public static final String CURRENT_SNAPSHOT_TIMESTAMP_MS
      = "current-snapshot-timestamp-ms";

  // Internal Iceberg table property that specifies the current default partition
  // specification of the table.
  public static final String DEFAULT_PARTITION_SPEC = "default-partition-spec";

  // Internal Iceberg table property that specifies the UUID of the table.
  public static final String UUID = "uuid";

  // Parquet compression codec and compression level table properties.
  public static final String PARQUET_COMPRESSION_CODEC =
      "write.parquet.compression-codec";
  public static final String PARQUET_COMPRESSION_LEVEL =
      "write.parquet.compression-level";

  public static final String MERGE_ON_READ = "merge-on-read";

  // Default values for parquet compression codec.
  public static final THdfsCompression DEFAULT_PARQUET_COMPRESSION_CODEC =
      THdfsCompression.SNAPPY;
  // Default values for parquet compression level (used with ZSTD codec).
  public static final int DEFAULT_PARQUET_ZSTD_COMPRESSION_LEVEL = 3;
  // Valid range for parquet compression level.
  public static final int MIN_PARQUET_COMPRESSION_LEVEL = 1;
  public static final int MAX_PARQUET_COMPRESSION_LEVEL = 22;

  // Parquet row group size table property.
  public static final String PARQUET_ROW_GROUP_SIZE =
      "write.parquet.row-group-size-bytes";
  // 0 means that the table property should be ignored.
  public static final long UNSET_PARQUET_ROW_GROUP_SIZE = 0;
  // Valid range for parquet row group size is [8MB, 2047MB]
  // (see HDFS_MIN_FILE_SIZE defined in hdfs-parquet-table-writer.h)
  public static final long MIN_PARQUET_ROW_GROUP_SIZE = 8 * 1024 * 1024;
  public static final long MAX_PARQUET_ROW_GROUP_SIZE = 2047 * 1024 * 1024;

  // Parquet plain page size table property.
  public static final String PARQUET_PLAIN_PAGE_SIZE = "write.parquet.page-size-bytes";
  // Parquet dictionary page size table property.
  public static final String PARQUET_DICT_PAGE_SIZE = "write.parquet.dict-size-bytes";
  // 0 means that the table property should be ignored.
  public static final long UNSET_PARQUET_PAGE_SIZE = 0;
  // Valid range for parquet plain and dictionary page size [64K, 1GB]
  // (see DEFAULT_DATA_PAGE_SIZE and MAX_DATA_PAGE_SIZE defined in
  // hdfs-parquet-table-writer.h)
  public static final long MIN_PARQUET_PAGE_SIZE = 64 * 1024;
  public static final long MAX_PARQUET_PAGE_SIZE = 1024 * 1024 * 1024;

  // Field IDs of the position delete files according to the Iceberg spec.
  public static final int V2_FILE_PATH_FIELD_ID = 2147483546;
  public static final int V2_POS_FIELD_ID = 2147483545;

  // The name of the folder where Iceberg metadata lives.
  public static final String METADATA_FOLDER_NAME = "metadata";

  // Iceberg catalog type dependent on table properties
  private TIcebergCatalog icebergCatalog_;

  // Iceberg file format dependent on table properties
  private TIcebergFileFormat icebergFileFormat_;

  // Iceberg parquet compression codec dependent on table properties
  private TCompressionCodec icebergParquetCompressionCodec_;

  // Iceberg parquet row group size dependent on table property
  private long icebergParquetRowGroupSize_;

  // Iceberg parquet plain page size dependent on table property
  private long icebergParquetPlainPageSize_;

  // Iceberg parquet dictionary page size dependent on table property
  private long icebergParquetDictPageSize_;

  // The iceberg file system table location
  private String icebergTableLocation_;

  // Partitioning schemes of this Iceberg table.
  private List<IcebergPartitionSpec> partitionSpecs_;

  // Index for partitionSpecs_ to show the current item in the list. Not always the
  // last item of the list is the latest.
  private int defaultPartitionSpecId_;

  // File descriptor store of all data and delete files.
  private IcebergContentFileStore fileStore_;

  // Treat iceberg table as a non-partitioned hdfs table in backend
  private HdfsTable hdfsTable_;

  // Cached Iceberg API table object.
  private org.apache.iceberg.Table icebergApiTable_;

  // The snapshot id cached in the CatalogD, necessary to syncronize the caches.
  private long catalogSnapshotId_ = -1;

  private Map<String, TIcebergPartitionStats> partitionStats_;

  protected IcebergTable(org.apache.hadoop.hive.metastore.api.Table msTable,
      Db db, String name, String owner) {
    super(msTable, db, name, owner);
    icebergTableLocation_ = msTable.getSd().getLocation();
    icebergCatalog_ = IcebergUtil.getTIcebergCatalog(msTable);
    icebergFileFormat_ = IcebergUtil.getIcebergFileFormat(msTable);
    icebergParquetCompressionCodec_ = Utils.getIcebergParquetCompressionCodec(msTable);
    icebergParquetRowGroupSize_ = Utils.getIcebergParquetRowGroupSize(msTable);
    icebergParquetPlainPageSize_ = Utils.getIcebergParquetPlainPageSize(msTable);
    icebergParquetDictPageSize_ = Utils.getIcebergParquetDictPageSize(msTable);
    hdfsTable_ = new HdfsTable(msTable, db, name, owner);
  }

  /**
   * A table is synchronized table if its Managed table or if its a external table with
   * <code>external.table.purge</code> property set to true.
   * We need to create/drop/etc. synchronized tables through the Iceberg APIs as well.
   */
  public static boolean isSynchronizedTable(
      org.apache.hadoop.hive.metastore.api.Table msTbl) {
    Preconditions.checkState(isIcebergTable(msTbl));
    return isManagedTable(msTbl) || isExternalPurgeTable(msTbl);
  }

  /**
   * Returns if this metastore table has managed table type
   */
  public static boolean isManagedTable(
      org.apache.hadoop.hive.metastore.api.Table msTbl) {
    return msTbl.getTableType().equalsIgnoreCase(TableType.MANAGED_TABLE.toString());
  }

  public HdfsTable getHdfsTable() {
    return hdfsTable_;
  }

  @Override
  public org.apache.iceberg.Table getIcebergApiTable() {
    return icebergApiTable_;
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.TABLE;
  }

  @Override
  public void setCatalogVersion(long newVersion) {
    // We use 'hdfsTable_' to answer CatalogServiceCatalog.doGetPartialCatalogObject(), so
    // its version number needs to be updated as well.
    super.setCatalogVersion(newVersion);
    hdfsTable_.setCatalogVersion(newVersion);
  }

  @Override
  public String getStorageHandlerClassName() {
    return ICEBERG_STORAGE_HANDLER;
  }

  public static boolean isIcebergStorageHandler(String handler) {
    return handler != null && handler.equals(ICEBERG_STORAGE_HANDLER);
  }

  public static boolean isIcebergTable(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    String inputFormat = msTbl.getSd().getInputFormat();
    HdfsFileFormat hdfsFileFormat = inputFormat != null ?
        HdfsFileFormat.fromHdfsInputFormatClass(inputFormat, null) :
        null;
    return isIcebergStorageHandler(msTbl.getParameters().get(KEY_STORAGE_HANDLER)) ||
        hdfsFileFormat == HdfsFileFormat.ICEBERG ||
        (hdfsFileFormat == null &&
         "ICEBERG".equals(msTbl.getParameters().get("table_type")));
  }

  @Override
  public TIcebergCatalog getIcebergCatalog() {
    return icebergCatalog_;
  }

  @Override
  public String getIcebergCatalogLocation() {
    return Utils.getIcebergCatalogLocation(this);
  }

  @Override
  public TIcebergFileFormat getIcebergFileFormat() {
    return icebergFileFormat_;
  }

  @Override
  public TCompressionCodec getIcebergParquetCompressionCodec() {
    return icebergParquetCompressionCodec_;
  }

  @Override
  public long getIcebergParquetRowGroupSize() {
    return icebergParquetRowGroupSize_;
  }

  @Override
  public long getIcebergParquetPlainPageSize() {
    return icebergParquetPlainPageSize_;
  }

  @Override
  public long getIcebergParquetDictPageSize() {
    return icebergParquetDictPageSize_;
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
  public List<IcebergPartitionSpec> getPartitionSpecs() {
    Preconditions.checkState(partitionSpecs_ != null);
    return ImmutableList.copyOf(partitionSpecs_);
  }

  @Override
  public IcebergPartitionSpec getDefaultPartitionSpec() {
    return Utils.getDefaultPartitionSpec(this);
  }

  @Override
  public int getDefaultPartitionSpecId() {
    return defaultPartitionSpecId_;
  }

  @Override
  public long snapshotId() {
    return catalogSnapshotId_;
  }

  @Override
  public Map<String, TIcebergPartitionStats> getIcebergPartitionStats() {
    return partitionStats_;
  }

  @Override
  public TTable toThrift() {
    TTable table = super.toThrift();
    table.setTable_type(TTableType.ICEBERG_TABLE);
    table.setIceberg_table(Utils.getTIcebergTable(this));
    table.setHdfs_table(transformToTHdfsTable(true, ThriftObjectType.FULL));
    return table;
  }

  @Override
  public TTable toHumanReadableThrift() {
    TTable table = super.toThrift();
    table.setTable_type(TTableType.ICEBERG_TABLE);
    table.setIceberg_table(Utils.getTIcebergTable(this));
    table.setHdfs_table(transformToTHdfsTable(true, ThriftObjectType.DESCRIPTOR_ONLY));
    return table;
  }

  /**
   * Loads the metadata of an Iceberg table.
   * <p>
   * Schema and partitioning schemes are loaded directly from Iceberg whereas column stats
   * are loaded from HMS. The function also updates the table schema in HMS in order to
   * propagate alterations made to the Iceberg table to HMS.
   */
  @Override
  public void load(boolean reuseMetadata, IMetaStoreClient msClient,
      org.apache.hadoop.hive.metastore.api.Table msTbl, String reason,
      EventSequence catalogTimeline) throws TableLoadingException {
    final Timer.Context context =
        getMetrics().getTimer(Table.LOAD_DURATION_METRIC).time();
    verifyTable(msTbl);
    try {
      // Copy the table to check later if anything has changed.
      msTable_ = msTbl.deepCopy();
      // Other engines might create Iceberg tables without setting the HiveIceberg*
      // storage descriptors. Impala relies on the storage descriptors being set to
      // certain classes, so we set it here for the in-memory metastore table.
      FeIcebergTable.setIcebergStorageDescriptor(msTable_);
      setTableStats(msTable_);
      // Load metadata from Iceberg
      final Timer.Context ctxStorageLdTime =
          getMetrics().getTimer(Table.LOAD_DURATION_STORAGE_METADATA).time();
      try {
        icebergApiTable_ = IcebergUtil.loadTable(this);
        catalogTimeline.markEvent("Loaded Iceberg API table");
        catalogSnapshotId_ = FeIcebergTable.super.snapshotId();
        loadSchemaFromIceberg();
        catalogTimeline.markEvent("Loaded schema from Iceberg");
        // Loading hdfs table after loaded schema from Iceberg,
        // in case we create external Iceberg table skipping column info in sql.
        icebergFileFormat_ = IcebergUtil.getIcebergFileFormat(msTbl);
        icebergParquetCompressionCodec_ = Utils.getIcebergParquetCompressionCodec(msTbl);
        icebergParquetRowGroupSize_ = Utils.getIcebergParquetRowGroupSize(msTbl);
        icebergParquetPlainPageSize_ = Utils.getIcebergParquetPlainPageSize(msTbl);
        icebergParquetDictPageSize_ = Utils.getIcebergParquetDictPageSize(msTbl);
        GroupedContentFiles icebergFiles = IcebergUtil.getIcebergFiles(this,
            new ArrayList<>(), /*timeTravelSpec=*/null);
        catalogTimeline.markEvent("Loaded Iceberg files");
        hdfsTable_.setIcebergFiles(icebergFiles);
        hdfsTable_.setCanDataBeOutsideOfTableLocation(
            !Utils.requiresDataFilesInTableLocation(this));
        hdfsTable_.load(reuseMetadata, msClient, msTable_, reason, catalogTimeline);
        fileStore_ = Utils.loadAllPartition(this, icebergFiles);
        partitionStats_ = Utils.loadPartitionStats(this, icebergFiles);
        setIcebergTableStats();
        loadAllColumnStats(msClient, catalogTimeline);
        setAvroSchema(msClient, msTbl, fileStore_, catalogTimeline);
      } catch (Exception e) {
        throw new IcebergTableLoadingException("Error loading metadata for Iceberg table "
            + icebergTableLocation_, e);
      } finally {
        storageMetadataLoadTime_ = ctxStorageLdTime.stop();
      }

      refreshLastUsedTime();

      // Let's reset the storage descriptors, so we don't update the table unnecessarily.
      FeIcebergTable.resetIcebergStorageDescriptor(msTable_, msTbl);
      // Avoid updating HMS if the schema didn't change.
      if (msTable_.equals(msTbl)) return;

      // Update the table schema in HMS.
      try {
        updateTimestampProperty(msTable_, TBL_PROP_LAST_DDL_TIME);
        msTable_.putToParameters(StatsSetupConst.DO_NOT_UPDATE_STATS,
            StatsSetupConst.TRUE);
        msClient.alter_table(msTable_.getDbName(), msTable_.getTableName(), msTable_);
        catalogTimeline.markEvent("Updated table schema in Metastore");
      } catch (TException e) {
        throw new TableLoadingException(e.getMessage());
      }
    } finally {
      context.stop();
    }
  }

  /**
   * @throws TableLoadingException when it is unsafe to load the table.
   */
  private void verifyTable(org.apache.hadoop.hive.metastore.api.Table msTbl)
      throws TableLoadingException {
    if (IcebergUtil.isHiveCatalog(msTbl.getParameters())) {
      String tableId = IcebergUtil.getIcebergTableIdentifier(
          msTbl.getDbName(), msTbl.getTableName()).toString();
      Map<String, String> params = msTbl.getParameters();
      if (!tableId.equalsIgnoreCase(
              params.getOrDefault(IcebergTable.ICEBERG_TABLE_IDENTIFIER, tableId)) ||
          !tableId.equalsIgnoreCase(
              params.getOrDefault(Catalogs.NAME, tableId)) ||
          !tableId.equalsIgnoreCase(
              params.getOrDefault(InputFormatConfig.TABLE_IDENTIFIER, tableId))) {
        throw new TableLoadingException(String.format(
            "Table %s cannot be loaded because it is an " +
            "EXTERNAL table in the HiveCatalog that points to another table. " +
            "Query the original table instead.",
            getFullName()));
      }
    }
  }

  /**
   * Load schema and partitioning schemes directly from Iceberg.
   */
  public void loadSchemaFromIceberg()
      throws TableLoadingException, ImpalaRuntimeException {
    loadSchema();
    addVirtualColumns();
    partitionSpecs_ = Utils.loadPartitionSpecByIceberg(this);
    defaultPartitionSpecId_ = icebergApiTable_.spec().specId();
  }

  /**
   * Loads the HMS schema by Iceberg schema
   */
  private void loadSchema() throws TableLoadingException {
    clearColumns();
    try {
      msTable_.getSd().setCols(IcebergSchemaConverter.convertToHiveSchema(
          getIcebergSchema()));
      for (Column col : IcebergSchemaConverter.convertToImpalaSchema(
          getIcebergSchema())) {
        addColumn(col);
      }
    } catch (ImpalaRuntimeException e) {
      throw new TableLoadingException(e.getMessage(), e);
    }
  }

  /**
   * Loads the AVRO schema if the table contains AVRO files.
   */
  private void setAvroSchema(IMetaStoreClient msClient,
      org.apache.hadoop.hive.metastore.api.Table msTbl,
      IcebergContentFileStore fileStore, EventSequence catalogTimeline) throws Exception {
    if (fileStore.hasAvro()) {
      hdfsTable_.setAvroSchemaInternal(msClient, msTbl, catalogTimeline);
    }
  }

  @Override
  public void addColumn(Column col) {
    Preconditions.checkState(col instanceof IcebergColumn);
    IcebergColumn iCol = (IcebergColumn) col;
    colsByPos_.add(iCol);
    colsByName_.put(iCol.getName().toLowerCase(), col);
    ((StructType) type_.getItemType()).addField(
        new IcebergStructField(col.getName(), col.getType(), col.getComment(),
            iCol.getFieldId()));
  }

  private void addVirtualColumns() {
    addVirtualColumn(VirtualColumn.INPUT_FILE_NAME);
    addVirtualColumn(VirtualColumn.FILE_POSITION);
    addVirtualColumn(VirtualColumn.PARTITION_SPEC_ID);
    addVirtualColumn(VirtualColumn.ICEBERG_PARTITION_SERIALIZED);
    addVirtualColumn(VirtualColumn.ICEBERG_DATA_SEQUENCE_NUMBER);
  }

  @Override
  protected void loadFromThrift(TTable thriftTable) throws TableLoadingException {
    super.loadFromThrift(thriftTable);
    TIcebergTable ticeberg = thriftTable.getIceberg_table();
    icebergTableLocation_ = ticeberg.getTable_location();
    icebergParquetCompressionCodec_ = ticeberg.getParquet_compression_codec();
    icebergParquetRowGroupSize_ = ticeberg.getParquet_row_group_size();
    icebergParquetPlainPageSize_ = ticeberg.getParquet_plain_page_size();
    icebergParquetDictPageSize_ = ticeberg.getParquet_dict_page_size();
    partitionSpecs_ = loadPartitionBySpecsFromThrift(ticeberg.getPartition_spec());
    defaultPartitionSpecId_ = ticeberg.getDefault_partition_spec_id();
    // Load file descriptors for the Iceberg snapshot. We are using the same host index,
    // so there's no need for translation.
    catalogSnapshotId_ = ticeberg.catalog_snapshot_id;
    // The Iceberg API table needs to be available and cached even when loaded through
    // thrift.
    icebergApiTable_ = IcebergUtil.loadTable(this);
    fileStore_ = IcebergContentFileStore.fromThrift(
        ticeberg.getContent_files(), null, null);
    hdfsTable_.loadFromThrift(thriftTable);
    partitionStats_ = ticeberg.getPartition_stats();
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
          Integer transformParam = null;
          if (field.getTransform().isSetTransform_param()) {
            transformParam = field.getTransform().getTransform_param();
          }
          fields.add(new IcebergPartitionField(field.getSource_id(), field.getField_id(),
              field.getOrig_field_name(), field.getField_name(),
              new IcebergPartitionTransform(field.getTransform().getTransform_type(),
                  transformParam),
              Type.fromTScalarType(field.getType())));
        }
        ret.add(new IcebergPartitionSpec(param.getSpec_id(),
            fields));
      } else {
        ret.add(new IcebergPartitionSpec(param.getSpec_id(), null));
      }
    }
    return ret;
  }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId,
      Set<Long> referencedPartitions) {
    TTableDescriptor desc = new TTableDescriptor(tableId, TTableType.ICEBERG_TABLE,
        getTColumnDescriptors(), numClusteringCols_, name_, db_.getName());
    desc.setIcebergTable(Utils.getTIcebergTable(this, ThriftObjectType.DESCRIPTOR_ONLY));
    desc.setHdfsTable(transformToTHdfsTable(false, ThriftObjectType.DESCRIPTOR_ONLY));
    return desc;
  }

  public THdfsTable transformToTHdfsTable(boolean updatePartitionFlag,
      ThriftObjectType type) {
    THdfsTable hdfsTable = hdfsTable_.getTHdfsTable(type, null);
    if (updatePartitionFlag) {
      // Iceberg table only has one THdfsPartition, we set this partition
      // file format by iceberg file format which depend on table properties
      Utils.updateIcebergPartitionFileFormat(this, hdfsTable);
    }
    return hdfsTable;
  }

  @Override
  public TGetPartialCatalogObjectResponse getPartialInfo(
      TGetPartialCatalogObjectRequest req) throws CatalogException {
    Preconditions.checkState(isLoaded(), "unloaded table: %s", getFullName());
    Map<HdfsPartition, TPartialPartitionInfo> missingPartialInfos = new HashMap<>();
    TGetPartialCatalogObjectResponse resp =
        getHdfsTable().getPartialInfo(req, missingPartialInfos);
    if (resp.table_info != null) {
      // Clear HdfsTable virtual columns and add IcebergTable virtual columns.
      resp.table_info.unsetVirtual_columns();
      for (VirtualColumn vCol : getVirtualColumns()) {
        resp.table_info.addToVirtual_columns(vCol.toThrift());
      }
    }
    if (req.table_info_selector.want_iceberg_table) {
      resp.table_info.setIceberg_table(Utils.getTIcebergTable(this));
      if (!resp.table_info.isSetNetwork_addresses()) {
        resp.table_info.setNetwork_addresses(getHostIndex().getList());
      }
      resp.table_info.iceberg_table.setCatalog_snapshot_id(catalogSnapshotId_);
    }
    return resp;
  }

  @Override
  public IcebergContentFileStore getContentFileStore() {
    return fileStore_;
  }
}
