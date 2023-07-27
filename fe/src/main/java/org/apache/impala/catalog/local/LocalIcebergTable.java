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

package org.apache.impala.catalog.local;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.analysis.IcebergPartitionSpec;
import org.apache.impala.catalog.CatalogObject.ThriftObjectType;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergContentFileStore;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.local.MetaProvider.TableMetaRef;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TCompressionCodec;
import org.apache.impala.thrift.THdfsPartition;
import org.apache.impala.thrift.THdfsTable;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.thrift.TIcebergFileFormat;
import org.apache.impala.thrift.TIcebergPartitionStats;
import org.apache.impala.thrift.TPartialTableInfo;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.IcebergSchemaConverter;
import org.apache.impala.util.IcebergUtil;

import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.Immutable;

import org.apache.log4j.Logger;

/**
 * Iceberg table for LocalCatalog
 */
public class LocalIcebergTable extends LocalTable implements FeIcebergTable {
  private static final Logger LOG = Logger.getLogger(LocalIcebergTable.class);
  private TableParams tableParams_;
  private TIcebergFileFormat icebergFileFormat_;
  private TCompressionCodec icebergParquetCompressionCodec_;
  private long icebergParquetRowGroupSize_;
  private long icebergParquetPlainPageSize_;
  private long icebergParquetDictPageSize_;
  private List<IcebergPartitionSpec> partitionSpecs_;
  private int defaultPartitionSpecId_;
  private IcebergContentFileStore fileStore_;
  private LocalFsTable localFsTable_;

  // The snapshot id of the current snapshot stored in the CatalogD.
  long catalogSnapshotId_;

  // Cached Iceberg API table object.
  private org.apache.iceberg.Table icebergApiTable_;

  private Map<String, TIcebergPartitionStats> partitionStats_;

  /**
   * Loads the Iceberg metadata from the CatalogD then initializes a LocalIcebergTable.
   */
  static LocalTable loadIcebergTableViaMetaProvider(LocalDb db, Table msTable,
      MetaProvider.TableMetaRef ref) throws TableLoadingException {
    Preconditions.checkNotNull(db);
    Preconditions.checkNotNull(msTable);
    try {
      FeIcebergTable.setIcebergStorageDescriptor(msTable);
      TableParams tableParams = new TableParams(msTable);
      TPartialTableInfo tableInfo = db.getCatalog().getMetaProvider()
          .loadIcebergTable(ref);
      LocalFsTable fsTable = LocalFsTable.load(db, msTable, ref);
      warmupMetaProviderCache(db, msTable, ref, fsTable);
      org.apache.iceberg.Table icebergApiTable = db.getCatalog().getMetaProvider()
          .loadIcebergApiTable(ref, tableParams, msTable);
      List<Column> iceColumns = IcebergSchemaConverter.convertToImpalaSchema(
          icebergApiTable.schema());
      validateColumns(iceColumns, msTable.getSd().getCols());
      ColumnMap colMap = new ColumnMap(iceColumns,
          /*numClusteringCols=*/ 0,
          db.getName() + "." + msTable.getTableName(),
          /*isFullAcidSchema=*/false);
      return new LocalIcebergTable(db, msTable, ref, fsTable, colMap, tableInfo,
          tableParams, icebergApiTable);
    } catch (InconsistentMetadataFetchException e) {
      // Just rethrow this so the query can be retried by the Frontend.
      throw e;
    } catch (Exception e) {
      String fullTableName = msTable.getDbName() + "." + msTable.getTableName();
      throw new TableLoadingException(
          String.format("Error opening Iceberg table '%s'", fullTableName), e);
    }
  }

  /**
   * Eagerly warmup metaprovider cache before calling loadIcebergApiTable(). So
   * later they can be served from cache when really needed. Otherwise, if there are
   * frequent updates to the table, CatalogD might refresh the Iceberg table during
   * loadIcebergApiTable().
   * If that happens, subsequent load*() calls via the metaprovider would
   * fail due to InconsistentMetadataFetchException.
   */
  private static void warmupMetaProviderCache(LocalDb db, Table msTable, TableMetaRef ref,
      LocalFsTable fsTable) throws Exception {
    db.getCatalog().getMetaProvider().loadTableColumnStatistics(ref,
        getHmsColumnNames(msTable));
    FeCatalogUtils.loadAllPartitions(fsTable);
  }

  private static List<String> getHmsColumnNames(Table msTable) {
    List<String> ret = new ArrayList<>();
    for (FieldSchema fs : msTable.getSd().getCols()) {
      ret.add(fs.getName());
    }
    return ret;
  }

  private LocalIcebergTable(LocalDb db, Table msTable, MetaProvider.TableMetaRef ref,
      LocalFsTable fsTable, ColumnMap cmap, TPartialTableInfo tableInfo,
      TableParams tableParams, org.apache.iceberg.Table icebergApiTable)
      throws ImpalaRuntimeException {
    super(db, msTable, ref, cmap);

    Preconditions.checkNotNull(tableInfo);
    localFsTable_ = fsTable;
    tableParams_ = tableParams;
    fileStore_ = IcebergContentFileStore.fromThrift(
        tableInfo.getIceberg_table().getContent_files(),
        tableInfo.getNetwork_addresses(),
        getHostIndex());
    if (fileStore_.hasAvro()) localFsTable_.setAvroSchema(msTable);
    icebergApiTable_ = icebergApiTable;
    catalogSnapshotId_ = tableInfo.getIceberg_table().getCatalog_snapshot_id();
    partitionSpecs_ = Utils.loadPartitionSpecByIceberg(this);
    defaultPartitionSpecId_ = tableInfo.getIceberg_table().getDefault_partition_spec_id();
    icebergFileFormat_ = IcebergUtil.getIcebergFileFormat(msTable);
    icebergParquetCompressionCodec_ = Utils.getIcebergParquetCompressionCodec(msTable);
    icebergParquetRowGroupSize_ = Utils.getIcebergParquetRowGroupSize(msTable);
    icebergParquetPlainPageSize_ = Utils.getIcebergParquetPlainPageSize(msTable);
    icebergParquetDictPageSize_ = Utils.getIcebergParquetDictPageSize(msTable);
    partitionStats_ = tableInfo.getIceberg_table().getPartition_stats();
    setIcebergTableStats();
    addVirtualColumns(ref.getVirtualColumns());
  }

  static void validateColumns(List<Column> impalaCols, List<FieldSchema> hmsCols) {
    Preconditions.checkState(impalaCols.size() == hmsCols.size());
    for (int i = 0; i < impalaCols.size(); ++i) {
      Preconditions.checkState(
          impalaCols.get(i).getName().equalsIgnoreCase(hmsCols.get(i).getName()));
    }
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
    return tableParams_.icebergTableLocation_;
  }

  @Override
  public TIcebergCatalog getIcebergCatalog() {
    return tableParams_.icebergCatalog_;
  }

  @Override
  public String getIcebergCatalogLocation() {
    return tableParams_.icebergCatalogLocation_;
  }

  @Override
  public FeFsTable getFeFsTable() {
    return localFsTable_;
  }

  @Override
  public List<IcebergPartitionSpec> getPartitionSpecs() {
    return partitionSpecs_;
  }

  @Override
  public int getDefaultPartitionSpecId() {
    return defaultPartitionSpecId_;
  }

  @Override
  public IcebergPartitionSpec getDefaultPartitionSpec() {
    return Utils.getDefaultPartitionSpec(this);
  }

  @Override
  public org.apache.iceberg.Table getIcebergApiTable() {
    return icebergApiTable_;
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
  public TTableDescriptor toThriftDescriptor(int tableId,
      Set<Long> referencedPartitions) {
    TTableDescriptor desc = new TTableDescriptor(tableId, TTableType.ICEBERG_TABLE,
        FeCatalogUtils.getTColumnDescriptors(this),
        getNumClusteringCols(),
        name_, db_.getName());
    desc.setIcebergTable(Utils.getTIcebergTable(this, ThriftObjectType.DESCRIPTOR_ONLY));
    desc.setHdfsTable(transformToTHdfsTable(false, ThriftObjectType.DESCRIPTOR_ONLY));
    return desc;
  }

  @Override
  public THdfsTable transformToTHdfsTable(boolean updatePartitionFlag,
      ThriftObjectType type) {
    Map<Long, THdfsPartition> idToPartition = new HashMap<>();
    // LocalFsTable transformed from iceberg table only has one partition
    Collection<? extends FeFsPartition> partitions =
        FeCatalogUtils.loadAllPartitions(localFsTable_);
    Preconditions.checkState(partitions.size() == 1);
    FeFsPartition partition = (FeFsPartition) partitions.toArray()[0];
    idToPartition.put(partition.getId(),
        FeCatalogUtils.fsPartitionToThrift(partition,
            ThriftObjectType.DESCRIPTOR_ONLY));

    THdfsPartition tPrototypePartition = FeCatalogUtils.fsPartitionToThrift(
        localFsTable_.createPrototypePartition(),
        ThriftObjectType.DESCRIPTOR_ONLY);
    THdfsTable hdfsTable = new THdfsTable(localFsTable_.getHdfsBaseDir(),
        getColumnNames(), localFsTable_.getNullPartitionKeyValue(),
        FeFsTable.DEFAULT_NULL_COLUMN_VALUE, idToPartition, tPrototypePartition);
    hdfsTable.setAvroSchema(localFsTable_.getAvroSchema());
    Utils.updateIcebergPartitionFileFormat(this, hdfsTable);
    hdfsTable.setPartition_prefixes(localFsTable_.getPartitionPrefixes());
    return hdfsTable;
  }

  @Immutable
  public static class TableParams {
    private final String icebergTableLocation_;
    private final TIcebergCatalog icebergCatalog_;
    private final String icebergCatalogLocation_;

    TableParams(Table msTable) {
      String fullTableName = msTable.getDbName() + "." + msTable.getTableName();
      if (msTable.getSd().isSetLocation()) {
        icebergTableLocation_ = msTable.getSd().getLocation();
      } else {
        throw new LocalCatalogException("Cannot find iceberg table location for table "
            + fullTableName);
      }
      icebergCatalog_ = IcebergUtil.getTIcebergCatalog(msTable);

      if (icebergCatalog_ == TIcebergCatalog.HADOOP_CATALOG) {
        icebergCatalogLocation_ = Utils.getIcebergCatalogLocation(msTable);
      } else {
        icebergCatalogLocation_ = icebergTableLocation_;
      }
    }

    public String getIcebergTableLocation() {
      return icebergTableLocation_;
    }

    public TIcebergCatalog getIcebergCatalog() {
      return icebergCatalog_;
    }

    public String getIcebergCatalogLocation() {
      return icebergCatalogLocation_;
    }
  }

  @Override
  public IcebergContentFileStore getContentFileStore() {
    return fileStore_;
  }
}
