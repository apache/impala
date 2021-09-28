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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.impala.analysis.IcebergPartitionSpec;
import org.apache.impala.catalog.CatalogObject;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.thrift.TCompressionCodec;
import org.apache.impala.thrift.THdfsPartition;
import org.apache.impala.thrift.THdfsTable;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.thrift.TIcebergFileFormat;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.IcebergSchemaConverter;
import org.apache.impala.util.IcebergUtil;

import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.Immutable;

/**
 * Iceberg table for LocalCatalog
 */
public class LocalIcebergTable extends LocalTable implements FeIcebergTable {
  private TableParams tableParams_;
  private TIcebergFileFormat icebergFileFormat_;
  private TCompressionCodec icebergParquetCompressionCodec_;
  private long icebergParquetRowGroupSize_;
  private long icebergParquetPlainPageSize_;
  private long icebergParquetDictPageSize_;
  private List<IcebergPartitionSpec> partitionSpecs_;
  private int defaultPartitionSpecId_;
  private Map<String, FileDescriptor> pathHashToFileDescMap_;
  private LocalFsTable localFsTable_;
  private long snapshotId_ = -1;
  private Schema icebergSchema_;

  static LocalTable loadFromIceberg(LocalDb db, Table msTable,
      MetaProvider.TableMetaRef ref) throws TableLoadingException {
    Preconditions.checkNotNull(db);
    Preconditions.checkNotNull(msTable);
    try {
      TableParams params = new TableParams(msTable);
      TableMetadata metadata =
          IcebergUtil.getIcebergTableMetadata(params.icebergCatalog_,
              IcebergUtil.getIcebergTableIdentifier(msTable),
              params.icebergCatalogLocation_,
              msTable.getParameters());
      List<Column> iceColumns =
          IcebergSchemaConverter.convertToImpalaSchema(metadata.schema());
      validateColumns(iceColumns, msTable.getSd().getCols());
      ColumnMap colMap = new ColumnMap(iceColumns,
          /*numClusteringCols=*/ 0,
          db.getName() + "." + msTable.getTableName(),
          /*isFullAcidSchema=*/false);

      return new LocalIcebergTable(db, msTable, ref, colMap, metadata);
    } catch (Exception e) {
      String fullTableName = msTable.getDbName() + "." + msTable.getTableName();
      throw new TableLoadingException(
          String.format("Error opening Iceberg table '%s'", fullTableName), e);
    }
  }

  private LocalIcebergTable(LocalDb db, Table msTable, MetaProvider.TableMetaRef ref,
      ColumnMap cmap, TableMetadata metadata)
      throws TableLoadingException {
    super(db, msTable, ref, cmap);
    tableParams_ = new TableParams(msTable);
    partitionSpecs_ = Utils.loadPartitionSpecByIceberg(metadata);
    defaultPartitionSpecId_ = metadata.defaultSpecId();
    localFsTable_ = LocalFsTable.load(db, msTable, ref);
    if (metadata.currentSnapshot() != null) {
      snapshotId_ = metadata.currentSnapshot().snapshotId();
    }
    icebergSchema_ = metadata.schema();
    try {
      pathHashToFileDescMap_ = Utils.loadAllPartition(this);
    } catch (IOException e) {
      throw new TableLoadingException(String.format(
          "Failed to load table: %s.%s", msTable.getDbName(), msTable.getTableName()),
          (Exception)e);
    }
    icebergFileFormat_ = IcebergUtil.getIcebergFileFormat(msTable);
    icebergParquetCompressionCodec_ = Utils.getIcebergParquetCompressionCodec(msTable);
    icebergParquetRowGroupSize_ = Utils.getIcebergParquetRowGroupSize(msTable);
    icebergParquetPlainPageSize_ = Utils.getIcebergParquetPlainPageSize(msTable);
    icebergParquetDictPageSize_ = Utils.getIcebergParquetDictPageSize(msTable);
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
  public Schema getIcebergSchema() {
    return icebergSchema_;
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
  public int getDefaultPartitionSpecId() { return defaultPartitionSpecId_; }

  @Override
  public IcebergPartitionSpec getDefaultPartitionSpec() {
    return Utils.getDefaultPartitionSpec(this);
  }

  @Override
  public Map<String, FileDescriptor> getPathHashToFileDescMap() {
    return pathHashToFileDescMap_;
  }

  @Override
  public long snapshotId() {
    return snapshotId_;
  }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId,
      Set<Long> referencedPartitions) {
    TTableDescriptor desc = new TTableDescriptor(tableId, TTableType.ICEBERG_TABLE,
        FeCatalogUtils.getTColumnDescriptors(this),
        getNumClusteringCols(),
        name_, db_.getName());

    desc.setIcebergTable(Utils.getTIcebergTable(this));
    desc.setHdfsTable(transfromToTHdfsTable());
    return desc;
  }

  private THdfsTable transfromToTHdfsTable() {
    Map<Long, THdfsPartition> idToPartition = new HashMap<>();
    // LocalFsTable transformed from iceberg table only has one partition
    Collection<? extends FeFsPartition> partitions =
        FeCatalogUtils.loadAllPartitions(localFsTable_);
    Preconditions.checkState(partitions.size() == 1);
    FeFsPartition partition = (FeFsPartition) partitions.toArray()[0];
    idToPartition.put(partition.getId(),
        FeCatalogUtils.fsPartitionToThrift(partition,
            CatalogObject.ThriftObjectType.DESCRIPTOR_ONLY));

    THdfsPartition tPrototypePartition = FeCatalogUtils.fsPartitionToThrift(
        localFsTable_.createPrototypePartition(),
        CatalogObject.ThriftObjectType.DESCRIPTOR_ONLY);
    THdfsTable hdfsTable = new THdfsTable(localFsTable_.getHdfsBaseDir(),
        getColumnNames(), localFsTable_.getNullPartitionKeyValue(),
        FeFsTable.DEFAULT_NULL_COLUMN_VALUE, idToPartition, tPrototypePartition);
    Utils.updateIcebergPartitionFileFormat(this, hdfsTable);
    hdfsTable.setPartition_prefixes(localFsTable_.getPartitionPrefixes());
    return hdfsTable;
  }

  @Immutable
  private static class TableParams {
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
  }
}
