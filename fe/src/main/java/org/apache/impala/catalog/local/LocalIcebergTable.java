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

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.impala.analysis.IcebergPartitionSpec;
import org.apache.impala.catalog.CatalogObject;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.thrift.THdfsPartition;
import org.apache.impala.thrift.THdfsTable;
import org.apache.impala.thrift.TIcebergFileFormat;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.IcebergUtil;

import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.Immutable;

/**
 * Iceberg table for LocalCatalog
 */
public class LocalIcebergTable extends LocalTable implements FeIcebergTable {
  private TableParams tableParams_;
  private TIcebergFileFormat icebergFileFormat_;
  private List<IcebergPartitionSpec> partitionSpecs_;
  private Map<String, FileDescriptor> pathMD5ToFileDescMap_;
  private LocalFsTable localFsTable_;

  static LocalTable loadFromIceberg(LocalDb db, Table msTable,
      MetaProvider.TableMetaRef ref) throws TableLoadingException {
    Preconditions.checkNotNull(db);
    Preconditions.checkNotNull(msTable);
    try {
      TableParams params = new TableParams(msTable);
      TableMetadata metadata =
          IcebergUtil.getIcebergTableMetadata(params.icebergTableLocation_);

      List<IcebergPartitionSpec> partitionSpecs =
          Utils.loadPartitionSpecByIceberg(metadata);

      return new LocalIcebergTable(db, msTable, ref, ColumnMap.fromMsTable(msTable),
          partitionSpecs);
    } catch (Exception e) {
      String fullTableName = msTable.getDbName() + "." + msTable.getTableName();
      throw new TableLoadingException(
          String.format("Error opening Icberg table '%s', error: %s",
              fullTableName, e.getMessage()));
    }
  }

  private LocalIcebergTable(LocalDb db, Table msTable, MetaProvider.TableMetaRef ref,
      ColumnMap cmap, List<IcebergPartitionSpec> partitionSpecs)
      throws TableLoadingException {
    super(db, msTable, ref, cmap);
    tableParams_ = new TableParams(msTable);
    partitionSpecs_ = partitionSpecs;
    localFsTable_ = LocalFsTable.load(db, msTable, ref);
    try {
      pathMD5ToFileDescMap_ = Utils.loadAllPartition(tableParams_.icebergTableLocation_,
          this);
    } catch (IOException e) {
      throw new TableLoadingException(e.getMessage());
    }

    icebergFileFormat_ = Utils.getIcebergFileFormat(msTable);
  }

  @Override
  public TIcebergFileFormat getIcebergFileFormat() {
    return icebergFileFormat_;
  }

  @Override
  public String getIcebergTableLocation() {
    return tableParams_.icebergTableLocation_;
  }

  @Override
  public FeFsTable getFeFsTable() {
    return localFsTable_;
  }

  @Override
  public List<IcebergPartitionSpec> getPartitionSpec() {
    return partitionSpecs_;
  }

  @Override
  public Map<String, FileDescriptor> getPathMD5ToFileDescMap() {
    return pathMD5ToFileDescMap_;
  }

  @Override
  protected void loadColumnStats() {
    localFsTable_.loadColumnStats();
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
    return hdfsTable;
  }

  @Immutable
  private static class TableParams {
    private final String icebergTableLocation_;

    TableParams(Table msTable) {
      String fullTableName = msTable.getDbName() + "." + msTable.getTableName();
      if (msTable.getSd().isSetLocation()) {
        icebergTableLocation_ = msTable.getSd().getLocation();
      } else {
        throw new LocalCatalogException("Cannot find iceberg table location for table "
            + fullTableName);
      }
    }
  }
}
