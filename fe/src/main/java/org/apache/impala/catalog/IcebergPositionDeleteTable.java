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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
import org.apache.iceberg.Table;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.analysis.IcebergPartitionSpec;
import org.apache.impala.thrift.TColumnStats;
import org.apache.impala.thrift.TCompressionCodec;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.thrift.TIcebergFileFormat;
import org.apache.impala.thrift.TIcebergPartitionStats;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableStats;

/**
 * Iceberg position delete table is created on the fly during planning. It belongs to an
 * actual Iceberg table (referred to as 'baseTable_'), but has a schema that corresponds
 * to the file schema of position delete files. Therefore with the help of it we can
 * do an ANTI JOIN between data files and delete files.
 */
public class IcebergPositionDeleteTable extends VirtualTable implements FeIcebergTable  {
  private FeIcebergTable baseTable_;
  private Set<FileDescriptor> deleteFiles_;
  private long deleteRecordsCount_;

  public static String FILE_PATH_COLUMN = "file_path";
  public static String POS_COLUMN = "pos";

  public IcebergPositionDeleteTable(FeIcebergTable baseTable, String name,
      Set<FileDescriptor> deleteFiles,
      long deleteRecordsCount, TColumnStats filePathsStats) {
    super(baseTable.getMetaStoreTable(), baseTable.getDb(), name,
        baseTable.getOwnerUser());
    baseTable_ = baseTable;
    deleteFiles_ = deleteFiles;
    deleteRecordsCount_ = deleteRecordsCount;
    Column filePath = new IcebergColumn(FILE_PATH_COLUMN, Type.STRING, /*comment=*/"",
        colsByPos_.size(), IcebergTable.V2_FILE_PATH_FIELD_ID, -1, -1,
        /*nullable=*/false);
    Column pos = new IcebergColumn(POS_COLUMN, Type.BIGINT, /*comment=*/"",
        colsByPos_.size(), IcebergTable.V2_POS_FIELD_ID, -1, -1, /*nullable=*/false);
    filePath.updateStats(filePathsStats);
    pos.updateStats(getPosStats(pos));
    addColumn(filePath);
    addColumn(pos);
  }

  private TColumnStats getPosStats(Column pos) {
    TColumnStats colStats = new TColumnStats();
    colStats.num_distinct_values = deleteRecordsCount_;
    colStats.num_nulls = 0;
    colStats.max_size = pos.getType().getSlotSize();
    return colStats;
  }

  @Override
  public long getNumRows() {
    return deleteRecordsCount_;
  }

  @Override
  public TTableStats getTTableStats() {
    long totalBytes = 0;
    for (FileDescriptor df : deleteFiles_) {
      totalBytes += df.getFileLength();
    }
    TTableStats ret = new TTableStats(getNumRows());
    ret.setTotal_file_bytes(totalBytes);
    return ret;
  }

  /**
   * Return same descriptor as the base table, but with a schema that corresponds to
   * the position delete file schema ('file_path', 'pos').
   */
  @Override
  public TTableDescriptor toThriftDescriptor(int tableId,
      Set<Long> referencedPartitions) {
    TTableDescriptor desc = baseTable_.toThriftDescriptor(tableId, referencedPartitions);
    desc.setColumnDescriptors(FeCatalogUtils.getTColumnDescriptors(this));
    return desc;
  }

  @Override
  public IcebergContentFileStore getContentFileStore() {
    throw new NotImplementedException("This should never be called.");
  }

  @Override
  public Map<String, TIcebergPartitionStats> getIcebergPartitionStats() {
    return null;
  }

  @Override
  public FeFsTable getFeFsTable() {
    return baseTable_.getFeFsTable();
  }

  @Override
  public TIcebergCatalog getIcebergCatalog() {
    return null;
  }

  @Override
  public Table getIcebergApiTable() {
    return baseTable_.getIcebergApiTable();
  }

  @Override
  public String getIcebergCatalogLocation() {
    return null;
  }

  @Override
  public TIcebergFileFormat getIcebergFileFormat() {
    return baseTable_.getIcebergFileFormat();
  }

  @Override
  public TCompressionCodec getIcebergParquetCompressionCodec() {
    return null;
  }

  @Override
  public long getIcebergParquetRowGroupSize() {
    return baseTable_.getIcebergParquetRowGroupSize();
  }

  @Override
  public long getIcebergParquetPlainPageSize() {
    return baseTable_.getIcebergParquetPlainPageSize();
  }

  @Override
  public long getIcebergParquetDictPageSize() {
    return baseTable_.getIcebergParquetDictPageSize();
  }

  @Override
  public String getIcebergTableLocation() {
    return null;
  }

  @Override
  public List<IcebergPartitionSpec> getPartitionSpecs() {
    return null;
  }

  @Override
  public IcebergPartitionSpec getDefaultPartitionSpec() {
    return null;
  }

  @Override
  public int getDefaultPartitionSpecId() {
    return -1;
  }
}
