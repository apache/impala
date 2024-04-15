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
import org.apache.impala.analysis.IcebergPartitionSpec;
import org.apache.impala.catalog.CatalogObject.ThriftObjectType;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.thrift.TCompressionCodec;
import org.apache.impala.thrift.THdfsTable;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.thrift.TIcebergFileFormat;
import org.apache.impala.thrift.TIcebergPartitionStats;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.util.AvroSchemaConverter;

/**
 * Base class for the virtual table implementations for Iceberg deletes, like position or
 * equality deletes.
 */
public abstract class IcebergDeleteTable extends VirtualTable implements FeIcebergTable  {
    protected final static int INVALID_MAP_KEY_ID = -1;
    protected final static int INVALID_MAP_VALUE_ID = -1;

    protected FeIcebergTable baseTable_;
    protected Set<FileDescriptor> deleteFiles_;
    protected long deleteRecordsCount_;

    public IcebergDeleteTable(FeIcebergTable baseTable, String name,
        Set<FileDescriptor> deleteFiles, long deleteRecordsCount) {
      super(baseTable.getMetaStoreTable(), baseTable.getDb(), name,
          baseTable.getOwnerUser());
      baseTable_ = baseTable;
      deleteFiles_ = deleteFiles;
      deleteRecordsCount_ = deleteRecordsCount;
    }

    public FeIcebergTable getBaseTable() { return baseTable_; }

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
     * the schema of this delete table (including virtual columns).
     */
    @Override
    public TTableDescriptor toThriftDescriptor(int tableId,
        Set<Long> referencedPartitions) {
      TTableDescriptor desc =
          baseTable_.toThriftDescriptor(tableId, referencedPartitions);
      desc.setColumnDescriptors(FeCatalogUtils.getTColumnDescriptors(this));
      if (desc.hdfsTable.isSetAvroSchema()) {
        desc.hdfsTable.setAvroSchema(AvroSchemaConverter.convertColumns(getColumns(),
            getFullName().replaceAll("-", "_")).toString());
      }
      return desc;
    }

    @Override
    public IcebergContentFileStore getContentFileStore() {
      throw new NotImplementedException("This should never be called.");
    }

    @Override
    public Map<String, TIcebergPartitionStats> getIcebergPartitionStats(){
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
        return baseTable_.getPartitionSpecs();
    }

    @Override
    public IcebergPartitionSpec getDefaultPartitionSpec() {
        return null;
    }

    @Override
    public int getDefaultPartitionSpecId() {
        return -1;
    }

    @Override
    public THdfsTable transformToTHdfsTable(boolean updatePartitionFlag,
        ThriftObjectType type) {
      throw new IllegalStateException("not implemented here");
    }
}