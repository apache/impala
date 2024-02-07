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

package org.apache.impala.catalog.iceberg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.TypeUtil;
import org.apache.impala.analysis.ColumnDef;
import org.apache.impala.analysis.IcebergPartitionSpec;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogObject.ThriftObjectType;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.CtasTargetTable;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.HdfsStorageDescriptor;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.IcebergColumn;
import org.apache.impala.catalog.IcebergContentFileStore;
import org.apache.impala.catalog.IcebergStructField;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.local.LocalDb;
import org.apache.impala.catalog.local.LocalFsTable;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.catalog.StructType;
import org.apache.impala.service.MetadataOp;
import org.apache.impala.thrift.CatalogObjectsConstants;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TCompressionCodec;
import org.apache.impala.thrift.THdfsPartition;
import org.apache.impala.thrift.THdfsStorageDescriptor;
import org.apache.impala.thrift.THdfsTable;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.thrift.TIcebergFileFormat;
import org.apache.impala.thrift.TIcebergPartitionStats;
import org.apache.impala.thrift.TImpalaTableType;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.IcebergSchemaConverter;
import org.apache.impala.util.IcebergUtil;

/**
 * Utility class that can be used as a temporary target table for CTAS statements.
 * It mimics an FeIcebergTable without actually creating it via Iceberg.
 */
public class IcebergCtasTarget extends CtasTargetTable implements FeIcebergTable {
  private FeFsTable fsTable_;
  private Schema iceSchema_;
  private List<IcebergPartitionSpec> partitionSpecs_ = new ArrayList<>();
  private TIcebergFileFormat icebergFileFormat_;
  private TCompressionCodec icebergParquetCompressionCodec_;
  private long icebergParquetRowGroupSize_;
  private long icebergParquetPlainPageSize_;
  private long icebergParquetDictPageSize_;
  private TIcebergCatalog icebergCatalog_;
  private String icebergTableLocation_;
  private String icebergCatalogLocation_;
  private HdfsStorageDescriptor hdfsSd_;

  public IcebergCtasTarget(FeDb db, org.apache.hadoop.hive.metastore.api.Table msTbl,
        List<ColumnDef> columnDefs, List<String> primaryKeyNames,
        IcebergPartitionSpec partSpec) throws CatalogException, ImpalaRuntimeException {
    super(msTbl, db, msTbl.getTableName(), msTbl.getOwner());
    createFsTable(db, msTbl);
    createIcebergSchema(columnDefs, primaryKeyNames);
    createPartitionSpec(partSpec);
    icebergCatalog_ = IcebergUtil.getTIcebergCatalog(msTbl);
    setLocations();
    icebergFileFormat_ = IcebergUtil.getIcebergFileFormat(msTbl);
    icebergParquetCompressionCodec_ = Utils.getIcebergParquetCompressionCodec(msTbl);
    icebergParquetRowGroupSize_ = Utils.getIcebergParquetRowGroupSize(msTbl);
    icebergParquetPlainPageSize_ = Utils.getIcebergParquetPlainPageSize(msTbl);
    icebergParquetDictPageSize_ = Utils.getIcebergParquetDictPageSize(msTbl);
    hdfsSd_ = HdfsStorageDescriptor.fromStorageDescriptor(name_, msTable_.getSd());
  }

  private void createIcebergSchema(List<ColumnDef> columnDefs,
      List<String> primaryKeyNames) throws CatalogException {
    List<TColumn> tcols = new ArrayList<>();
    for (ColumnDef col : columnDefs) {
      tcols.add(col.toThrift());
    }
    try {
      iceSchema_ = IcebergSchemaConverter.genIcebergSchema(tcols, primaryKeyNames);
      // In genIcebergSchema() we did our best to assign correct field ids to columns,
      // but to be sure, let's use Iceberg's API function to assign field ids.
      iceSchema_ = TypeUtil.assignIncreasingFreshIds(iceSchema_);
      for (Column col : IcebergSchemaConverter.convertToImpalaSchema(iceSchema_)) {
        addColumn((IcebergColumn)col);
      }
    } catch (ImpalaRuntimeException ex) {
      throw new CatalogException(
        "Exception caught during generating Iceberg schema:", ex);
    }
  }

  private void createPartitionSpec(IcebergPartitionSpec partSpec)
      throws CatalogException, ImpalaRuntimeException {
    Preconditions.checkState(iceSchema_ != null);
    PartitionSpec iceSpec = null;
    try {
      // Let's create an Iceberg PartitionSpec with the help of Iceberg from 'partSpec',
      // then convert it back to an IcebergPartitionSpec.
      if (partSpec == null) {
        iceSpec = PartitionSpec.unpartitioned();
      } else {
        iceSpec = IcebergUtil.createIcebergPartition(iceSchema_, partSpec.toThrift());
      }
    } catch (ImpalaRuntimeException ex) {
      throw new CatalogException(
        "Exception caught during generating Iceberg schema:", ex);
    }
    IcebergPartitionSpec resolvedIcebergSpec =
        FeIcebergTable.Utils.convertPartitionSpec(iceSchema_, iceSpec);
    partitionSpecs_.add(resolvedIcebergSpec);
  }

  private void setLocations() {
    Preconditions.checkState(msTable_ != null);
    Preconditions.checkState(icebergCatalog_ != null);
    TIcebergCatalog underlyingCatalog = IcebergUtil.getUnderlyingCatalog(msTable_);
    if (underlyingCatalog == TIcebergCatalog.HADOOP_CATALOG) {
      if (icebergCatalog_ == TIcebergCatalog.CATALOGS) {
        String catName = msTable_.getParameters().get(IcebergTable.ICEBERG_CATALOG);
        icebergCatalogLocation_ = IcebergCatalogs.getInstance().getCatalogProperty(
            catName, CatalogProperties.WAREHOUSE_LOCATION);
      } else {
        icebergCatalogLocation_ = IcebergUtil.getIcebergCatalogLocation(msTable_);
      }
      TableIdentifier tId = IcebergUtil.getIcebergTableIdentifier(msTable_);
      Namespace ns = tId.namespace();
      List<String> components = new ArrayList<>();
      Collections.addAll(components, ns.levels());
      components.add(tId.name());
      icebergTableLocation_ =
          icebergCatalogLocation_ + "/" + String.join("/", components);
      return;
    }
    Preconditions.checkState(icebergCatalog_ == TIcebergCatalog.HADOOP_TABLES ||
                             icebergCatalog_ == TIcebergCatalog.HIVE_CATALOG ||
                             icebergCatalog_ == TIcebergCatalog.CATALOGS);
    icebergTableLocation_ = msTable_.getSd().getLocation();
    icebergCatalogLocation_ = icebergTableLocation_;
  }

  private void createFsTable(FeDb db, org.apache.hadoop.hive.metastore.api.Table msTbl)
      throws CatalogException {
    if (db instanceof Db) {
      fsTable_ = HdfsTable.createCtasTarget((Db)db, msTbl);
    } else {
      fsTable_ = LocalFsTable.createCtasTarget((LocalDb)db, msTbl);
    }
  }

  @Override
  public IcebergContentFileStore getContentFileStore() {
    return new IcebergContentFileStore();
  }

  @Override
  public FeFsTable getFeFsTable() {
    return fsTable_;
  }

  @Override
  public TIcebergCatalog getIcebergCatalog() {
    return icebergCatalog_;
  }

  @Override
  public String getIcebergCatalogLocation() {
    return icebergCatalogLocation_;
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
  public List<IcebergPartitionSpec> getPartitionSpecs() {
    return partitionSpecs_;
  }

  @Override
  public IcebergPartitionSpec getDefaultPartitionSpec() {
    return partitionSpecs_.get(0);
  }

  @Override
  public int getDefaultPartitionSpecId() {
    return 0;
  }

  @Override
  public Schema getIcebergSchema() {
    return iceSchema_;
  }

  @Override
  public THdfsTable transformToTHdfsTable(boolean updatePartitionFlag,
      ThriftObjectType type) {
    throw new IllegalStateException("not implemented here");
  }


  @Override
  public long snapshotId() {
    return -1;
  }

  @Override
  public Map<String, TIcebergPartitionStats> getIcebergPartitionStats() {
    return null;
  }

  @Override
  public org.apache.iceberg.Table getIcebergApiTable() {
    return null;
  }

  public void addColumn(IcebergColumn col) {
    colsByPos_.add(col);
    colsByName_.put(col.getName().toLowerCase(), col);
    ((StructType) type_.getItemType()).addField(
        new IcebergStructField(col.getName(), col.getType(), col.getComment(),
            col.getFieldId()));
  }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId,
      Set<Long> referencedPartitions) {
    TTableDescriptor desc = new TTableDescriptor(tableId, TTableType.ICEBERG_TABLE,
        FeCatalogUtils.getTColumnDescriptors(this),
        getNumClusteringCols(),
        getName(), db_.getName());

    desc.setIcebergTable(Utils.getTIcebergTable(this, ThriftObjectType.DESCRIPTOR_ONLY));
    desc.setHdfsTable(transformToTHdfsTable());
    return desc;
  }

  private THdfsTable transformToTHdfsTable() {
    if (fsTable_ instanceof HdfsTable) {
      return transformOldToTHdfsTable();
    } else {
      return transformLocalToTHdfsTable();
    }
  }

  private THdfsTable transformOldToTHdfsTable() {
    THdfsTable hdfsTable = ((HdfsTable)fsTable_).getTHdfsTable(
        ThriftObjectType.FULL, null);
    hdfsTable.setPrototype_partition(createPrototypePartition());
    return hdfsTable;
  }

  private THdfsTable transformLocalToTHdfsTable() {
    LocalFsTable localFsTable = (LocalFsTable)fsTable_;
    Map<Long, THdfsPartition> idToPartition = new HashMap<>();
    THdfsPartition tPrototypePartition = createPrototypePartition();
    return new THdfsTable(localFsTable.getHdfsBaseDir(),
        getColumnNames(), localFsTable.getNullPartitionKeyValue(),
        FeFsTable.DEFAULT_NULL_COLUMN_VALUE, idToPartition, tPrototypePartition);
  }

  private THdfsPartition createPrototypePartition() {
    THdfsPartition prototypePart = new THdfsPartition();
    THdfsStorageDescriptor sd = new THdfsStorageDescriptor();
    sd.setFileFormat(IcebergUtil.toTHdfsFileFormat(icebergFileFormat_));
    sd.setBlockSize(hdfsSd_.getBlockSize());
    prototypePart.setHdfs_storage_descriptor(sd);
    prototypePart.setId(CatalogObjectsConstants.PROTOTYPE_PARTITION_ID);
    return prototypePart;
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.TABLE;
  }

  @Override
  public TImpalaTableType getTableType() {
    return TImpalaTableType.TABLE;
  }

  @Override
  public String getTableComment() {
    return MetadataOp.getTableComment(msTable_);
  }
}
