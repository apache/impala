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

import static org.apache.impala.analysis.TimeTravelSpec.Kind.TIME_AS_OF;
import static org.apache.impala.analysis.TimeTravelSpec.Kind.VERSION_AS_OF;

import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.Schema;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.impala.analysis.IcebergPartitionSpec;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.TableName;
import org.apache.impala.analysis.TimeTravelSpec;
import org.apache.impala.catalog.CatalogObject.ThriftObjectType;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TColumnDescriptor;
import org.apache.impala.thrift.TCompressionCodec;
import org.apache.impala.thrift.THdfsTable;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.thrift.TIcebergFileFormat;
import org.apache.impala.thrift.TIcebergPartitionStats;
import org.apache.impala.thrift.TImpalaTableType;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TSortingOrder;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.IcebergSchemaConverter;
import org.apache.impala.util.ListMap;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Represents an Iceberg Table involved in Time Travel.
 * This is implemented by embedding a reference to the base Iceberg Table.
 * All methods that are not Time Travel delegated are delegated to the base table.
 */
public class IcebergTimeTravelTable
    extends ForwardingFeIcebergTable implements FeIcebergTable {
  // The base table to which non-Time Travel related methods are delegated
  private final FeIcebergTable base_;

  // The Time Travel parameters that control the schema for the table.
  private final TimeTravelSpec timeTravelSpec_;

  // colsByPos[i] refers to the ith column in the table.
  protected final ArrayList<Column> colsByPos_ = new ArrayList<>();

  // map from lowercase column name to Column object.
  protected final Map<String, Column> colsByName_ = new HashMap<>();

  // Type of this table (array of struct) that mirrors the columns. Useful for analysis.
  protected final ArrayType type_ = new ArrayType(new StructType());

  public IcebergTimeTravelTable(FeIcebergTable base, TimeTravelSpec timeTravelSpec)
      throws AnalysisException {
    super(base);
    base_ = base;
    timeTravelSpec_ = timeTravelSpec;
    this.readSchema();
  }

  public FeIcebergTable getBase() { return base_; }

  /**
   * Initialize the columns from the schema corresponding to the time travel
   * specification.
   */
  private void readSchema() throws AnalysisException {
    org.apache.iceberg.Table icebergApiTable = getIcebergApiTable();
    Schema icebergSchema;
    if (timeTravelSpec_.getKind() == VERSION_AS_OF) {
      long snapshotId = timeTravelSpec_.getAsOfVersion();
      icebergSchema = SnapshotUtil.schemaFor(icebergApiTable, snapshotId, null);
    } else {
      Preconditions.checkState(timeTravelSpec_.getKind() == TIME_AS_OF);
      long timestampMillis = timeTravelSpec_.getAsOfMillis();
      try {
        icebergSchema = SnapshotUtil.schemaFor(icebergApiTable, null, timestampMillis);
      } catch (IllegalArgumentException e) {
        // Use time with local TZ in exception so that it's clearer.
        throw new IllegalArgumentException(
            "Cannot find a snapshot older than " + timeTravelSpec_.toTimeString());
      }
    }
    try {
      for (Column col : IcebergSchemaConverter.convertToImpalaSchema(icebergSchema)) {
        addColumn(col);
      }
    } catch (ImpalaRuntimeException e) {
      throw new AnalysisException("Could not create iceberg schema.", e);
    }
  }

  @Override
  public List<Column> getColumnsInHiveOrder() {
    Preconditions.checkState(base_.getNumClusteringCols() == 0);
    return colsByPos_;
  }

  @Override
  public List<String> getColumnNames() {
    return Column.toColumnNames(colsByPos_);
  }

  @Override
  public List<Column> getColumns() {
    return colsByPos_;
  }

  @Override
  public List<Column> getClusteringColumns() {
    return Collections.emptyList();
  }

  @Override
  public Column getColumn(String name) {
    return colsByName_.get(name.toLowerCase());
  }

  @Override // FeTable
  public List<Column> getNonClusteringColumns() {
    return colsByPos_;
  }

  @Override
  public boolean isClusteringColumn(Column c) {
    Preconditions.checkArgument(colsByPos_.get(c.getPosition()) == c);
    return false;
  }

  @Override
  public TTableDescriptor toThriftDescriptor(
      int tableId, Set<Long> referencedPartitions) {
    TTableDescriptor desc = new TTableDescriptor(tableId, TTableType.ICEBERG_TABLE,
        getTColumnDescriptors(), 0, getName(), getDb().getName());
    desc.setIcebergTable(Utils.getTIcebergTable(this, ThriftObjectType.DESCRIPTOR_ONLY));
    desc.setHdfsTable(transformToTHdfsTable(false, ThriftObjectType.DESCRIPTOR_ONLY));
    return desc;
  }

  /**
   * Returns a list of thrift column descriptors ordered by position.
   */
  public List<TColumnDescriptor> getTColumnDescriptors() {
    return FeCatalogUtils.getTColumnDescriptors(this);
  }

  public ArrayType getType() { return type_; }

  public void addColumn(Column col) {
    Preconditions.checkState(col instanceof IcebergColumn);
    IcebergColumn iCol = (IcebergColumn) col;
    colsByPos_.add(iCol);
    colsByName_.put(iCol.getName().toLowerCase(), col);

    ((StructType) type_.getItemType())
        .addField(new IcebergStructField(
            col.getName(), col.getType(), col.getComment(), iCol.getFieldId()));
  }

  @Override
  public THdfsTable transformToTHdfsTable(boolean updatePartitionFlag,
      ThriftObjectType type) {
    return base_.transformToTHdfsTable(updatePartitionFlag, type);
  }
}

/**
 * A forwarding class for FeIcebergTable.
 * This is just boilerplate code that delegates all methods to base.
 * See Effective Java: "Favor composition over inheritance."
 */
class ForwardingFeIcebergTable implements FeIcebergTable {
  private final FeIcebergTable base;

  public ForwardingFeIcebergTable(FeIcebergTable base) { this.base = base; }

  @Override
  public FileSystem getFileSystem() throws CatalogException {
    return base.getFileSystem();
  }

  public static FileSystem getFileSystem(Path filePath) throws CatalogException {
    return FeFsTable.getFileSystem(filePath);
  }

  @Override
  public List<String> getPrimaryKeyColumnNames() throws TException {
    return base.getPrimaryKeyColumnNames();
  }

  @Override
  public boolean isPartitioned() {
    return base.isPartitioned();
  }

  @Override
  public List<String> getForeignKeysSql() throws TException {
    return base.getForeignKeysSql();
  }

  @Override
  public int parseSkipHeaderLineCount(StringBuilder error) {
    return base.parseSkipHeaderLineCount(error);
  }

  @Override
  public int getSortByColumnIndex(String col_name) {
    return base.getSortByColumnIndex(col_name);
  }

  @Override
  public boolean isLeadingSortByColumn(String col_name) {
    return base.isLeadingSortByColumn(col_name);
  }

  @Override
  public boolean isSortByColumn(String col_name) {
    return base.isSortByColumn(col_name);
  }

  @Override
  public TSortingOrder getSortOrderForSortByColumn() {
    return base.getSortOrderForSortByColumn();
  }

  @Override
  public boolean IsLexicalSortByColumn() {
    return base.IsLexicalSortByColumn();
  }

  @Override
  public IcebergContentFileStore getContentFileStore() {
    return base.getContentFileStore();
  }

  @Override
  public Map<String, TIcebergPartitionStats> getIcebergPartitionStats() {
    return base.getIcebergPartitionStats();
  }

  @Override
  public FeFsTable getFeFsTable() {
    return base.getFeFsTable();
  }

  @Override
  public TIcebergCatalog getIcebergCatalog() {
    return base.getIcebergCatalog();
  }

  @Override
  public org.apache.iceberg.Table getIcebergApiTable() {
    return base.getIcebergApiTable();
  }

  @Override
  public String getIcebergCatalogLocation() {
    return base.getIcebergCatalogLocation();
  }

  @Override
  public TIcebergFileFormat getIcebergFileFormat() {
    return base.getIcebergFileFormat();
  }

  @Override
  public TCompressionCodec getIcebergParquetCompressionCodec() {
    return base.getIcebergParquetCompressionCodec();
  }

  @Override
  public long getIcebergParquetRowGroupSize() {
    return base.getIcebergParquetRowGroupSize();
  }

  @Override
  public long getIcebergParquetPlainPageSize() {
    return base.getIcebergParquetPlainPageSize();
  }

  @Override
  public long getIcebergParquetDictPageSize() {
    return base.getIcebergParquetDictPageSize();
  }

  @Override
  public String getIcebergTableLocation() {
    return base.getIcebergTableLocation();
  }

  @Override
  public List<IcebergPartitionSpec> getPartitionSpecs() {
    return base.getPartitionSpecs();
  }

  @Override
  public IcebergPartitionSpec getDefaultPartitionSpec() {
    return base.getDefaultPartitionSpec();
  }

  @Override
  public int getDefaultPartitionSpecId() {
    return base.getDefaultPartitionSpecId();
  }

  @Override
  public Schema getIcebergSchema() {
    return base.getIcebergSchema();
  }

  @Override
  public boolean isCacheable() {
    return base.isCacheable();
  }

  @Override
  public boolean isLocationCacheable() {
    return base.isLocationCacheable();
  }

  @Override
  public boolean isMarkedCached() {
    return base.isMarkedCached();
  }

  @Override
  public String getLocation() {
    return base.getLocation();
  }

  @Override
  public String getNullPartitionKeyValue() {
    return base.getNullPartitionKeyValue();
  }

  @Override
  public String getHdfsBaseDir() {
    return base.getHdfsBaseDir();
  }

  @Override
  public FileSystemUtil.FsType getFsType() {
    return base.getFsType();
  }

  @Override
  public long getTotalHdfsBytes() {
    return base.getTotalHdfsBytes();
  }

  @Override
  public boolean usesAvroSchemaOverride() {
    return base.usesAvroSchemaOverride();
  }

  @Override
  public Set<HdfsFileFormat> getFileFormats() {
    return base.getFileFormats();
  }

  @Override
  public boolean hasWriteAccessToBaseDir() {
    return base.hasWriteAccessToBaseDir();
  }

  @Override
  public String getFirstLocationWithoutWriteAccess() {
    return base.getFirstLocationWithoutWriteAccess();
  }

  @Override
  public TResultSet getTableStats() {
    return base.getTableStats();
  }

  @Override
  public Collection<? extends PrunablePartition> getPartitions() {
    return base.getPartitions();
  }

  @Override
  public Set<Long> getPartitionIds() {
    return base.getPartitionIds();
  }

  @Override
  public Map<Long, ? extends PrunablePartition> getPartitionMap() {
    return base.getPartitionMap();
  }

  @Override
  public TreeMap<LiteralExpr, Set<Long>> getPartitionValueMap(int col) {
    return base.getPartitionValueMap(col);
  }

  @Override
  public Set<Long> getNullPartitionIds(int colIdx) {
    return base.getNullPartitionIds(colIdx);
  }

  @Override
  public List<? extends FeFsPartition> loadPartitions(Collection<Long> ids) {
    return base.loadPartitions(ids);
  }

  @Override
  public SqlConstraints getSqlConstraints() {
    return base.getSqlConstraints();
  }

  @Override
  public ListMap<TNetworkAddress> getHostIndex() {
    return base.getHostIndex();
  }

  @Override
  public boolean isComputedPartitionColumn(Column col) {
    return base.isComputedPartitionColumn(col);
  }

  @Override
  public THdfsTable transformToTHdfsTable(boolean updatePartitionFlag,
      ThriftObjectType type) {
    return base.transformToTHdfsTable(updatePartitionFlag, type);
  }

  @Override
  public long snapshotId() {
    return base.snapshotId();
  }

  @Override
  public void setIcebergTableStats() {
    FeIcebergTable.super.setIcebergTableStats();
  }

  @Override
  public boolean isLoaded() {
    return base.isLoaded();
  }

  @Override
  public Table getMetaStoreTable() {
    return base.getMetaStoreTable();
  }

  @Override
  public String getStorageHandlerClassName() {
    return base.getStorageHandlerClassName();
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    return base.getCatalogObjectType();
  }

  @Override
  public String getName() {
    return base.getName();
  }

  @Override
  public String getFullName() {
    return base.getFullName();
  }

  @Override
  public TableName getTableName() {
    return base.getTableName();
  }

  @Override
  public TImpalaTableType getTableType() {
    return base.getTableType();
  }

  @Override
  public String getTableComment() {
    return base.getTableComment();
  }

  @Override
  public List<Column> getColumns() {
    return base.getColumns();
  }

  @Override
  public List<VirtualColumn> getVirtualColumns() {
    return base.getVirtualColumns();
  }

  @Override
  public List<Column> getColumnsInHiveOrder() {
    return base.getColumnsInHiveOrder();
  }

  @Override
  public List<String> getColumnNames() {
    return base.getColumnNames();
  }

  @Override
  public List<Column> getClusteringColumns() {
    return base.getClusteringColumns();
  }

  @Override
  public List<Column> getNonClusteringColumns() {
    return base.getNonClusteringColumns();
  }

  @Override
  public int getNumClusteringCols() {
    return base.getNumClusteringCols();
  }

  @Override
  public boolean isClusteringColumn(Column c) {
    return base.isClusteringColumn(c);
  }

  @Override
  public Column getColumn(String name) {
    return base.getColumn(name);
  }

  @Override
  public ArrayType getType() {
    return base.getType();
  }

  @Override
  public FeDb getDb() {
    return base.getDb();
  }

  @Override
  public long getNumRows() {
    return base.getNumRows();
  }

  @Override
  public TTableStats getTTableStats() {
    return base.getTTableStats();
  }

  @Override
  public TTableDescriptor toThriftDescriptor(
      int tableId, Set<Long> referencedPartitions) {
    return base.toThriftDescriptor(tableId, referencedPartitions);
  }

  @Override
  public long getWriteId() {
    return base.getWriteId();
  }

  @Override
  public ValidWriteIdList getValidWriteIds() {
    return base.getValidWriteIds();
  }

  @Override
  public String getOwnerUser() {
    return base.getOwnerUser();
  }
}
