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
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.DataSourceTable;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.HBaseTable;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.IcebergColumn;
import org.apache.impala.catalog.IcebergStructField;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.SideloadTableStats;
import org.apache.impala.catalog.SqlConstraints;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.SystemTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.VirtualColumn;
import org.apache.impala.catalog.local.MetaProvider.TableMetaRef;
import org.apache.impala.common.Pair;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.service.MetadataOp;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TImpalaTableType;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.util.AcidUtils;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Table instance loaded from {@link LocalCatalog}.
 *
 * This class is not thread-safe. A new instance is created for
 * each catalog instance.
 */
abstract class LocalTable implements FeTable {
  private static final Logger LOG = LoggerFactory.getLogger(LocalTable.class);

  protected final LocalDb db_;
  /** The lower-case name of the table. */
  protected final String name_;
  protected final TImpalaTableType tableType_;
  protected final String tableComment_;

  private final ColumnMap cols_;

  protected final Table msTable_;

  private final TTableStats tableStats_;

  // Virtual columns of this table.
  protected final ArrayList<VirtualColumn> virtualCols_ = new ArrayList<>();

  // Test only stats that will be injected in place of stats obtained from HMS.
  protected SideloadTableStats testStats_ = null;

  // Scale factor to multiply table stats with. Only used for testing.
  protected double testMetadataScale_ = 1.0;

  /**
   * Table reference as provided by the initial call to the metadata provider.
   * This must be passed back to any further calls to the metadata provider
   * in order to verify consistency.
   *
   * In the case of CTAS target tables, this may be null. Since the tables don't
   * exist yet in any metadata storage, it would be invalid to try to load any metadata
   * about them.
   */
  @Nullable
  @VisibleForTesting
  protected final TableMetaRef ref_;

  public static LocalTable load(LocalDb db, String tblName) throws TableLoadingException {
    Pair<Table, TableMetaRef> tableMeta = loadTableMetadata(db, tblName);
    return load(db, tableMeta);
  }

  public static LocalTable load(LocalDb db, Pair<Table, TableMetaRef> tableMeta)
      throws TableLoadingException {
    // In order to know which kind of table subclass to instantiate, we need
    // to eagerly grab and parse the top-level Table object from the HMS.
    LocalTable t = null;
    Table msTbl = tableMeta.first;
    TableMetaRef ref = tableMeta.second;
    if (TableType.valueOf(msTbl.getTableType()) == TableType.VIRTUAL_VIEW) {
      t = new LocalView(db, msTbl, ref);
    } else if (HBaseTable.isHBaseTable(msTbl)) {
      t = LocalHbaseTable.loadFromHbase(db, msTbl, ref);
    } else if (KuduTable.isKuduTable(msTbl)) {
      t = LocalKuduTable.loadFromKudu(db, msTbl, ref);
    } else if (IcebergTable.isIcebergTable(msTbl)) {
      t = LocalIcebergTable.loadIcebergTableViaMetaProvider(db, msTbl, ref);
    } else if (DataSourceTable.isDataSourceTable(msTbl)) {
      t = LocalDataSourceTable.load(db, msTbl, ref);
    } else if (SystemTable.isSystemTable(msTbl)) {
      t = LocalSystemTable.load(db, msTbl, ref);
    } else if (HdfsFileFormat.isHdfsInputFormatClass(
        msTbl.getSd().getInputFormat())) {
      t = LocalFsTable.load(db, msTbl, ref);
    }

    if (t == null) {
      throw new LocalCatalogException("Unknown table type for table " +
          db.getName() + "." + msTbl.getTableName());
    }

    // TODO(todd): it would be preferable to only load stats for those columns
    // referenced in a query, but there doesn't seem to be a convenient spot
    // in between slot reference resolution and where the stats are needed.
    // So, for now, we'll just load all the column stats up front.
    t.loadColumnStats();
    return t;
  }


  /**
   * Load the Table instance from the metastore.
   */
  private static Pair<Table, TableMetaRef> loadTableMetadata(LocalDb db, String tblName)
      throws TableLoadingException {
    Preconditions.checkArgument(tblName.toLowerCase().equals(tblName));

    try {
      return db.getCatalog().getMetaProvider().loadTable(db.getName(), tblName);
    } catch (TException e) {
      throw new TableLoadingException(String.format(
          "Could not load table %s.%s from catalog",
          db.getName(), tblName), e);
    }
  }

  public LocalTable(LocalDb db, Table msTbl, TableMetaRef ref, ColumnMap cols) {
    this.db_ = Preconditions.checkNotNull(db);
    this.name_ = msTbl.getTableName();
    this.tableType_ = MetastoreShim.mapToInternalTableType(msTbl.getTableType());
    this.tableComment_ = MetadataOp.getTableComment(msTbl);
    this.cols_ = cols;
    this.ref_ = ref;
    this.msTable_ = msTbl;

    if (RuntimeEnv.INSTANCE.hasSideloadStats(db.getName(), name_)) {
      testStats_ = RuntimeEnv.INSTANCE.getSideloadStats(db.getName(), name_);
    }

    tableStats_ = new TTableStats(-1);
    long rowCount = FeCatalogUtils.getRowCount(msTable_.getParameters());
    if (testStats_ != null) {
      tableStats_.setTotal_file_bytes(testStats_.getTotalSize());
      testMetadataScale_ = (double) testStats_.getNumRows() / rowCount;
      rowCount = testStats_.getNumRows();
    } else {
      tableStats_.setTotal_file_bytes(
          FeCatalogUtils.getTotalSize(msTable_.getParameters()));
    }
    tableStats_.setNum_rows(rowCount);
  }

  public LocalTable(LocalDb db, Table msTbl, TableMetaRef ref) {
    this(db, msTbl, ref, ColumnMap.fromMsTable(msTbl));
  }

  protected LocalTable(LocalDb db, String tblName) {
    this.db_ = Preconditions.checkNotNull(db);
    this.name_ = tblName;
    this.tableType_ = TImpalaTableType.TABLE;
    this.tableComment_ = null;
    this.ref_ = null;
    this.msTable_ = null;
    this.cols_ = null;
    this.tableStats_ = null;
  }

  protected void addVirtualColumns(List<VirtualColumn> virtualColumns) {
    for (VirtualColumn virtCol : virtualColumns) addVirtualColumn(virtCol);
  }

  protected void addVirtualColumn(VirtualColumn col) {
    virtualCols_.add(col);
  }

  @Override
  public boolean isLoaded() {
    return true;
  }

  @Override
  public Table getMetaStoreTable() {
    return msTable_;
  }

  @Override
  public String getOwnerUser() {
    if (msTable_ == null) {
      LOG.warn("Owner of {} is unknown due to msTable is unloaded", getFullName());
      return null;
    }
    return msTable_.getOwnerType() == PrincipalType.USER ? msTable_.getOwner() : null;
  }

  @Override
  public String getStorageHandlerClassName() {
    // Subclasses should override as appropriate.
    return null;
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.TABLE;
  }

  @Override
  public String getName() {
    return name_;
  }

  @Override
  public String getFullName() {
    return db_.getName() + "." + name_;
  }

  @Override
  public TableName getTableName() {
    return new TableName(db_.getName(), name_);
  }

  @Override
  public TImpalaTableType getTableType() {
    return tableType_;
  }

  @Override
  public String getTableComment() {
    return tableComment_;
  }

  @Override
  public List<Column> getColumns() {
    return cols_ == null ? Collections.emptyList() : cols_.colsByPos_;
  }

  @Override
  public SqlConstraints getSqlConstraints() {
    return new SqlConstraints(new ArrayList<>(), new ArrayList<>());
  }

  @Override
  public List<Column> getColumnsInHiveOrder() {
    List<Column> columns = Lists.newArrayList(getNonClusteringColumns());
    columns.addAll(getClusteringColumns());
    return columns;
  }

  @Override
  public List<String> getColumnNames() {
    return cols_ == null ? Collections.emptyList() : cols_.getColumnNames();
  }

  @Override
  public List<Column> getClusteringColumns() {
    return cols_ == null ? Collections.emptyList() : cols_.getClusteringColumns();
  }

  @Override
  public List<Column> getNonClusteringColumns() {
    return cols_ == null ? Collections.emptyList() : cols_.getNonClusteringColumns();
  }

  @Override
  public List<VirtualColumn> getVirtualColumns() { return virtualCols_; }

  @Override
  public int getNumClusteringCols() {
    return cols_ == null ? 0 : cols_.getNumClusteringCols();
  }

  @Override
  public boolean isClusteringColumn(Column c) {
    return cols_ != null && cols_.isClusteringColumn(c);
  }

  @Override
  public Column getColumn(String name) {
    return cols_ == null ? null : cols_.getByName(name);
  }

  @Override
  public ArrayType getType() {
    return cols_ == null ? null : cols_.getType();
  }

  @Override
  public FeDb getDb() {
    return db_;
  }

  @Override
  public long getNumRows() {
    return tableStats_.num_rows;
  }

  @Override
  public TTableStats getTTableStats() {
    return tableStats_;
  }

  @Override
  public long getWriteId() {
    return -1l;
  }

  @Override
  public ValidWriteIdList getValidWriteIds() {
    return null;
  }

  protected void loadColumnStats() {
    try {
      List<ColumnStatisticsObj> stats = db_.getCatalog().getMetaProvider()
          .loadTableColumnStatistics(ref_, getColumnNames());
      FeCatalogUtils.injectColumnStats(stats, this, testStats_);
    } catch (TException e) {
      LOG.warn("Could not load column statistics for: " + getFullName(), e);
    }
  }

  protected double getDebugMetadataScale() { return testMetadataScale_; }

  protected static class ColumnMap {
    private final ArrayType type_;
    private final ImmutableList<Column> colsByPos_;
    private final ImmutableMap<String, Column> colsByName_;

    private final int numClusteringCols_;
    private final boolean hasRowIdCol_;

    public static ColumnMap fromMsTable(Table msTbl) {
      final String fullName = msTbl.getDbName() + "." + msTbl.getTableName();

      // The number of clustering columns is the number of partition keys.
      int numClusteringCols = msTbl.getPartitionKeys().size();
      // Add all columns to the table. Ordering is important: partition columns first,
      // then all other columns.
      List<Column> cols;
      try {
        cols = FeCatalogUtils.fieldSchemasToColumns(msTbl);
        boolean isFullAcidTable = AcidUtils.isFullAcidTable(msTbl.getParameters());
        return new ColumnMap(cols, numClusteringCols, fullName, isFullAcidTable);
      } catch (TableLoadingException e) {
        throw new LocalCatalogException(e);
      }
    }

    public ColumnMap(List<Column> cols, int numClusteringCols,
        String fullTableName, boolean isFullAcidSchema) {
      hasRowIdCol_ = isFullAcidSchema;
      this.colsByPos_ = ImmutableList.copyOf(cols);
      this.numClusteringCols_ = numClusteringCols;
      colsByName_ = indexColumnNames(colsByPos_);
      type_ = new ArrayType(columnsToStructType(colsByPos_));

      try {
        FeCatalogUtils.validateClusteringColumns(
            colsByPos_.subList(0, numClusteringCols_),
            fullTableName);
      } catch (TableLoadingException e) {
        throw new LocalCatalogException(e);
      }
    }

    public ArrayType getType() {
      return type_;
    }


    public Column getByName(String name) {
      return colsByName_.get(name.toLowerCase());
    }

    public int getNumClusteringCols() {
      return numClusteringCols_;
    }


    public List<Column> getNonClusteringColumns() {
      return colsByPos_.subList(numClusteringCols_ + (hasRowIdCol_ ? 1 : 0),
          colsByPos_.size());
    }

    public List<Column> getClusteringColumns() {
      return colsByPos_.subList(0, numClusteringCols_);
    }

    public List<String> getColumnNames() {
      return Column.toColumnNames(colsByPos_);
    }

    private static StructType columnsToStructType(List<Column> cols) {
      List<StructField> fields = Lists.newArrayListWithCapacity(cols.size());
      for (Column col : cols) {
        if (col instanceof IcebergColumn) {
          // Get 'IcebergStructField' for Iceberg tables.
          IcebergColumn iCol = (IcebergColumn) col;
          fields.add(new IcebergStructField(iCol.getName(), iCol.getType(),
              iCol.getComment(), iCol.getFieldId()));
        } else {
          fields.add(new StructField(col.getName(), col.getType(), col.getComment()));
        }
      }
      return new StructType(fields);
    }

    private static ImmutableMap<String, Column> indexColumnNames(List<Column> cols) {
      ImmutableMap.Builder<String, Column> builder = ImmutableMap.builder();
      for (Column col : cols) {
        builder.put(col.getName().toLowerCase(), col);
      }
      return builder.build();
    }

    boolean isClusteringColumn(Column c) {
      Preconditions.checkArgument(colsByPos_.get(c.getPosition()) == c);
      return c.getPosition() < numClusteringCols_;
    }
  }
}
