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
import java.util.List;

import javax.annotation.Nullable;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
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
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.local.MetaProvider.TableMetaRef;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TTableStats;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Table instance loaded from {@link LocalCatalog}.
 *
 * This class is not thread-safe. A new instance is created for
 * each catalog instance.
 */
abstract class LocalTable implements FeTable {
  private static final Logger LOG = Logger.getLogger(LocalTable.class);

  protected final LocalDb db_;
  /** The lower-case name of the table. */
  protected final String name_;

  private final ColumnMap cols_;

  protected final Table msTable_;

  private final TTableStats tableStats_;

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
  protected final TableMetaRef ref_;

  public static LocalTable load(LocalDb db, String tblName) {
    // In order to know which kind of table subclass to instantiate, we need
    // to eagerly grab and parse the top-level Table object from the HMS.
    LocalTable t = null;
    Pair<Table, TableMetaRef> tableMeta = loadTableMetadata(db, tblName);
    Table msTbl = tableMeta.first;
    TableMetaRef ref = tableMeta.second;
    if (TableType.valueOf(msTbl.getTableType()) == TableType.VIRTUAL_VIEW) {
      t = new LocalView(db, msTbl, ref);
    } else if (HBaseTable.isHBaseTable(msTbl)) {
      t = LocalHbaseTable.loadFromHbase(db, msTbl, ref);
    } else if (KuduTable.isKuduTable(msTbl)) {
      t = LocalKuduTable.loadFromKudu(db, msTbl, ref);
    } else if (DataSourceTable.isDataSourceTable(msTbl)) {
      // TODO(todd) support datasource table
    } else if (HdfsFileFormat.isHdfsInputFormatClass(
        msTbl.getSd().getInputFormat())) {
      t = LocalFsTable.load(db, msTbl, ref);
    }

    if (t == null) {
      throw new LocalCatalogException("Unknown table type for table " +
          db.getName() + "." + tblName);
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
  private static Pair<Table, TableMetaRef> loadTableMetadata(LocalDb db, String tblName) {
    Preconditions.checkArgument(tblName.toLowerCase().equals(tblName));

    try {
      return db.getCatalog().getMetaProvider().loadTable(db.getName(), tblName);
    } catch (TException e) {
      throw new LocalCatalogException(String.format(
          "Could not load table %s.%s from metastore",
          db.getName(), tblName), e);
    }
  }

  public LocalTable(LocalDb db, Table msTbl, TableMetaRef ref, ColumnMap cols) {
    this.db_ = Preconditions.checkNotNull(db);
    this.name_ = msTbl.getTableName();
    this.cols_ = cols;
    this.ref_ = ref;
    this.msTable_ = msTbl;

    tableStats_ = new TTableStats(
        FeCatalogUtils.getRowCount(msTable_.getParameters()));
    tableStats_.setTotal_file_bytes(
        FeCatalogUtils.getTotalSize(msTable_.getParameters()));
  }

  public LocalTable(LocalDb db, Table msTbl, TableMetaRef ref) {
    this(db, msTbl, ref, ColumnMap.fromMsTable(msTbl));
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
  public List<Column> getColumns() {
    // TODO(todd) why does this return ArrayList instead of List?
    return new ArrayList<>(cols_.colsByPos_);
  }

  @Override
  public List<Column> getColumnsInHiveOrder() {
    List<Column> columns = Lists.newArrayList(getNonClusteringColumns());
    columns.addAll(getClusteringColumns());
    return columns;
  }

  @Override
  public List<String> getColumnNames() {
    return cols_.getColumnNames();
  }

  @Override
  public List<Column> getClusteringColumns() {
    return cols_.getClusteringColumns();
  }

  @Override
  public List<Column> getNonClusteringColumns() {
    return cols_.getNonClusteringColumns();
  }

  @Override
  public int getNumClusteringCols() {
    return cols_.getNumClusteringCols();
  }

  @Override
  public boolean isClusteringColumn(Column c) {
    return cols_.isClusteringColumn(c);
  }

  @Override
  public Column getColumn(String name) {
    return cols_.getByName(name);
  }

  @Override
  public ArrayType getType() {
    return cols_.getType();
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
  public String getValidWriteIds() {
    return null;
  }

  protected void loadColumnStats() {
    try {
      List<ColumnStatisticsObj> stats = db_.getCatalog().getMetaProvider()
          .loadTableColumnStatistics(ref_, getColumnNames());
      FeCatalogUtils.injectColumnStats(stats, this);
    } catch (TException e) {
      LOG.warn("Could not load column statistics for: " + getFullName(), e);
    }
  }

  protected static class ColumnMap {
    private final ArrayType type_;
    private final ImmutableList<Column> colsByPos_;
    private final ImmutableMap<String, Column> colsByName_;

    private final int numClusteringCols_;

    public static ColumnMap fromMsTable(Table msTbl) {
      final String fullName = msTbl.getDbName() + "." + msTbl.getTableName();

      // The number of clustering columns is the number of partition keys.
      int numClusteringCols = msTbl.getPartitionKeys().size();
      // Add all columns to the table. Ordering is important: partition columns first,
      // then all other columns.
      List<Column> cols;
      try {
        cols = FeCatalogUtils.fieldSchemasToColumns(
            Iterables.concat(msTbl.getPartitionKeys(),
                             msTbl.getSd().getCols()),
            msTbl.getTableName());
        return new ColumnMap(cols, numClusteringCols, fullName);
      } catch (TableLoadingException e) {
        throw new LocalCatalogException(e);
      }
    }

    public ColumnMap(List<Column> cols, int numClusteringCols,
        String fullTableName) {
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
      return colsByPos_.subList(numClusteringCols_, colsByPos_.size());
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
        fields.add(new StructField(col.getName(), col.getType(), col.getComment()));
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
