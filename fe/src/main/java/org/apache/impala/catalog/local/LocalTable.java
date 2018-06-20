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

import javax.annotation.concurrent.Immutable;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.TableLoadingException;
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
  protected final SchemaInfo schemaInfo_;

  public static LocalTable load(LocalDb db, String tblName) {
    // In order to know which kind of table subclass to instantiate, we need
    // to eagerly grab and parse the top-level Table object from the HMS.
    SchemaInfo schemaInfo = SchemaInfo.load(db, tblName);
    LocalTable t;
    if (HdfsFileFormat.isHdfsInputFormatClass(
        schemaInfo.msTable_.getSd().getInputFormat())) {
      t = new LocalFsTable(db, tblName, schemaInfo);
    } else {
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
  public LocalTable(LocalDb db, String tblName, SchemaInfo schemaInfo) {
    this.db_ = Preconditions.checkNotNull(db);
    this.name_ = Preconditions.checkNotNull(tblName);
    this.schemaInfo_ = Preconditions.checkNotNull(schemaInfo);
    Preconditions.checkArgument(tblName.toLowerCase().equals(tblName));
  }

  @Override
  public boolean isLoaded() {
    return true;
  }

  @Override
  public Table getMetaStoreTable() {
    return schemaInfo_.msTable_;
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
  public ArrayList<Column> getColumns() {
    // TODO(todd) why does this return ArrayList instead of List?
    return new ArrayList<>(schemaInfo_.colsByPos_);
  }

  @Override
  public List<Column> getColumnsInHiveOrder() {
    ArrayList<Column> columns = Lists.newArrayList(getNonClusteringColumns());
    columns.addAll(getClusteringColumns());
    return columns;
  }

  @Override
  public List<String> getColumnNames() {
    return Column.toColumnNames(schemaInfo_.colsByPos_);
  }

  @Override
  public List<Column> getClusteringColumns() {
    return ImmutableList.copyOf(
        schemaInfo_.colsByPos_.subList(0, schemaInfo_.numClusteringCols_));
  }

  @Override
  public List<Column> getNonClusteringColumns() {
    return ImmutableList.copyOf(schemaInfo_.colsByPos_.subList(
        schemaInfo_.numClusteringCols_,
        schemaInfo_.colsByPos_.size()));
  }

  @Override
  public int getNumClusteringCols() {
    return schemaInfo_.numClusteringCols_;
  }

  @Override
  public boolean isClusteringColumn(Column c) {
    return schemaInfo_.isClusteringColumn(c);
  }

  @Override
  public Column getColumn(String name) {
    return schemaInfo_.colsByName_.get(name.toLowerCase());
  }

  @Override
  public ArrayType getType() {
    return schemaInfo_.type_;
  }

  @Override
  public FeDb getDb() {
    return db_;
  }

  @Override
  public long getNumRows() {
    return schemaInfo_.tableStats_.num_rows;
  }

  @Override
  public TTableStats getTTableStats() {
    return schemaInfo_.tableStats_;
  }

  protected void loadColumnStats() {
    try {
      List<ColumnStatisticsObj> stats = db_.getCatalog().getMetaProvider()
          .loadTableColumnStatistics(db_.getName(), getName(), getColumnNames());
      FeCatalogUtils.injectColumnStats(stats, this);
    } catch (TException e) {
      LOG.warn("Could not load column statistics for: " + getFullName(), e);
    }
  }

  /**
   * The table schema, loaded from the HMS Table object. This is common
   * to all Table implementations and includes the column definitions and
   * table-level stats.
   *
   * TODO(todd): some of this code is lifted from 'Table' and, with some
   * effort, could be refactored to avoid duplication.
   */
  @Immutable
  protected static class SchemaInfo {
    private final Table msTable_;

    private final ArrayType type_;
    private final ImmutableList<Column> colsByPos_;
    private final ImmutableMap<String, Column> colsByName_;

    private final int numClusteringCols_;
    private final String nullColumnValue_;

    private final TTableStats tableStats_;

    /**
     * Load the schema info from the metastore.
     */
    static SchemaInfo load(LocalDb db, String tblName) {
      try {
        Table msTbl = db.getCatalog().getMetaProvider().loadTable(
            db.getName(), tblName);
        return new SchemaInfo(msTbl);
      } catch (TException e) {
        throw new LocalCatalogException(String.format(
            "Could not load table %s.%s from metastore",
            db.getName(), tblName), e);
      } catch (TableLoadingException e) {
        // In this case, the exception message already has the table name
        // in the exception message.
        throw new LocalCatalogException(e);
      }
    }

    SchemaInfo(Table msTbl) throws TableLoadingException {
      msTable_ = msTbl;
      // set NULL indicator string from table properties
      String tableNullFormat =
          msTbl.getParameters().get(serdeConstants.SERIALIZATION_NULL_FORMAT);
      nullColumnValue_ = tableNullFormat != null ? tableNullFormat :
          FeFsTable.DEFAULT_NULL_COLUMN_VALUE;

      final String fullName = msTbl.getDbName() + "." + msTbl.getTableName();

      // The number of clustering columns is the number of partition keys.
      numClusteringCols_ = msTbl.getPartitionKeys().size();
      // Add all columns to the table. Ordering is important: partition columns first,
      // then all other columns.
      colsByPos_ = FeCatalogUtils.fieldSchemasToColumns(
          Iterables.concat(msTbl.getPartitionKeys(),
                           msTbl.getSd().getCols()),
          fullName);
      FeCatalogUtils.validateClusteringColumns(
          colsByPos_.subList(0, numClusteringCols_), fullName);
      colsByName_ = indexColumnNames(colsByPos_);
      type_ = new ArrayType(columnsToStructType(colsByPos_));

      tableStats_ = new TTableStats(
          FeCatalogUtils.getRowCount(msTable_.getParameters()));
      tableStats_.setTotal_file_bytes(
          FeCatalogUtils.getTotalSize(msTable_.getParameters()));
    }

    private static StructType columnsToStructType(List<Column> cols) {
      ArrayList<StructField> fields = Lists.newArrayListWithCapacity(cols.size());
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

    private boolean isClusteringColumn(Column c) {
      Preconditions.checkArgument(colsByPos_.get(c.getPosition()) == c);
      return c.getPosition() < numClusteringCols_;
    }

    protected String getNullColumnValue() {
      return nullColumnValue_;
    }
  }
}
