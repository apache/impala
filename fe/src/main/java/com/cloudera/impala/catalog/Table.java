// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.catalog;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.planner.DataSink;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TTableDescriptor;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Base class for table metadata.
 *
 * This includes the concept of clustering columns, which are columns by which the table
 * data is physically clustered. In other words, if two rows share the same values
 * for the clustering columns, those two rows are most likely colocated. Note that this
 * is more general than Hive's CLUSTER BY ... INTO BUCKETS clause (which partitions
 * a key range into a fixed number of buckets).
 */
public abstract class Table {
  protected final TableId id;
  private final org.apache.hadoop.hive.metastore.api.Table msTable;
  protected final Db db;
  protected final String name;
  protected final String owner;

  /** Number of clustering columns. */
  protected int numClusteringCols;

  // estimated number of rows in table; -1: unknown
  protected long numRows = -1;

  /**
   * colsByPos[i] refers to the ith column in the table. The first numClusteringCols are
   * the clustering columns.
   */
  protected final ArrayList<Column> colsByPos;

  /**  map from lowercase col. name to Column */
  protected final Map<String, Column> colsByName;

  // The lastDdlTime recorded in the table parameter; -1 if not set
  protected final long lastDdlTime;

  // Set of supported table types.
  protected static EnumSet<TableType> SUPPORTED_TABLE_TYPES =
      EnumSet.of(TableType.EXTERNAL_TABLE, TableType.MANAGED_TABLE,
      TableType.VIRTUAL_VIEW);

  protected Table(TableId id, org.apache.hadoop.hive.metastore.api.Table msTable, Db db,
      String name, String owner) {
    this.id = id;
    this.msTable = msTable;
    this.db = db;
    this.name = name;
    this.owner = owner;
    this.colsByPos = Lists.newArrayList();
    this.colsByName = Maps.newHashMap();
    this.lastDdlTime = (msTable != null) ? Catalog.getLastDdlTime(msTable) : -1;
  }

  //number of nodes that contain data for this table; -1: unknown
  public abstract int getNumNodes();
  public abstract TTableDescriptor toThrift();
  /**
   * @return
   *         An output sink appropriate for writing to this table.
   */
  public abstract DataSink createDataSink(
      List<Expr> partitionKeyExprs, boolean overwrite);
  public abstract TCatalogObjectType getCatalogObjectType();

  /**
   * Populate members of 'this' from metastore info. Reuse metadata from oldValue if the
   * metadata is still valid.
   */
  public abstract void load(Table oldValue, HiveMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws TableLoadingException;

  public TableId getId() { return id; }
  public long getNumRows() { return numRows; }

  /**
   * Returns the metastore.api.Table object this Table was created from. Returns null
   * if the derived Table object was not created from a metastore Table (ex. InlineViews).
   */
  public org.apache.hadoop.hive.metastore.api.Table getMetaStoreTable() {
    return msTable;
  }

  /**
   * Creates the Impala representation of Hive/HBase metadata for one table.
   * Calls load() on the appropriate instance of Table subclass.
   * oldCacheEntry is the existing cache entry and might still contain valid info to help
   * speed up metadata loading. oldCacheEntry is null if there is no existing cache entry
   * (i.e. during fresh load).
   * @return new instance of HdfsTable or HBaseTable
   *         null if the table does not exist
   * @throws TableLoadingException if there was an error loading the table.
   * @throws TableNotFoundException if the table was not found
   */
  public static Table load(TableId id, HiveMetaStoreClient client, Db db,
      String tblName, Table oldCacheEntry) throws TableLoadingException,
      TableNotFoundException {
    // turn all exceptions into TableLoadingException
    try {
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          client.getTable(db.getName(), tblName);

      // Check that the Hive TableType is supported
      TableType tableType = TableType.valueOf(msTbl.getTableType());
      if (!SUPPORTED_TABLE_TYPES.contains(tableType)) {
        throw new TableLoadingException(String.format(
            "Unsupported table type '%s' for: %s.%s", tableType, db.getName(), tblName));
      }

      // Create a table of appropriate type and have it load itself
      Table table = fromMetastoreTable(id, client, db, msTbl);
      if (table == null) {
        throw new TableLoadingException(
            "Unrecognized table type for table: " + msTbl.getTableName());
      }
      table.load(oldCacheEntry, client, msTbl);
      return table;
    } catch (TableLoadingException e) {
      throw e;
    } catch (NoSuchObjectException e) {
      throw new TableNotFoundException("Table not found: " + tblName, e);
    } catch (Exception e) {
      throw new TableLoadingException(
          "Failed to load metadata for table: " + tblName, e);
    }
  }

  /**
   * Creates a table of the appropriate type based on the given hive.metastore.api.Table
   * object.
   */
  public static Table fromMetastoreTable(TableId id,
      HiveMetaStoreClient client, Db db,
      org.apache.hadoop.hive.metastore.api.Table msTbl) {
    // Create a table of appropriate type
    Table table = null;
    if (TableType.valueOf(msTbl.getTableType()) == TableType.VIRTUAL_VIEW) {
      table = new View(id, msTbl, db, msTbl.getTableName(), msTbl.getOwner());
    } else if (msTbl.getSd().getInputFormat().equals(HBaseTable.getInputFormat())) {
      table = new HBaseTable(id, msTbl, db, msTbl.getTableName(), msTbl.getOwner());
    } else if (HdfsFileFormat.isHdfsFormatClass(msTbl.getSd().getInputFormat())) {
      table = new HdfsTable(id, msTbl, db, msTbl.getTableName(), msTbl.getOwner());
    }
    return table;
  }

  protected static PrimitiveType getPrimitiveType(String typeName) {
    if (typeName.toLowerCase().equals("tinyint")) {
      return PrimitiveType.TINYINT;
    } else if (typeName.toLowerCase().equals("smallint")) {
      return PrimitiveType.SMALLINT;
    } else if (typeName.toLowerCase().equals("int")) {
      return PrimitiveType.INT;
    } else if (typeName.toLowerCase().equals("bigint")) {
      return PrimitiveType.BIGINT;
    } else if (typeName.toLowerCase().equals("boolean")) {
      return PrimitiveType.BOOLEAN;
    } else if (typeName.toLowerCase().equals("float")) {
      return PrimitiveType.FLOAT;
    } else if (typeName.toLowerCase().equals("double")) {
      return PrimitiveType.DOUBLE;
    } else if (typeName.toLowerCase().equals("date")) {
      return PrimitiveType.DATE;
    } else if (typeName.toLowerCase().equals("datetime")) {
      return PrimitiveType.DATETIME;
    } else if (typeName.toLowerCase().equals("timestamp")) {
      return PrimitiveType.TIMESTAMP;
    } else if (typeName.toLowerCase().equals("string")) {
      return PrimitiveType.STRING;
    } else if (typeName.toLowerCase().equals("binary")) {
      return PrimitiveType.BINARY;
    } else if (typeName.toLowerCase().equals("decimal")) {
      return PrimitiveType.DECIMAL;
    } else {
      return PrimitiveType.INVALID_TYPE;
    }
  }

  /**
   * Returns true if this table is not a base table that stores data (e.g., a view).
   * Virtual tables should not be added to the descriptor table sent to the BE, i.e.,
   * toThrift() should not work on virtual tables.
   */
  public boolean isVirtualTable() { return false; }
  public Db getDb() { return db; }
  public String getName() { return name; }
  public String getFullName() { return (db != null ? db.getName() + "." : "") + name; }
  public String getOwner() { return owner; }
  public ArrayList<Column> getColumns() { return colsByPos; }

  /**
   * Returns the list of all columns, but with partition columns at the end of
   * the list rather than the beginning. This is equivalent to the order in
   * which Hive enumerates columns.
   */
  public ArrayList<Column> getColumnsInHiveOrder() {
    ArrayList<Column> columns = Lists.newArrayList();
    for (Column column: colsByPos.subList(numClusteringCols, colsByPos.size())) {
      columns.add(column);
    }

    for (Column column: colsByPos.subList(0, numClusteringCols)) {
      columns.add(column);
    }

    return columns;
  }

  /**
   * Case-insensitive lookup.
   */
  public Column getColumn(String name) { return colsByName.get(name.toLowerCase()); }
  public int getNumClusteringCols() { return numClusteringCols; }
}
