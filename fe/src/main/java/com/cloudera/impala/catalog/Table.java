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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.catalog.Catalog.TableNotFoundException;
import com.cloudera.impala.catalog.Db.TableLoadingException;
import com.cloudera.impala.planner.DataSink;
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
  protected final Db db;
  protected final String name;
  protected final String owner;

  /** Number of clustering columns. */
  protected int numClusteringCols;

  /**
   * colsByPos[i] refers to the ith column in the table. The first numClusteringCols are
   * the clustering columns.
   */
  protected final ArrayList<Column> colsByPos;

  /**  map from lowercase col. name to Column */
  protected final Map<String, Column> colsByName;

  protected Table(TableId id, Db db, String name, String owner) {
    this.id = id;
    this.db = db;
    this.name = name;
    this.owner = owner;
    this.colsByPos = Lists.newArrayList();
    this.colsByName = Maps.newHashMap();
  }

  public TableId getId() {
    return id;
  }

  /**
   * Populate members of 'this' from metastore info.
   *
   * @param client
   * @param msTbl
   * @return
   *         this if successful
   *         null if loading table failed
   */
  public abstract Table load(
      HiveMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws TableLoadingException;


  /**
   * Creates the Impala representation of Hive/HBase metadata for one table.
   * Calls load() on the appropriate instance of Table subclass.
   * @return new instance of HdfsTable or HBaseTable
   *         null if the table does not exist
   * @throws TableLoadingException if there was an error loading the table.
   * @throws TableNotFoundException if the table was not found
   */
  public static Table load(TableId id, HiveMetaStoreClient client, Db db,
      String tblName) throws TableLoadingException, TableNotFoundException {
    // turn all exceptions into TableLoadingException
    try {
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          client.getTable(db.getName(), tblName);

      // Check that the Hive TableType is supported
      TableType tableType = TableType.valueOf(msTbl.getTableType());
      if (tableType != TableType.EXTERNAL_TABLE && tableType != TableType.MANAGED_TABLE) {
        throw new TableLoadingException(String.format(
            "Unsupported table type '%s' for: %s.%s", tableType, db.getName(), tblName));
      }

      // Determine the table type
      Table table = null;
      if (HBaseTable.isHBaseTable(msTbl)) {
        table = new HBaseTable(id, db, tblName, msTbl.getOwner());
      } else if (HdfsTable.isHdfsTable(msTbl)) {
        table = new HdfsTable(id, db, tblName, msTbl.getOwner());
      } else {
        throw new TableLoadingException("Unrecognized table type for table: " + tblName);
      }
      // Have the table load itself.
      return table.load(client, msTbl);
    } catch (TableLoadingException e) {
      throw e;
    } catch (NoSuchObjectException e) {
      throw new TableNotFoundException("Table not found: " + tblName, e);
    } catch (Exception e) {
      throw new TableLoadingException("Failed to load metadata for table: " + tblName, e);
    }
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

  public abstract TTableDescriptor toThrift();

  public Db getDb() {
    return db;
  }

  public String getName() {
    return name;
  }

  public String getFullName() {
    return (db != null ? db.getName() + "." : "") + name;
  }

  public String getOwner() {
    return owner;
  }

  public ArrayList<Column> getColumns() {
    return colsByPos;
  }

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
  public Column getColumn(String name) {
    return colsByName.get(name.toLowerCase());
  }

  public int getNumClusteringCols() {
    return numClusteringCols;
  }

  /**
   * @return
   *         An output sink appropriate for writing to this table.
   */
  public abstract DataSink createDataSink(
      List<Expr> partitionKeyExprs, boolean overwrite);
}
