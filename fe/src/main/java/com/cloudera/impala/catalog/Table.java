// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TException;

import com.cloudera.impala.analysis.Expr;
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
      org.apache.hadoop.hive.metastore.api.Table msTbl);


  /**
   * Creates the Impala representation of Hive/HBase metadata for one table.
   * Calls load() on the appropriate instance of Table subclass.
   * @param client
   * @param db
   * @param tblName
   * @return
   *         new instance of HdfsTable or HBaseTable
   *         null if loading table failed
   */
  public static Table load(TableId id, HiveMetaStoreClient client, Db db,
                           String tblName) {
    // turn all exceptions into unchecked exception
    try {
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          client.getTable(db.getName(), tblName);

      // Determine the table type
      Table table = null;
      if (HBaseTable.isHBaseTable(msTbl)) {
        table = new HBaseTable(id, db, tblName, msTbl.getOwner());
      } else if (HdfsTable.isHdfsTable(msTbl)) {
        table = new HdfsTable(id, db, tblName, msTbl.getOwner());
      } else {
        throw new UnsupportedOperationException("Unrecognized table type");
      }
      // Have the table load itself.
      return table.load(client, msTbl);
    } catch (TException e) {
      throw new UnsupportedOperationException(e.toString());
    } catch (NoSuchObjectException e) {
      throw new UnsupportedOperationException(e.toString());
    } catch (MetaException e) {
      throw new UnsupportedOperationException(e.toString());
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
