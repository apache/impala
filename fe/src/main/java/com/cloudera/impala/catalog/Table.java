// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Base class for table metadata.
 * Current subclasses are HdfsTable and HBaseTable.
 *
 */
public abstract class Table {
  protected final Db db;
  protected final String name;
  protected final String owner;

  /** for concrete type HdfsTable the first 'numPartitionKeys' cols are partition keys */
  protected final ArrayList<Column> colsByPos;

  /**  map from lowercase col. name to Column */
  protected final Map<String, Column> colsByName;

  protected Table(Db db, String name, String owner) {
    this.db = db;
    this.name = name;
    this.owner = owner;
    this.colsByPos = Lists.newArrayList();
    this.colsByName = Maps.newHashMap();
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
  public abstract Table load(HiveMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl);


  /**
   * Creates the Impala representation of Hive metadata for one table.
   * Distinguishes between HdfsTables and HBaseTables.
   * Calls load() on the appropriate instance of Table subclass.
   * @param client
   * @param db
   * @param tblName
   * @return
   *         new instance of HdfsTable or HBaseTable
   *         null if loading table failed
   */
  public static Table load(HiveMetaStoreClient client, Db db,
      String tblName) {
    // turn all exceptions into unchecked exception
    try {
      org.apache.hadoop.hive.metastore.api.Table msTbl = client.getTable(db.getName(), tblName);

      // Hdfs table or HBase table?
      Table table = null;
      if (msTbl.getTableType().equals("EXTERNAL_TABLE") &&
          msTbl.getSd().getInputFormat().equals(HBaseTable.hbaseInputFormat)) {
        table = new HBaseTable(db, tblName, msTbl.getOwner());
      } else {
        table = new HdfsTable(db, tblName, msTbl.getOwner());
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

  public Db getDb() {
    return db;
  }

  public String getName() {
    return name;
  }

  public String getFullName() {
    return db.getName() + "." + name;
  }

  public String getOwner() {
    return owner;
  }

  public List<Column> getColumns() {
    return colsByPos;
  }

  /**
   * Case-insensitive lookup.
   */
  public Column getColumn(String name) {
    return colsByName.get(name.toLowerCase());
  }
}
