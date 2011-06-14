// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.thrift.TException;

/**
 * Internal representation of table-related metadata. Owned by Catalog instance.
 */
public class Table {
  private final Db db;
  private final String name;
  private final String owner;
  private final ArrayList<Column> colsByPos;

  // map from lowercase col. name to Column
  private final Map<String, Column> colsByName;

  private Table(Db db, String name, String owner) {
    this.db = db;
    this.name = name;
    this.owner = owner;
    this.colsByPos = new ArrayList<Column>();
    this.colsByName = new HashMap<String, Column>();
  }

  public static Table loadTable(HiveMetaStoreClient client, Db db,
                                 String tblName) {
    // turn all exceptions into unchecked exception
    try {
      org.apache.hadoop.hive.metastore.api.Table msTbl = client.getTable(db.getName(), tblName);
      Table table = new Table(db, tblName, msTbl.getOwner());
      List<FieldSchema> fieldSchemas = client.getFields(db.getName(), tblName);
      int pos = 0;
      for (FieldSchema s : fieldSchemas) {
        Column col = new Column(s.getName(), getPrimitiveType(s.getType()), pos);
        table.colsByPos.add(col);
        table.colsByName.put(s.getName(), col);
        ++pos;
      }
      return table;
    } catch (TException e) {
      throw new UnsupportedOperationException(e.toString());
    } catch (NoSuchObjectException e) {
      throw new UnsupportedOperationException(e.toString());
    } catch (UnknownDBException e) {
      throw new UnsupportedOperationException(e.toString());
    } catch (MetaException e) {
      throw new UnsupportedOperationException(e.toString());
    } catch (UnknownTableException e) {
      throw new UnsupportedOperationException(e.toString());
    }
  }

  private static PrimitiveType getPrimitiveType(String typeName) {
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
