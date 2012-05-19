// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

import com.google.common.collect.Maps;

/**
 * Interface to metadata stored in MetaStore instance.
 * Caches all db-, table- and column-related md during construction.
 *
 */
public class Catalog {
  public static final String DEFAULT_DB = "default";

  private int nextTableId;

  // map from db name to DB
  private final Map<String, Db> dbs;

  private final HiveMetaStoreClient msClient;

  public Catalog() {
    this(false);
  }

  /**
   * If lazy is true, tables are loaded on read, otherwise they are loaded eagerly in
   * the constructor.
   */
  public Catalog(boolean lazy) {
    this.nextTableId = 0;
    this.dbs = Maps.newHashMap();
    try {
      this.msClient = new HiveMetaStoreClient(new HiveConf(Catalog.class));
      List<String> msDbs = msClient.getAllDatabases();
      for (String dbName: msDbs) {
        Db db = Db.loadDb(this, msClient, dbName, lazy);
        dbs.put(dbName, db);
      }
    } catch (MetaException e) {
      // turn into unchecked exception
      throw new UnsupportedOperationException(e);
    }
  }

  public Collection<Db> getDbs() {
    return dbs.values();
  }

  public TableId getNextTableId() {
    return new TableId(nextTableId++);
  }

  /**
   * Case-insensitive lookup. Null and empty string is mapped onto
   * Hive's default db.
   */
  public Db getDb(String db) {
    if (db == null || db.isEmpty()) {
      return dbs.get(DEFAULT_DB);
    } else {
      return dbs.get(db.toLowerCase());
    }
  }

  public HiveMetaStoreClient getMetaStoreClient() {
    return msClient;
  }

  /**
   * Marks a table metadata as invalid, to be reloaded
   * the next time it is read.
   */
  public void invalidateTable(String tableName) throws TableNotFoundException {
    invalidateTable(DEFAULT_DB, tableName);
  }

  public static class TableNotFoundException extends Exception {
    private static final long serialVersionUID = -2203080667446640542L;

    public TableNotFoundException(String s) { super(s); }
  }

  public void invalidateTable(String dbName, String tableName)
      throws TableNotFoundException {
    Db db = getDb(dbName);
    // If no db known by that name, silently do nothing.
    if (db == null) {
      return;
    }

    db.invalidateTable(tableName);
  }
}
