// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

import com.google.common.collect.Maps;

/**
 * Interface to metadata stored in MetaStore instance.
 * Caches all db-, table- and column-related md during construction.
 */
public class Catalog {
  // map from db name to DB
  private final Map<String, Db> dbs;

  public Catalog(HiveMetaStoreClient msClient) {
    this.dbs = Maps.newHashMap();
    try {
      List<String> msDbs = msClient.getAllDatabases();
      for (String dbName: msDbs) {
        Db db = Db.loadDb(msClient, dbName);
        dbs.put(dbName, db);
      }
    } catch (MetaException e) {
      // turn into unchecked exception
      throw new UnsupportedOperationException(e.toString());
    }
  }

  public Collection<Db> getDbs() {
    return dbs.values();
  }

  /**
   * Case-insensitive lookup. Null and empty string is mapped onto
   * Hive's default db.
   */
  public Db getDb(String db) {
    if (db == null || db.isEmpty()) {
      return dbs.get("default");
    } else {
      return dbs.get(db.toLowerCase());
    }
  }

}
