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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.log4j.Logger;

import com.cloudera.impala.catalog.Catalog.TableNotFoundException;
import com.google.common.collect.Sets;
import com.google.common.collect.Lists;

/**
 * Internal representation of db-related metadata. Owned by Catalog instance.
 * Not thread safe.
 */
public class Db {
  private static final Logger LOG = Logger.getLogger(Db.class);

  private final String name;

  private final Catalog parentCatalog;
  private final HiveMetaStoreClient client;

  // map from table name to Table
  private final Map<String, Table> tables;

  // If true, table map values are populated lazily on read.
  final boolean lazy;

  private Table loadTable(String tableName) {
    try {
      return Table.load(parentCatalog.getNextTableId(), client, this, tableName);
    } catch (UnsupportedOperationException ex) {
      LOG.warn(ex);
    }

    return null;
  }

  /**
   * Loads all tables in the the table map, forcing removal of any tables that don't
   * load correctly.
   */
  private void forceLoadAllTables() {
    // Need to copy the keyset to avoid concurrent modification exceptions
    // if we try to remove a table in error
    Set<String> keys = Sets.newHashSet(tables.keySet());
    for (String s: keys) {
      tables.get(s);
    }
  }

  /**
   * Extends the usual HashMap to lazily load tables on read (through 'get').
   *
   * Differs from the usual behaviour of Map in that calling get(...) can
   * alter the key set if a lazy load fails (see comments on get, below). If this
   * map is exposed outside of the catalog, make sure to call forceLoadAllTables to
   * make this behave exactly like a usual Map.
   *
   * This class is not thread safe.
   */
  private class LazyTableMap extends HashMap<String, Table> {
    // Required because HashMap implements Serializable
    private static final long serialVersionUID = 1974243714395998559L;

    /**
     * If a table name is present in the map, but its associated value is null,
     * try and load the table from the metastore and populate the map. If loading
     * fails, the table name (the key) is removed from the map to avoid repeatedly
     * trying to load the table - future accesses of that key will still return
     * null.
     */
    @Override
    public Table get(Object key) {
      if (!super.containsKey(key)) {
        return null;
      }

      Table ret = super.get(key);
      if (ret != null) {
        // Already loaded
        return ret;
      }

      ret = loadTable((String)key);
      if (ret == null) {
        remove(key);
      } else {
        put((String)key, ret);
      }
      return ret;
    }
  }

  private Db(String name, Catalog catalog, HiveMetaStoreClient hiveClient,
      boolean lazy) {
    this.name = name;
    this.lazy = lazy;
    this.tables = new LazyTableMap();
    this.parentCatalog = catalog;
    this.client = hiveClient;
  }

  /**
   * Load the metadata of a Hive database into our own
   * in-memory metadata representation.
   * Ignore tables with columns of unsupported types (all complex types).
   *
   * @param client
   *          HiveMetaStoreClient to communicate with Metastore
   * @param dbName
   * @param lazy
   *          if true, tables themselves are loaded lazily on read, otherwise they are
   *          read eagerly in this method. The set of table names is always loaded.
   * @return non-null Db instance (possibly containing no tables)
   */
  public static Db loadDb(Catalog catalog, HiveMetaStoreClient client, String dbName,
      boolean lazy) {
    try {
      Db db = new Db(dbName, catalog, client, lazy);
      List<String> tblNames = null;
      tblNames = client.getTables(dbName, "*");
      for (String s: tblNames) {
        db.tables.put(s, null);
      }

      if (!lazy) {
        db.forceLoadAllTables();
      }

      return db;
    } catch (MetaException e) {
      // turn into unchecked exception
      throw new UnsupportedOperationException(e.toString());
    }
  }

  public String getName() {
    return name;
  }

  public Map<String, Table> getTables() {
    // We need to force all tables to be loaded at this point. Callers of this method
    // assume that only successfully loaded tables will appear in this map, but until
    // we try to load them, we can't be sure.
    if (lazy) {
      forceLoadAllTables();
    }
    return tables;
  }

  public List<String> getAllTableNames() {
    return Lists.newArrayList(tables.keySet());
  }

  /**
   * Case-insensitive lookup
   */
  public Table getTable(String tbl) {
    return tables.get(tbl.toLowerCase());
  }

  /**
   * Forces reload of named table on next access
   */
  public void invalidateTable(String table) throws TableNotFoundException {
    if (tables.containsKey(table)) {
      tables.put(table, null);
    } else {
      throw new TableNotFoundException("Could not invalidate non-existent table: "
          + table);
    }
  }
}
