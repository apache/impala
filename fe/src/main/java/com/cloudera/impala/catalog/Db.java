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
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.log4j.Logger;

import com.cloudera.impala.catalog.Catalog.TableNotFoundException;
import com.google.common.collect.Sets;
import com.google.common.collect.Maps;
import com.google.common.collect.Lists;

import com.cloudera.impala.common.ImpalaException;

/**
 * Internal representation of db-related metadata. Owned by Catalog instance.
 * Not thread safe.
 *
 * The static initialisation method loadDb is the only way to construct a Db
 * object.
 *
 * Tables are stored in a map from the table name to the table object. They may
 * be loaded 'eagerly' at construction or 'lazily' on first reference. 
 * Tables are accessed via getTable which may trigger a metadata read in two cases:
 *  * if the table has never been loaded
 *  * if the table loading failed on the previous attempt
 */
public class Db {
  private static final Logger LOG = Logger.getLogger(Db.class);

  private final String name;

  private final Catalog parentCatalog;
  private final HiveMetaStoreClient client;

  // map from table name to Table
  private final LazyTableMap tables;

  // If true, table map values are populated lazily on read.
  final boolean lazy;

  /**
   * Thrown when a table cannot be loaded due to an error. 
   */
  public static class TableLoadingException extends ImpalaException {
    public TableLoadingException(String s, Throwable cause) {
      super(s, cause);
    }

    public TableLoadingException(String s) {
      super(s);
    }
  };

  /**
   * Loads all tables in the the table map, ignoring any tables that don't load
   * correctly.
   */
  private void forceLoadAllTables() {
    // Need to copy the keyset to avoid concurrent modification exceptions
    // if we try to remove a table in error
    Set<String> keys = Sets.newHashSet(tables.keySet());
    for (String s: keys) {
      try {
        tables.get(s);
      } catch (Exception ex) {
        LOG.warn("Ignoring table: " + s + " due to error when loading", ex);
      }
    }
  }

  /**
   * Lazily loads tables on read (through 'get'). 
   *
   * From the caller's perspective, a table never has a null value; either the
   * table is successfully loaded or an exception is thrown by
   * get(). Internally, however, backingMap tracks all table names and those
   * that have not yet been loaded successfully have a null value in that map.
   *
   * If a table has not yet been loaded successfully, get() will attempt to load it. 
   *
   * Tables may be invalidated, which means the next get() will reload the table metadata.
   *
   * This class is not thread safe.
   */
  private class LazyTableMap {
    private final Map<String, Table> backingMap = Maps.newHashMap();
    
    /**
     * Resets the table for the given table name to null, effectively removing
     * it from the cache. If the table is not in the map, nothing is done and
     * false is returned, otherwise returns true.
     */
    public boolean invalidate(String name) {
      if (containsKey(name)) {
        put(name, null);
        return true;
      }
      return false;
    }

    public void put(String name, Table table) {
      backingMap.put(name, table);
    }

    public Set<String> keySet() {
      return backingMap.keySet();
    }

    public boolean containsKey(String key) {
      return backingMap.containsKey(key);
    }

    /**
     * Returns the table object corresponding to the supplied table name. 
     * If a table name is present in the map, but its associated value is null,
     * try and load the table from the metastore and populate the map. 
     * Throws an exception if the table metadata cannot be loaded.
     */
    public Table get(String tableName) throws TableLoadingException {
      if (!backingMap.containsKey(tableName)) {
        return null;
      }

      Table ret = backingMap.get(tableName);
      if (ret != null) {
        // Already loaded
        return ret;
      }

      // May throw TableLoadingException
      ret = Table.load(parentCatalog.getNextTableId(), client, Db.this, tableName);
      put(tableName, ret);
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
   * Load the metadata of a Hive database into our own in-memory metadata
   * representation.  Ignore tables with columns of unsupported types (all
   * complex types). Throws an exception if there is an error communicating with
   * the metastore.
   *
   * @param client
   *          HiveMetaStoreClient to communicate with Metastore
   * @param dbName
   * @param lazy
   *          if true, tables themselves are loaded lazily on read, otherwise
   *          they are read eagerly in this method. The set of table names is
   *          always loaded. If false - meaning all tables are read - malformed
   *          tables that do not load are logged and ignored with no exception
   *          thrown.
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
      throw new UnsupportedOperationException(e);
    }
  }

  public String getName() {
    return name;
  }

  public List<String> getAllTableNames() {
    return Lists.newArrayList(tables.keySet());
  }

  /**
   * Case-insensitive lookup. Returns null if a table does not exist, throws an
   * exception if the table metadata could not be loaded.
   */
  public Table getTable(String tbl) throws TableLoadingException {
    return tables.get(tbl.toLowerCase());
  }

  /**
   * Forces reload of named table on next access. Throws TableNotFoundException
   * if the requested table can't be found.
   */
  public void invalidateTable(String table) throws TableNotFoundException {
    if (!tables.invalidate(table)) {
      throw new TableNotFoundException("Could not invalidate non-existent table: "
          + table);
    }
  }
}
