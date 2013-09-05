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

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.log4j.Logger;

import com.cloudera.impala.analysis.FunctionName;
import com.cloudera.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

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
  private static final Object tableMapCreationLock = new Object();
  private final String name;
  private final Catalog parentCatalog;

  // All of the registered user functions. The key is the user facing name (e.g. "myUdf"),
  // and the values are all the overloaded variants (e.g. myUdf(double), myUdf(string))
  private HashMap<String, List<Udf>> udfs;

  // Table metadata cache.
  private final CatalogObjectCache<Table> tableCache = new CatalogObjectCache<Table>(
      new CacheLoader<String, Table>() {
        @Override
        public Table load(String tableName) throws TableNotFoundException,
            TableLoadingException {
          return loadTable(tableName, null);
        }

        @Override
        public ListenableFuture<Table> reload(String tableName, Table oldValue)
            throws ImpalaException {
          SettableFuture<Table> newValue = SettableFuture.create();
          try {
            newValue.set(loadTable(tableName, oldValue));
          } catch (ImpalaException e) {
            // Invalidate the table metadata if load fails.
            Db.this.invalidateTable(tableName);
            throw e;
          }
          return newValue;
        }
      });

  private Table loadTable(String tableName, Table oldValue) throws TableLoadingException,
      TableNotFoundException {
    tableName = tableName.toLowerCase();
    MetaStoreClient msClient = parentCatalog.getMetaStoreClient();
    try {
      // Try to load the table Metadata
      return Table.load(parentCatalog.getNextTableId(), msClient.getHiveClient(),
          this, tableName, oldValue);
    } finally {
      msClient.release();
    }
  }

  /**
   * Loads all tables in the the table map, ignoring any tables that don't load
   * correctly.
   */
  private void forceLoadAllTables() {
    for (String tableName: getAllTableNames()) {
      try {
        tableCache.get(tableName);
      } catch (Exception ex) {
        LOG.warn("Ignoring table: " + tableName + " due to error when loading", ex);
      }
    }
  }

  private Db(String name, Catalog catalog, HiveMetaStoreClient hiveClient)
      throws MetaException {
    this.name = name;
    this.parentCatalog = catalog;
    // Need to serialize calls to getAllTables() due to HIVE-3521
    synchronized (tableMapCreationLock) {
      tableCache.add(hiveClient.getAllTables(name));
    }

    loadUdfs();
  }

  private void loadUdfs() {
    udfs = new HashMap<String, List<Udf>>();
    // TODO: figure out how to persist udfs.
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
      Db db = new Db(dbName, catalog, client);
      // Load all the table metadata
      if (!lazy) db.forceLoadAllTables();
      return db;
    } catch (MetaException e) {
      // turn into unchecked exception
      throw new IllegalStateException(e);
    }
  }

  public String getName() { return name; }
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.DATABASE;
  }

  public List<String> getAllTableNames() {
    return Lists.newArrayList(tableCache.getAllNames());
  }

  /**
   * Case-insensitive lookup. Returns null if a table does not exist, throws an
   * exception if the table metadata could not be loaded.
   */
  public Table getTable(String tbl) throws TableLoadingException {
    try {
      return tableCache.get(tbl);
    } catch (TableNotFoundException e) {
      return null;
    } catch (TableLoadingException e) {
      throw e;
    } catch (ImpalaException e) {
      throw new IllegalStateException(e);
    }
  }

  public boolean containsTable(String tableName) {
    return tableCache.contains(tableName);
  }

  /**
   * Adds a table to the table list. Table cache will be populated on the next
   * getTable().
   */
  public void addTable(String tableName) {
    tableCache.add(tableName);
  }

  /**
   * Removes the table name and any cached metadata from the Table cache.
   */
  public void removeTable(String tableName) {
    tableCache.remove(tableName);
  }

  /**
   * Refresh the metadata for the given table name if the table already exists in the
   * cache, or load the table metadata if the table has not been loaded.
   * If refreshing the table metadata failed, no exception will be thrown but the
   * existing metadata will be invalidated.
   */
  public void refreshTable(String tableName) {
    tableCache.refresh(tableName);
  }

  /**
   * Marks the table as invalid so the next access will trigger a metadata load.
   */
  public void invalidateTable(String tableName) {
    tableCache.invalidate(tableName);
  }

  /**
   * Returns all the UDFs in this DB.
   */
  public List<String> getAllUdfs() {
    List<String> names = Lists.newArrayList();
    synchronized (udfs) {
      for (List<Udf> functions: udfs.values()) {
        for (Udf f: functions) {
          names.add(f.signatureString());
        }
      }
    }
    return names;
  }

  /**
   * Returns the number of udfs in this database.
   */
  public int numUdfs() {
    synchronized (udfs) {
      return udfs.size();
    }
  }

  /**
   * See comment in Catalog.
   */
  public boolean udfExists(FunctionName name) {
    synchronized (udfs) {
      List<Udf> functions = udfs.get(name.getFunction());
      return functions != null;
    }
  }

  /*
   * See comment in Catalog.
   */
  public Udf getUdf(Function desc, boolean exactMatch) {
    synchronized (udfs) {
      List<Udf> functions = udfs.get(desc.functionName());
      if (functions == null) return null;

      for (Udf f: functions) {
        if (desc.equals(f)) return f;
      }
      if (exactMatch) return null;

      for (Udf f: functions) {
        if (f.isSupertype(desc)) return f;
      }
    }
    return null;
  }

  /**
   * See comment in Catalog.
   */
  public boolean addUdf(Udf udf) {
    // TODO: add this to persistent store
    synchronized (udfs) {
      Udf fn = getUdf(udf, true);
      if (fn != null) return false;
      List<Udf> functions = udfs.get(udf.functionName());
      if (functions == null) {
        functions = Lists.newArrayList();
        udfs.put(udf.functionName(), functions);
      }
      functions.add(udf);
    }
    return true;
  }

  /**
   * See comment in Catalog.
   */
  public boolean removeUdf(Function desc) {
    // TODO: remove this from persistent store.
    synchronized (udfs) {
      Udf udf = getUdf(desc, true);
      if (udf == null) return false;
      List<Udf> functions = udfs.get(desc.functionName());
      Preconditions.checkNotNull(functions);
      boolean exists = functions.remove(udf);
      if (functions.isEmpty()) {
        udfs.remove(desc.functionName());
      }
      return exists;
    }
  }
}
