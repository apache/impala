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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.thrift.TCatalog;
import com.cloudera.impala.thrift.TCatalogObject;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TGetAllCatalogObjectsResponse;
import com.cloudera.impala.thrift.TTable;
import com.cloudera.impala.thrift.TTableName;
import com.cloudera.impala.thrift.TUniqueId;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Specialized Catalog that implements the CatalogService specific Catalog
 * APIs. The CatalogServiceCatalog manages loading of all the catalog metadata
 * and processing of DDL requests. For each DDL request, the CatalogServiceCatalog
 * will return the catalog version that the update will show up in. The client
 * can then wait until the statestore sends an update that contains that catalog
 * version.
 */
public class CatalogServiceCatalog extends Catalog {
  private static final Logger LOG = Logger.getLogger(CatalogServiceCatalog.class);
  private final TUniqueId catalogServiceId_;

  // Last assigned catalog version. Starts at INITIAL_CATALOG_VERSION and is incremented
  // with each update to the Catalog. Continued across the lifetime of the process.
  // Atomic to ensure versions are always sequentially increasing, even when updated
  // from different threads.
  // TODO: This probably doesn't need to be atomic and can be updated while holding
  // the catalogLock_.
  private final static AtomicLong catalogVersion_ =
      new AtomicLong(INITIAL_CATALOG_VERSION);

  /**
   * Initialize the CatalogServiceCatalog, loading all table metadata
   * lazily.
   * TODO: Support background loading of the table metadata.
   */
  public CatalogServiceCatalog(TUniqueId catalogServiceId) {
    this(catalogServiceId, CatalogInitStrategy.LAZY);
  }

  /**
   * Constructor used to speed up testing by allowing for lazily loading
   * the Catalog metadata.
   */
  public CatalogServiceCatalog(TUniqueId catalogServiceId,
      CatalogInitStrategy initStrategy) {
    super(initStrategy);
    catalogServiceId_ = catalogServiceId;
  }

  /**
   * Returns all known objects in the Catalog (Tables, Views, Databases, and
   * Functions). Some metadata may be skipped for objects that have a catalog
   * version < the specified "fromVersion".
   */
  public TGetAllCatalogObjectsResponse getCatalogObjects(long fromVersion) {
    TGetAllCatalogObjectsResponse resp = new TGetAllCatalogObjectsResponse();
    resp.setObjects(new ArrayList<TCatalogObject>());
    resp.setMax_catalog_version(Catalog.INITIAL_CATALOG_VERSION);

    // Take a lock on the catalog to ensure this update contains a consistent snapshot
    // of all items in the catalog.
    catalogLock_.readLock().lock();
    try {
      for (String dbName: getDbNames(null)) {
        Db db = getDb(dbName);
        if (db == null) {
          LOG.error("Database: " + dbName + " was expected to be in the catalog " +
              "cache. Skipping database and all child objects for this update.");
          continue;
        }
        TCatalogObject catalogDb = new TCatalogObject(TCatalogObjectType.DATABASE,
            db.getCatalogVersion());
        catalogDb.setDb(db.toThrift());
        resp.addToObjects(catalogDb);

        for (String tblName: db.getAllTableNames()) {
          TCatalogObject catalogTbl = new TCatalogObject(TCatalogObjectType.TABLE,
              Catalog.INITIAL_CATALOG_VERSION);
          Table tbl = db.getTable(tblName);
          if (tbl == null) {
            LOG.error("Table: " + tblName + " was expected to be in the catalog " +
                "cache. Skipping table for this update.");
            continue;
          }

          // Only add the extended metadata if this table's version is >=
          // the fromVersion.
          if (tbl.getCatalogVersion() >= fromVersion) {
            try {
              catalogTbl.setTable(tbl.toThrift());
            } catch (Exception e) {
              LOG.debug(String.format("Error calling toThrift() on table %s.%s: %s",
                  dbName, tblName, e.getMessage()), e);
              continue;
            }
            catalogTbl.setCatalog_version(tbl.getCatalogVersion());
          } else {
            catalogTbl.setTable(new TTable(dbName, tblName));
          }
          resp.addToObjects(catalogTbl);
        }

        for (String signature: db.getAllFunctionSignatures(null)) {
          Function fn = db.getFunction(signature);
          if (fn == null) continue;
          TCatalogObject function = new TCatalogObject(TCatalogObjectType.FUNCTION,
              fn.getCatalogVersion());
          function.setType(TCatalogObjectType.FUNCTION);
          function.setFn(fn.toThrift());
          resp.addToObjects(function);
        }
      }

      // Each update should contain a single "TCatalog" object which is used to
      // pass overall state on the catalog, such as the current version and the
      // catalog service id.
      TCatalogObject catalog = new TCatalogObject();
      catalog.setType(TCatalogObjectType.CATALOG);
      // By setting the catalog version to the latest catalog version at this point,
      // it ensure impalads will always bump their versions, even in the case where
      // an object has been dropped.
      catalog.setCatalog_version(CatalogServiceCatalog.getCatalogVersion());
      catalog.setCatalog(new TCatalog(catalogServiceId_));
      resp.addToObjects(catalog);

      // The max version is the max catalog version of all items in the update.
      resp.setMax_catalog_version(CatalogServiceCatalog.getCatalogVersion());
      return resp;
    } finally {
      catalogLock_.readLock().unlock();
    }
  }

  /**
   * Returns all user defined functions (aggregate and scalar) in the specified database.
   * Functions are not returned in a defined order.
   */
  public List<Function> getFunctions(String dbName) throws DatabaseNotFoundException {
    catalogLock_.readLock().lock();
    try {
      Db db = getDb(dbName);
      if (db == null) {
        throw new DatabaseNotFoundException("Database does not exist: " + dbName);
      }

      // Contains map of overloaded function names to all functions matching that name.
      HashMap<String, List<Function>> dbFns = db.getAllFunctions();
      List<Function> fns = new ArrayList<Function>(dbFns.size());
      for (List<Function> fnOverloads: dbFns.values()) {
        for (Function fn: fnOverloads) {
          fns.add(fn);
        }
      }
      return fns;
    } finally {
      catalogLock_.readLock().unlock();
    }
  }

  @Override
  public long reset() {
    catalogLock_.writeLock().lock();
    try {
      // Since UDFs/UDAs are not persisted in the metastore, we won't clear
      // them across reset. To do this, we store all the functions before
      // clearing and restore them after.
      // TODO: Everything about this. Persist them.
      List<Pair<String, HashMap<String, List<Function>>>> functions =
          Lists.newArrayList();
      for (Db db: dbCache_.values()) {
        if (db.numFunctions() == 0) continue;
        functions.add(Pair.create(db.getName(), db.getAllFunctions()));
      }

      // Reset the dbs.
      resetInternal();

      // Restore UDFs/UDAs.
      for (Pair<String, HashMap<String, List<Function>>> dbFns: functions) {
        Db db = null;
        try {
          db = dbCache_.get(dbFns.first);
        } catch (Exception e) {
          continue;
        }
        if (db == null) {
          // DB no longer exists.
          // TODO: We could restore this DB and then add the functions back.
          continue;
        }

        for (List<Function> fns: dbFns.second.values()) {
          for (Function fn: fns) {
            fn.setCatalogVersion(CatalogServiceCatalog.incrementAndGetCatalogVersion());
            db.addFunction(fn);
          }
        }
      }
      return getCatalogVersion();
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * Adds a database name to the metadata cache and returns the database's
   * Thrift representation. Used by CREATE DATABASE statements.
   */
  public TCatalogObject addDb(String dbName) throws ImpalaException {
    TCatalogObject thriftDb = new TCatalogObject(TCatalogObjectType.DATABASE,
        Catalog.INITIAL_CATALOG_VERSION);
    catalogLock_.writeLock().lock();
    try {
      Db newDb = new Db(dbName, this);
      newDb.setCatalogVersion(CatalogServiceCatalog.incrementAndGetCatalogVersion());
      addDb(newDb);
      thriftDb.setCatalog_version(newDb.getCatalogVersion());
      thriftDb.setDb(newDb.toThrift());
      return thriftDb;
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * Removes a database from the metadata cache. Used by DROP DATABASE statements.
   */
  @Override
  public Db removeDb(String dbName) {
    catalogLock_.writeLock().lock();
    try {
      Db removedDb = super.removeDb(dbName);
      if (removedDb != null) {
        removedDb.setCatalogVersion(
            CatalogServiceCatalog.incrementAndGetCatalogVersion());
      }
      return removedDb;
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * Adds a table with the given name to the catalog and returns the new table,
   * loading the metadata if needed.
   * TODO: Should this add an IncompleteTable instead of loading the metadata?
   */
  public Table addTable(String dbName, String tblName) throws TableNotFoundException {
    Db db;
    catalogLock_.writeLock().lock();
    try {
      db = getDb(dbName);
      if (db == null) return null;
      db.addTableName(tblName);
    } finally {
      catalogLock_.writeLock().unlock();
    }
    return db.getTable(tblName);
  }

  public Table removeTable(String dbName, String tblName)
      throws DatabaseNotFoundException {
    catalogLock_.writeLock().lock();
    try {
      Db parentDb = getDb(dbName);
      if (parentDb == null) return null;

      Table removedTable = parentDb.removeTable(tblName);
      if (removedTable != null) {
        removedTable.setCatalogVersion(
            CatalogServiceCatalog.incrementAndGetCatalogVersion());
      }
      return removedTable;
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * Removes a function from the catalog. Increments the catalog version and returns
   * the Function object that was removed if the function existed, otherwise returns
   * null.
   */
  @Override
  public Function removeFunction(Function desc) {
    catalogLock_.writeLock().lock();
    try {
      Function removedFn = super.removeFunction(desc);
      if (removedFn != null) {
        removedFn.setCatalogVersion(
            CatalogServiceCatalog.incrementAndGetCatalogVersion());
      }
      return removedFn;
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * Removes a function from the catalog. Increments the catalog version and returns
   * the Function object that was removed if the function existed, otherwise returns
   * null.
   * @throws DatabaseNotFoundException
   */
  @Override
  public boolean addFunction(Function fn) {
    catalogLock_.writeLock().lock();
    try {
      Db db = getDb(fn.getFunctionName().getDb());
      if (db == null) return false;
      if (db.addFunction(fn)) {
        fn.setCatalogVersion(CatalogServiceCatalog.incrementAndGetCatalogVersion());
        return true;
      }
      return false;
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * Returns the table parameter 'transient_lastDdlTime', or -1 if it's not set.
   * TODO: move this to a metastore helper class.
   */
  public static long getLastDdlTime(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    Preconditions.checkNotNull(msTbl);
    Map<String, String> params = msTbl.getParameters();
    String lastDdlTimeStr = params.get("transient_lastDdlTime");
    if (lastDdlTimeStr != null) {
      try {
        return Long.parseLong(lastDdlTimeStr);
      } catch (NumberFormatException e) {}
    }
    return -1;
  }

  /**
   * Updates the cached lastDdlTime for the given table. The lastDdlTime is used during
   * the metadata refresh() operations to determine if there have been any external
   * (outside of Impala) modifications to the table.
   */
  public void updateLastDdlTime(TTableName tblName, long ddlTime) {
    catalogLock_.writeLock().lock();
    try {
      Db db = getDb(tblName.getDb_name());
      if (db == null) return;
      Table tbl = db.getTable(tblName.getTable_name());
      if (tbl == null) return;
      tbl.updateLastDdlTime(ddlTime);
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * Renames a table. Equivalent to an atomic drop + add of the table. Returns
   * the new Table object with an incremented catalog version or null if operation
   * was not successful.
   */
  public Table renameTable(TTableName oldTableName, TTableName newTableName)
      throws CatalogException {
    // Ensure the removal of the old table and addition of the new table happen
    // atomically.
    catalogLock_.writeLock().lock();
    try {
      // Remove the old table name from the cache and add the new table.
      Db db = getDb(oldTableName.getDb_name());
      if (db != null) db.removeTable(oldTableName.getTable_name());
      return addTable(newTableName.getDb_name(), newTableName.getTable_name());
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * If isRefresh is false, invalidates a specific table's metadata, forcing the
   * metadata to be reloaded. As part the invalidation, an IncompleteTable with
   * the updated catalog version will be added to the metadata cache. This will
   * indicate to an impalad that it needs to contact the catalog server to load
   * the complete metadata.
   *
   * If isRefresh is true, performs an immediate incremental refresh, loading the table
   * metadata.
   * TODO: Make "invalidate metadata <table name>" sync the catalog state with the
   * metastore so the command can be used to add/remove tables that have been
   * added/dropped externally.
   */
  public Table resetTable(TTableName tableName, boolean isRefresh)
      throws CatalogException {
    if (isRefresh) {
      Table table = getTable(tableName.getDb_name(), tableName.getTable_name());
      if (table == null) return null;
      LOG.debug("Refreshing table metadata: " + table.getFullName());
      table.getDb().reloadTable(table.getName());
      return getTable(tableName.getDb_name(), tableName.getTable_name());
    } else {
      catalogLock_.writeLock().lock();
      try {
        Table existingTable = getTable(tableName.getDb_name(),
            tableName.getTable_name());
        if (existingTable == null) return null;
        LOG.debug("Invalidating table metadata: " + existingTable.getFullName());
        // Instead of calling invalidate() on the cache entry, which would mean the next
        // access would trigger a metadata load, the existing table is replaced with
        // an IncompleteTable. The IncompleteTable will be sent to all impalads in the
        // cluster, triggering an invalidation on each node.
        Table newTable = IncompleteTable.createUninitializedTable(getNextTableId(),
            existingTable.getDb(), existingTable.getName());
        newTable.setCatalogVersion(CatalogServiceCatalog.incrementAndGetCatalogVersion());
        existingTable.getDb().addTable(newTable);
        return newTable;
      } finally {
        catalogLock_.writeLock().unlock();
      }
    }
  }

  /**
   * Gets a table from the catalog. This method will load any uninitialized
   * IncompleteTables into the table cache. Returns the Table object or null if no
   * table existed in the cache with this name.
   */
  public Table getOrReloadTable(String dbName, String tblName)
      throws DatabaseNotFoundException {
    Table tbl = getTable(dbName, tblName);
    if (tbl == null) return null;

    // Check if this table needs to have its metadata loaded (is an uninitialized
    // IncompleteTable). To avoid having the same table reloaded many times for
    // concurrent requests to the same object, synchronize on the Table and then
    // check if it is still incomplete while holding the lock. This allows concurrent
    // loads for different tables while still protecting against a flood of reloads
    // of the same table.
    synchronized (tbl) {
      // Read the table again while holding the lock.
      Table reReadTbl = getTable(dbName, tblName);
      if (reReadTbl == null) return null;
      if (reReadTbl instanceof IncompleteTable
          && ((IncompleteTable) reReadTbl).isUninitialized()) {
        // Perform the reload
        return reReadTbl.getDb().reloadTable(tblName);
      } else {
        return reReadTbl;
      }
    }
  }

  /**
   * See comment in Catalog.java.
   * This method will load any uninitialized IncompleteTables into the table cache.
   */
  @Override
  public TCatalogObject getTCatalogObject(TCatalogObject objectDesc)
      throws CatalogException {
    if (objectDesc.isSetTable()) {
      Table tbl = getOrReloadTable(objectDesc.getTable().getDb_name(),
          objectDesc.getTable().getTbl_name());
      if (tbl == null) {
        throw new TableNotFoundException("Table not found: " +
            objectDesc.getTable().getTbl_name());
      }
    }
    return super.getTCatalogObject(objectDesc);
  }

  /**
   * Increments the current Catalog version and returns the new value.
   */
  public static long incrementAndGetCatalogVersion() {
    return catalogVersion_.incrementAndGet();
  }

  /**
   * Returns the current Catalog version.
   */
  public static long getCatalogVersion() { return catalogVersion_.get(); }
}
