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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import com.cloudera.impala.catalog.MetaStoreClientPool.MetaStoreClient;
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
 * The CatalogServiceCatalog also manages a global "catalog version". The version
 * is incremented and assigned to a CatalogObject whenever it is
 * added/modified/removed from the catalog. This means each CatalogObject
 * will have a unique version and assigned versions are strictly increasing.
 */
public class CatalogServiceCatalog extends Catalog {
  private static final Logger LOG = Logger.getLogger(CatalogServiceCatalog.class);
  private final TUniqueId catalogServiceId_;

  // Fair lock used to synchronize reads/writes of catalogVersion_. Because this lock
  // protects catalogVersion_, it can be used to perform atomic bulk catalog operations
  // since catalogVersion_ cannot change externally while the lock is being held.
  // In addition to protecting catalogVersion_, it is currently used for the
  // following bulk operations:
  // * Building a delta update to send to the statestore in getAllCatalogObjects(),
  //   so a snapshot of the catalog can be taken without any version changes.
  // * During a catalog invalidation (call to reset()), which re-reads all dbs and tables
  //   from the metastore.
  // * During renameTable(), because a table must be removed and added to the catalog
  //   atomically (potentially in a different database).
  private final ReentrantReadWriteLock catalogLock_ = new ReentrantReadWriteLock(true);

  // Last assigned catalog version. Starts at INITIAL_CATALOG_VERSION and is incremented
  // with each update to the Catalog. Continued across the lifetime of the object.
  // Protected by catalogLock_.
  // TODO: Handle overflow of catalogVersion_ and nextTableId_.
  private long catalogVersion_ = INITIAL_CATALOG_VERSION;

  protected final AtomicInteger nextTableId_ = new AtomicInteger(0);

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

          Table tbl = db.getTableNoLoad(tblName);
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
      catalog.setCatalog_version(getCatalogVersion());
      catalog.setCatalog(new TCatalog(catalogServiceId_));
      resp.addToObjects(catalog);

      // The max version is the max catalog version of all items in the update.
      resp.setMax_catalog_version(getCatalogVersion());
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
  }

  @Override
  public void reset() throws CatalogException {
    catalogLock_.writeLock().lock();
    try {
      resetInternal();
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * Executes the underlying reset logic. catalogLock_.writeLock() must
   * be taken before calling this.
   */
  private void resetInternal() throws CatalogException {
    try {
      nextTableId_.set(0);
      if (initStrategy_ == CatalogInitStrategy.EMPTY) {
        dbCache_.get().clear();
        return;
      }

      // Since UDFs/UDAs are not persisted in the metastore, we won't clear
      // them across reset. To do this, we store all the functions before
      // clearing and restore them after.
      // TODO: Everything about this. Persist them.
      List<Pair<String, HashMap<String, List<Function>>>> functions =
          Lists.newArrayList();
      for (Db db: dbCache_.get().values()) {
        if (db.numFunctions() == 0) continue;
        functions.add(Pair.create(db.getName(), db.getAllFunctions()));
      }

      // Build a new DB cache, populate it, and replace the existing cache in one
      // step.
      ConcurrentHashMap<String, Db> newDbCache = new ConcurrentHashMap<String, Db>();

      MetaStoreClient msClient = metaStoreClientPool_.getClient();
      try {
        for (String dbName: msClient.getHiveClient().getAllDatabases()) {
          Db db = new Db(dbName, this);
          db.setCatalogVersion(incrementAndGetCatalogVersion());
          newDbCache.put(db.getName().toLowerCase(), db);

          for (String tableName: msClient.getHiveClient().getAllTables(dbName)) {
            Table incompleteTbl = IncompleteTable.createUninitializedTable(
                getNextTableId(), db, tableName);
            incompleteTbl.setCatalogVersion(incrementAndGetCatalogVersion());
            db.addTable(incompleteTbl);
          }
        }
      } finally {
        msClient.release();
      }

      // Restore UDFs/UDAs.
      for (Pair<String, HashMap<String, List<Function>>> dbFns: functions) {
        Db db = null;
        try {
          db = newDbCache.get(dbFns.first);
        } catch (Exception e) {
          continue;
        }
        if (db == null) {
          // DB no longer exists - it was probably dropped externally.
          // TODO: We could restore this DB and then add the functions back?
          continue;
        }

        for (List<Function> fns: dbFns.second.values()) {
          for (Function fn: fns) {
            fn.setCatalogVersion(incrementAndGetCatalogVersion());
            db.addFunction(fn);
          }
        }
      }
      dbCache_.set(newDbCache);
    } catch (Exception e) {
      LOG.error(e);
      throw new CatalogException("Error initializing Catalog. Catalog may be empty.", e);
    }
  }

  /**
   * Adds a database name to the metadata cache and returns the database's
   * new Db object. Used by CREATE DATABASE statements.
   */
  public Db addDb(String dbName) throws ImpalaException {
    Db newDb = new Db(dbName, this);
    newDb.setCatalogVersion(incrementAndGetCatalogVersion());
    addDb(newDb);
    return newDb;
  }

  /**
   * Removes a database from the metadata cache and returns the removed database,
   * or null if the database did not exist in the cache.
   * Used by DROP DATABASE statements.
   */
  @Override
  public Db removeDb(String dbName) {
    Db removedDb = super.removeDb(dbName);
    if (removedDb != null) {
      removedDb.setCatalogVersion(incrementAndGetCatalogVersion());
    }
    return removedDb;
  }

  /**
   * Adds a table with the given name to the catalog and returns the new table,
   * loading the metadata if needed.
   * TODO: Should this add an IncompleteTable instead of loading the metadata?
   */
  public Table addTable(String dbName, String tblName) throws TableNotFoundException {
    Db db = getDb(dbName);
    if (db == null) return null;
    Table incompleteTable =
        IncompleteTable.createUninitializedTable(getNextTableId(), db, tblName);
    db.addTable(incompleteTable);
    return db.getTable(tblName);
  }

  public Table removeTable(String dbName, String tblName)
      throws DatabaseNotFoundException {
    Db parentDb = getDb(dbName);
    if (parentDb == null) return null;

    Table removedTable = parentDb.removeTable(tblName);
    if (removedTable != null) {
      removedTable.setCatalogVersion(incrementAndGetCatalogVersion());
    }
    return removedTable;
  }

  /**
   * Removes a function from the catalog. Increments the catalog version and returns
   * the Function object that was removed. If the function did not exist, null will
   * be returned.
   */
  @Override
  public Function removeFunction(Function desc) {
    Function removedFn = super.removeFunction(desc);
    if (removedFn != null) {
      removedFn.setCatalogVersion(incrementAndGetCatalogVersion());
    }
    return removedFn;
  }

  /**
   * Removes a function from the catalog. Increments the catalog version and returns
   * the Function object that was removed if the function existed, otherwise returns
   * null.
   * @throws DatabaseNotFoundException
   */
  @Override
  public boolean addFunction(Function fn) {
    Db db = getDb(fn.getFunctionName().getDb());
    if (db == null) return false;
    if (db.addFunction(fn)) {
      fn.setCatalogVersion(incrementAndGetCatalogVersion());
      return true;
    }
    return false;
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
    Db db = getDb(tblName.getDb_name());
    if (db == null) return;
    Table tbl = db.getTable(tblName.getTable_name());
    if (tbl == null) return;
    tbl.updateLastDdlTime(ddlTime);
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
      return table.getDb().reloadTable(table.getName());
    } else {
      Table existingTable = getTable(tableName.getDb_name(),
          tableName.getTable_name());
      if (existingTable == null) return null;
      LOG.debug("Invalidating table metadata: " + existingTable.getFullName());

      // The existing table is replaced with an uninitialized IncompleteTable, which
      // effectively invalidates the existing cached value. The IncompleteTable
      // will then be sent to all impalads in the cluster, triggering an invalidation on
      // each node.
      Table newTable = IncompleteTable.createUninitializedTable(getNextTableId(),
          existingTable.getDb(), existingTable.getName());
      newTable.setCatalogVersion(incrementAndGetCatalogVersion());
      existingTable.getDb().addTable(newTable);
      return newTable;
    }
  }

  /**
   * Increments the current Catalog version and returns the new value.
   */
  public long incrementAndGetCatalogVersion() {
    catalogLock_.writeLock().lock();
    try {
      return ++catalogVersion_;
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * Returns the current Catalog version.
   */
  public long getCatalogVersion() {
    catalogLock_.readLock().lock();
    try {
      return catalogVersion_;
    } finally {
      catalogLock_.readLock().unlock();
    }
  }

  /**
   * Gets the next table ID and increments the table ID counter.
   */
  public TableId getNextTableId() { return new TableId(nextTableId_.getAndIncrement()); }
}
