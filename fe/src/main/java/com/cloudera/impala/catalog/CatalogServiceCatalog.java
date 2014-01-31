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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

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
 * added/modified/removed from the catalog. This means each CatalogObject will have a
 * unique version and assigned versions are strictly increasing.
 * Table metadata is loaded in the background, by a pool of table loading threads.
 * These threads read from a deque of table names to determine which table to load
 * next. Tables added to the head of the deque will be loaded before tables added to the
 * tail, so the loading order can be prioritized (see prioritizeLoad()).
 * Additionally, a background collection thread can be enabled that will periodically
 * scan for missing tables (tables that are not yet loaded) and add them to the tail
 * of the loading deque.
 * Accessing a table that is not yet loaded (via getTable()), will load the table's
 * metadata on-demand, out-of-band of the table loading thread pool.
 * TODO: Consider removing on-demand loading and have everything go through the table
 * loading thread pool.
 */
public class CatalogServiceCatalog extends Catalog {
  private static final Logger LOG = Logger.getLogger(CatalogServiceCatalog.class);

  // The number of parallel threads to use to load table metadata. Should be set to a
  // value that provides good throughput while not putting too much stress on the
  // metastore.
  private final int numLoadingThreads_;

  // Number of milliseconds to sleep between calls to search for tables that do not have
  // their metadata.
  private final int BACKGROUND_TBL_COLLECTION_SLEEP_MS = 3 * 1000;

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

  // A thread safe blocking deque that is used to prioritize the loading of table
  // metadata. The CatalogServer has a background thread that will always add unloaded
  // tables to the tail of the deque. However, a call to prioritizeLoad() will add
  // tables to the head of the deque. The next table to load is always taken from the
  // head of the deque. May contain the same table multiple times, but a secondary
  // attempt to load the table metadata will be a no-op.
  private final LinkedBlockingDeque<TTableName> tableLoadingDeque_ =
      new LinkedBlockingDeque<TTableName>();

  // A thread safe HashSet of table names that are in the tableLoadingDeque_. Used to
  // efficiently check for existence of items in the deque.
  private final Set<TTableName> tableLoadingSet_ =
      Collections.synchronizedSet(new HashSet<TTableName>());

  // Last assigned catalog version. Starts at INITIAL_CATALOG_VERSION and is incremented
  // with each update to the Catalog. Continued across the lifetime of the object.
  // Protected by catalogLock_.
  // TODO: Handle overflow of catalogVersion_ and nextTableId_.
  private long catalogVersion_ = INITIAL_CATALOG_VERSION;

  protected final AtomicInteger nextTableId_ = new AtomicInteger(0);

  /**
   * Initialize the CatalogServiceCatalog, loading all table metadata
   * lazily.
   */
  public CatalogServiceCatalog(boolean loadInBackground, int numLoadingThreads,
      TUniqueId catalogServiceId) {
    super(true);
    catalogServiceId_ = catalogServiceId;
    numLoadingThreads_ = numLoadingThreads;
    if (loadInBackground) {
      // Start a background thread that queues unloaded tables to be loaded.
      Executors.newSingleThreadExecutor().execute(new Runnable() {
        @Override
        public void run() {
          while(true) {
            collectMissingTbls();
            try {
              Thread.sleep(BACKGROUND_TBL_COLLECTION_SLEEP_MS);
            } catch (InterruptedException e) {
              // Ignore exception.
            }
          }
        }
      });
    }

    // Start the background table loading threads.
    startTableLoadingThreads();
  }

  /**
   * Only used for testing. Creates a catalog server with default options.
   */
  public static CatalogServiceCatalog createForTesting(boolean loadInBackground) {
    return new CatalogServiceCatalog(loadInBackground, 16, new TUniqueId());
  }

  /**
   * Starts table loading threads in a fixed sized thread pool with a size
   * defined by NUM_TBL_LOADING_THREADS. Each thread polls the tableLoadingDeque_
   * for new tables to load.
   */
  private void startTableLoadingThreads() {
    ExecutorService executor = Executors.newFixedThreadPool(numLoadingThreads_);
    try {
      for (int i = 0; i < numLoadingThreads_; ++i) {
        executor.execute(new Runnable() {
          @Override
          public void run() {
            while (true) {
              try {
                loadNextTable();
              } catch (InterruptedException e) {
                LOG.error("Error loading table: " + e.getMessage(), e);
              }
            }
          }
        });
      }
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Gets the next table name to load off the head of the table loading queue. If
   * the queue is empty, this will block until a new table is added.
   */
  private void loadNextTable() throws InterruptedException {
    // Always get the next table from the head of the deque.
    TTableName tblName = tableLoadingDeque_.takeFirst();
    tableLoadingSet_.remove(tblName);
    LOG.debug("Loading next table. Remaining items in queue: "
        + tableLoadingDeque_.size());

    Db db = getDb(tblName.getDb_name());
    if (db == null) return;

    // Get the table, which will load the table's metadata if needed.
    db.getTable(tblName.getTable_name());
  }

  /**
   * Called on a periodic basis and adds all tables that are not yet loaded to the
   * tail of the table loading deque.
   */
  private void collectMissingTbls() {
    for (String dbName: getDbNames(null)) {
      Db db = getDb(dbName);
      if (db == null) continue;
      for (String tblName: db.getAllTableNames()) {
        Table existingTbl = db.getTableNoLoad(tblName);

        // Only add this table if it isn't loaded and doesn't exist in the deque.
        if (existingTbl != null && !existingTbl.isLoaded()) {
          TTableName name = new TTableName(dbName.toLowerCase(), tblName.toLowerCase());
          if (!tableLoadingSet_.contains(name)) {
            tableLoadingSet_.add(name);
            tableLoadingDeque_.offerLast(name);
          }
        }
      }
    }
  }

  /**
   * Prioritizes the loading of the given list TCatalogObjects. Currently only support
   * loading Table/View metadata since Db and Function metadata is not loaded lazily.
   */
  public void prioritizeLoad(List<TCatalogObject> objectDescs) {
    for (TCatalogObject catalogObject: objectDescs) {
      Preconditions.checkState(catalogObject.isSetTable());
      TTable table = catalogObject.getTable();
      TTableName tblName = new TTableName(table.getDb_name().toLowerCase(),
          table.getTbl_name().toLowerCase());
      tableLoadingSet_.add(tblName);
      tableLoadingDeque_.offerFirst(tblName);
    }
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

  /**
   * Resets this catalog instance by clearing all cached table and database metadata.
   */
  public void reset() throws CatalogException {
    catalogLock_.writeLock().lock();
    try {
      nextTableId_.set(0);

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
    } finally {
      catalogLock_.writeLock().unlock();
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
   */
  public Table addTable(String dbName, String tblName) throws TableNotFoundException {
    Db db = getDb(dbName);
    if (db == null) return null;
    Table incompleteTable =
        IncompleteTable.createUninitializedTable(getNextTableId(), db, tblName);
    db.addTable(incompleteTable);

    return db.getTable(tblName);
  }

  public Table removeTable(String dbName, String tblName) {
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
   * Reloads a table's metadata, reusing any existing cached metadata to speed up
   * the operation. Returns the updated Table object or null if no table with
   * this name exists in the catalog.
   */
  public Table reloadTable(TTableName tableName) throws CatalogException {
    LOG.debug(String.format("Refreshing table metadata: %s.%s",
        tableName.getDb_name(), tableName.getTable_name()));
    Table table = getTable(tableName.getDb_name(), tableName.getTable_name());
    if (table == null) return null;
    return table.getDb().reloadTable(table.getName());
  }

  /**
   * Invalidates the table in the catalog cache, potentially adding/removing the table
   * from the cache based on whether it exists in the Hive Metastore.
   * The invalidation logic is:
   * - If the table exists in the metastore, add it to the catalog as an uninitialized
   *   IncompleteTable (replacing any existing entry). The table metadata will be
   *   loaded lazily, on the next access. If the parent database for this table does not
   *   yet exist in Impala's cache it will also be added.
   * - If the table does not exist in the metastore, remove it from the catalog cache.
   * - If we are unable to determine whether the table exists in the metastore (there was
   *   an exception thrown making the RPC), invalidate any existing Table by replacing
   *   it with an uninitialized IncompleteTable.
   *
   * The parameter updatedObjects is a Pair that contains details on what catalog objects
   * were modified as a result of the invalidateTable() call. The first item in the Pair
   * is a Db which will only be set if a new database was added as a result of this call,
   * otherwise it will be null. The second item in the Pair is the Table that was
   * modified/added/removed.
   * Returns a flag that indicates whether the items in updatedObjects were removed
   * (returns true) or added/modified (return false). Only Tables should ever be removed.
   */
  public boolean invalidateTable(TTableName tableName, Pair<Db, Table> updatedObjects) {
    Preconditions.checkNotNull(updatedObjects);
    updatedObjects.first = null;
    updatedObjects.second = null;
    LOG.debug(String.format("Invalidating table metadata: %s.%s",
        tableName.getDb_name(), tableName.getTable_name()));
    String dbName = tableName.getDb_name();
    String tblName = tableName.getTable_name();

    // Stores whether the table exists in the metastore. Can have three states:
    // 1) true - Table exists in metastore.
    // 2) false - Table does not exist in metastore.
    // 3) unknown (null) - There was exception thrown by the metastore client.
    Boolean tableExistsInMetaStore;
    MetaStoreClient msClient = getMetaStoreClient();
    try {
      tableExistsInMetaStore = msClient.getHiveClient().tableExists(dbName, tblName);
    } catch (UnknownDBException e) {
      // The parent database does not exist in the metastore. Treat this the same
      // as if the table does not exist.
      tableExistsInMetaStore = false;
    } catch (TException e) {
      LOG.error("Error executing tableExists() metastore call: " + tblName, e);
      tableExistsInMetaStore = null;
    }

    if (tableExistsInMetaStore != null && !tableExistsInMetaStore) {
      updatedObjects.second = removeTable(dbName, tblName);
      return true;
    } else {
      Db db = getDb(dbName);
      if ((db == null || !db.containsTable(tblName)) && tableExistsInMetaStore == null) {
        // The table does not exist in our cache AND it is unknown whether the table
        // exists in the metastore. Do nothing.
        return false;
      } else if (db == null && tableExistsInMetaStore) {
        // The table exists in the metastore, but our cache does not contain the parent
        // database. A new db will be added to the cache along with the new table.
        db = new Db(dbName, this);
        db.setCatalogVersion(incrementAndGetCatalogVersion());
        addDb(db);
        updatedObjects.first = db;
      }

      // Add a new uninitialized table to the table cache, effectively invalidating
      // any existing entry. The metadata for the table will be loaded lazily, on the
      // on the next access to the table.
      Table newTable = IncompleteTable.createUninitializedTable(
          getNextTableId(), db, tblName);
      newTable.setCatalogVersion(incrementAndGetCatalogVersion());
      db.addTable(newTable);
      updatedObjects.second = newTable;
      return false;
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
