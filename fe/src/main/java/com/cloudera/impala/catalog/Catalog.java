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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.cloudera.impala.analysis.FunctionName;
import com.cloudera.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.thrift.TCatalogObject;
import com.cloudera.impala.thrift.TFunctionType;
import com.cloudera.impala.thrift.TPartitionKeyValue;
import com.cloudera.impala.thrift.TTableName;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Lists;

/**
 * Thread safe interface for reading and updating metadata stored in the Hive MetaStore.
 * This class caches db-, table- and column-related metadata. Each time one of these
 * catalog objects is updated/added/removed, the catalogVersion is incremented.
 * Although this class is thread safe, it does not guarantee consistency with the
 * MetaStore. It is important to keep in mind that there may be external (potentially
 * conflicting) concurrent metastore updates occurring at any time.
 * All reads and writes of catalog objects are synchronized using the catalogLock_. To
 * perform atomic bulk operations on the Catalog, the getReadLock()/getWriteLock()
 * functions can be leveraged.
 */
public abstract class Catalog {
  // Initial catalog version.
  public final static long INITIAL_CATALOG_VERSION = 0L;
  public static final String DEFAULT_DB = "default";

  private static final Logger LOG = Logger.getLogger(Catalog.class);

  // Last assigned catalog version. Atomic to ensure catalog versions are always
  // sequentially increasing, even when updated from different threads.
  // TODO: This probably doesn't need to be atomic and be updated while holding
  // the catalogLock_.
  private final static AtomicLong catalogVersion =
      new AtomicLong(INITIAL_CATALOG_VERSION);
  private static final int META_STORE_CLIENT_POOL_SIZE = 5;
  private final MetaStoreClientPool metaStoreClientPool_ = new MetaStoreClientPool(0);
  private final CatalogInitStrategy initStrategy_;
  private final AtomicInteger nextTableId = new AtomicInteger(0);

  // Cache of database metadata.
  protected final CatalogObjectCache<Db> dbCache_ = new CatalogObjectCache<Db>(
      new CacheLoader<String, Db>() {
        @Override
        public Db load(String dbName) {
          MetaStoreClient msClient = getMetaStoreClient();
          try {
            return Db.loadDb(Catalog.this, msClient.getHiveClient(),
                dbName.toLowerCase(), true);
          } finally {
            msClient.release();
          }
        }
      });

  // Fair lock used to synchronize catalog accesses and updates.
  protected final ReentrantReadWriteLock catalogLock_ =
      new ReentrantReadWriteLock(true);

  // Determines how the Catalog should be initialized.
  public enum CatalogInitStrategy {
    // Load only db and table names on startup.
    LAZY,
    // Load all metadata on startup
    IMMEDIATE,
    // Don't load anything on startup (creates an empty catalog).
    EMPTY,
  }

  /**
   * Creates a new instance of the Catalog, initializing it based on
   * the given CatalogInitStrategy.
   */
  public Catalog(CatalogInitStrategy initStrategy) {
    this.initStrategy_ = initStrategy;
    this.metaStoreClientPool_.addClients(META_STORE_CLIENT_POOL_SIZE);
    reset();
  }

  public Catalog() { this(CatalogInitStrategy.LAZY); }

  /**
   * Adds a database name to the metadata cache and marks the metadata as
   * uninitialized. Used by CREATE DATABASE statements.
   */
  public long addDb(String dbName) {
    catalogLock_.writeLock().lock();
    try {
      return dbCache_.add(dbName);
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * Gets the Db object from the Catalog using a case-insensitive lookup on the name.
   * Returns null if no matching database is found.
   */
  public Db getDb(String dbName) {
    Preconditions.checkState(dbName != null && !dbName.isEmpty(),
        "Null or empty database name given as argument to Catalog.getDb");
    try {
      return dbCache_.get(dbName);
    } catch (ImpalaException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Returns a list of databases that match dbPattern. See filterStringsByPattern
   * for details of the pattern match semantics.
   *
   * dbPattern may be null (and thus matches everything).
   */
  public List<String> getDbNames(String dbPattern) {
    catalogLock_.readLock().lock();
    try {
      return filterStringsByPattern(dbCache_.getAllNames(), dbPattern);
    } finally {
      catalogLock_.readLock().unlock();
    }
  }

  /**
   * Removes a database from the metadata cache. Used by DROP DATABASE statements.
   */
  public long removeDb(String dbName) {
    catalogLock_.writeLock().lock();
    try {
      return dbCache_.remove(dbName);
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * Adds a new table to the catalog and marks its metadata as uninitialized.
   * Returns the catalog version that include this change, or INITIAL_CATALOG_VERSION
   * if the database does not exist.
   */
  public long addTable(String dbName, String tblName) {
    catalogLock_.writeLock().lock();
    try {
      Db db = getDb(dbName);
      if (db != null) return db.addTable(tblName);
    } finally {
      catalogLock_.writeLock().unlock();
    }
    return Catalog.INITIAL_CATALOG_VERSION;
  }

  /**
   * Returns the Table object for the given dbName/tableName. This will trigger a
   * metadata load if the table metadata is not yet cached.
   */
  public Table getTable(String dbName, String tableName) throws
      DatabaseNotFoundException, TableNotFoundException, TableLoadingException {
    catalogLock_.readLock().lock();
    try {
      Db db = getDb(dbName);
      if (db == null) {
        throw new DatabaseNotFoundException("Database not found: " + dbName);
      }
      Table table = db.getTable(tableName);
      if (table == null) {
        throw new TableNotFoundException(
            String.format("Table not found: %s.%s", dbName, tableName));
      }
      return table;
    } finally {
      catalogLock_.readLock().unlock();
    }
  }

  /**
   * Returns a list of tables in the supplied database that match
   * tablePattern. See filterStringsByPattern for details of the pattern match semantics.
   *
   * dbName must not be null, but tablePattern may be null (and thus matches
   * everything).
   *
   * Table names are returned unqualified.
   */
  public List<String> getTableNames(String dbName, String tablePattern)
      throws DatabaseNotFoundException {
    Preconditions.checkNotNull(dbName);
    catalogLock_.readLock().lock();
    try {
      Db db = getDb(dbName);
      if (db == null) {
        throw new DatabaseNotFoundException("Database '" + dbName + "' not found");
      }
      return filterStringsByPattern(db.getAllTableNames(), tablePattern);
    } finally {
      catalogLock_.readLock().unlock();
    }
  }

  public boolean containsTable(String dbName, String tableName) {
    catalogLock_.readLock().lock();
    try {
      Db db = getDb(dbName);
      return (db == null) ? false : db.containsTable(tableName);
    } finally {
      catalogLock_.readLock().unlock();
    }
  }

  /**
   * Renames a table and returns the catalog version that contains the change.
   * This is equivalent to an atomic drop + add of the table. Returns
   * the current catalog version if the target parent database does not exist
   * in the catalog.
   */
  public long renameTable(TTableName oldTableName, TTableName newTableName) {
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
   * Removes a table from the catalog and returns the catalog version that
   * contains the change. Returns INITIAL_CATALOG_VERSION if the parent
   * database or table does not exist in the catalog.
   */
  public long removeTable(TTableName tableName) {
    catalogLock_.writeLock().lock();
    try {
      // Remove the old table name from the cache and add the new table.
      Db db = getDb(tableName.getDb_name());
      if (db == null) return Catalog.INITIAL_CATALOG_VERSION;
      return db.removeTable(tableName.getTable_name());
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * If isRefresh is false, invalidates a specific table's metadata, forcing the
   * metadata to be reloaded on the next access.
   * If isRefresh is true, performs an immediate incremental refresh.
   * Returns the catalog version that will contain the updated metadata.
   */
  public long resetTable(TTableName tableName, boolean isRefresh) {
    catalogLock_.writeLock().lock();
    try {
      Db db = getDb(tableName.getDb_name());
      if (db == null) return Catalog.INITIAL_CATALOG_VERSION;
      if (isRefresh) {
        // TODO: This is not good because refreshes might take a long time we
        // shouldn't hold the catalog write lock the entire time. Instead,
        // we could consider making refresh() happen in the background or something
        // similar.
        LOG.debug("Refreshing table metadata: " + db.getName() + "." + tableName);
        return db.refreshTable(tableName.getTable_name());
      } else {
        LOG.debug("Invalidating table metadata: " + db.getName() + "." + tableName);
        return db.invalidateTable(tableName.getTable_name());
      }
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * Adds a function to the catalog.
   * Returns true if the function was successfully added.
   * Returns false if the function already exists.
   * TODO: allow adding a function to a global scope. We probably want this to resolve
   * after the local scope.
   * e.g. if we had fn() and db.fn(). If the current database is 'db', fn() would
   * resolve first to db.fn().
   */
  public boolean addFunction(Function fn) {
    catalogLock_.writeLock().lock();
    try {
      Db db = getDb(fn.dbName());
      if (db == null) return false;
      return db.addFunction(fn);
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * Returns the function that best matches 'desc' that is registered with the
   * catalog using 'mode' to check for matching. If desc matches multiple functions
   * in the catalog, it will return the function with the strictest matching mode.
   */
  public Function getFunction(Function desc, Function.CompareMode mode) {
    catalogLock_.readLock().lock();
    try {
      Db db = getDb(desc.dbName());
      if (db == null) return null;
      return db.getFunction(desc, mode);
    } finally {
      catalogLock_.readLock().unlock();
    }
  }

  /**
   * Removes a function from the catalog. Returns true if the UDF was removed.
   * Returns the catalog version that will reflect this change. Returns a version of
   * INITIAL_CATALOG_VERSION if the function did not exist.
   */
  public long removeFunction(Function desc) {
    catalogLock_.writeLock().lock();
    try {
      Db db = getDb(desc.dbName());
      if (db == null) return Catalog.INITIAL_CATALOG_VERSION;
      return db.removeFunction(desc) ? Catalog.incrementAndGetCatalogVersion() :
        Catalog.INITIAL_CATALOG_VERSION;
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * Returns all the function for 'type' in this DB.
   */
  public List<String> getFunctionSignatures(TFunctionType type, String dbName,
      String pattern) throws DatabaseNotFoundException {
    catalogLock_.readLock().lock();
    try {
      Db db = getDb(dbName);
      if (db == null) {
        throw new DatabaseNotFoundException("Database '" + dbName + "' not found");
      }
      return filterStringsByPattern(db.getAllFunctionSignatures(type), pattern);
    } finally {
      catalogLock_.readLock().unlock();
    }
  }

  /**
   * Returns true if there is a function with this function name. Parameters
   * are ignored.
   */
  public boolean functionExists(FunctionName name) {
    catalogLock_.readLock().lock();
    try {
      Db db = getDb(name.getDb());
      if (db == null) return false;
      return db.functionExists(name);
    } finally {
      catalogLock_.readLock().unlock();
    }
  }

  /**
   * Release the Hive Meta Store Client resources. Can be called multiple times
   * (additional calls will be no-ops).
   */
  public void close() { metaStoreClientPool_.close(); }

  /**
   * Gets the next table ID and increments the table ID counter.
   */
  public TableId getNextTableId() { return new TableId(nextTableId.getAndIncrement()); }

  /**
   * Returns a managed meta store client from the client connection pool.
   */
  public MetaStoreClient getMetaStoreClient() { return metaStoreClientPool_.getClient(); }

  /**
   * Returns the current Catalog version.
   */
  public static long getCatalogVersion() { return catalogVersion.get(); }

  /**
   * Increments the current Catalog version and returns the new value.
   */
  public static long incrementAndGetCatalogVersion() {
    return catalogVersion.incrementAndGet();
  }

  /**
   * Resets this catalog instance by clearing all cached metadata and reloading
   * it from the metastore. How the metadata is loaded is based on the
   * CatalogInitStrategy that was set in the c'tor. If the CatalogInitStrategy is
   * IMMEDIATE, the table metadata will be loaded in parallel.
   */
  public long reset() {
    catalogLock_.writeLock().lock();
    try {
      return resetInternal();
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * Executes the underlying reset logic. catalogLock_.writeLock() must
   * be taken before calling this.
   */
  protected long resetInternal() {
    try {
      nextTableId.set(0);
      dbCache_.clear();

      if (initStrategy_ == CatalogInitStrategy.EMPTY) {
        return Catalog.getCatalogVersion();
      }
      MetaStoreClient msClient = metaStoreClientPool_.getClient();

      try {
        dbCache_.add(msClient.getHiveClient().getAllDatabases());
      } finally {
        msClient.release();
      }

      if (initStrategy_ == CatalogInitStrategy.IMMEDIATE) {
        // The number of parallel threads to use to load table metadata. This number
        // was chosen based on experimentation of what provided good throughput while not
        // putting too much stress on the metastore.
        ExecutorService executor = Executors.newFixedThreadPool(16);
        try {
          for (String dbName: dbCache_.getAllNames()) {
            final Db db = dbCache_.get(dbName);
            for (final String tableName: db.getAllTableNames()) {
              executor.execute(new Runnable() {
                @Override
                public void run() {
                  try {
                    db.getTable(tableName);
                  } catch (ImpalaException e) {
                    LOG.warn("Error: " + e.getMessage());
                  }
                }
              });
            }
          }
        } finally {
          executor.shutdown();
        }
      }
      return Catalog.getCatalogVersion();
    } catch (Exception e) {
      LOG.error(e);
      LOG.error("Error initializing Catalog. Catalog may be empty.");
      throw new IllegalStateException(e);
    }
  }

  /**
   * Implement Hive's pattern-matching semantics for SHOW statements. The only
   * metacharacters are '*' which matches any string of characters, and '|'
   * which denotes choice.  Doing the work here saves loading tables or
   * databases from the metastore (which Hive would do if we passed the call
   * through to the metastore client).
   *
   * If matchPattern is null, all strings are considered to match. If it is the
   * empty string, no strings match.
   */
  private List<String> filterStringsByPattern(Iterable<String> candidates,
      String matchPattern) {
    List<String> filtered = Lists.newArrayList();
    if (matchPattern == null) {
      filtered = Lists.newArrayList(candidates);
    } else {
      List<String> patterns = Lists.newArrayList();
      // Hive ignores pretty much all metacharacters, so we have to escape them.
      final String metaCharacters = "+?.^()]\\/{}";
      final Pattern regex = Pattern.compile("([" + Pattern.quote(metaCharacters) + "])");

      for (String pattern: Arrays.asList(matchPattern.split("\\|"))) {
        Matcher matcher = regex.matcher(pattern);
        pattern = matcher.replaceAll("\\\\$1").replace("*", ".*");
        patterns.add(pattern);
      }

      for (String candidate: candidates) {
        for (String pattern: patterns) {
          // Empty string matches nothing in Hive's implementation
          if (!pattern.isEmpty() && candidate.matches(pattern)) {
            filtered.add(candidate);
          }
        }
      }
    }
    Collections.sort(filtered, String.CASE_INSENSITIVE_ORDER);
    return filtered;
  }

  /**
   * Returns the HdfsPartition object for the given dbName/tableName and partition spec.
   * This will trigger a metadata load if the table metadata is not yet cached.
   * @throws DatabaseNotFoundException - If the database does not exist.
   * @throws TableNotFoundException - If the table does not exist.
   * @throws PartitionNotFoundException - If the partition does not exist.
   * @throws TableLoadingException - If there is an error loading the table metadata.
   */
  public HdfsPartition getHdfsPartition(String dbName, String tableName,
      List<TPartitionKeyValue> partitionSpec) throws DatabaseNotFoundException,
      PartitionNotFoundException, TableNotFoundException, TableLoadingException {
    String partitionNotFoundMsg =
        "Partition not found: " + Joiner.on(", ").join(partitionSpec);
    catalogLock_.readLock().lock();
    try {
      Table table = getTable(dbName, tableName);
      // This is not an Hdfs table, throw an error.
      if (!(table instanceof HdfsTable)) {
        throw new PartitionNotFoundException(partitionNotFoundMsg);
      }
      // Get the HdfsPartition object for the given partition spec.
      HdfsPartition partition =
          ((HdfsTable) table).getPartitionFromThriftPartitionSpec(partitionSpec);
      if (partition == null) throw new PartitionNotFoundException(partitionNotFoundMsg);
      return partition;
    } finally {
      catalogLock_.readLock().unlock();
    }
  }

  /**
   * Returns true if the table contains the given partition spec, otherwise false.
   * This may trigger a metadata load if the table metadata is not yet cached.
   * @throws DatabaseNotFoundException - If the database does not exist.
   * @throws TableNotFoundException - If the table does not exist.
   * @throws TableLoadingException - If there is an error loading the table metadata.
   */
  public boolean containsHdfsPartition(String dbName, String tableName,
      List<TPartitionKeyValue> partitionSpec) throws DatabaseNotFoundException,
      TableNotFoundException, TableLoadingException {
    try {
      return getHdfsPartition(dbName, tableName, partitionSpec) != null;
    } catch (PartitionNotFoundException e) {
      return false;
    }
  }

  /**
   * Gets the thrift representation of a catalog object, given the "object
   * description". The object description is just a TCatalogObject with only the
   * catalog object type and object name set.
   * If the object is not found, a CatalogException is thrown.
   */
  public TCatalogObject getTCatalogObject(TCatalogObject objectDesc)
      throws CatalogException {
    TCatalogObject result = new TCatalogObject();
    switch (objectDesc.getType()) {
      case DATABASE: {
        Db db = getDb(objectDesc.getDb().getDb_name());
        if (db == null) {
          throw new CatalogException(
              "Database not found: " + objectDesc.getDb().getDb_name());
        }
        result.setType(db.getCatalogObjectType());
        result.setCatalog_version(db.getCatalogVersion());
        result.setDb(db.toThrift());
        break;
      }
      case TABLE:
      case VIEW: {
        Table table = getTable(objectDesc.getTable().getDb_name(),
            objectDesc.getTable().getTbl_name());
        if (table == null) {
          throw new CatalogException("Table not found: " +
              objectDesc.getTable().getTbl_name());
        }
        result.setType(table.getCatalogObjectType());
        result.setCatalog_version(table.getCatalogVersion());
        result.setTable(table.toThrift());
        break;
      }
      case FUNCTION: {
        for (String dbName: getDbNames(null)) {
          Db db = getDb(dbName);
          if (db == null) continue;
          Function fn = db.getFunction(objectDesc.getFn().getSignature());
          if (fn == null) continue;
          result.setType(fn.getCatalogObjectType());
          result.setCatalog_version(fn.getCatalogVersion());
          result.setFn(fn.toThrift());
          break;
        }
        if (!result.isSetFn()) throw new CatalogException("Function not found.");
        break;
      }
      default: throw new IllegalStateException(
          "Unexpected TCatalogObject type: " + objectDesc.getType());
    }
    return result;
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
}
