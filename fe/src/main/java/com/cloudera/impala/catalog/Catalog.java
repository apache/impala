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
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.cloudera.impala.analysis.FunctionName;
import com.cloudera.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import com.cloudera.impala.thrift.TCatalogObject;
import com.cloudera.impala.thrift.TFunctionType;
import com.cloudera.impala.thrift.TPartitionKeyValue;
import com.cloudera.impala.thrift.TTableName;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Thread safe interface for reading and updating metadata stored in the Hive MetaStore.
 * This class caches db-, table- and column-related metadata. Although this class is
 * thread safe, it does not guarantee consistency with the MetaStore. It is important
 * to keep in mind that there may be external (potentially conflicting) concurrent
 * metastore updates occurring at any time.
 * All reads and writes of catalog objects should be synchronized using the catalogLock_.
 */
public abstract class Catalog {
  // Initial catalog version.
  public final static long INITIAL_CATALOG_VERSION = 0L;
  public static final String DEFAULT_DB = "default";
  private static final int META_STORE_CLIENT_POOL_SIZE = 5;

  private static final Logger LOG = Logger.getLogger(Catalog.class);

  private final MetaStoreClientPool metaStoreClientPool_ = new MetaStoreClientPool(0);
  private final CatalogInitStrategy initStrategy_;
  private final AtomicInteger nextTableId_ = new AtomicInteger(0);

  // Cache of database metadata.
  protected final HashMap<String, Db> dbCache_ = new HashMap<String, Db>();

  // Fair lock used to synchronize catalog accesses and updates.
  protected final ReentrantReadWriteLock catalogLock_ =
      new ReentrantReadWriteLock(true);

  // Determines how the Catalog should be initialized.
  public enum CatalogInitStrategy {
    // Load only db and table names on startup.
    LAZY,
    // Don't load anything on startup (creates an empty catalog).
    EMPTY,
  }

  /**
   * Creates a new instance of the Catalog, initializing it based on
   * the given CatalogInitStrategy.
   */
  public Catalog(CatalogInitStrategy initStrategy) {
    initStrategy_ = initStrategy;
    // Don't create any metastore clients for empty catalogs.
    if (initStrategy != CatalogInitStrategy.EMPTY) {
      metaStoreClientPool_.addClients(META_STORE_CLIENT_POOL_SIZE);
    }
    reset();
  }

  public Catalog() { this(CatalogInitStrategy.LAZY); }

  /**
   * Adds a new database to the catalog, replacing any existing database with the same
   * name. Returns the previous database with this name, or null if there was no
   * previous database.
   */
  public Db addDb(Db db) {
    catalogLock_.writeLock().lock();
    try {
      return dbCache_.put(db.getName().toLowerCase(), db);
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
    catalogLock_.readLock().lock();
    try {
      return dbCache_.get(dbName.toLowerCase());
    } finally {
      catalogLock_.readLock().unlock();
    }
  }

  /**
   * Removes a database from the metadata cache. Returns the value removed or null
   * if not database was removed as part of this operation. Used by DROP DATABASE
   * statements.
   */
  public Db removeDb(String dbName) {
    catalogLock_.writeLock().lock();
    try {
      return dbCache_.remove(dbName.toLowerCase());
    } finally {
      catalogLock_.writeLock().unlock();
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
      return filterStringsByPattern(dbCache_.keySet(), dbPattern);
    } finally {
      catalogLock_.readLock().unlock();
    }
  }

  /**
   * Returns the Table object for the given dbName/tableName. This will trigger a
   * metadata load if the table metadata is not yet cached.
   */
  public Table getTable(String dbName, String tableName) throws
      DatabaseNotFoundException {
    catalogLock_.readLock().lock();
    try {
      Db db = getDb(dbName);
      if (db == null) {
        throw new DatabaseNotFoundException("Database '" + dbName + "' not found");
      }
      return db.getTable(tableName);
    } finally {
      catalogLock_.readLock().unlock();
    }
  }

  /**
   * Removes a table from the catalog and returns the table that was removed, or null
   * if the table/database does not exist.
   */
  public Table removeTable(TTableName tableName) {
    catalogLock_.writeLock().lock();
    try {
      // Remove the old table name from the cache and add the new table.
      Db db = getDb(tableName.getDb_name());
      if (db == null) return null;
      return db.removeTable(tableName.getTable_name());
    } finally {
      catalogLock_.writeLock().unlock();
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
   * Removes a function from the catalog. Increments the catalog version and returns
   * the Function object that was removed if the function existed, otherwise returns
   * null.
   */
  public Function removeFunction(Function desc) {
    catalogLock_.writeLock().lock();
    try {
      Db db = getDb(desc.dbName());
      if (db == null) return null;
      return db.removeFunction(desc);
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
  public TableId getNextTableId() { return new TableId(nextTableId_.getAndIncrement()); }

  /**
   * Returns a managed meta store client from the client connection pool.
   */
  public MetaStoreClient getMetaStoreClient() { return metaStoreClientPool_.getClient(); }

  /**
   * Resets this catalog instance by clearing all cached metadata and potentially
   * reloading the metadata. How the metadata is loaded is based on the
   * CatalogInitStrategy that was set in the c'tor.
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
      nextTableId_.set(0);
      dbCache_.clear();

      if (initStrategy_ == CatalogInitStrategy.EMPTY) {
        return CatalogServiceCatalog.getCatalogVersion();
      }
      MetaStoreClient msClient = metaStoreClientPool_.getClient();

      try {
        for (String dbName: msClient.getHiveClient().getAllDatabases()) {
          Db db = new Db(dbName, this);
          db.setCatalogVersion(CatalogServiceCatalog.incrementAndGetCatalogVersion());
          addDb(db);
          for (final String tableName: msClient.getHiveClient().getAllTables(dbName)) {
            Table incompleteTbl =
                IncompleteTable.createUninitializedTable(getNextTableId(), db, tableName);
            incompleteTbl.setCatalogVersion(
                CatalogServiceCatalog.incrementAndGetCatalogVersion());
            db.addTable(incompleteTbl);
          }
        }
      } finally {
        msClient.release();
      }
      return CatalogServiceCatalog.getCatalogVersion();
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
}