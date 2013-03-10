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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.conf.HiveConf;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.TableType;

import org.apache.log4j.Logger;

import com.cloudera.impala.catalog.Db.TableLoadingException;
import com.cloudera.impala.catalog.HiveStorageDescriptorFactory;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.MetaStoreClientPool;
import com.cloudera.impala.common.MetaStoreClientPool.MetaStoreClient;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;

import com.cloudera.impala.thrift.TColumnDef;
import com.cloudera.impala.thrift.TColumnDesc;

/**
 * Thread safe interface for reading and updating metadata stored in the Hive MetaStore.
 * This class caches db-, table- and column-related metadata.
 * Although this class is thread safe, it does not guarantee consistency with the
 * MetaStore. It is important to keep in mind that there may be external (potentially
 * conflicting) concurrent metastore updates occurring at any time. This class does
 * guarantee any MetaStore updates done via this class will be reflected consistently.
 */
public class Catalog {
  public static final String DEFAULT_DB = "default";
  private static final Logger LOG = Logger.getLogger(Catalog.class);
  private static final int META_STORE_CLIENT_POOL_SIZE = 5;
  private final boolean lazy;
  private int nextTableId;
  private final MetaStoreClientPool metaStoreClientPool;
  // Lock used to synchronize metastore CREATE/DROP TABLE/DATABASE requests.
  private final Object metastoreCreateDropLock = new Object();

  // map from db name to DB
  private final LazyDbMap dbs;

  // Tracks whether a Table/Db has all of its metadata loaded.
  enum MetadataLoadState {
    LOADED,
    UNINITIALIZED,
  }

  /**
   * Lazily loads database metadata on read (through 'get') and tracks the valid/known
   * database names. This class is thread safe.
   *
   * If a database has not yet been loaded successfully, get() will attempt to load it.
   * It is only possible to load metadata for databases that are in the known db name
   * map.
   *
   * Getting all the metadata is a heavy-weight operation, but Impala still needs
   * to know what databases exist (one use case is for SHOW commands). To support this,
   * there is a parallel mapping of known database names to their metadata load state.
   * When Impala starts up (and on refresh commands) the database name map is populated
   * with all database names available.
   *
   * Before loading any metadata, the database name map is checked to ensure the given
   * database is "known". If it is not, no metadata is loaded and an exception
   * is thrown.
   */
  private class LazyDbMap {
    // Cache of Db metadata with a key of lower-case database name
    private final LoadingCache<String, Db> dbMetadataCache =
        CacheBuilder.newBuilder().build(
            new CacheLoader<String, Db>() {
              public Db load(String dbName) throws DatabaseNotFoundException {
                return loadDb(dbName);
              }
            });

    // Map of lower-case database names to their metadata load state. It is only possible
    // to load metadata for databases that exist in this map.
    private final ConcurrentMap<String, MetadataLoadState> dbNameMap = new MapMaker()
        .makeMap();

    /**
     * Initializes the class with a list of valid database names and marks each
     * database's metadata as uninitialized.
     */
    public LazyDbMap(List<String> dbNames) {
      for (String dbName: dbNames) {
        dbNameMap.put(dbName.toLowerCase(), MetadataLoadState.UNINITIALIZED);
      }
    }

    /**
     * Invalidate the metadata for the given db name and marks the db metadata load
     * state as uninitialized. Invalidating the metadata will cause the next access to
     * the db to reload (synchronize) its metadata from the metastore. 
     * If ifExists is true, this will only invalidate if the db name already exists in
     * the dbNameMap. If ifExists is false, the db metadata will be invalidated and the
     * metadata state will be set as UNINITIALIZED (potentially adding a new item to the
     * db name map).
     */
    public void invalidate(String dbName, boolean ifExists) {
      dbName = dbName.toLowerCase();
      if (ifExists) {
        if (dbNameMap.replace(dbName, MetadataLoadState.UNINITIALIZED) != null) {
          // TODO: Should we always invalidate the metadata cache even if the db
          // doesn't exist in the db name map?
          dbMetadataCache.invalidate(dbName);
        }
      } else {
        dbNameMap.put(dbName, MetadataLoadState.UNINITIALIZED);
        dbMetadataCache.invalidate(dbName);
      }
    }

    /**
     * Removes the database from the metadata cache 
     */
    public void remove(String dbName) {
      dbName = dbName.toLowerCase();
      dbNameMap.remove(dbName);
      dbMetadataCache.invalidate(dbName);
    }

    /**
     * Returns all known database names.
     */
    public Set<String> getAllDbNames() {
      return dbNameMap.keySet();
    }

    /**
     * Returns the Db object corresponding to the supplied database name. The database
     * name must exist in the database name map for the metadata load to succeed. Returns
     * null if the database does not exist.
     *
     * The exact behavior is:
     * - If the database already exists in the metadata cache, its value will be returned.
     * - If the database is not present in the metadata cache AND the database exists in
     *   the known database map the metadata will be loaded
     * - If the database is not present the database name map, null is returned.
     */
    public Db get(String dbName) {
      try {
        return dbMetadataCache.get(dbName.toLowerCase());
      } catch (ExecutionException e) {
        // Search for the cause of the exception. If a load failed due to the database not
        // being found, callers should get 'null' instead of having to handle the
        // exception.
        Throwable cause = e.getCause();
        while(cause != null) {
          if (cause instanceof DatabaseNotFoundException) {
            return null;
          }
          cause = cause.getCause();
        }
        throw new IllegalStateException(e);
      }
    }

    private Db loadDb(String dbName) throws DatabaseNotFoundException {
      dbName = dbName.toLowerCase();
      MetadataLoadState metadataState = dbNameMap.get(dbName);

      // This database doesn't exist in the database name cache. Throw an exception.
      if (metadataState == null) {
        throw new DatabaseNotFoundException("Database not found: " + dbName);
      }

      // We should never have a case where we make it here and the metadata is marked
      // as already loaded.
      Preconditions.checkState(metadataState != MetadataLoadState.LOADED);
      MetaStoreClient msClient = getMetaStoreClient();
      Db db = null;
      try {
        db = Db.loadDb(Catalog.this, msClient.getHiveClient(), dbName, lazy);
      } finally {
        msClient.release();
      }

      // Mark the metadata as loaded. If the database was removed while loading then
      // throw a DatbaseNotFoundException.
      if (dbNameMap.replace(dbName, MetadataLoadState.LOADED) == null) {
        throw new DatabaseNotFoundException("Database not found: " + dbName);
      }
      return db;
    }
  }

  /**
   * Thrown by some methods when a table can't be found in the metastore
   */
  public static class TableNotFoundException extends ImpalaException {
    // Dummy serial UID to avoid warnings
    private static final long serialVersionUID = -2203080667446640542L;

    public TableNotFoundException(String s) { super(s); }

    public TableNotFoundException(String s, Exception cause) { super(s, cause); }
  }

  /**
   * Thrown by some methods when a database is not found in the metastore
   */
  public static class DatabaseNotFoundException extends ImpalaException {
    // Dummy serial ID to satisfy Eclipse
    private static final long serialVersionUID = -2203080667446640542L;

    public DatabaseNotFoundException(String s) { super(s); }
  }


  public Catalog() {
    this(true, true);
  }

  /**
   * If lazy is true, tables are loaded on read, otherwise they are loaded eagerly in
   * the constructor. If raiseExceptions is false, exceptions will be logged and
   * swallowed. Otherwise, exceptions are re-raised.
   */
  public Catalog(boolean lazy, boolean raiseExceptions) {
    this.nextTableId = 0;
    this.lazy = lazy;

    MetaStoreClientPool clientPool = null;
    LazyDbMap dbMap = null;
    try {
      clientPool = new MetaStoreClientPool(META_STORE_CLIENT_POOL_SIZE);
      MetaStoreClient msClient = clientPool.getClient();

      try {
        dbMap = new LazyDbMap(msClient.getHiveClient().getAllDatabases());
      } finally {
        msClient.release();
      }

      if (!lazy) {
        // Load all the metadata
        for (String dbName: dbMap.getAllDbNames()) {
          dbMap.get(dbName);
        }
      }
    } catch (Exception e) {
      if (raiseExceptions) {
        // If exception is already an IllegalStateException, don't wrap it.
        if (e instanceof IllegalStateException) {
          throw (IllegalStateException) e;
        }
        throw new IllegalStateException(e);
      }
      
      LOG.error(e);
      LOG.error("Error initializing Catalog. Catalog may be empty.");
    }

    metaStoreClientPool = clientPool == null ? new MetaStoreClientPool(0) : clientPool;
    dbs = dbMap == null ? new LazyDbMap(new ArrayList<String>()) : dbMap;
  }

  /**
   * Release the Hive Meta Store Client resources. Can be called multiple times
   * (additional calls will be no-ops).
   */
  public void close() {
    metaStoreClientPool.close();
  }

  /**
   * Creates a new database in the metastore and adds the db name to the internal
   * metadata cache, marking its metadata to be lazily loaded on the next access.
   * Re-throws any Hive Meta Store exceptions encountered during the create, these
   * may vary depending on the Meta Store connection type (thrift vs direct db).
   *
   * @param dbName - The name of the new database.
   * @param comment - Comment to attach to the database, or null for no comment.
   * @param location - Hdfs path to use as the default location for new table data or
   *                   null to use default location.
   * @param ifNotExists - If true, no errors are thrown if the database already exists
   */
  public void createDatabase(String dbName, String comment, String location,
      boolean ifNotExists) throws MetaException, AlreadyExistsException,
      InvalidObjectException, org.apache.thrift.TException {
    Preconditions.checkState(dbName != null && !dbName.isEmpty(),
        "Null or empty database name passed as argument to Catalog.createDatabase");
    if (ifNotExists && getDb(dbName) != null) {
      LOG.info("Skipping database creation because " + dbName + " already exists and " +
          "ifNotExists is true.");
      return;
    }
    org.apache.hadoop.hive.metastore.api.Database db =
        new org.apache.hadoop.hive.metastore.api.Database();
    db.setName(dbName);
    if (comment != null) {
      db.setDescription(comment);
    }
    if (location != null) {
      db.setLocationUri(location);
    }
    LOG.info("Creating database " + dbName);
    synchronized (metastoreCreateDropLock) {
      try {
        getMetaStoreClient().getHiveClient().createDatabase(db);
      } catch (AlreadyExistsException e) {
        if (!ifNotExists) {
          throw e;
        }
        LOG.info(String.format("Ignoring '%s' when creating database %s because " +
            "ifNotExists is true.", e, dbName));
      }
      dbs.invalidate(dbName, false);
    }
  }

  /**
   * Drops a database from the metastore and removes the database's metadata from the
   * internal cache. The database must be empty (contain no tables) for the drop operation
   * to succeed. Re-throws any Hive Meta Store exceptions encountered during the drop.
   *
   * @param dbName - The name of the database to drop
   * @param ifExists - If true, no errors will be thrown if the database does not exist.
   */
  public void dropDatabase(String dbName, boolean ifExists)
      throws MetaException, NoSuchObjectException, InvalidOperationException,
      org.apache.thrift.TException {
    Preconditions.checkState(dbName != null && !dbName.isEmpty(),
        "Null or empty database name passed as argument to Catalog.dropDatabase");
    LOG.info("Dropping database " + dbName);
    synchronized (metastoreCreateDropLock) {
      getMetaStoreClient().getHiveClient().dropDatabase(dbName, false, ifExists);
      dbs.remove(dbName);
    }
  }

  /**
   * Creates a new table in the metastore and adds the table name to the internal
   * metadata cache, marking its metadata to be lazily loaded on the next access.
   * Re-throws any Hive Meta Store exceptions encountered during the drop.
   *
   * @param dbName - The database that contains this table.
   * @param tableName - The name of the table to drop.
   * @param ifExists - If true, no errors will be thrown if the table does not exist.
   */
  public void dropTable(String dbName, String tableName, boolean ifExists)
      throws MetaException, NoSuchObjectException, InvalidOperationException,
      org.apache.thrift.TException {
    Preconditions.checkState(dbName != null && !dbName.isEmpty(),
        "Null or empty database name passed as argument to Catalog.dropTable");
    Preconditions.checkState(tableName != null && !tableName.isEmpty(),
        "Null or empty table name passed as argument to Catalog.dropTable");
    LOG.info(String.format("Dropping table %s.%s", dbName, tableName));
    synchronized (metastoreCreateDropLock) {
      getMetaStoreClient().getHiveClient().dropTable(dbName, tableName, true, ifExists);
      dbs.get(dbName).removeTable(tableName);
    }
  }

  /**
   * Creates a new table in the metastore and adds an entry to the metadata cache to
   * lazily load the new metadata on the next access. Re-throws any Hive Meta Store
   * exceptions encountered during the create.
   *
   * @param dbName - Database to create the table within.
   * @param tableName - Name of the new table.
   * @param column - List of column definitions for the new table.
   * @param partitionColumn - List of partition column definitions for the new table.
   * @param isExternal 
   *    If true, table is created as external which means the data will be persisted
   *    if dropped. External tables can also be created on top of existing data.
   * @param comment - Optional comment to attach to the table (null for no comment).
   * @param location - Hdfs path to use as the location for table data or null to use
   *                   default location.
   */
  public void createTable(String dbName, String tableName, List<TColumnDef> columns,
      List<TColumnDef> partitionColumns, boolean isExternal, String comment,
      RowFormat rowFormat, FileFormat fileFormat, String location, boolean ifNotExists)
      throws MetaException, NoSuchObjectException, AlreadyExistsException,
      InvalidObjectException, org.apache.thrift.TException {
    Preconditions.checkState(tableName != null && !tableName.isEmpty(),
        "Null or empty table name given as argument to Catalog.createTable");
    Preconditions.checkState(columns != null && columns.size() > 0,
        "Null or empty column list given as argument to Catalog.createTable");
    if (ifNotExists && containsTable(dbName, tableName)) {
      LOG.info(String.format("Skipping table creation because %s.%s already exists and " +
          "ifNotExists is true.", dbName, tableName));
      return;
    }
    org.apache.hadoop.hive.metastore.api.Table tbl =
        new org.apache.hadoop.hive.metastore.api.Table();
    tbl.setDbName(dbName);
    tbl.setTableName(tableName);
    tbl.setParameters(new HashMap<String, String>());

    if (comment != null) {
      tbl.getParameters().put("comment", comment);
    }
    if (isExternal) {
      tbl.setTableType(TableType.EXTERNAL_TABLE.toString());
      tbl.putToParameters("EXTERNAL", "TRUE");
    }

    StorageDescriptor sd = HiveStorageDescriptorFactory.createSd(fileFormat, rowFormat);
    if (location != null) {
      sd.setLocation(location);
    }
    List<FieldSchema> fsList = Lists.newArrayList();
    // Add in all the columns
    sd.setCols(buildFieldSchemaList(columns));
    tbl.setSd(sd);
    if (partitionColumns != null) {
      // Add in any partition keys that were specified
      tbl.setPartitionKeys(buildFieldSchemaList(partitionColumns));
    }

    LOG.info(String.format("Creating table %s.%s", dbName, tableName));
    synchronized (metastoreCreateDropLock) {
      try {
        getMetaStoreClient().getHiveClient().createTable(tbl);
      } catch (AlreadyExistsException e) {
        if (!ifNotExists) {
          throw e;
        }
        LOG.info(String.format("Ignoring '%s' when creating table %s.%s because " +
            "ifNotExists is true.", e, dbName, tableName));
      }
      dbs.get(dbName).invalidateTable(tableName, false);
    }
  }

  private static List<FieldSchema> buildFieldSchemaList(List<TColumnDef> columnDefs) {
    List<FieldSchema> fsList = Lists.newArrayList();
    // Add in all the columns
    for (TColumnDef c: columnDefs) {
      TColumnDesc colDesc = c.getColumnDesc();
      FieldSchema fs = new FieldSchema(colDesc.getColumnName(),
          colDesc.getColumnType().toString().toLowerCase(), c.getComment());
      fsList.add(fs);
    }
    return fsList;
  }

  public TableId getNextTableId() {
    return new TableId(nextTableId++);
  }

  /**
   * Returns a managed meta store client from the client connection pool.
   */
  public MetaStoreClient getMetaStoreClient() {
    return metaStoreClientPool.getClient();
  }

  /**
   * Case-insensitive lookup. Returns null if the database does not exist.
   */
  public Db getDb(String db) {
    Preconditions.checkState(db != null && !db.isEmpty(),
        "Null or empty database name given as argument to Catalog.getDb");
    return dbs.get(db);
  }

  /**
   * Returns a list of tables in the supplied database that match
   * tablePattern. See filterStringsByPattern for details of the pattern match
   * semantics.
   *
   * dbName must not be null. tablePattern may be null (and thus matches
   * everything).
   *
   * Table names are returned unqualified.
   */
  public List<String> getTableNames(String dbName, String tablePattern)
      throws DatabaseNotFoundException {
    Preconditions.checkNotNull(dbName);
    List<String> matchingTables = Lists.newArrayList();

    Db db = getDb(dbName);
    if (db == null) {
      throw new DatabaseNotFoundException("Database '" + dbName + "' not found");
    }
    return filterStringsByPattern(db.getAllTableNames(), tablePattern);
  }

  /**
   * Returns a list of databases that match dbPattern. See
   * filterStringsByPattern for details of the pattern match semantics.
   *
   * dbPattern may be null (and thus matches
   * everything).
   */
  public List<String> getDbNames(String dbPattern) {
    return filterStringsByPattern(dbs.getAllDbNames(), dbPattern);
  }

  /**
   * Returns a list of all known databases in the Catalog.
   */
  public List<String> getAllDbNames() {
    return getDbNames(null);
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
   * Returns true if the table and the database exist in the Impala Catalog. Returns
   * false if the database does not exist or the table does not exist. This will
   * not trigger a metadata load for the given table name.
   */
  public boolean containsTable(String dbName, String tableName) {
    Db db = getDb(dbName);
    return db != null && db.containsTable(tableName);
  }
}
