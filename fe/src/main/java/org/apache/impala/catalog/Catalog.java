// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.catalog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.impala.analysis.FunctionName;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TFunction;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TPrincipalType;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.util.PatternMatcher;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Thread safe interface for reading and updating metadata stored in the Hive MetaStore.
 * This class provides a storage API for caching CatalogObjects: databases, tables,
 * and functions and the relevant metadata to go along with them. Although this class is
 * thread safe, it does not guarantee consistency with the MetaStore. It is important
 * to keep in mind that there may be external (potentially conflicting) concurrent
 * metastore updates occurring at any time.
 * The CatalogObject storage hierarchy is:
 * Catalog -> Db -> Table
 *               -> Function
 * Each level has its own synchronization, so the cache of Dbs is synchronized and each
 * Db has a cache of tables which is synchronized independently.
 *
 * The catalog is populated with the impala builtins on startup. Builtins and user
 * functions are treated identically by the catalog. The builtins go in a specific
 * database that the user cannot modify.
 * Builtins are populated on startup in initBuiltins().
 */
public abstract class Catalog implements AutoCloseable {
  // Initial catalog version and ID.
  public final static long INITIAL_CATALOG_VERSION = 0L;
  public static final TUniqueId INITIAL_CATALOG_SERVICE_ID = new TUniqueId(0L, 0L);
  public static final String DEFAULT_DB = "default";

  private final MetaStoreClientPool metaStoreClientPool_;

  // Cache of authorization policy metadata. Populated from data retried from the
  // Sentry Service, if configured.
  protected AuthorizationPolicy authPolicy_ = new AuthorizationPolicy();

  // Thread safe cache of database metadata. Uses an AtomicReference so reset()
  // operations can atomically swap dbCache_ references.
  // TODO: Update this to use a CatalogObjectCache?
  protected AtomicReference<Map<String, Db>> dbCache_ =
      new AtomicReference<>(new ConcurrentHashMap<String, Db>());

  // Cache of data sources.
  protected final CatalogObjectCache<DataSource> dataSources_;

  // Cache of known HDFS cache pools. Allows for checking the existence
  // of pools without hitting HDFS.
  protected final CatalogObjectCache<HdfsCachePool> hdfsCachePools_ =
      new CatalogObjectCache<HdfsCachePool>(false);

  // Cache of authorization cache invalidation markers.
  protected final CatalogObjectCache<AuthzCacheInvalidation> authzCacheInvalidation_ =
      new CatalogObjectCache<>();

  /**
   * Creates a new instance of Catalog backed by a given MetaStoreClientPool.
   */
  public Catalog(MetaStoreClientPool metaStoreClientPool) {
    dataSources_ = new CatalogObjectCache<DataSource>();
    metaStoreClientPool_ = Preconditions.checkNotNull(metaStoreClientPool);
  }

  /**
   * Creates a Catalog instance with the default MetaStoreClientPool implementation.
   * Refer to MetaStoreClientPool class for more details.
   */
  public Catalog() {
    this(new MetaStoreClientPool(0, 0));
  }

  /**
   * Adds a new database to the catalog, replacing any existing database with the same
   * name.
   */
  public void addDb(Db db) {
    dbCache_.get().put(db.getName().toLowerCase(), db);
  }

  /**
   * Gets the Db object from the Catalog using a case-insensitive lookup on the name.
   * Returns null if no matching database is found.
   */
  public Db getDb(String dbName) {
    Preconditions.checkArgument(dbName != null && !dbName.isEmpty(),
        "Null or empty database name given as argument to Catalog.getDb");
    return dbCache_.get().get(dbName.toLowerCase());
  }

  /**
   * Removes a database from the metadata cache. Returns the value removed or null
   * if not database was removed as part of this operation. Used by DROP DATABASE
   * statements.
   */
  public Db removeDb(String dbName) {
    return dbCache_.get().remove(dbName.toLowerCase());
  }

  /**
   * Returns all databases that match 'matcher'.
   */
  public List<Db> getDbs(PatternMatcher matcher) {
    return filterCatalogObjectsByPattern(dbCache_.get().values(), matcher);
  }

  /**
   * Returns the Table object for the given dbName/tableName or null if the database or
   * table does not exist.
   */
  public Table getTableNoThrow(String dbName, String tableName) {
    Db db = getDb(dbName);
    if (db == null) return null;
    return db.getTable(tableName);
  }

  /**
   * Returns the Table object for the given dbName/tableName. Throws if the database
   * does not exists. Returns null if the table does not exist.
   * TODO: Clean up the inconsistent error behavior (throwing vs. returning null).
   */
  public Table getTable(String dbName, String tableName)
      throws DatabaseNotFoundException {
    Db db = getDb(dbName);
    if (db == null) {
      throw new DatabaseNotFoundException("Database '" + dbName + "' not found");
    }
    return db.getTable(tableName);
  }

  /**
   * Removes a table from the catalog and returns the table that was removed, or null
   * if the table/database does not exist.
   */
  public Table removeTable(TTableName tableName) {
    // Remove the old table name from the cache and add the new table.
    Db db = getDb(tableName.getDb_name());
    if (db == null) return null;
    Table tbl = db.removeTable(tableName.getTable_name());
    if (tbl != null && !tbl.isStoredInImpaladCatalogCache()) {
      CatalogUsageMonitor.INSTANCE.removeTable(tbl);
    }
    return tbl;
  }

  /**
   * Returns all tables in 'dbName' that match 'matcher'.
   *
   * dbName must not be null.
   *
   * Table names are returned unqualified.
   */
  public List<String> getTableNames(String dbName, PatternMatcher matcher)
      throws DatabaseNotFoundException {
    Preconditions.checkNotNull(dbName);
    Db db = getDb(dbName);
    if (db == null) {
      throw new DatabaseNotFoundException("Database '" + dbName + "' not found");
    }
    return filterStringsByPattern(db.getAllTableNames(), matcher);
  }

  /**
   * Returns true if the table and the database exist in the Impala Catalog. Returns
   * false if either the table or the database do not exist.
   */
  public boolean containsTable(String dbName, String tableName) {
    Db db = getDb(dbName);
    return (db == null) ? false : db.containsTable(tableName);
  }

  /**
   * Adds a data source to the in-memory map of data sources. It is not
   * persisted to the metastore.
   * @return true if this item was added or false if the existing value was preserved.
   */
  public boolean addDataSource(DataSource dataSource) {
    return dataSources_.add(dataSource);
  }

  /**
   * Removes a data source from the in-memory map of data sources.
   * @return the item that was removed if it existed in the cache, null otherwise.
   */
  public DataSource removeDataSource(String dataSourceName) {
    Preconditions.checkNotNull(dataSourceName);
    return dataSources_.remove(dataSourceName.toLowerCase());
  }

  /**
   * Gets the specified data source.
   */
  public DataSource getDataSource(String dataSourceName) {
    Preconditions.checkNotNull(dataSourceName);
    return dataSources_.get(dataSourceName.toLowerCase());
  }

  /**
   * Gets a list of all data sources.
   */
  public List<DataSource> getDataSources() {
    return dataSources_.getValues();
  }

  /**
   * Returns a list of data sources names that match pattern.
   *
   * @see PatternMatcher#matches(String) for details of the pattern match semantics.
   *
   * pattern may be null (and thus matches everything).
   */
  public List<String> getDataSourceNames(String pattern) {
    return filterStringsByPattern(dataSources_.keySet(),
        PatternMatcher.createHivePatternMatcher(pattern));
  }

  /**
   * Returns all DataSources that match 'matcher'.
   */
  public List<DataSource> getDataSources(PatternMatcher matcher) {
    return filterCatalogObjectsByPattern(dataSources_.getValues(), matcher);
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
    Db db = getDb(fn.dbName());
    if (db == null) return false;
    return db.addFunction(fn);
  }

  /**
   * Returns the function that best matches 'desc' that is registered with the
   * catalog using 'mode' to check for matching.
   * If desc matches multiple functions in the catalog, it will return the function with
   * the strictest matching mode.
   * If multiple functions match at the same matching mode, best match is defined as the
   * one that requires the least number of arguments to be converted.
   * Ties are broken by comparing argument types in lexical order. Argument types are
   * ordered by argument precision (e.g. double is preferred over float) and then by
   * alphabetical order of argument type name, to guarantee deterministic results.
   */
  public Function getFunction(Function desc, Function.CompareMode mode) {
    Db db = getDb(desc.dbName());
    if (db == null) return null;
    return db.getFunction(desc, mode);
  }

  /**
   * Removes a function from the catalog. Increments the catalog version and returns
   * the Function object that was removed if the function existed, otherwise returns
   * null.
   */
  public Function removeFunction(Function desc) {
    Db db = getDb(desc.dbName());
    if (db == null) return null;
    return db.removeFunction(desc);
  }

  /**
   * Returns true if there is a function with this function name. Parameters
   * are ignored.
   */
  public boolean containsFunction(FunctionName name) {
    Db db = getDb(name.getDb());
    if (db == null) return false;
    return db.containsFunction(name.getFunction());
  }

  /**
   * Adds a new HdfsCachePool to the catalog.
   */
  public boolean addHdfsCachePool(HdfsCachePool cachePool) {
    return hdfsCachePools_.add(cachePool);
  }

  /**
   * Gets a HdfsCachePool given a cache pool name. Returns null if the cache
   * pool does not exist.
   */
  public HdfsCachePool getHdfsCachePool(String poolName) {
    return hdfsCachePools_.get(poolName);
  }

  /**
   * Gets the {@link AuthzCacheInvalidation} for a given marker name.
   */
  public AuthzCacheInvalidation getAuthzCacheInvalidation(String markerName) {
    return authzCacheInvalidation_.get(Preconditions.checkNotNull(markerName));
  }

  /**
   * Release the Hive Meta Store Client resources. Can be called multiple times
   * (additional calls will be no-ops).
   */
  @Override
  public void close() { metaStoreClientPool_.close(); }

  /**
   * Returns a managed meta store client from the client connection pool.
   */
  public MetaStoreClient getMetaStoreClient() { return metaStoreClientPool_.getClient(); }

  /**
   * Return all members of 'candidates' that match 'matcher'.
   * The results are sorted in String.CASE_INSENSITIVE_ORDER.
   * matcher must not be null.
   */
  public static List<String> filterStringsByPattern(Iterable<String> candidates,
      PatternMatcher matcher) {
    Preconditions.checkNotNull(matcher);
    List<String> filtered = new ArrayList<>();
    for (String candidate: candidates) {
      if (matcher.matches(candidate)) filtered.add(candidate);
    }
    Collections.sort(filtered, String.CASE_INSENSITIVE_ORDER);
    return filtered;
  }

  private static class CatalogObjectOrder implements Comparator<HasName> {
    @Override
    public int compare(HasName o1, HasName o2) {
      return String.CASE_INSENSITIVE_ORDER.compare(o1.getName(), o2.getName());
    }
  }

  private static final CatalogObjectOrder CATALOG_OBJECT_ORDER = new CatalogObjectOrder();

  /**
   * Return all members of 'candidates' that match 'matcher'.
   * The results are sorted in CATALOG_OBJECT_ORDER.
   * matcher must not be null.
   */
  public static <T extends HasName> List<T> filterCatalogObjectsByPattern(
      Iterable<? extends T> candidates, PatternMatcher matcher) {
    Preconditions.checkNotNull(matcher);
    List<T> filtered = new ArrayList<>();
    for (T candidate: candidates) {
      if (matcher.matches(candidate.getName())) filtered.add(candidate);
    }
    Collections.sort(filtered, CATALOG_OBJECT_ORDER);
    return filtered;
  }

  public HdfsPartition getHdfsPartition(String dbName, String tableName,
      org.apache.hadoop.hive.metastore.api.Partition msPart) throws CatalogException {
    List<TPartitionKeyValue> partitionSpec = new ArrayList<>();
    Table table = getTable(dbName, tableName);
    if (!(table instanceof HdfsTable)) {
      throw new PartitionNotFoundException(
          "Not an HdfsTable: " + dbName + "." + tableName);
    }
    for (int i = 0; i < msPart.getValues().size(); ++i) {
      partitionSpec.add(new TPartitionKeyValue(
          ((HdfsTable)table).getColumns().get(i).getName(), msPart.getValues().get(i)));
    }
    return getHdfsPartition(table.getDb().getName(), table.getName(), partitionSpec);
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
      List<TPartitionKeyValue> partitionSpec) throws CatalogException {
    String partitionNotFoundMsg =
        "Partition not found: " + Joiner.on(", ").join(partitionSpec);
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
  }

  /**
   * Returns true if the table contains the given partition spec, otherwise false.
   * This may trigger a metadata load if the table metadata is not yet cached.
   * @throws DatabaseNotFoundException - If the database does not exist.
   * @throws TableNotFoundException - If the table does not exist.
   * @throws TableLoadingException - If there is an error loading the table metadata.
   */
  public boolean containsHdfsPartition(String dbName, String tableName,
      List<TPartitionKeyValue> partitionSpec) throws CatalogException {
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
        table.getLock().lock();
        try {
          result.setType(table.getCatalogObjectType());
          result.setCatalog_version(table.getCatalogVersion());
          result.setTable(table.toThrift());
        } finally {
          table.getLock().unlock();
        }
        break;
      }
      case FUNCTION: {
        TFunction tfn = objectDesc.getFn();
        Function desc = Function.fromThrift(tfn);
        Function fn = getFunction(desc, Function.CompareMode.IS_INDISTINGUISHABLE);
        if (fn == null) {
          throw new CatalogException("Function not found: " + tfn);
        }
        result.setType(fn.getCatalogObjectType());
        result.setCatalog_version(fn.getCatalogVersion());
        result.setFn(fn.toThrift());
        break;
      }
      case DATA_SOURCE: {
        String dataSrcName = objectDesc.getData_source().getName();
        DataSource dataSrc = getDataSource(dataSrcName);
        if (dataSrc == null) {
          throw new CatalogException("Data source not found: " + dataSrcName);
        }
        result.setType(dataSrc.getCatalogObjectType());
        result.setCatalog_version(dataSrc.getCatalogVersion());
        result.setData_source(dataSrc.toThrift());
        break;
      }
      case HDFS_CACHE_POOL: {
        HdfsCachePool pool = getHdfsCachePool(objectDesc.getCache_pool().getPool_name());
        if (pool == null) {
          throw new CatalogException(
              "Hdfs cache pool not found: " + objectDesc.getCache_pool().getPool_name());
        }
        result.setType(pool.getCatalogObjectType());
        result.setCatalog_version(pool.getCatalogVersion());
        result.setCache_pool(pool.toThrift());
        break;
      }
      case PRINCIPAL:
        Principal principal = authPolicy_.getPrincipal(
            objectDesc.getPrincipal().getPrincipal_name(),
            objectDesc.getPrincipal().getPrincipal_type());
        if (principal == null) {
          throw new CatalogException("Principal not found: " +
              objectDesc.getPrincipal().getPrincipal_name());
        }
        result.setType(principal.getCatalogObjectType());
        result.setCatalog_version(principal.getCatalogVersion());
        result.setPrincipal(principal.toThrift());
        break;
      case PRIVILEGE:
        Principal tmpPrincipal = authPolicy_.getPrincipal(
            objectDesc.getPrivilege().getPrincipal_id(),
            objectDesc.getPrivilege().getPrincipal_type());
        if (tmpPrincipal == null) {
          throw new CatalogException(String.format("No %s associated with ID: %d",
              Principal.toString(objectDesc.getPrivilege().getPrincipal_type())
                  .toLowerCase(), objectDesc.getPrivilege().getPrincipal_id()));
        }
        String privilegeName = PrincipalPrivilege.buildPrivilegeName(
            objectDesc.getPrivilege());
        PrincipalPrivilege privilege = tmpPrincipal.getPrivilege(privilegeName);
        if (privilege != null) {
          result.setType(privilege.getCatalogObjectType());
          result.setCatalog_version(privilege.getCatalogVersion());
          result.setPrivilege(privilege.toThrift());
          return result;
        }
        throw new CatalogException(String.format("%s '%s' does not contain " +
            "privilege: '%s'", Principal.toString(tmpPrincipal.getPrincipalType()),
            tmpPrincipal.getName(), privilegeName));
      case AUTHZ_CACHE_INVALIDATION:
        AuthzCacheInvalidation authzCacheInvalidation = getAuthzCacheInvalidation(
            objectDesc.getAuthz_cache_invalidation().getMarker_name());
        if (authzCacheInvalidation == null) {
          // Authorization cache invalidation requires a single catalog object and it
          // needs to exist.
          throw new CatalogException("Authz cache invalidation not found: " +
              objectDesc.getAuthz_cache_invalidation().getMarker_name());
        }
        result.setType(authzCacheInvalidation.getCatalogObjectType());
        result.setCatalog_version(authzCacheInvalidation.getCatalogVersion());
        result.setAuthz_cache_invalidation(authzCacheInvalidation.toThrift());
        break;
      default: throw new IllegalStateException(
          "Unexpected TCatalogObject type: " + objectDesc.getType());
    }
    return result;
  }

  public static boolean isDefaultDb(String dbName) {
    return DEFAULT_DB.equals(dbName.toLowerCase());
  }

  /**
   * Returns a unique string key of a catalog object.
   *
   * This method may initially seem counter-intuitive because Catalog::getUniqueName()
   * uses this method to build a unique name instead of Catalog::getUniqueName()
   * providing the implementation on how to build a catalog object key. The reason is
   * building CatalogObject from TCatalogObject in order to call getUniqueName() can
   * be an expensive operation, especially for constructing a Table catalog object
   * from TCatalogObject.
   */
  public static String toCatalogObjectKey(TCatalogObject catalogObject) {
    Preconditions.checkNotNull(catalogObject);
    switch (catalogObject.getType()) {
      case DATABASE:
        return "DATABASE:" + catalogObject.getDb().getDb_name().toLowerCase();
      case TABLE:
      case VIEW:
        TTable tbl = catalogObject.getTable();
        return "TABLE:" + tbl.getDb_name().toLowerCase() + "." +
            tbl.getTbl_name().toLowerCase();
      case FUNCTION:
        return "FUNCTION:" + catalogObject.getFn().getName() + "(" +
            catalogObject.getFn().getSignature() + ")";
      case PRINCIPAL:
        // It is important to make the principal object key unique since it is possible
        // to have the same name for both role and user.
        String principalName = catalogObject.getPrincipal().getPrincipal_name();
        if (catalogObject.getPrincipal().getPrincipal_type() == TPrincipalType.ROLE) {
          principalName = principalName.toLowerCase();
        }
        return "PRINCIPAL:" + principalName + "." +
            catalogObject.getPrincipal().getPrincipal_type().name();
      case PRIVILEGE:
        // The combination of privilege name + principal ID + principal type is
        // guaranteed to be unique.
        return "PRIVILEGE:" +
            PrincipalPrivilege.buildPrivilegeName(catalogObject.getPrivilege()) + "." +
            catalogObject.getPrivilege().getPrincipal_id() + "." +
            catalogObject.getPrivilege().getPrincipal_type();
      case HDFS_CACHE_POOL:
        return "HDFS_CACHE_POOL:" +
            catalogObject.getCache_pool().getPool_name().toLowerCase();
      case DATA_SOURCE:
        return "DATA_SOURCE:" + catalogObject.getData_source().getName().toLowerCase();
      case AUTHZ_CACHE_INVALIDATION:
        return "AUTHZ_CACHE_INVALIDATION:" + catalogObject.getAuthz_cache_invalidation()
            .getMarker_name().toLowerCase();
      case CATALOG:
        return "CATALOG_SERVICE_ID";
      default:
        throw new IllegalStateException(
            "Unsupported catalog object type: " + catalogObject.getType());
    }
  }

  /**
   * Returns true if the two objects have the same object type and key (generated using
   * toCatalogObjectKey()).
   */
  public static boolean keyEquals(TCatalogObject first, TCatalogObject second) {
    return toCatalogObjectKey(first).equals(toCatalogObjectKey(second));
  }
}
