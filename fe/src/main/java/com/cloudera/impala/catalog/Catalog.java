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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.cloudera.impala.authorization.AuthorizationChecker;
import com.cloudera.impala.authorization.AuthorizationConfig;
import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.authorization.PrivilegeRequest;
import com.cloudera.impala.authorization.PrivilegeRequestBuilder;
import com.cloudera.impala.authorization.User;
import com.cloudera.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.thrift.TPartitionKeyValue;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Lists;

/**
 * Thread safe interface for reading and updating metadata stored in the Hive MetaStore.
 * This class caches db-, table- and column-related metadata. Metadata updates (via DDL
 * operations like CREATE and DROP) are currently serialized for simplicity.
 * Although this class is thread safe, it does not guarantee consistency with the
 * MetaStore. It is important to keep in mind that there may be external (potentially
 * conflicting) concurrent metastore updates occurring at any time. This class does
 * guarantee any MetaStore updates done via this class will be reflected consistently.
 */
public class Catalog {
  public static final String DEFAULT_DB = "default";
  private static final Logger LOG = Logger.getLogger(Catalog.class);
  private static final int META_STORE_CLIENT_POOL_SIZE = 5;
  //TODO: Make the reload interval configurable.
  private static final int AUTHORIZATION_POLICY_RELOAD_INTERVAL_SECS = 5 * 60;
  private final boolean lazy;
  private AtomicInteger nextTableId;
  private final MetaStoreClientPool metaStoreClientPool = new MetaStoreClientPool(0);
  // Cache of database metadata.
  private final CatalogObjectCache<Db> dbCache = new CatalogObjectCache<Db>(
      new CacheLoader<String, Db>() {
        @Override
        public Db load(String dbName) {
          MetaStoreClient msClient = getMetaStoreClient();
          try {
            return Db.loadDb(Catalog.this, msClient.getHiveClient(),
                dbName.toLowerCase(), lazy);
          } finally {
            msClient.release();
          }
        }
      });

  // All of the registered user functions. The key is the user facing name (e.g. "myUdf"),
  // and the values are all the overloaded variants (e.g. myUdf(double), myUdf(string))
  private HashMap<String, List<Udf>> udfs;

  private final ScheduledExecutorService policyReader =
      Executors.newScheduledThreadPool(1);
  private final AuthorizationConfig authzConfig;
  // Lock used to synchronize refreshing the AuthorizationChecker.
  private final ReentrantReadWriteLock authzCheckerLock = new ReentrantReadWriteLock();
  private AuthorizationChecker authzChecker;

  /**
   * If lazy is true, tables are loaded on read, otherwise they are loaded eagerly in
   * the constructor. If raiseExceptions is false, exceptions will be logged and
   * swallowed. Otherwise, exceptions are re-raised.
   */
  public Catalog(boolean lazy, boolean raiseExceptions,
      AuthorizationConfig authzConfig) {
    this.nextTableId = new AtomicInteger();
    this.lazy = lazy;
    this.authzConfig = authzConfig;
    this.authzChecker = new AuthorizationChecker(authzConfig);
    // If authorization is enabled, reload the policy on a regular basis.
    if (authzConfig.isEnabled()) {
      // Stagger the reads across nodes
      Random randomGen = new Random(UUID.randomUUID().hashCode());
      int delay = AUTHORIZATION_POLICY_RELOAD_INTERVAL_SECS + randomGen.nextInt(60);

      policyReader.scheduleAtFixedRate(
          new AuthorizationPolicyReader(authzConfig),
          delay, AUTHORIZATION_POLICY_RELOAD_INTERVAL_SECS, TimeUnit.SECONDS);
    }

    try {
      metaStoreClientPool.addClients(META_STORE_CLIENT_POOL_SIZE);
      MetaStoreClient msClient = metaStoreClientPool.getClient();

      try {
        dbCache.add(msClient.getHiveClient().getAllDatabases());
      } finally {
        msClient.release();
      }

      if (!lazy) {
        // Load all the metadata
        for (String dbName: dbCache.getAllNames()) {
          dbCache.get(dbName);
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

    loadUdfs();
  }

  public Catalog() {
    this(true, true, AuthorizationConfig.createAuthDisabledConfig());
  }

  private class AuthorizationPolicyReader implements Runnable {
    private final AuthorizationConfig config;

    public AuthorizationPolicyReader(AuthorizationConfig config) {
      this.config = config;
    }

    public void run() {
      LOG.info("Reloading authorization policy file from: " + config.getPolicyFile());
      authzCheckerLock.writeLock().lock();
      try {
        authzChecker = new AuthorizationChecker(config);
      } finally {
        authzCheckerLock.writeLock().unlock();
      }
    }
  }

  /**
   * Adds a database name to the metadata cache and marks the metadata as
   * uninitialized. Used by CREATE DATABASE statements.
   */
  public void addDb(String dbName) {
    dbCache.add(dbName);
  }

  /**
   * Removes a database from the metadata cache. Used by DROP DATABASE statements.
   */
  public void removeDb(String dbName) {
    dbCache.remove(dbName);
  }

  /**
   * Loads the registered UDFs to the catalog. This shouldn't take very long
   * so we can probably just do this up front.
   */
  public void loadUdfs() {
    udfs = new HashMap<String, List<Udf>>();
    // TODO: figure out how to persist udfs and load this now.
  }

  /**
   * Adds a UDF to the catalog.
   * Returns true if the UDF was successfully added.
   * Returns false if the UDF already exists or the user doesn't have create
   * permissions.
   */
  public boolean addUdf(Udf udf, User user) {
    // TODO: add this to persistent store
    synchronized (udfs) {
      Udf fn = getUdf(udf.getDesc(), user, Privilege.CREATE, true);
      if (fn != null) return false;
      List<Udf> functions = udfs.get(udf.getDesc().getName());
      if (functions == null) {
        functions = Lists.newArrayList();
        udfs.put(udf.getDesc().getName(), functions);
      }
      functions.add(udf);
    }
    return true;
  }

  /**
   * Removes a UDF from the catalog. Returns true if the UDF was removed.
   */
  public boolean removeUdf(Function desc, User user) {
    // TODO: remove this from persistent store.
    synchronized (udfs) {
      Udf udf = getUdf(desc, user, Privilege.DROP, true);
      if (udf == null) return false;
      List<Udf> functions = udfs.get(desc.getName());
      Preconditions.checkNotNull(functions);
      boolean exists = functions.remove(udf);
      if (functions.isEmpty()) {
        udfs.remove(desc.getName());
      }
      return exists;
    }
  }

  /**
   * Returns the function that best matches 'desc' that is registered with the
   * catalog. If exactMatch is true, only function signatures that are identical
   * will be returned. If exactMatch is false, an arbitrary compatible
   * (i.e. implicitly castable) function will be returned.
   */
  public Udf getUdf(Function desc, User user,
      Privilege privilege, boolean exactMatch) {
    // TODO: authorization
    synchronized (udfs) {
      List<Udf> functions = udfs.get(desc.getName());
      if (functions == null) return null;
      for (Udf f: functions) {
        if (desc.equals(f.getDesc())) return f;
      }
      if (exactMatch) return null;
      for (Udf f: functions) {
        if (desc.isCompatible(f.getDesc())) return f;
      }
    }
    return null;
  }

  /**
   * Returns all the UDFs visible to this user.
   */
  public List<String> getUdfNames(String pattern, User user) {
    List<String> names = Lists.newArrayList();
    synchronized (udfs) {
      for (List<Udf> functions: udfs.values()) {
        for (Udf f: functions) {
          names.add(f.getDesc().signatureString());
        }
      }
    }
    // TODO: authorization
    names = filterStringsByPattern(names, pattern);
    return names;
  }

  /**
   * Release the Hive Meta Store Client resources. Can be called multiple times
   * (additional calls will be no-ops).
   */
  public void close() {
    metaStoreClientPool.close();
  }

  public TableId getNextTableId() {
    return new TableId(nextTableId.getAndIncrement());
  }

  /**
   * Returns a managed meta store client from the client connection pool.
   */
  public MetaStoreClient getMetaStoreClient() {
    return metaStoreClientPool.getClient();
  }

  /**
   * Checks whether a given user has sufficient privileges to access an authorizeable
   * object.
   * @throws AuthorizationException - If the user does not have sufficient privileges.
   */
  public void checkAccess(User user, PrivilegeRequest privilegeRequest)
      throws AuthorizationException {
    Preconditions.checkNotNull(user);
    Preconditions.checkNotNull(privilegeRequest);

    if (!hasAccess(user, privilegeRequest)) {
      Privilege privilege = privilegeRequest.getPrivilege();
      if (EnumSet.of(Privilege.ANY, Privilege.ALL, Privilege.VIEW_METADATA)
          .contains(privilege)) {
        throw new AuthorizationException(String.format(
            "User '%s' does not have privileges to access: %s",
            user.getName(), privilegeRequest.getName()));
      } else {
        throw new AuthorizationException(String.format(
            "User '%s' does not have privileges to execute '%s' on: %s",
            user.getName(), privilege, privilegeRequest.getName()));
      }
    }
  }

  private boolean hasAccess(User user, PrivilegeRequest request) {
    authzCheckerLock.readLock().lock();
    try {
      Preconditions.checkNotNull(authzChecker);
      return authzChecker.hasAccess(user, request);
    } finally {
      authzCheckerLock.readLock().unlock();
    }
  }

  /**
   * Gets the Db object from the Catalog using a case-insensitive lookup on the name.
   * Returns null if no matching database is found.
   */
  private Db getDbInternal(String dbName) {
    Preconditions.checkState(dbName != null && !dbName.isEmpty(),
        "Null or empty database name given as argument to Catalog.getDb");
    try {
      return dbCache.get(dbName);
    } catch (ImpalaException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Gets the Db object from the Catalog using a case-insensitive lookup on the name.
   * Returns null if no matching database is found. Throws an AuthorizationException
   * if the given user doesn't have enough privileges to access the database.
   */
  public Db getDb(String dbName, User user, Privilege privilege)
      throws AuthorizationException {
    Preconditions.checkState(dbName != null && !dbName.isEmpty(),
        "Null or empty database name given as argument to Catalog.getDb");
    PrivilegeRequestBuilder pb = new PrivilegeRequestBuilder();
    if (privilege == Privilege.ANY) {
      checkAccess(user, pb.any().onAnyTable(dbName).toRequest());
    } else {
      checkAccess(user, pb.allOf(privilege).onDb(dbName).toRequest());
    }
    return getDbInternal(dbName);
  }

  /**
   * Returns a list of tables in the supplied database that match
   * tablePattern and the user has privilege to access. See filterStringsByPattern
   * for details of the pattern match semantics.
   *
   * dbName must not be null. tablePattern may be null (and thus matches
   * everything).
   *
   * User is the user from the current session or ImpalaInternalUser for internal
   * metadata requests (for example, populating the debug webpage Catalog view).
   *
   * Table names are returned unqualified.
   */
  public List<String> getTableNames(String dbName, String tablePattern, User user)
      throws DatabaseNotFoundException {
    Preconditions.checkNotNull(dbName);

    Db db = getDbInternal(dbName);
    if (db == null) {
      throw new DatabaseNotFoundException("Database '" + dbName + "' not found");
    }

    List<String> tables = filterStringsByPattern(db.getAllTableNames(), tablePattern);
    if (authzConfig.isEnabled()) {
      Iterator<String> iter = tables.iterator();
      while (iter.hasNext()) {
        PrivilegeRequest privilegeRequest = new PrivilegeRequestBuilder()
            .allOf(Privilege.ANY).onTable(dbName, iter.next()).toRequest();
        if (!hasAccess(user, privilegeRequest)) {
          iter.remove();
        }
      }
    }
    return tables;
  }

  /**
   * Returns a list of databases that match dbPattern and the user has privilege to
   * access. See filterStringsByPattern for details of the pattern match semantics.
   *
   * dbPattern may be null (and thus matches everything).
   *
   * User is the user from the current session or ImpalaInternalUser for internal
   * metadata requests (for example, populating the debug webpage Catalog view).
   */
  public List<String> getDbNames(String dbPattern, User user) {
    List<String> matchingDbs = filterStringsByPattern(dbCache.getAllNames(), dbPattern);

    // If authorization is enabled, filter out the databases the user does not
    // have permissions on.
    if (authzConfig.isEnabled()) {
      Iterator<String> iter = matchingDbs.iterator();
      while (iter.hasNext()) {
        String dbName = iter.next();
        PrivilegeRequest request = new PrivilegeRequestBuilder()
            .any().onAnyTable(dbName).toRequest();
        if (!hasAccess(user, request)) {
          iter.remove();
        }
      }
    }
    return matchingDbs;
  }

  /**
   * Returns a list of all known databases in the Catalog that the given user
   * has privileges to access.
   */
  public List<String> getAllDbNames(User user) {
    return getDbNames(null, user);
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

  private boolean containsTable(String dbName, String tableName) {
    Db db = getDbInternal(dbName);
    return (db == null) ? false : db.containsTable(tableName);
  }

  /**
   * Returns true if the table and the database exist in the Impala Catalog. Returns
   * false if either the table or the database do not exist. This will
   * not trigger a metadata load for the given table name.
   * @throws AuthorizationException - If the user does not have sufficient privileges.
   */
  public boolean containsTable(String dbName, String tableName, User user,
      Privilege privilege) throws AuthorizationException {
    // Make sure the user has privileges to check if the table exists.
    checkAccess(user, new PrivilegeRequestBuilder()
        .allOf(privilege).onTable(dbName, tableName).toRequest());
    return containsTable(dbName, tableName);
  }

  /**
   * Returns true if the table and the database exist in the Impala Catalog. Returns
   * false if the database does not exist or the table does not exist. This will
   * not trigger a metadata load for the given table name.
   * @throws AuthorizationException - If the user does not have sufficient privileges.
   * @throws DatabaseNotFoundException - If the database does not exist.
   */
  public boolean dbContainsTable(String dbName, String tableName, User user,
      Privilege privilege) throws AuthorizationException, DatabaseNotFoundException {
    // Make sure the user has privileges to check if the table exists.
    checkAccess(user, new PrivilegeRequestBuilder()
        .allOf(privilege).onTable(dbName, tableName).toRequest());
    Db db = getDbInternal(dbName);
    if (db == null) {
      throw new DatabaseNotFoundException("Database not found: " + dbName);
    }
    return db.containsTable(tableName);
  }

  /**
   * Returns true if the table contains the given partition spec, otherwise false.
   * This may will trigger a metadata load if the table metadata is not yet cached.
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
   * Returns the Table object for the given dbName/tableName. This will trigger a
   * metadata load if the table metadata is not yet cached.
   * @throws DatabaseNotFoundException - If the database does not exist.
   * @throws TableNotFoundException - If the table does not exist.
   * @throws TableLoadingException - If there is an error loading the table metadata.
   */
  private Table getTableInternal(String dbName, String tableName) throws
      DatabaseNotFoundException, TableNotFoundException, TableLoadingException {
    Db db = getDbInternal(dbName);
    if (db == null) {
      throw new DatabaseNotFoundException("Database not found: " + dbName);
    }
    Table table = db.getTable(tableName);
    if (table == null) {
      throw new TableNotFoundException(
          String.format("Table not found: %s.%s", dbName, tableName));
    }
    return table;
  }

  /**
   * Returns the Table object for the given dbName/tableName. This will trigger a
   * metadata load if the table metadata is not yet cached.
   * @throws DatabaseNotFoundException - If the database does not exist.
   * @throws TableNotFoundException - If the table does not exist.
   * @throws TableLoadingException - If there is an error loading the table metadata.
   * @throws AuthorizationException - If the user does not have sufficient privileges.
   */
  public Table getTable(String dbName, String tableName, User user,
      Privilege privilege) throws DatabaseNotFoundException, TableNotFoundException,
      TableLoadingException, AuthorizationException {
    checkAccess(user, new PrivilegeRequestBuilder()
        .allOf(privilege).onTable(dbName, tableName).toRequest());
    return getTableInternal(dbName, tableName);
  }

  /**
   * Returns the HdfsPartition oject for the given dbName/tableName and partition spec.
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
    Table table = getTableInternal(dbName, tableName);
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
   * Returns the HDFS path where the metastore would create the given table. If the table
   * has a "location" set, that will be returned. Otherwise the path will be resolved
   * based on the location of the parent database. The metastore folder hierarchy is:
   * <warehouse directory>/<db name>.db/<table name>
   * Except for items in the default database which will be:
   * <warehouse directory>/<table name>
   * This method handles both of these cases.
   */
  public Path getTablePath(org.apache.hadoop.hive.metastore.api.Table msTbl)
      throws NoSuchObjectException, MetaException, TException {
    MetaStoreClient client = getMetaStoreClient();
    try {
      // If the table did not have its path set, build the path based on the the
      // location property of the parent database.
      if (msTbl.getSd().getLocation() == null || msTbl.getSd().getLocation().isEmpty()) {
        String dbLocation =
            client.getHiveClient().getDatabase(msTbl.getDbName()).getLocationUri();
        return new Path(dbLocation, msTbl.getTableName().toLowerCase());
      } else {
        return new Path(msTbl.getSd().getLocation());
      }
    } finally {
      client.release();
    }
  }
}
