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

import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
import com.cloudera.impala.thrift.TCatalogObject;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TDataSource;
import com.cloudera.impala.thrift.TDatabase;
import com.cloudera.impala.thrift.TFunction;
import com.cloudera.impala.thrift.TPrivilege;
import com.cloudera.impala.thrift.TRole;
import com.cloudera.impala.thrift.TTable;
import com.cloudera.impala.thrift.TUniqueId;
import com.cloudera.impala.thrift.TUpdateCatalogCacheRequest;
import com.cloudera.impala.thrift.TUpdateCatalogCacheResponse;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Thread safe Catalog for an Impalad. The Impalad Catalog provides an interface to
 * access Catalog objects that this Impalad knows about and authorizes access requests
 * to these objects. It also manages reading and updating the authorization policy file
 * from HDFS. The Impalad Catalog provides APIs for checking whether a user is authorized
 * to access a particular catalog object. Any catalog access that requires privilege
 * checks should go through this class.
 * TODO: The CatalogService should also handle updating and disseminating the
 * authorization policy.
 * The Impalad catalog can be updated either via a StateStore heartbeat or by directly
 * applying the result of a catalog operation to the CatalogCache. All updates are
 * applied using the updateCatalog() function.
 * Table metadata is loaded lazily. The CatalogServer initially broadcasts (via the
 * statestore) the known table names (as IncompleteTables). These table names are added
 * to the Impalad catalog cache and when one of the tables is accessed, the impalad will
 * make an RPC to the CatalogServer to request loading the complete table metadata.
 * In both cases, we need to ensure that work from one update is not "undone" by another
 * update. To handle this the ImpaladCatalog does the following:
 * - Tracks the overall catalog version last received in a state store heartbeat, this
 *   version is maintained by the catalog server and it is always guaranteed that
 *   this impalad's catalog will never contain any objects < than this version
 *   (any updates with a lower version number are ignored).
 * - For updated/new objects, check if the object already exists in the
 *   catalog cache. If it does, only apply the update if the catalog version is > the
 *   existing object's catalog version. Also keep a log of all dropped catalog objects
 *   (and the version they were dropped in). Before updating any object, check if it was
 *   dropped in a later version. If so, ignore the update.
 * - Before dropping any catalog object, see if the object already exists in the catalog
 *   cache. If it does, only drop the object if the version of the drop is > that
 *   object's catalog version.
 * The CatalogServiceId is also tracked to detect if a different instance of the catalog
 * service has been started, in which case a full topic update is required.
 */
public class ImpaladCatalog extends Catalog {
  private static final Logger LOG = Logger.getLogger(ImpaladCatalog.class);
  private static final TUniqueId INITIAL_CATALOG_SERVICE_ID = new TUniqueId(0L, 0L);

  // The last known Catalog Service ID. If the ID changes, it indicates the CatalogServer
  // has restarted.
  private TUniqueId catalogServiceId_ = INITIAL_CATALOG_SERVICE_ID;

  // The catalog version received in the last StateStore heartbeat. It is guaranteed
  // all objects in the catalog have at a minimum, this version. Because updates may
  // be applied out of band of a StateStore heartbeat, it is possible the catalog
  // contains some objects > than this version.
  private long lastSyncedCatalogVersion_ = Catalog.INITIAL_CATALOG_VERSION;

  //TODO: Make the reload interval configurable.
  private static final int AUTHORIZATION_POLICY_RELOAD_INTERVAL_SECS = 5 * 60;

  private final ScheduledExecutorService policyReader_ =
      Executors.newScheduledThreadPool(1);
  private final AuthorizationConfig authzConfig_;
  // Lock used to synchronize refreshing the AuthorizationChecker.
  private final ReentrantReadWriteLock authzCheckerLock_ = new ReentrantReadWriteLock();
  // Visibility is protected for testing.
  protected AuthorizationChecker authzChecker_;

  // Flag to determine if the Catalog is ready to accept user requests. See isReady().
  private final AtomicBoolean isReady_ = new AtomicBoolean(false);

  // Tracks modifications to this Impalad's catalog from direct updates to the cache.
  private final CatalogDeltaLog catalogDeltaLog_ = new CatalogDeltaLog();

  // Object that is used to synchronize on and signal when a catalog update is received.
  private final Object catalogUpdateEventNotifier_ = new Object();

  /**
   * C'tor used by tests that need to validate the ImpaladCatalog outside of the
   * CatalogServer.
   */
  public ImpaladCatalog(AuthorizationConfig authzConfig) {
    super(false);
    authzConfig_ = authzConfig;
    authzChecker_ = new AuthorizationChecker(authzConfig, authPolicy_);
    if (authzConfig.isEnabled() && !Strings.isNullOrEmpty(authzConfig.getPolicyFile())) {
      // If file based authorization is enabled, reload the policy file on a regular
      // basis.

      // Stagger the reads across nodes
      Random randomGen = new Random(UUID.randomUUID().hashCode());
      int delay = AUTHORIZATION_POLICY_RELOAD_INTERVAL_SECS + randomGen.nextInt(60);
      policyReader_.scheduleAtFixedRate(
          new AuthorizationPolicyReader(authzConfig),
          delay, AUTHORIZATION_POLICY_RELOAD_INTERVAL_SECS, TimeUnit.SECONDS);
    }
  }

  private class AuthorizationPolicyReader implements Runnable {
    private final AuthorizationConfig config;

    public AuthorizationPolicyReader(AuthorizationConfig config) {
      this.config = config;
    }

    public void run() {
      LOG.info("Reloading authorization policy file from: " + config.getPolicyFile());
      authzCheckerLock_.writeLock().lock();
      try {
        authzChecker_ = new AuthorizationChecker(config, authPolicy_);
      } finally {
        authzCheckerLock_.writeLock().unlock();
      }
    }
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

  public void checkCreateDropFunctionAccess(User user) throws AuthorizationException {
    Preconditions.checkNotNull(user);
    if (!hasAccess(user, new PrivilegeRequest(Privilege.ALL))) {
      throw new AuthorizationException(String.format(
          "User '%s' does not have privileges to CREATE/DROP functions.",
          user.getName()));
    }
  }

  /**
   * Updates the internal Catalog based on the given TCatalogUpdateReq.
   * This method:
   * 1) Updates all databases in the Catalog
   * 2) Updates all tables, views, and functions in the Catalog
   * 3) Removes all dropped tables, views, and functions
   * 4) Removes all dropped databases
   *
   * This method is called once per statestore heartbeat and is guaranteed the same
   * object will not be in both the "updated" list and the "removed" list (it is
   * a detail handled by the statestore).
   * Catalog updates are ordered by the object type with the dependent objects coming
   * first. That is, database "foo" will always come before table "foo.bar".
   * Synchronized because updateCatalog() can be called by during a statestore update or
   * during a direct-DDL operation and catalogServiceId_ and lastSyncedCatalogVersion_
   * must be protected.
   */
  public synchronized TUpdateCatalogCacheResponse updateCatalog(
    TUpdateCatalogCacheRequest req) throws CatalogException {
    // Check for changes in the catalog service ID.
    if (!catalogServiceId_.equals(req.getCatalog_service_id())) {
      boolean firstRun = catalogServiceId_.equals(INITIAL_CATALOG_SERVICE_ID);
      catalogServiceId_ = req.getCatalog_service_id();
      if (!firstRun) {
        // Throw an exception which will trigger a full topic update request.
        throw new CatalogException("Detected catalog service ID change. Aborting " +
            "updateCatalog()");
      }
    }

    // First process all updates
    long newCatalogVersion = lastSyncedCatalogVersion_;
    for (TCatalogObject catalogObject: req.getUpdated_objects()) {
      if (catalogObject.getType() == TCatalogObjectType.CATALOG) {
        newCatalogVersion = catalogObject.getCatalog_version();
      } else {
        try {
          addCatalogObject(catalogObject);
        } catch (Exception e) {
          LOG.error("Error adding catalog object: " + e.getMessage(), e);
        }
      }
    }

    // Now remove all objects from the catalog. Removing a database before removing
    // its child tables/functions is fine. If that happens, the removal of the child
    // object will be a no-op.
    for (TCatalogObject catalogObject: req.getRemoved_objects()) {
      removeCatalogObject(catalogObject, newCatalogVersion);
    }
    lastSyncedCatalogVersion_ = newCatalogVersion;
    // Cleanup old entries in the log.
    catalogDeltaLog_.garbageCollect(lastSyncedCatalogVersion_);
    isReady_.set(true);

    // Notify all the threads waiting on a catalog update.
    synchronized (catalogUpdateEventNotifier_) {
      catalogUpdateEventNotifier_.notifyAll();
    }

    return new TUpdateCatalogCacheResponse(catalogServiceId_);
  }

  /**
   * Causes the calling thread to wait until a catalog update notification has been sent
   * or the given timeout has been reached. A timeout value of 0 indicates an indefinite
   * wait. Does not protect against spurious wakeups, so this should be called in a loop.
   *
   */
  public void waitForCatalogUpdate(long timeoutMs) {
    synchronized (catalogUpdateEventNotifier_) {
      try {
        catalogUpdateEventNotifier_.wait(timeoutMs);
      } catch (InterruptedException e) {
        // Ignore
      }
    }
  }

  /**
   * Gets the Db object from the Catalog using a case-insensitive lookup on the name.
   * Returns null if no matching database is found.
   */
  public Db getDb(String dbName, User user, Privilege privilege)
      throws AuthorizationException {
    Preconditions.checkState(dbName != null && !dbName.isEmpty(),
        "Null or empty database name given as argument to Catalog.getDb");
    if (checkSystemDbAccess(dbName, privilege)) return getDb(dbName);
    PrivilegeRequestBuilder pb = new PrivilegeRequestBuilder();
    if (privilege == Privilege.ANY) {
      checkAccess(user, pb.any().onAnyTable(dbName).toRequest());
    } else {
      checkAccess(user, pb.allOf(privilege).onDb(dbName).toRequest());
    }
    return getDb(dbName);
  }

  /**
   * Returns a list of databases that match dbPattern and the user has privilege to
   * access. See filterStringsByPattern for details of the pattern matching semantics.
   *
   * dbPattern may be null (and thus matches everything).
   *
   * User is the user from the current session or ImpalaInternalUser for internal
   * metadata requests (for example, populating the debug webpage Catalog view).
   */
  public List<String> getDbNames(String dbPattern, User user) {
    List<String> matchingDbs = getDbNames(dbPattern);

    // If authorization is enabled, filter out the databases the user does not
    // have permissions on.
    if (authzConfig_.isEnabled()) {
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
   * Returns true if the table and the database exist in the Catalog. Returns
   * false if the table does not exist in the database. Throws an exception if the
   * database does not exist.
   */
  public boolean dbContainsTable(String dbName, String tableName, User user,
      Privilege privilege) throws AuthorizationException, DatabaseNotFoundException {
    if (checkSystemDbAccess(dbName, privilege)) {
      return containsTable(dbName, tableName);
    }

    // Make sure the user has privileges to check if the table exists.
    checkAccess(user, new PrivilegeRequestBuilder()
        .allOf(privilege).onTable(dbName, tableName).toRequest());

    Db db = getDb(dbName);
    if (db == null) {
      throw new DatabaseNotFoundException("Database not found: " + dbName);
    }
    return db.containsTable(tableName);
  }

  /**
   * Returns the Table object for the given dbName/tableName. If the table is
   * found to be uninitialized (does not yet have its metadata loaded) then this
   * will attempt to load the table's metadata from the catalog server.
   */
  public Table getTable(String dbName, String tableName, User user,
      Privilege privilege) throws AuthorizationException, DatabaseNotFoundException,
      TableLoadingException {
    if (checkSystemDbAccess(dbName, privilege)) {
      return getTable(dbName, tableName);
    }
    checkAccess(user, new PrivilegeRequestBuilder()
        .allOf(privilege).onTable(dbName, tableName).toRequest());

    Table table = getTable(dbName, tableName);
    if (table == null) return null;

    if (table.isLoaded() && table instanceof IncompleteTable) {
      // If there were problems loading this table's metadata, throw an exception
      // when it is accessed.
      ImpalaException cause = ((IncompleteTable) table).getCause();
      if (cause instanceof TableLoadingException) throw (TableLoadingException) cause;
      throw new TableLoadingException("Missing metadata for table: " + tableName, cause);
    }
    return table;
  }

  /**
   * Returns true if the table and the database exist in the Impala Catalog. Returns
   * false if either the table or the database do not exist.
   */
  public boolean containsTable(String dbName, String tableName, User user,
      Privilege privilege) throws AuthorizationException {
    // Make sure the user has privileges to check if the table exists.
    checkAccess(user, new PrivilegeRequestBuilder()
        .allOf(privilege).onTable(dbName, tableName).toRequest());
    return containsTable(dbName, tableName);
  }

  /**
   * Returns a list of tables in the supplied database that match
   * tablePattern and the user has privilege to access. See filterStringsByPattern
   * for details of the pattern matching semantics.
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
    List<String> tables = getTableNames(dbName, tablePattern);
    if (authzConfig_.isEnabled()) {
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

  /**
   * Checks whether the given User has permission to perform the given request.
   * Returns true if the User has privileges, false if the User does not.
   */
  private boolean hasAccess(User user, PrivilegeRequest request) {
    authzCheckerLock_.readLock().lock();
    try {
      Preconditions.checkNotNull(authzChecker_);
      return authzChecker_.hasAccess(user, request);
    } finally {
      authzCheckerLock_.readLock().unlock();
    }
  }

  /**
   * Throws an authorization exception if the dbName is a system db
   * and p is trying to modify it.
   * Returns true if this is a system db and the action is allowed.
   */
  public boolean checkSystemDbAccess(String dbName, Privilege privilege)
      throws AuthorizationException {
    Db db = getDb(dbName);
    if (db != null && db.isSystemDb()) {
      switch (privilege) {
        case VIEW_METADATA:
        case ANY:
          return true;
        default:
          throw new AuthorizationException("Cannot modify system database.");
      }
    }
    return false;
  }

  /**
   *  Adds the given TCatalogObject to the catalog cache. The update may be ignored
   *  (considered out of date) if:
   *  1) An item exists in the catalog cache with a version > than the given
   *     TCatalogObject's version.
   *  2) The catalogDeltaLog_ contains an entry for this object with a version
   *     > than the given TCatalogObject's version.
   */
  private void addCatalogObject(TCatalogObject catalogObject)
      throws TableLoadingException, DatabaseNotFoundException {
    // This item is out of date and should not be applied to the catalog.
    if (catalogDeltaLog_.wasObjectRemovedAfter(catalogObject)) {
      LOG.debug(String.format("Skipping update because a matching object was removed " +
          "in a later catalog version: %s", catalogObject));
      return;
    }

    switch(catalogObject.getType()) {
      case DATABASE:
        addDb(catalogObject.getDb(), catalogObject.getCatalog_version());
        break;
      case TABLE:
      case VIEW:
        addTable(catalogObject.getTable(), catalogObject.getCatalog_version());
        break;
      case FUNCTION:
        addFunction(catalogObject.getFn(), catalogObject.getCatalog_version());
        break;
      case DATA_SOURCE:
        addDataSource(catalogObject.getData_source(), catalogObject.getCatalog_version());
        break;
      case ROLE:
        Role role = Role.fromThrift(catalogObject.getRole());
        role.setCatalogVersion(catalogObject.getCatalog_version());
        authPolicy_.addRole(role);
        break;
      case PRIVILEGE:
        RolePrivilege privilege =
            RolePrivilege.fromThrift(catalogObject.getPrivilege());
        privilege.setCatalogVersion(catalogObject.getCatalog_version());
        try {
          authPolicy_.addPrivilege(privilege);
        } catch (CatalogException e) {
          LOG.error("Error adding privilege: ", e);
        }
        break;
      case HDFS_CACHE_POOL:
        HdfsCachePool cachePool = new HdfsCachePool(catalogObject.getCache_pool());
        cachePool.setCatalogVersion(catalogObject.getCatalog_version());
        hdfsCachePools_.add(cachePool);
        break;
      default:
        throw new IllegalStateException(
            "Unexpected TCatalogObjectType: " + catalogObject.getType());
    }
  }

  /**
   *  Removes the matching TCatalogObject from the catalog, if one exists and its
   *  catalog version is < the catalog version of this drop operation.
   *  Note that drop operations that come from statestore heartbeats always have a
   *  version of 0. To determine the drop version for statestore updates,
   *  the catalog version from the current update is used. This is okay because there
   *  can never be a catalog update from the statestore that contains a drop
   *  and an addition of the same object. For more details on how drop
   *  versioning works, see CatalogServerCatalog.java
   */
  private void removeCatalogObject(TCatalogObject catalogObject,
      long currentCatalogUpdateVersion) {
    // The TCatalogObject associated with a drop operation from a state store
    // heartbeat will always have a version of zero. Because no update from
    // the state store can contain both a drop and an addition of the same object,
    // we can assume the drop version is the current catalog version of this update.
    // If the TCatalogObject contains a version that != 0, it indicates the drop
    // came from a direct update.
    long dropCatalogVersion = catalogObject.getCatalog_version() == 0 ?
        currentCatalogUpdateVersion : catalogObject.getCatalog_version();

    switch(catalogObject.getType()) {
      case DATABASE:
        removeDb(catalogObject.getDb(), dropCatalogVersion);
        break;
      case TABLE:
      case VIEW:
        removeTable(catalogObject.getTable(), dropCatalogVersion);
        break;
      case FUNCTION:
        removeFunction(catalogObject.getFn(), dropCatalogVersion);
        break;
      case DATA_SOURCE:
        removeDataSource(catalogObject.getData_source(), dropCatalogVersion);
        break;
      case ROLE:
        removeRole(catalogObject.getRole(), dropCatalogVersion);
        break;
      case PRIVILEGE:
        removePrivilege(catalogObject.getPrivilege(), dropCatalogVersion);
        break;
      case HDFS_CACHE_POOL:
        HdfsCachePool existingItem =
            hdfsCachePools_.get(catalogObject.getCache_pool().getPool_name());
        if (existingItem.getCatalogVersion() > catalogObject.getCatalog_version()) {
          hdfsCachePools_.remove(catalogObject.getCache_pool().getPool_name());
        }
        break;
      default:
        throw new IllegalStateException(
            "Unexpected TCatalogObjectType: " + catalogObject.getType());
    }

    if (catalogObject.getCatalog_version() > lastSyncedCatalogVersion_) {
      catalogDeltaLog_.addRemovedObject(catalogObject);
    }
  }

  private void addDb(TDatabase thriftDb, long catalogVersion) {
    Db existingDb = getDb(thriftDb.getDb_name());
    if (existingDb == null ||
        existingDb.getCatalogVersion() < catalogVersion) {
      Db newDb = Db.fromTDatabase(thriftDb, this);
      newDb.setCatalogVersion(catalogVersion);
      addDb(newDb);
    }
  }

  private void addTable(TTable thriftTable, long catalogVersion)
      throws TableLoadingException {
    Db db = getDb(thriftTable.db_name);
    if (db == null) {
      LOG.debug("Parent database of table does not exist: " +
          thriftTable.db_name + "." + thriftTable.tbl_name);
      return;
    }

    Table newTable = Table.fromThrift(db, thriftTable);
    newTable.setCatalogVersion(catalogVersion);
    db.addTable(newTable);
  }

  private void addFunction(TFunction fn, long catalogVersion) {
    Function function = Function.fromThrift(fn);
    function.setCatalogVersion(catalogVersion);
    Db db = getDb(function.getFunctionName().getDb());
    if (db == null) {
      LOG.debug("Parent database of function does not exist: " + function.getName());
      return;
    }
    Function existingFn = db.getFunction(fn.getSignature());
    if (existingFn == null ||
        existingFn.getCatalogVersion() < catalogVersion) {
      db.addFunction(function);
    }
  }

  private void addDataSource(TDataSource thrift, long catalogVersion) {
    DataSource dataSource = DataSource.fromThrift(thrift);
    dataSource.setCatalogVersion(catalogVersion);
    addDataSource(dataSource);
  }

  private void removeDataSource(TDataSource thrift, long dropCatalogVersion) {
    removeDataSource(thrift.getName());
  }

  private void removeDb(TDatabase thriftDb, long dropCatalogVersion) {
    Db db = getDb(thriftDb.getDb_name());
    if (db != null && db.getCatalogVersion() < dropCatalogVersion) {
      removeDb(db.getName());
    }
  }

  private void removeTable(TTable thriftTable, long dropCatalogVersion) {
    Db db = getDb(thriftTable.db_name);
    // The parent database doesn't exist, nothing to do.
    if (db == null) return;

    Table table = db.getTable(thriftTable.getTbl_name());
    if (table != null && table.getCatalogVersion() < dropCatalogVersion) {
      db.removeTable(thriftTable.tbl_name);
    }
  }

  private void removeFunction(TFunction thriftFn, long dropCatalogVersion) {
    Db db = getDb(thriftFn.name.getDb_name());
    // The parent database doesn't exist, nothing to do.
    if (db == null) return;

    // If the function exists and it has a catalog version less than the
    // version of the drop, remove the function.
    Function fn = db.getFunction(thriftFn.getSignature());
    if (fn != null && fn.getCatalogVersion() < dropCatalogVersion) {
      db.removeFunction(thriftFn.getSignature());
    }
  }

  private void removeRole(TRole thriftRole, long dropCatalogVersion) {
    Role existingRole = authPolicy_.getRole(thriftRole.getRole_name());
    // version of the drop, remove the function.
    if (existingRole != null && existingRole.getCatalogVersion() < dropCatalogVersion) {
      authPolicy_.removeRole(thriftRole.getRole_name());
    }
  }

  private void removePrivilege(TPrivilege thriftPrivilege, long dropCatalogVersion) {
    Role role = authPolicy_.getRole(thriftPrivilege.getRole_id());
    if (role == null) return;
    RolePrivilege existingPrivilege =
        role.getPrivilege(thriftPrivilege.getPrivilege_name());
    // version of the drop, remove the function.
    if (existingPrivilege != null &&
        existingPrivilege.getCatalogVersion() < dropCatalogVersion) {
      role.removePrivilege(thriftPrivilege.getPrivilege_name());
    }
  }

  /**
   * Returns true if the ImpaladCatalog is ready to accept requests (has
   * received and processed a valid catalog topic update from the StateStore),
   * false otherwise.
   */
  public boolean isReady() { return isReady_.get(); }

  // Only used for testing.
  public void setIsReady() { isReady_.set(true); }
}
