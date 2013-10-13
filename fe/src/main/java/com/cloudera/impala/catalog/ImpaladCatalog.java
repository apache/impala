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
import com.cloudera.impala.thrift.TDatabase;
import com.cloudera.impala.thrift.TFunction;
import com.cloudera.impala.thrift.TInternalCatalogUpdateRequest;
import com.cloudera.impala.thrift.TInternalCatalogUpdateResponse;
import com.cloudera.impala.thrift.TTable;
import com.cloudera.impala.thrift.TUniqueId;
import com.google.common.base.Preconditions;

/**
 * Thread safe Catalog for an Impalad. The Impalad Catalog provides an interface to
 * access Catalog objects that this Impalad knows about and authorize access requests
 * to these objects. It also manages reading and updating the authorization policy file
 * from HDFS.
 * TODO: The CatalogService should also handle updating and disseminating the
 * authorization policy.
 * The only updates to the Impalad catalog objects come from the Catalog Service (via
 * StateStore heartbeats). These updates are applied in the updateCatalog() function
 * which takes the catalogLock_.writeLock() for the duration of its execution to ensure
 * all updates are applied atomically.
 * Additionally, the Impalad Catalog provides interfaces for checking whether
 * a user is authorized to access a particular object. Any catalog access that requires
 * privilege checks should go through this class.
 * The CatalogServiceId is also tracked to detect if a different instance of the catalog
 * service has been started, in which case a full topic update is required.
 * TODO: Currently, there is some some inconsistency in whether catalog methods throw
 * or return null of the target object does not exist. We should update all
 * methods to return null if the object doesn't exist.
 */
public class ImpaladCatalog extends Catalog {
  private static final Logger LOG = Logger.getLogger(ImpaladCatalog.class);
  private static final TUniqueId INITIAL_CATALOG_SERVICE_ID = new TUniqueId(0L, 0L);
  private TUniqueId catalogServiceId_ = INITIAL_CATALOG_SERVICE_ID;

  //TODO: Make the reload interval configurable.
  private static final int AUTHORIZATION_POLICY_RELOAD_INTERVAL_SECS = 5 * 60;

  private final ScheduledExecutorService policyReader_ =
      Executors.newScheduledThreadPool(1);
  private final AuthorizationConfig authzConfig_;
  // Lock used to synchronize refreshing the AuthorizationChecker.
  private final ReentrantReadWriteLock authzCheckerLock_ = new ReentrantReadWriteLock();
  private AuthorizationChecker authzChecker_;
  // Flag to determine if the Catalog is ready to accept user requests. See isReady().
  private final AtomicBoolean isReady_ = new AtomicBoolean(false);

  public ImpaladCatalog(AuthorizationConfig authzConfig) {
    this(CatalogInitStrategy.EMPTY, authzConfig);
  }

  /**
   * C'tor used by tests that need to validate the ImpaladCatalog outside of the
   * CatalogServer.
   */
  public ImpaladCatalog(CatalogInitStrategy loadStrategy,
      AuthorizationConfig authzConfig) {
    super(loadStrategy);
    authzConfig_ = authzConfig;
    authzChecker_ = new AuthorizationChecker(authzConfig);
    // If authorization is enabled, reload the policy on a regular basis.
    if (authzConfig.isEnabled()) {
      // Stagger the reads across nodes
      Random randomGen = new Random(UUID.randomUUID().hashCode());
      int delay = AUTHORIZATION_POLICY_RELOAD_INTERVAL_SECS + randomGen.nextInt(60);

      policyReader_.scheduleAtFixedRate(
          new AuthorizationPolicyReader(authzConfig),
          delay, AUTHORIZATION_POLICY_RELOAD_INTERVAL_SECS, TimeUnit.SECONDS);
    }
    if (loadStrategy != CatalogInitStrategy.EMPTY) isReady_.set(true);
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
        authzChecker_ = new AuthorizationChecker(config);
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
   * a detail handled by the statestore). This method takes the catalogLock_ writeLock
   * for the duration of the method to ensure all updates are applied atomically. Since
   * updates are sent from the statestore as deltas, this should generally not block
   * execution for a significant amount of time.
   * Catalog updates are ordered by the object type with the dependent objects coming
   * first. That is, database "foo" will always come before table "foo.bar".
   */
  public TInternalCatalogUpdateResponse updateCatalog(
      TInternalCatalogUpdateRequest req) throws CatalogException {
    catalogLock_.writeLock().lock();
    try {
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
      for (TCatalogObject catalogObject: req.getUpdated_objects()) {
        switch(catalogObject.getType()) {
          case DATABASE:
            addDb(catalogObject.getDb());
            break;
          case TABLE:
          case VIEW:
            addTable(catalogObject.getTable());
            break;
          case FUNCTION:
            addFunction(Function.fromThrift(catalogObject.getFn()));
            break;
          default:
            throw new IllegalStateException(
                "Unexpected TCatalogObjectType: " + catalogObject.getType());
        }
      }

      // Now remove all objects from the catalog. Removing a database before removing
      // its child tables/functions is fine. If that happens, the removal of the child
      // object will be a no-op.
      for (TCatalogObject catalogObject: req.getRemoved_objects()) {
        switch(catalogObject.getType()) {
          case DATABASE:
            removeDb(catalogObject.getDb().getDb_name());
            break;
          case TABLE:
          case VIEW:
            removeTable(catalogObject.getTable());
            break;
          case FUNCTION:
            removeUdf(catalogObject.getFn());
            break;
          default:
            throw new IllegalStateException(
                "Unexpected TCatalogObjectType: " + catalogObject.getType());
        }
      }
      isReady_.set(true);
    } finally {
      catalogLock_.writeLock().unlock();
    }
    return new TInternalCatalogUpdateResponse(catalogServiceId_);
  }

  /**
   * Gets the Db object from the Catalog using a case-insensitive lookup on the name.
   * Returns null if no matching database is found.
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
    // Make sure the user has privileges to check if the table exists.
    checkAccess(user, new PrivilegeRequestBuilder()
        .allOf(privilege).onTable(dbName, tableName).toRequest());

    catalogLock_.readLock().lock();
    try {
      Db db = getDb(dbName);
      if (db == null) {
        throw new DatabaseNotFoundException("Database not found: " + dbName);
      }
      return db.containsTable(tableName);
    } finally {
      catalogLock_.readLock().unlock();
    }
  }

  /**
   * Returns the Table object for the given dbName/tableName.
   */
  public Table getTable(String dbName, String tableName, User user,
      Privilege privilege) throws DatabaseNotFoundException, TableNotFoundException,
      TableLoadingException, AuthorizationException {
    checkAccess(user, new PrivilegeRequestBuilder()
        .allOf(privilege).onTable(dbName, tableName).toRequest());

    Table table = getTable(dbName, tableName);
    // If there were problems loading this table's metadata, throw an exception
    // when it is accessed.
    if (table instanceof IncompleteTable) {
      ImpalaException cause = ((IncompleteTable) table).getCause();
      if (cause instanceof TableLoadingException) throw (TableLoadingException) cause;
      throw new TableLoadingException("Missing table metadata: ", cause);
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

  private long addDb(TDatabase thriftDb) {
    return dbCache_.add(Db.fromTDatabase(thriftDb, this));
  }

  private void addTable(TTable thriftTable)
      throws TableLoadingException, DatabaseNotFoundException {
    Db db = getDb(thriftTable.db_name);
    if (db == null) {
      throw new DatabaseNotFoundException("Parent database of table does not exist: " +
          thriftTable.db_name + "." + thriftTable.tbl_name);
    }
    db.addTable(thriftTable);
  }

  private void removeTable(TTable thriftTable) {
    Db db = getDb(thriftTable.db_name);
    // The parent database doesn't exist, nothing to do.
    if (db == null) return;
    db.removeTable(thriftTable.tbl_name);
  }

  private void removeUdf(TFunction thriftUdf) {
    // Loops through all databases in the catalog looking for a matching UDF.
    // TODO: Parse the signature string to find out the target database?
    for (String dbName: dbCache_.getAllNames()) {
      Db db = getDb(dbName);
      if (db == null) continue;
      if (db.removeFunction(thriftUdf.getSignature())) return;
    }
  }

  /**
   * Returns true if the ImpaladCatalog is ready to accept requests (has
   * received and processed a valid catalog topic update from the StateStore),
   * false otherwise.
   */
  public boolean isReady() { return isReady_.get(); }
}
