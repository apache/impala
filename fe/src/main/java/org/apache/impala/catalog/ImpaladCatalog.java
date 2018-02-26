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

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.Path;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.Pair;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TDataSource;
import org.apache.impala.thrift.TDatabase;
import org.apache.impala.thrift.TFunction;
import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TRole;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.thrift.TUpdateCatalogCacheRequest;
import org.apache.impala.thrift.TUpdateCatalogCacheResponse;
import org.apache.impala.util.PatternMatcher;
import org.apache.impala.util.TByteBuffer;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.google.common.base.Preconditions;

/**
 * Thread safe Catalog for an Impalad.  The Impalad catalog can be updated either via
 * a StateStore heartbeat or by directly applying the result of a catalog operation to
 * the CatalogCache. All updates are applied using the updateCatalog() function.
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
  private AtomicLong lastSyncedCatalogVersion_ =
      new AtomicLong(Catalog.INITIAL_CATALOG_VERSION);

  // Tracks modifications to this Impalad's catalog from direct updates to the cache.
  private final CatalogDeltaLog catalogDeltaLog_ = new CatalogDeltaLog();

  // Object that is used to synchronize on and signal when a catalog update is received.
  private final Object catalogUpdateEventNotifier_ = new Object();

  // The addresses of the Kudu masters to use if no Kudu masters were explicitly provided.
  // Used during table creation.
  private final String defaultKuduMasterHosts_;

  public ImpaladCatalog(String defaultKuduMasterHosts) {
    super();
    defaultKuduMasterHosts_ = defaultKuduMasterHosts;
    // Ensure the contents of the CatalogObjectVersionQueue instance are cleared when a
    // new instance of ImpaladCatalog is created (see IMPALA-6486).
    CatalogObjectVersionQueue.INSTANCE.clear();
  }

  /**
   * Returns true if the given object does not depend on any other object already
   * existing in the catalog in order to be added.
   */
  private boolean isTopLevelCatalogObject(TCatalogObject catalogObject) {
    return catalogObject.getType() == TCatalogObjectType.DATABASE ||
        catalogObject.getType() == TCatalogObjectType.DATA_SOURCE ||
        catalogObject.getType() == TCatalogObjectType.HDFS_CACHE_POOL ||
        catalogObject.getType() == TCatalogObjectType.ROLE;
  }

  /**
   * Update the catalog service Id. Trigger a full update if the service ID changes.
   */
  private void setCatalogServiceId(TUniqueId catalog_service_id) throws CatalogException {
    // Check for changes in the catalog service ID.
    if (!catalogServiceId_.equals(catalog_service_id)) {
      boolean firstRun = catalogServiceId_.equals(INITIAL_CATALOG_SERVICE_ID);
      catalogServiceId_ = catalog_service_id;
      if (!firstRun) {
        // Throw an exception which will trigger a full topic update request.
        throw new CatalogException("Detected catalog service ID change. Aborting " +
            "updateCatalog()");
      }
    }
  }

  /**
   * Updates the internal Catalog based on the given TCatalogUpdateReq.
   * This method:
   * 1) Calls NativeGetNextCatalogObjectUpdate() to get all the updates from the backend.
   * 2) Updates all top level objects (such as databases and roles).
   * 3) Updates all objects that depend on top level objects (such as functions, tables,
   *    privileges).
   * 4) Removes all dropped catalog objects.
   *
   * This method is called once per statestore heartbeat and is guaranteed the same
   * object will not be in both the "updated" list and the "removed" list (it is
   * a detail handled by the statestore).
   * Catalog objects are ordered by version, which is not necessarily the same as ordering
   * by dependency. This is handled by doing two passes and first updating the top level
   * objects, followed by updating the dependent objects. This method is synchronized
   * because updateCatalog() can be called by during a statestore update or during a
   * direct-DDL operation and catalogServiceId_ and lastSyncedCatalogVersion_ must be
   * protected.
   */
  public synchronized TUpdateCatalogCacheResponse updateCatalog(
    TUpdateCatalogCacheRequest req) throws CatalogException, TException {
    // For updates from catalog op results, the service ID is set in the request.
    if (req.isSetCatalog_service_id()) setCatalogServiceId(req.catalog_service_id);
    ArrayDeque<TCatalogObject> updatedObjects = new ArrayDeque<>();
    ArrayDeque<TCatalogObject> deletedObjects = new ArrayDeque<>();
    long newCatalogVersion = lastSyncedCatalogVersion_.get();
    Pair<Boolean, ByteBuffer> update;
    while ((update = FeSupport.NativeGetNextCatalogObjectUpdate(req.native_iterator_ptr))
        != null) {
      TCatalogObject obj = new TCatalogObject();
      obj.read(new TBinaryProtocol(new TByteBuffer(update.second)));
      String key = Catalog.toCatalogObjectKey(obj);
      int len = update.second.capacity();
      if (len > 100 * 1024 * 1024 /* 100MB */) {
        LOG.info("Received large catalog object(>100mb): " + key + " is " + len +
            "bytes");
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace((update.first ? "Deleting " : "Adding ") + "item: " + key + " version: "
            + obj.catalog_version + " of size: " + len);
      }
      // For statestore updates, the service ID and updated version is wrapped in a
      // CATALOG catalog object.
      if (obj.type == TCatalogObjectType.CATALOG) {
        setCatalogServiceId(obj.catalog.catalog_service_id);
        newCatalogVersion = obj.catalog_version;
      } else if (!update.first) {
        // Update top-level objects first.
        if (isTopLevelCatalogObject(obj)) {
          updatedObjects.addFirst(obj);
        } else {
          updatedObjects.addLast(obj);
        }
      } else {
        // Remove low-level objects first.
        if (isTopLevelCatalogObject(obj)) {
          deletedObjects.addLast(obj);
        } else {
          deletedObjects.addFirst(obj);
        }
      }
    }

    for (TCatalogObject catalogObject: updatedObjects) {
      try {
        addCatalogObject(catalogObject);
      } catch (Exception e) {
        LOG.error("Error adding catalog object: " + e.getMessage(), e);
      }
    }

    for (TCatalogObject catalogObject: deletedObjects) removeCatalogObject(catalogObject);

    lastSyncedCatalogVersion_.set(newCatalogVersion);
    // Cleanup old entries in the log.
    catalogDeltaLog_.garbageCollect(newCatalogVersion);
    // Notify all the threads waiting on a catalog update.
    synchronized (catalogUpdateEventNotifier_) {
      catalogUpdateEventNotifier_.notifyAll();
    }
    return new TUpdateCatalogCacheResponse(catalogServiceId_,
        CatalogObjectVersionQueue.INSTANCE.getMinimumVersion(), newCatalogVersion);
  }


  /**
   * Issues a load request to the catalogd for the given tables.
   */
  public void prioritizeLoad(Set<TableName> tableNames) throws InternalException {
    FeSupport.PrioritizeLoad(tableNames);
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
   * Returns the HDFS path where the metastore would create the given table. If the table
   * has a "location" set, that will be returned. Otherwise the path will be resolved
   * based on the location of the parent database. The metastore folder hierarchy is:
   * <warehouse directory>/<db name>.db/<table name>
   * Except for items in the default database which will be:
   * <warehouse directory>/<table name>
   * This method handles both of these cases.
   */
  public Path getTablePath(org.apache.hadoop.hive.metastore.api.Table msTbl)
      throws TException {
    try (MetaStoreClient msClient = getMetaStoreClient()) {
      // If the table did not have its path set, build the path based on the the
      // location property of the parent database.
      if (msTbl.getSd().getLocation() == null || msTbl.getSd().getLocation().isEmpty()) {
        String dbLocation =
            msClient.getHiveClient().getDatabase(msTbl.getDbName()).getLocationUri();
        return new Path(dbLocation, msTbl.getTableName().toLowerCase());
      } else {
        return new Path(msTbl.getSd().getLocation());
      }
    }
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
      throws TableLoadingException {
    // This item is out of date and should not be applied to the catalog.
    if (catalogDeltaLog_.wasObjectRemovedAfter(catalogObject)) {
      if (LOG.isTraceEnabled()) {
        LOG.trace(String.format("Skipping update because a matching object was removed " +
            "in a later catalog version: %s", catalogObject));
      }
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
        // Remove the function first, in case there is an existing function with the same
        // name and signature.
        removeFunction(catalogObject.getFn(), catalogObject.getCatalog_version());
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
   */
  private void removeCatalogObject(TCatalogObject catalogObject) {
    Preconditions.checkState(catalogObject.getCatalog_version() != 0);
    long dropCatalogVersion = catalogObject.getCatalog_version();
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
        if (existingItem.getCatalogVersion() <= catalogObject.getCatalog_version()) {
          hdfsCachePools_.remove(catalogObject.getCache_pool().getPool_name());
        }
        break;
      default:
        throw new IllegalStateException(
            "Unexpected TCatalogObjectType: " + catalogObject.getType());
    }

    if (catalogObject.getCatalog_version() > lastSyncedCatalogVersion_.get()) {
      catalogDeltaLog_.addRemovedObject(catalogObject);
    }
  }

  private void addDb(TDatabase thriftDb, long catalogVersion) {
    Db existingDb = getDb(thriftDb.getDb_name());
    if (existingDb == null ||
        existingDb.getCatalogVersion() < catalogVersion) {
      Db newDb = Db.fromTDatabase(thriftDb);
      newDb.setCatalogVersion(catalogVersion);
      addDb(newDb);
      if (existingDb != null) {
        CatalogObjectVersionQueue.INSTANCE.updateVersions(
            existingDb.getCatalogVersion(), catalogVersion);
        CatalogObjectVersionQueue.INSTANCE.removeAll(existingDb.getTables());
        CatalogObjectVersionQueue.INSTANCE.removeAll(
            existingDb.getFunctions(null, new PatternMatcher()));
      } else {
        CatalogObjectVersionQueue.INSTANCE.addVersion(catalogVersion);
      }
    }
  }

  private void addTable(TTable thriftTable, long catalogVersion)
      throws TableLoadingException {
    Db db = getDb(thriftTable.db_name);
    if (db == null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Parent database of table does not exist: " +
            thriftTable.db_name + "." + thriftTable.tbl_name);
      }
      return;
    }

    Table newTable = Table.fromThrift(db, thriftTable);
    newTable.setCatalogVersion(catalogVersion);
    db.addTable(newTable);
  }

  private void addFunction(TFunction fn, long catalogVersion) {
    LibCacheSetNeedsRefresh(fn.hdfs_location);
    Function function = Function.fromThrift(fn);
    function.setCatalogVersion(catalogVersion);
    Db db = getDb(function.getFunctionName().getDb());
    if (db == null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Parent database of function does not exist: " + function.getName());
      }
      return;
    }
    Function existingFn = db.getFunction(fn.getSignature());
    if (existingFn == null ||
        existingFn.getCatalogVersion() < catalogVersion) {
      db.addFunction(function);
      if (existingFn != null) {
        CatalogObjectVersionQueue.INSTANCE.updateVersions(
            existingFn.getCatalogVersion(), catalogVersion);
      } else {
        CatalogObjectVersionQueue.INSTANCE.addVersion(catalogVersion);
      }
    }
  }

  private void addDataSource(TDataSource thrift, long catalogVersion) {
    LibCacheSetNeedsRefresh(thrift.hdfs_location);
    DataSource dataSource = DataSource.fromThrift(thrift);
    dataSource.setCatalogVersion(catalogVersion);
    addDataSource(dataSource);
  }

  private void removeDataSource(TDataSource thrift, long dropCatalogVersion) {
    DataSource src = dataSources_.get(thrift.name);
    if (src != null && src.getCatalogVersion() < dropCatalogVersion) {
      LibCacheRemoveEntry(src.getLocation());
    }
    removeDataSource(thrift.getName());
  }

  private void removeDb(TDatabase thriftDb, long dropCatalogVersion) {
    Db db = getDb(thriftDb.getDb_name());
    if (db != null && db.getCatalogVersion() < dropCatalogVersion) {
      removeDb(db.getName());
      CatalogObjectVersionQueue.INSTANCE.removeVersion(
          db.getCatalogVersion());
      CatalogObjectVersionQueue.INSTANCE.removeAll(db.getTables());
      CatalogObjectVersionQueue.INSTANCE.removeAll(
          db.getFunctions(null, new PatternMatcher()));
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
      LibCacheRemoveEntry(fn.getLocation().getLocation());
      db.removeFunction(thriftFn.getSignature());
      CatalogObjectVersionQueue.INSTANCE.removeVersion(
          fn.getCatalogVersion());
    }
  }

  private void removeRole(TRole thriftRole, long dropCatalogVersion) {
    Role existingRole = authPolicy_.getRole(thriftRole.getRole_name());
    // version of the drop, remove the function.
    if (existingRole != null && existingRole.getCatalogVersion() < dropCatalogVersion) {
      authPolicy_.removeRole(thriftRole.getRole_name());
      CatalogObjectVersionQueue.INSTANCE.removeAll(existingRole.getPrivileges());
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
  public boolean isReady() {
    return lastSyncedCatalogVersion_.get() > INITIAL_CATALOG_VERSION;
  }

  // Only used for testing.
  public void setIsReady(boolean isReady) {
    lastSyncedCatalogVersion_.incrementAndGet();
    synchronized (catalogUpdateEventNotifier_) {
      catalogUpdateEventNotifier_.notifyAll();
    }
  }
  public AuthorizationPolicy getAuthPolicy() { return authPolicy_; }
  public String getDefaultKuduMasterHosts() { return defaultKuduMasterHosts_; }

  private void LibCacheSetNeedsRefresh(String hdfsLocation) {
    if (!FeSupport.NativeLibCacheSetNeedsRefresh(hdfsLocation)) {
      LOG.error("NativeLibCacheSetNeedsRefresh(" + hdfsLocation + ") failed.");
    }
  }
  private void LibCacheRemoveEntry(String hdfsLibFile) {
    if (!FeSupport.NativeLibCacheRemoveEntry(hdfsLibFile)) {
      LOG.error("LibCacheRemoveEntry(" + hdfsLibFile + ") failed.");
    }
  }
  public TUniqueId getCatalogServiceId() { return catalogServiceId_; }
}
