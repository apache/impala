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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.impala.analysis.TableName;
import org.apache.impala.authorization.AuthorizationChecker;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.Pair;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TAuthzCacheInvalidation;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TDataSource;
import org.apache.impala.thrift.TDatabase;
import org.apache.impala.thrift.TFunction;
import org.apache.impala.thrift.TGetPartitionStatsResponse;
import org.apache.impala.thrift.THdfsPartition;
import org.apache.impala.thrift.THdfsTable;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.thrift.TUpdateCatalogCacheRequest;
import org.apache.impala.thrift.TUpdateCatalogCacheResponse;
import org.apache.impala.util.PatternMatcher;
import org.apache.impala.util.TByteBuffer;
import org.apache.impala.util.TUniqueIdUtil;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

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
public class ImpaladCatalog extends Catalog implements FeCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(ImpaladCatalog.class);
  // The last known Catalog Service ID. If the ID changes, it indicates the CatalogServer
  // has restarted.
  private TUniqueId catalogServiceId_ = Catalog.INITIAL_CATALOG_SERVICE_ID;

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
  private final AtomicReference<? extends AuthorizationChecker> authzChecker_;

  public ImpaladCatalog(String defaultKuduMasterHosts,
      AtomicReference<? extends AuthorizationChecker> authzChecker) {
    super();
    authzChecker_ = authzChecker;
    addDb(BuiltinsDb.getInstance());
    defaultKuduMasterHosts_ = defaultKuduMasterHosts;
    // Ensure the contents of the CatalogObjectVersionSet instance are cleared when a
    // new instance of ImpaladCatalog is created (see IMPALA-6486).
    CatalogObjectVersionSet.INSTANCE.clear();
  }

  /**
   * Utility class for sequencing the order in which a set of updated catalog objects
   * need to be applied to the catalog in order to satisfy referential constraints.
   *
   * If one type of object refers to another type of object, it needs to be added
   * after it and deleted before it.
   */
  public static class ObjectUpdateSequencer {
    private final ArrayDeque<TCatalogObject> updatedObjects = new ArrayDeque<>();
    private final ArrayDeque<TCatalogObject> deletedObjects = new ArrayDeque<>();

    public void add(TCatalogObject obj, boolean isDeleted) {
      if (!isDeleted) {
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

    public Iterable<TCatalogObject> getUpdatedObjects() { return updatedObjects; }
    public Iterable<TCatalogObject> getDeletedObjects() { return deletedObjects; }

    /**
     * Returns true if the given object does not depend on any other object already
     * existing in the catalog in order to be added.
     */
    private static boolean isTopLevelCatalogObject(TCatalogObject catalogObject) {
      return catalogObject.getType() == TCatalogObjectType.DATABASE ||
          catalogObject.getType() == TCatalogObjectType.DATA_SOURCE ||
          catalogObject.getType() == TCatalogObjectType.HDFS_CACHE_POOL ||
          catalogObject.getType() == TCatalogObjectType.PRINCIPAL ||
          catalogObject.getType() == TCatalogObjectType.AUTHZ_CACHE_INVALIDATION;
    }
  }

  /**
   * Update the catalog service Id. Trigger a full update if the service ID changes.
   */
  private void setCatalogServiceId(TUniqueId catalogServiceId) throws CatalogException {
    // Check for changes in the catalog service ID.
    if (!catalogServiceId_.equals(catalogServiceId)) {
      boolean firstRun = catalogServiceId_.equals(INITIAL_CATALOG_SERVICE_ID);
      TUniqueId oldCatalogServiceId = catalogServiceId_;
      catalogServiceId_ = catalogServiceId;
      if (!firstRun) {
        // Throw an exception which will trigger a full topic update request.
        throw new CatalogException("Detected catalog service ID changes from " +
            TUniqueIdUtil.PrintId(oldCatalogServiceId) + " to " +
            TUniqueIdUtil.PrintId(catalogServiceId) + ". Aborting updateCatalog()");
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
    ObjectUpdateSequencer sequencer = new ObjectUpdateSequencer();
    // Maps that group incremental partition updates by table names so we can apply them
    // when updating the table.
    Map<TableName, List<THdfsPartition>> newPartitionsByTable = new HashMap<>();
    Map<TableName, PartitionMetaSummary> partUpdates = new HashMap<>();
    long newCatalogVersion = lastSyncedCatalogVersion_.get();
    Pair<Boolean, ByteBuffer> update;
    int maxMessageSize = BackendConfig.INSTANCE.getThriftRpcMaxMessageSize();
    final TConfiguration config = new TConfiguration(maxMessageSize,
        TConfiguration.DEFAULT_MAX_FRAME_SIZE, TConfiguration.DEFAULT_RECURSION_DEPTH);
    while ((update = FeSupport.NativeGetNextCatalogObjectUpdate(req.native_iterator_ptr))
        != null) {
      boolean isDelete = update.first;
      TCatalogObject obj = new TCatalogObject();
      obj.read(new TBinaryProtocol(new TByteBuffer(config, update.second)));
      String key = Catalog.toCatalogObjectKey(obj);
      int len = update.second.capacity();
      if (len > 100 * 1024 * 1024 /* 100MB */) {
        LOG.info("Received large catalog object(>100mb): " + key + " is " + len +
            "bytes");
      }
      if (!key.contains("HDFS_PARTITION")) {
        LOG.info((isDelete ? "Deleting: " : "Adding: ") + key + " version: "
            + obj.catalog_version + " size: " + len);
      }
      // For statestore updates, the service ID and updated version is wrapped in a
      // CATALOG catalog object.
      if (obj.type == TCatalogObjectType.CATALOG) {
        setCatalogServiceId(obj.catalog.catalog_service_id);
        newCatalogVersion = obj.catalog_version;
      } else if (obj.type == TCatalogObjectType.HDFS_PARTITION) {
        TableName tblName = new TableName(obj.getHdfs_partition().db_name,
            obj.getHdfs_partition().tbl_name);
        partUpdates.computeIfAbsent(tblName,
            (s) -> new PartitionMetaSummary(tblName.toString(), /*inCatalogd*/false,
                /*hasV1Updates*/true, /*hasV2Updates*/false))
            .update(/*isV1Key*/true, isDelete, obj.hdfs_partition.partition_name,
                obj.catalog_version, len, /*unused*/-1);
        if (!isDelete) {
          newPartitionsByTable.computeIfAbsent(tblName, (s) -> new ArrayList<>())
              .add(obj.getHdfs_partition());
        }
      } else {
        sequencer.add(obj, isDelete);
      }
    }
    for (PartitionMetaSummary summary : partUpdates.values()) {
      if (summary.hasUpdates()) LOG.info(summary.toString());
    }

    for (TCatalogObject catalogObject: sequencer.getUpdatedObjects()) {
      try {
        addCatalogObject(catalogObject, newPartitionsByTable);
      } catch (Exception e) {
        LOG.error("Error adding catalog object: " + e.getMessage(), e);
      }
    }

    for (TCatalogObject catalogObject: sequencer.getDeletedObjects()) {
      removeCatalogObject(catalogObject);
    }

    lastSyncedCatalogVersion_.set(newCatalogVersion);
    // Cleanup old entries in the log.
    catalogDeltaLog_.garbageCollect(newCatalogVersion);
    // Notify all the threads waiting on a catalog update.
    synchronized (catalogUpdateEventNotifier_) {
      catalogUpdateEventNotifier_.notifyAll();
    }
    return new TUpdateCatalogCacheResponse(catalogServiceId_,
        CatalogObjectVersionSet.INSTANCE.getMinimumVersion(), newCatalogVersion);
  }


  @Override // FeCatalog
  public void prioritizeLoad(Set<TableName> tableNames, @Nullable TUniqueId queryId)
      throws InternalException {
    FeSupport.PrioritizeLoad(tableNames, queryId);
  }

  @Override // FeCatalog
  public TGetPartitionStatsResponse getPartitionStats(
      TableName table) throws InternalException {
    return FeSupport.GetPartitionStats(table);
  }

  @Override // FeCatalog
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
   *  Adds the given TCatalogObject to the catalog cache. The update may be ignored
   *  (considered out of date) if:
   *  1) An item exists in the catalog cache with a version > than the given
   *     TCatalogObject's version.
   *  2) The catalogDeltaLog_ contains an entry for this object with a version
   *     > than the given TCatalogObject's version.
   */
  private void addCatalogObject(TCatalogObject catalogObject,
      Map<TableName, List<THdfsPartition>> newPartitions) throws TableLoadingException {
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
        TTable table = catalogObject.getTable();
        TableName tblName = new TableName(table.getDb_name(), table.getTbl_name());
        addTable(table,
            newPartitions.getOrDefault(tblName, Collections.emptyList()),
            catalogObject.getCatalog_version());
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
      case PRINCIPAL:
        Principal principal = Principal.fromThrift(catalogObject.getPrincipal());
        principal.setCatalogVersion(catalogObject.getCatalog_version());
        authPolicy_.addPrincipal(principal);
        break;
      case PRIVILEGE:
        PrincipalPrivilege privilege =
            PrincipalPrivilege.fromThrift(catalogObject.getPrivilege());
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
      case AUTHZ_CACHE_INVALIDATION:
        AuthzCacheInvalidation authzCacheInvalidation = new AuthzCacheInvalidation(
            catalogObject.getAuthz_cache_invalidation());
        authzCacheInvalidation.setCatalogVersion(catalogObject.getCatalog_version());
        authzCacheInvalidation_.add(authzCacheInvalidation);
        authzChecker_.get().invalidateAuthorizationCache();
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
      case PRINCIPAL:
        authPolicy_.removePrincipalIfLowerVersion(catalogObject.getPrincipal(),
            dropCatalogVersion);
        break;
      case PRIVILEGE:
        authPolicy_.removePrivilegeIfLowerVersion(catalogObject.getPrivilege(),
            dropCatalogVersion);
        break;
      case HDFS_CACHE_POOL:
        HdfsCachePool existingItem =
            hdfsCachePools_.get(catalogObject.getCache_pool().getPool_name());
        if (existingItem.getCatalogVersion() <= catalogObject.getCatalog_version()) {
          hdfsCachePools_.remove(catalogObject.getCache_pool().getPool_name());
        }
        break;
      case AUTHZ_CACHE_INVALIDATION:
        removeAuthzCacheInvalidation(catalogObject.getAuthz_cache_invalidation(),
            dropCatalogVersion);
        break;
      case HDFS_PARTITION:
        // Ignore partition deletions in catalog-v1 mode since they are handled during
        // table updates to atomically update the table.
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
      if (existingDb != null) {
        CatalogObjectVersionSet.INSTANCE.updateVersions(
            existingDb.getCatalogVersion(), catalogVersion);
        CatalogObjectVersionSet.INSTANCE.removeAll(existingDb.getTables());
        CatalogObjectVersionSet.INSTANCE.removeAll(
            existingDb.getFunctions(null, new PatternMatcher()));
        // IMPALA-8434: add back the existing tables/functions. Note that their version
        // counters in CatalogObjectVersionSet have been decreased by the above removeAll
        // statements, meaning their references from the old db are deleted since the old
        // db object has been replaced by newDb. addTable and addFunction will add their
        // versions back.
        for (Table tbl: existingDb.getTables()) {
          newDb.addTable(tbl);
        }
        for (List<Function> functionList: existingDb.getAllFunctions().values()) {
          for (Function func: functionList) {
            newDb.addFunction(func);
          }
        }
      } else {
        CatalogObjectVersionSet.INSTANCE.addVersion(catalogVersion);
      }
      addDb(newDb);
    }
  }

  private void addTable(TTable thriftTable, List<THdfsPartition> newPartitions,
      long catalogVersion) throws TableLoadingException {
    Db db = getDb(thriftTable.db_name);
    if (db == null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Parent database of table does not exist: " +
            thriftTable.db_name + "." + thriftTable.tbl_name);
      }
      return;
    }

    Preconditions.checkNotNull(newPartitions);
    Table existingTable = db.getTable(thriftTable.tbl_name);
    Table newTable = Table.fromThrift(db, thriftTable);
    newTable.setCatalogVersion(catalogVersion);
    if (existingTable != null && existingTable.getCatalogVersion() >= catalogVersion) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Ignore stale update on table {}: currentVersion={}, updateVersion={}",
            existingTable.getFullName(), existingTable.getCatalogVersion(),
            catalogVersion);
      }
      return;
    }
    // Apply partition updates if this is a partial THdfsTable object with minimal
    // partitions. Currently, catalogd returns full table objects in DDL responses and
    // sends partial table objects in catalog topic updates. When sending partial table
    // objects, partition updates are sent in the same topic update so we can apply them
    // here.
    if (newTable instanceof HdfsTable
        && !thriftTable.getHdfs_table().has_full_partitions) {
      THdfsTable tHdfsTable = thriftTable.getHdfs_table();
      int numExistingParts = 0;
      int numNewParts = 0;
      int numDeletedParts = 0;
      HdfsTable newHdfsTable = (HdfsTable) newTable;
      // HdfsPartitions are immutable so we can reuse them in the old table instance.
      // Copy existing partitions before we apply incremental updates.
      if (existingTable instanceof HdfsTable) {
        for (PrunablePartition part : ((HdfsTable) existingTable).getPartitions()) {
          numExistingParts++;
          if (tHdfsTable.partitions.containsKey(part.getId())) {
            // Create a new partition instance under the new table object and copy all
            // the fields of the existing partition.
            HdfsPartition newPart = new HdfsPartition.Builder(newHdfsTable, part.getId())
                .copyFromPartition((HdfsPartition) part)
                .build();
            Preconditions.checkState(newHdfsTable.addPartitionNoThrow(newPart));
          } else {
            numDeletedParts++;
          }
        }
      }
      // Apply incremental updates.
      List<String> stalePartitionNames = new ArrayList<>();
      for (THdfsPartition tPart : newPartitions) {
        // TODO: remove this after IMPALA-9937. It's only illegal in statestore updates,
        //  which indicates a leak of partitions in the catalog topic - a stale partition
        //  should already have a corresponding deletion so won't get here.
        if (!tHdfsTable.partitions.containsKey(tPart.id)) {
          stalePartitionNames.add(tPart.partition_name);
          LOG.warn("Stale partition: {}.{}:{} id={}", tPart.db_name, tPart.tbl_name,
              tPart.partition_name, tPart.id);
          continue;
        }
        // The existing table could have a newer version than the last sent table version
        // in catalogd, so some partition instances may already exist here. This happens
        // when we have executed DDL/DMLs on this table on this coordinator since the last
        // catalog update. (IMPALA-10283)
        if (newHdfsTable.getPartitionMap().containsKey(tPart.id)) {
          LOG.info("Skip adding existing partition (id:{}, name:{}) to table {}",
              tPart.id, tPart.partition_name, newHdfsTable.getFullName());
          continue;
        }
        HdfsPartition part = new HdfsPartition.Builder(newHdfsTable, tPart.id)
            .fromThrift(tPart)
            .build();
        Preconditions.checkState(newHdfsTable.addPartitionNoThrow(part),
            "Failed adding new partition (id:{}, name:{}) to table {}",
            tPart.id, tPart.partition_name, newHdfsTable.getFullName());
        LOG.trace("Added partition (id:{}, name:{}) to table {}",
            tPart.id, tPart.partition_name, newHdfsTable.getFullName());
        numNewParts++;
      }
      if (!stalePartitionNames.isEmpty()) {
        LOG.warn("Received {} stale partitions of table {}.{} in the statestore update:" +
                " {}. There are possible leaks in the catalog topic values. To resolve " +
                "the leak, add them back and then drop them again.",
            stalePartitionNames.size(), thriftTable.db_name, thriftTable.tbl_name,
            String.join(",", stalePartitionNames));
      }
      // Validate that all partitions are set.
      ((HdfsTable) newTable).validatePartitions(tHdfsTable.partitions.keySet());
      LOG.info("Applied incremental table updates on {} existing partitions of " +
          "table {}.{}: added {} new partitions, deleted {} stale partitions.",
          numExistingParts, thriftTable.db_name, thriftTable.tbl_name,
          numNewParts, numDeletedParts);
    }
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
        CatalogObjectVersionSet.INSTANCE.updateVersions(
            existingFn.getCatalogVersion(), catalogVersion);
      } else {
        CatalogObjectVersionSet.INSTANCE.addVersion(catalogVersion);
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
      CatalogObjectVersionSet.INSTANCE.removeVersion(
          db.getCatalogVersion());
      CatalogObjectVersionSet.INSTANCE.removeAll(db.getTables());
      CatalogObjectVersionSet.INSTANCE.removeAll(
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
      CatalogObjectVersionSet.INSTANCE.removeVersion(
          fn.getCatalogVersion());
    }
  }

  private void removeAuthzCacheInvalidation(
      TAuthzCacheInvalidation authzCacheInvalidation, long dropCatalogVersion) {
    AuthzCacheInvalidation existingItem = authzCacheInvalidation_.get(
        authzCacheInvalidation.getMarker_name());
    if (existingItem != null && existingItem.getCatalogVersion() < dropCatalogVersion) {
      authzCacheInvalidation_.remove(authzCacheInvalidation.getMarker_name());
    }
  }

  @Override // FeCatalog
  public boolean isReady() {
    return lastSyncedCatalogVersion_.get() > INITIAL_CATALOG_VERSION;
  }

  // Only used for testing.
  @Override // FeCatalog
  public void setIsReady(boolean isReady) {
    lastSyncedCatalogVersion_.incrementAndGet();
    synchronized (catalogUpdateEventNotifier_) {
      catalogUpdateEventNotifier_.notifyAll();
    }
  }
  @Override // FeCatalog
  public AuthorizationPolicy getAuthPolicy() { return authPolicy_; }
  @Override // FeCatalog
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

  @Override
  public String getAcidUserId() {
    return String.format("Impala Catalog %s",
        TUniqueIdUtil.PrintId(getCatalogServiceId()));
  }

  @Override
  public TUniqueId getCatalogServiceId() { return catalogServiceId_; }
}
