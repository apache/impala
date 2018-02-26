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

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.impala.authorization.SentryConfig;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.common.Pair;
import org.apache.impala.common.Reference;
import org.apache.impala.hive.executor.UdfExecutor;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TCatalog;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TCatalogUpdateResult;
import org.apache.impala.thrift.TFunction;
import org.apache.impala.thrift.TGetCatalogUsageResponse;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TTableUsageMetrics;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.util.PatternMatcher;
import org.apache.impala.util.SentryProxy;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;

import com.codahale.metrics.Timer;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Specialized Catalog that implements the CatalogService specific Catalog
 * APIs. The CatalogServiceCatalog manages loading of all the catalog metadata
 * and processing of DDL requests. The CatalogServiceCatalog maintains a global
 * "catalog version". The version is incremented and assigned to a CatalogObject whenever
 * it is added/modified/removed from the catalog. This means each CatalogObject will have
 * a unique version and assigned versions are strictly increasing.
 *
 * Periodically, the CatalogServiceCatalog collects a delta of catalog updates (based on a
 * specified catalog version) and constructs a topic update to be sent to the statestore.
 * Each catalog topic update is defined by a range of catalog versions (from, to] and the
 * CatalogServiceCatalog guarantees that every catalog object that has a version in the
 * specified range is included in the catalog topic update. Concurrent DDL requests are
 * allowed while a topic update is in progress. Hence, there is a non-zero probability
 * that frequently modified catalog objects may keep skipping topic updates. That can
 * happen when by the time a topic update thread tries to collect an object update, that
 * object is being modified by another metadata operation, causing its version to surpass
 * the 'to' version of the topic update. To ensure that all catalog updates
 * are eventually included in a catalog topic update, we keep track of the number of times
 * each catalog object has skipped a topic update and if that number exceeds a specified
 * threshold, we add the catalog object to the next topic update even if its version is
 * higher than the 'to' version of the topic update. As a result, the same version of an
 * object might be sent in two subsequent topic updates.
 *
 * The CatalogServiceCatalog maintains two logs:
 * - Delete log. Since deleted objects are removed from the cache, the cache itself is
 *   not useful for tracking deletions. This log is used for populating the list of
 *   deleted objects during a topic update by recording the catalog objects that
 *   have been removed from the catalog. An entry with a new version is added to this log
 *   every time an object is removed (e.g. dropTable). Incrementing an object's version
 *   and adding it to the delete log should be performed atomically. An entry is removed
 *   from this log by the topic update thread when the associated deletion entry is
 *   added to a topic update.
 * - Topic update log. This log records information about the catalog objects that have
 *   been included in a catalog topic update. Only the thread that is processing the
 *   topic update is responsible for adding, updating, and removing entries from the log.
 *   All other operations (e.g. addTable) only read topic update log entries but never
 *   modify them. Each entry includes the number of times a catalog object has
 *   skipped a topic update, which version of the object was last sent in a topic update
 *   and what was the version of that topic update. Entries of the topic update log are
 *   garbage-collected every TOPIC_UPDATE_LOG_GC_FREQUENCY topic updates by the topic
 *   update processing thread to prevent the log from growing indefinitely. Metadata
 *   operations using SYNC_DDL are inspecting this log to identify the catalog topic
 *   version that the issuing impalad must wait for in order to ensure that the effects
 *   of this operation have been broadcast to all the coordinators.
 *
 * Known anomalies with SYNC_DDL:
 *   The time-based cleanup process of the topic update log entries may cause metadata
 *   operations that use SYNC_DDL to hang while waiting for specific topic update log
 *   entries. That could happen if the thread processing the metadata operation stalls
 *   for a long period of time (longer than the time to process
 *   TOPIC_UPDATE_LOG_GC_FREQUENCY topic updates) between the time the operation was
 *   applied in the catalog cache and the time the SYNC_DDL version was checked. To reduce
 *   the probability of such an event, we set the value of the
 *   TOPIC_UPDATE_LOG_GC_FREQUENCY to a large value. Also, to prevent metadata operations
 *   from hanging in that path due to unknown issues (e.g. bugs), operations using
 *   SYNC_DDL are not allowed to wait indefinitely for specific topic log entries and an
 *   exception is thrown if the specified max wait time is exceeded. See
 *   waitForSyncDdlVersion() for more details.
 *
 * Table metadata for IncompleteTables (not fully loaded tables) are loaded in the
 * background by the TableLoadingMgr; tables can be prioritized for loading by calling
 * prioritizeLoad(). Background loading can also be enabled for the catalog, in which
 * case missing tables (tables that are not yet loaded) are submitted to the
 * TableLoadingMgr any table metadata is invalidated and on startup. The metadata of
 * fully loaded tables (e.g. HdfsTable, HBaseTable, etc) are updated in-place and don't
 * trigger a background metadata load through the TableLoadingMgr. Accessing a table
 * that is not yet loaded (via getTable()), will load the table's metadata on-demand,
 * out-of-band of the table loading thread pool.
 *
 * See the class comments in CatalogOpExecutor for a description of the locking protocol
 * that should be employed if both the version lock and table locks need to be held at
 * the same time.
 *
 * TODO: Consider removing on-demand loading and have everything go through the table
 * loading thread pool.
 */
public class CatalogServiceCatalog extends Catalog {
  private static final Logger LOG = Logger.getLogger(CatalogServiceCatalog.class);

  private static final int INITIAL_META_STORE_CLIENT_POOL_SIZE = 10;
  private static final int MAX_NUM_SKIPPED_TOPIC_UPDATES = 2;
  private final TUniqueId catalogServiceId_;

  // Fair lock used to synchronize reads/writes of catalogVersion_. Because this lock
  // protects catalogVersion_, it can be used to perform atomic bulk catalog operations
  // since catalogVersion_ cannot change externally while the lock is being held.
  // In addition to protecting catalogVersion_, it is currently used for the
  // following bulk operations:
  // * Building a delta update to send to the statestore in getCatalogObjects(),
  //   so a snapshot of the catalog can be taken without any version changes.
  // * During a catalog invalidation (call to reset()), which re-reads all dbs and tables
  //   from the metastore.
  // * During renameTable(), because a table must be removed and added to the catalog
  //   atomically (potentially in a different database).
  private final ReentrantReadWriteLock versionLock_ = new ReentrantReadWriteLock(true);

  // Last assigned catalog version. Starts at INITIAL_CATALOG_VERSION and is incremented
  // with each update to the Catalog. Continued across the lifetime of the object.
  // Protected by versionLock_.
  // TODO: Handle overflow of catalogVersion_ and nextTableId_.
  // TODO: The name of this variable is misleading and can be interpreted as a property
  // of the catalog server. Rename into something that indicates its role as a global
  // sequence number assigned to catalog objects.
  private long catalogVersion_ = INITIAL_CATALOG_VERSION;

  // Manages the scheduling of background table loading.
  private final TableLoadingMgr tableLoadingMgr_;

  private final boolean loadInBackground_;

  // Periodically polls HDFS to get the latest set of known cache pools.
  private final ScheduledExecutorService cachePoolReader_ =
      Executors.newScheduledThreadPool(1);

  // Proxy to access the Sentry Service and also periodically refreshes the
  // policy metadata. Null if Sentry Service is not enabled.
  private final SentryProxy sentryProxy_;

  // Local temporary directory to copy UDF Jars.
  private static String localLibraryPath_;

  // Log of deleted catalog objects.
  private final CatalogDeltaLog deleteLog_;

  // Version of the last topic update returned to the statestore.
  // The version of a topic update is the catalog version of the CATALOG object
  // that is added to it.
  private final AtomicLong lastSentTopicUpdate_ = new AtomicLong(-1);

  // Wait time for a topic update.
  private static final long TOPIC_UPDATE_WAIT_TIMEOUT_MS = 10000;

  private final TopicUpdateLog topicUpdateLog_ = new TopicUpdateLog();

  /**
   * Initialize the CatalogServiceCatalog. If 'loadInBackground' is true, table metadata
   * will be loaded in the background. 'initialHmsCnxnTimeoutSec' specifies the time (in
   * seconds) CatalogServiceCatalog will wait to establish an initial connection to the
   * HMS before giving up. Using this setting allows catalogd and HMS to be started
   * simultaneously.
   */
  public CatalogServiceCatalog(boolean loadInBackground, int numLoadingThreads,
      int initialHmsCnxnTimeoutSec, SentryConfig sentryConfig, TUniqueId catalogServiceId,
      String kerberosPrincipal, String localLibraryPath) {
    super(INITIAL_META_STORE_CLIENT_POOL_SIZE, initialHmsCnxnTimeoutSec);
    catalogServiceId_ = catalogServiceId;
    tableLoadingMgr_ = new TableLoadingMgr(this, numLoadingThreads);
    loadInBackground_ = loadInBackground;
    try {
      // We want only 'true' HDFS filesystems to poll the HDFS cache (i.e not S3,
      // local, etc.)
      if (FileSystemUtil.getDefaultFileSystem() instanceof DistributedFileSystem) {
        cachePoolReader_.scheduleAtFixedRate(
            new CachePoolReader(false), 0, 1, TimeUnit.MINUTES);
      }
    } catch (IOException e) {
      LOG.error("Couldn't identify the default FS. Cache Pool reader will be disabled.");
    }
    if (sentryConfig != null) {
      sentryProxy_ = new SentryProxy(sentryConfig, this, kerberosPrincipal);
    } else {
      sentryProxy_ = null;
    }
    localLibraryPath_ = new String("file://" + localLibraryPath);
    deleteLog_ = new CatalogDeltaLog();
  }

  // Timeout for acquiring a table lock
  // TODO: Make this configurable
  private static final long TBL_LOCK_TIMEOUT_MS = 7200000;
  // Time to sleep before retrying to acquire a table lock
  private static final int TBL_LOCK_RETRY_MS = 10;

  /**
   * Tries to acquire versionLock_ and the lock of 'tbl' in that order. Returns true if it
   * successfully acquires both within TBL_LOCK_TIMEOUT_MS millisecs; both locks are held
   * when the function returns. Returns false otherwise and no lock is held in this case.
   */
  public boolean tryLockTable(Table tbl) {
    long begin = System.currentTimeMillis();
    long end;
    do {
      versionLock_.writeLock().lock();
      if (tbl.getLock().tryLock()) {
        if (LOG.isTraceEnabled()) {
          end = System.currentTimeMillis();
          LOG.trace(String.format("Lock for table %s was acquired in %d msec",
              tbl.getFullName(), end - begin));
        }
        return true;
      }
      versionLock_.writeLock().unlock();
      try {
        // Sleep to avoid spinning and allow other operations to make progress.
        Thread.sleep(TBL_LOCK_RETRY_MS);
      } catch (InterruptedException e) {
        // ignore
      }
      end = System.currentTimeMillis();
    } while (end - begin < TBL_LOCK_TIMEOUT_MS);
    return false;
  }

  /**
   * Reads the current set of cache pools from HDFS and updates the catalog.
   * Called periodically by the cachePoolReader_.
   */
  protected class CachePoolReader implements Runnable {
    // If true, existing cache pools will get a new catalog version and, consequently,
    // they will be added to the next topic update, triggering an update in each
    // coordinator's local catalog cache. This is needed for the case of INVALIDATE
    // METADATA where a new catalog version needs to be assigned to every catalog object.
    private final boolean incrementVersions_;
    /**
     * This constructor is needed to create a non-threaded execution of the class.
     */
    public CachePoolReader(boolean incrementVersions) {
      super();
      incrementVersions_ = incrementVersions;
    }

    public void run() {
      if (LOG.isTraceEnabled()) LOG.trace("Reloading cache pool names from HDFS");

      // Map of cache pool name to CachePoolInfo. Stored in a map to allow Set operations
      // to be performed on the keys.
      Map<String, CachePoolInfo> currentCachePools = Maps.newHashMap();
      try {
        DistributedFileSystem dfs = FileSystemUtil.getDistributedFileSystem();
        RemoteIterator<CachePoolEntry> itr = dfs.listCachePools();
        while (itr.hasNext()) {
          CachePoolInfo cachePoolInfo = itr.next().getInfo();
          currentCachePools.put(cachePoolInfo.getPoolName(), cachePoolInfo);
        }
      } catch (Exception e) {
        LOG.error("Error loading cache pools: ", e);
        return;
      }

      versionLock_.writeLock().lock();
      try {
        // Determine what has changed relative to what we have cached.
        Set<String> droppedCachePoolNames = Sets.difference(
            hdfsCachePools_.keySet(), currentCachePools.keySet());
        Set<String> createdCachePoolNames = Sets.difference(
            currentCachePools.keySet(), hdfsCachePools_.keySet());
        Set<String> survivingCachePoolNames = Sets.difference(
            hdfsCachePools_.keySet(), droppedCachePoolNames);
        // Add all new cache pools.
        for (String createdCachePool: createdCachePoolNames) {
          HdfsCachePool cachePool = new HdfsCachePool(
              currentCachePools.get(createdCachePool));
          cachePool.setCatalogVersion(incrementAndGetCatalogVersion());
          hdfsCachePools_.add(cachePool);
        }
        // Remove dropped cache pools.
        for (String cachePoolName: droppedCachePoolNames) {
          HdfsCachePool cachePool = hdfsCachePools_.remove(cachePoolName);
          if (cachePool != null) {
            cachePool.setCatalogVersion(incrementAndGetCatalogVersion());
            TCatalogObject removedObject =
                new TCatalogObject(TCatalogObjectType.HDFS_CACHE_POOL,
                    cachePool.getCatalogVersion());
            removedObject.setCache_pool(cachePool.toThrift());
            deleteLog_.addRemovedObject(removedObject);
          }
        }
        if (incrementVersions_) {
          // Increment the version of existing pools in order to be added to the next
          // topic update.
          for (String survivingCachePoolName: survivingCachePoolNames) {
            HdfsCachePool cachePool = hdfsCachePools_.get(survivingCachePoolName);
            Preconditions.checkNotNull(cachePool);
            cachePool.setCatalogVersion(incrementAndGetCatalogVersion());
          }
        }
      } finally {
        versionLock_.writeLock().unlock();
      }
    }
  }

  /**
   * Adds a list of cache directive IDs for the given table name. Asynchronously
   * refreshes the table metadata once all cache directives complete.
   */
  public void watchCacheDirs(List<Long> dirIds, TTableName tblName) {
    tableLoadingMgr_.watchCacheDirs(dirIds, tblName);
  }

  /**
   * Prioritizes the loading of the given list TCatalogObjects. Currently only support
   * loading Table/View metadata since Db and Function metadata is not loaded lazily.
   */
  public void prioritizeLoad(List<TCatalogObject> objectDescs) {
    for (TCatalogObject catalogObject: objectDescs) {
      Preconditions.checkState(catalogObject.isSetTable());
      TTable table = catalogObject.getTable();
      tableLoadingMgr_.prioritizeLoad(new TTableName(table.getDb_name().toLowerCase(),
          table.getTbl_name().toLowerCase()));
    }
  }

  /**
   * The context for add*ToCatalogDelta(), called by getCatalogDelta. It contains
   * callback information, version range and collected topics.
   */
  class GetCatalogDeltaContext {
    // The CatalogServer pointer for NativeAddPendingTopicItem() callback.
    long nativeCatalogServerPtr;
    // The from and to version of this delta.
    long fromVersion;
    long toVersion;
    // The keys of the updated topics.
    Set<String> updatedCatalogObjects;
    TSerializer serializer;

    GetCatalogDeltaContext(long nativeCatalogServerPtr, long fromVersion, long toVersion)
    {
      this.nativeCatalogServerPtr = nativeCatalogServerPtr;
      this.fromVersion = fromVersion;
      this.toVersion = toVersion;
      updatedCatalogObjects = new HashSet<>();
      serializer = new TSerializer(new TBinaryProtocol.Factory());
    }

    void addCatalogObject(TCatalogObject obj, boolean delete) throws TException {
      String key = Catalog.toCatalogObjectKey(obj);
      if (obj.type != TCatalogObjectType.CATALOG) {
        topicUpdateLog_.add(key,
            new TopicUpdateLog.Entry(0, obj.getCatalog_version(), toVersion));
        if (!delete) updatedCatalogObjects.add(key);
      }
      // TODO: TSerializer.serialize() returns a copy of the internal byte array, which
      // could be elided.
      byte[] data = serializer.serialize(obj);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Collected catalog " + (delete ? "deletion: " : "update: ") + key +
            " version: " + obj.catalog_version);
      }
      if (!FeSupport.NativeAddPendingTopicItem(nativeCatalogServerPtr, key, data, delete))
      {
        LOG.error("NativeAddPendingTopicItem failed in BE. key=" + key + ", delete="
            + delete + ", data_size=" + data.length);
      }
    }
  }

  /**
   * Identifies the catalog objects that were added/modified/deleted in the catalog with
   * versions > 'fromVersion'. It operates on a snaphsot of the catalog without holding
   * the catalog lock which means that other concurrent metadata operations can still make
   * progress while the catalog delta is computed. An entry in the topic update log is
   * added for every catalog object that is included in the catalog delta. The log is
   * examined by operations using SYNC_DDL to determine which topic update covers the
   * result set of metadata operation. Once the catalog delta is computed, the entries in
   * the delete log with versions less than 'fromVersion' are garbage collected.
   * The catalog delta is passed to the backend by calling NativeAddPendingTopicItem().
   */
  public long getCatalogDelta(long nativeCatalogServerPtr, long fromVersion) throws
      TException {
    GetCatalogDeltaContext ctx = new GetCatalogDeltaContext(nativeCatalogServerPtr,
        fromVersion, getCatalogVersion());
    for (Db db: getAllDbs()) {
      addDatabaseToCatalogDelta(db, ctx);
    }
    for (DataSource dataSource: getAllDataSources()) {
      addDataSourceToCatalogDelta(dataSource, ctx);
    }
    for (HdfsCachePool cachePool: getAllHdfsCachePools()) {
      addHdfsCachePoolToCatalogDelta(cachePool, ctx);
    }
    for (Role role: getAllRoles()) {
      addRoleToCatalogDelta(role, ctx);
    }
    // Identify the catalog objects that were removed from the catalog for which their
    // versions are in range ('ctx.fromVersion', 'ctx.toVersion']. We need to make sure
    // that we don't include "deleted" objects that were re-added to the catalog.
    for (TCatalogObject removedObject:
        getDeletedObjects(ctx.fromVersion, ctx.toVersion)) {
      if (!ctx.updatedCatalogObjects.contains(
          Catalog.toCatalogObjectKey(removedObject))) {
        ctx.addCatalogObject(removedObject, true);
      }
    }
    // Each topic update should contain a single "TCatalog" object which is used to
    // pass overall state on the catalog, such as the current version and the
    // catalog service id. By setting the catalog version to the latest catalog
    // version at this point, it ensures impalads will always bump their versions,
    // even in the case where an object has been dropped.
    TCatalogObject catalog =
        new TCatalogObject(TCatalogObjectType.CATALOG, ctx.toVersion);
    catalog.setCatalog(new TCatalog(catalogServiceId_));
    ctx.addCatalogObject(catalog, false);
    // Garbage collect the delete and topic update log.
    deleteLog_.garbageCollect(ctx.toVersion);
    topicUpdateLog_.garbageCollectUpdateLogEntries(ctx.toVersion);
    lastSentTopicUpdate_.set(ctx.toVersion);
    // Notify any operation that is waiting on the next topic update.
    synchronized (topicUpdateLog_) {
      topicUpdateLog_.notifyAll();
    }
    return ctx.toVersion;
  }


  /**
   * Get a snapshot view of all the catalog objects that were deleted between versions
   * ('fromVersion', 'toVersion'].
   */
  private List<TCatalogObject> getDeletedObjects(long fromVersion, long toVersion) {
    versionLock_.readLock().lock();
    try {
      return deleteLog_.retrieveObjects(fromVersion, toVersion);
    } finally {
      versionLock_.readLock().unlock();
    }
  }

  /**
   * Get a snapshot view of all the databases in the catalog.
   */
  private List<Db> getAllDbs() {
    versionLock_.readLock().lock();
    try {
      return ImmutableList.copyOf(dbCache_.get().values());
    } finally {
      versionLock_.readLock().unlock();
    }
  }

  /**
   * Get a snapshot view of all the data sources in the catalog.
   */
   private List<DataSource> getAllDataSources() {
    versionLock_.readLock().lock();
    try {
      return ImmutableList.copyOf(getDataSources());
    } finally {
      versionLock_.readLock().unlock();
    }
  }

  /**
   * Get a snapshot view of all the Hdfs cache pools in the catalog.
   */
  private List<HdfsCachePool> getAllHdfsCachePools() {
    versionLock_.readLock().lock();
    try {
      return ImmutableList.copyOf(hdfsCachePools_);
    } finally {
      versionLock_.readLock().unlock();
    }
  }

  /**
   * Get a snapshot view of all the roles in the catalog.
   */
  private List<Role> getAllRoles() {
    versionLock_.readLock().lock();
    try {
      return ImmutableList.copyOf(authPolicy_.getAllRoles());
    } finally {
      versionLock_.readLock().unlock();
    }
  }

  /**
   * Adds a database in the topic update if its version is in the range
   * ('ctx.fromVersion', 'ctx.toVersion']. It iterates through all the tables and
   * functions of this database to determine if they can be included in the topic update.
   */
  private void addDatabaseToCatalogDelta(Db db, GetCatalogDeltaContext ctx)
      throws TException {
    long dbVersion = db.getCatalogVersion();
    if (dbVersion > ctx.fromVersion && dbVersion <= ctx.toVersion) {
      TCatalogObject catalogDb =
          new TCatalogObject(TCatalogObjectType.DATABASE, dbVersion);
      catalogDb.setDb(db.toThrift());
      ctx.addCatalogObject(catalogDb, false);
    }
    for (Table tbl: getAllTables(db)) {
      addTableToCatalogDelta(tbl, ctx);
    }
    for (Function fn: getAllFunctions(db)) {
      addFunctionToCatalogDelta(fn, ctx);
    }
  }

  /**
   * Get a snapshot view of all the tables in a database.
   */
  private List<Table> getAllTables(Db db) {
    Preconditions.checkNotNull(db);
    versionLock_.readLock().lock();
    try {
      return ImmutableList.copyOf(db.getTables());
    } finally {
      versionLock_.readLock().unlock();
    }
  }

  /**
   * Get a snapshot view of all the functions in a database.
   */
  private List<Function> getAllFunctions(Db db) {
    Preconditions.checkNotNull(db);
    versionLock_.readLock().lock();
    try {
      return ImmutableList.copyOf(db.getFunctions(null, new PatternMatcher()));
    } finally {
      versionLock_.readLock().unlock();
    }
  }

  /**
   * Adds a table in the topic update if its version is in the range
   * ('ctx.fromVersion', 'ctx.toVersion']. If the table's version is larger than
   * 'ctx.toVersion' and the table has skipped a topic update
   * 'MAX_NUM_SKIPPED_TOPIC_UPDATES' times, it is included in the topic update. This
   * prevents tables that are updated frequently from skipping topic updates indefinitely,
   * which would also violate the semantics of SYNC_DDL.
   */
  private void addTableToCatalogDelta(Table tbl, GetCatalogDeltaContext ctx)
      throws TException  {
    if (tbl.getCatalogVersion() <= ctx.toVersion) {
      addTableToCatalogDeltaHelper(tbl, ctx);
    } else {
      TopicUpdateLog.Entry topicUpdateEntry =
          topicUpdateLog_.getOrCreateLogEntry(tbl.getUniqueName());
      Preconditions.checkNotNull(topicUpdateEntry);
      if (topicUpdateEntry.getNumSkippedTopicUpdates() == MAX_NUM_SKIPPED_TOPIC_UPDATES) {
        addTableToCatalogDeltaHelper(tbl, ctx);
      } else {
        LOG.info("Table " + tbl.getFullName() + " is skipping topic update " +
            ctx.toVersion);
        topicUpdateLog_.add(tbl.getUniqueName(),
            new TopicUpdateLog.Entry(
                topicUpdateEntry.getNumSkippedTopicUpdates() + 1,
                topicUpdateEntry.getLastSentVersion(),
                topicUpdateEntry.getLastSentCatalogUpdate()));
      }
    }
  }

  /**
   * Helper function that tries to add a table in a topic update. It acquires table's
   * lock and checks if its version is in the ('ctx.fromVersion', 'ctx.toVersion'] range
   * and how many consecutive times (if any) has the table skipped a topic update.
   */
  private void addTableToCatalogDeltaHelper(Table tbl, GetCatalogDeltaContext ctx)
      throws TException {
    TCatalogObject catalogTbl =
        new TCatalogObject(TCatalogObjectType.TABLE, Catalog.INITIAL_CATALOG_VERSION);
    tbl.getLock().lock();
    try {
      long tblVersion = tbl.getCatalogVersion();
      if (tblVersion <= ctx.fromVersion) return;
      TopicUpdateLog.Entry topicUpdateEntry =
          topicUpdateLog_.getOrCreateLogEntry(tbl.getUniqueName());
      if (tblVersion > ctx.toVersion &&
          topicUpdateEntry.getNumSkippedTopicUpdates() < MAX_NUM_SKIPPED_TOPIC_UPDATES) {
        LOG.info("Table " + tbl.getFullName() + " is skipping topic update " +
            ctx.toVersion);
        topicUpdateLog_.add(tbl.getUniqueName(),
            new TopicUpdateLog.Entry(
                topicUpdateEntry.getNumSkippedTopicUpdates() + 1,
                topicUpdateEntry.getLastSentVersion(),
                topicUpdateEntry.getLastSentCatalogUpdate()));
        return;
      }
      try {
        catalogTbl.setTable(tbl.toThrift());
      } catch (Exception e) {
        LOG.error(String.format("Error calling toThrift() on table %s: %s",
            tbl.getFullName(), e.getMessage()), e);
        return;
      }
      catalogTbl.setCatalog_version(tbl.getCatalogVersion());
      ctx.addCatalogObject(catalogTbl, false);
    } finally {
      tbl.getLock().unlock();
    }
  }

  /**
   * Adds a function to the topic update if its version is in the range
   * ('ctx.fromVersion', 'ctx.toVersion'].
   */
  private void addFunctionToCatalogDelta(Function fn, GetCatalogDeltaContext ctx)
      throws TException {
    long fnVersion = fn.getCatalogVersion();
    if (fnVersion <= ctx.fromVersion || fnVersion > ctx.toVersion) return;
    TCatalogObject function =
        new TCatalogObject(TCatalogObjectType.FUNCTION, fnVersion);
    function.setFn(fn.toThrift());
    ctx.addCatalogObject(function, false);
  }

  /**
   * Adds a data source to the topic update if its version is in the range
   * ('ctx.fromVersion', 'ctx.toVersion'].
   */
  private void addDataSourceToCatalogDelta(DataSource dataSource,
      GetCatalogDeltaContext ctx) throws TException  {
    long dsVersion = dataSource.getCatalogVersion();
    if (dsVersion <= ctx.fromVersion || dsVersion > ctx.toVersion) return;
    TCatalogObject catalogObj =
        new TCatalogObject(TCatalogObjectType.DATA_SOURCE, dsVersion);
    catalogObj.setData_source(dataSource.toThrift());
    ctx.addCatalogObject(catalogObj, false);
  }

  /**
   * Adds a HDFS cache pool to the topic update if its version is in the range
   * ('ctx.fromVersion', 'ctx.toVersion'].
   */
  private void addHdfsCachePoolToCatalogDelta(HdfsCachePool cachePool,
      GetCatalogDeltaContext ctx) throws TException  {
    long cpVersion = cachePool.getCatalogVersion();
    if (cpVersion <= ctx.fromVersion || cpVersion > ctx.toVersion) {
      return;
    }
    TCatalogObject pool =
        new TCatalogObject(TCatalogObjectType.HDFS_CACHE_POOL, cpVersion);
    pool.setCache_pool(cachePool.toThrift());
    ctx.addCatalogObject(pool, false);
  }


  /**
   * Adds a role to the topic update if its version is in the range
   * ('ctx.fromVersion', 'ctx.toVersion']. It iterates through all the privileges of
   * this role to determine if they can be inserted in the topic update.
   */
  private void addRoleToCatalogDelta(Role role, GetCatalogDeltaContext ctx)
      throws TException  {
    long roleVersion = role.getCatalogVersion();
    if (roleVersion > ctx.fromVersion && roleVersion <= ctx.toVersion) {
      TCatalogObject thriftRole =
          new TCatalogObject(TCatalogObjectType.ROLE, roleVersion);
      thriftRole.setRole(role.toThrift());
      ctx.addCatalogObject(thriftRole, false);
    }
    for (RolePrivilege p: getAllPrivileges(role)) {
      addRolePrivilegeToCatalogDelta(p, ctx);
    }
  }

  /**
   * Get a snapshot view of all the privileges in a role.
   */
  private List<RolePrivilege> getAllPrivileges(Role role) {
    Preconditions.checkNotNull(role);
    versionLock_.readLock().lock();
    try {
      return ImmutableList.copyOf(role.getPrivileges());
    } finally {
      versionLock_.readLock().unlock();
    }
  }

  /**
   * Adds a role privilege to the topic update if its version is in the range
   * ('ctx.fromVersion', 'ctx.toVersion'].
   */
  private void addRolePrivilegeToCatalogDelta(RolePrivilege priv,
      GetCatalogDeltaContext ctx) throws TException  {
    long privVersion = priv.getCatalogVersion();
    if (privVersion <= ctx.fromVersion || privVersion > ctx.toVersion) return;
    TCatalogObject privilege =
        new TCatalogObject(TCatalogObjectType.PRIVILEGE, privVersion);
    privilege.setPrivilege(priv.toThrift());
    ctx.addCatalogObject(privilege, false);
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
   * Checks if the Hive function 'fn' is Impala compatible. A function is Impala
   * compatible iff
   *
   * 1. The function is JAVA based,
   * 2. Has exactly one binary resource associated (We don't support loading
   *    dependencies yet) and
   * 3. The binary is of type JAR.
   *
   * Returns true if compatible and false otherwise. In case of incompatible
   * functions 'incompatMsg' has the reason for the incompatibility.
   * */
   public static boolean isFunctionCompatible(
       org.apache.hadoop.hive.metastore.api.Function fn, StringBuilder incompatMsg) {
    boolean isCompatible = true;
    if (fn.getFunctionType() != FunctionType.JAVA) {
      isCompatible = false;
      incompatMsg.append("Function type: " + fn.getFunctionType().name()
          + " is not supported. Only " + FunctionType.JAVA.name() + " functions "
          + "are supported.");
    } else if (fn.getResourceUrisSize() == 0) {
      isCompatible = false;
      incompatMsg.append("No executable binary resource (like a JAR file) is " +
          "associated with this function. To fix this, recreate the function by " +
          "specifying a 'location' in the function create statement.");
    } else if (fn.getResourceUrisSize() != 1) {
      isCompatible = false;
      List<String> resourceUris = Lists.newArrayList();
      for (ResourceUri resource: fn.getResourceUris()) {
        resourceUris.add(resource.getUri());
      }
      incompatMsg.append("Impala does not support multiple Jars for dependencies."
          + "(" + Joiner.on(",").join(resourceUris) + ") ");
    } else if (fn.getResourceUris().get(0).getResourceType() != ResourceType.JAR) {
      isCompatible = false;
      incompatMsg.append("Function binary type: " +
        fn.getResourceUris().get(0).getResourceType().name()
        + " is not supported. Only " + ResourceType.JAR.name()
        + " type is supported.");
    }
    return isCompatible;
  }

  /**
   * Returns a list of Impala Functions, one per compatible "evaluate" method in the UDF
   * class referred to by the given Java function. This method copies the UDF Jar
   * referenced by "function" to a temporary file in localLibraryPath_ and loads it
   * into the jvm. Then we scan all the methods in the class using reflection and extract
   * those methods and create corresponding Impala functions. Currently Impala supports
   * only "JAR" files for symbols and also a single Jar containing all the dependent
   * classes rather than a set of Jar files.
   */
  public static List<Function> extractFunctions(String db,
      org.apache.hadoop.hive.metastore.api.Function function)
      throws ImpalaRuntimeException{
    List<Function> result = Lists.newArrayList();
    List<String> addedSignatures = Lists.newArrayList();
    StringBuilder warnMessage = new StringBuilder();
    if (!isFunctionCompatible(function, warnMessage)) {
      LOG.warn("Skipping load of incompatible function: " +
          function.getFunctionName() + ". " + warnMessage.toString());
      return result;
    }
    String jarUri = function.getResourceUris().get(0).getUri();
    Class<?> udfClass = null;
    Path localJarPath = null;
    try {
      localJarPath = new Path(localLibraryPath_, UUID.randomUUID().toString() + ".jar");
      try {
        FileSystemUtil.copyToLocal(new Path(jarUri), localJarPath);
      } catch (IOException e) {
        String errorMsg = "Error loading Java function: " + db + "." +
            function.getFunctionName() + ". Couldn't copy " + jarUri +
            " to local path: " + localJarPath.toString();
        LOG.error(errorMsg, e);
        throw new ImpalaRuntimeException(errorMsg);
      }
      URL[] classLoaderUrls = new URL[] {new URL(localJarPath.toString())};
      URLClassLoader urlClassLoader = new URLClassLoader(classLoaderUrls);
      udfClass = urlClassLoader.loadClass(function.getClassName());
      // Check if the class is of UDF type. Currently we don't support other functions
      // TODO: Remove this once we support Java UDAF/UDTF
      if (FunctionUtils.getUDFClassType(udfClass) != FunctionUtils.UDFClassType.UDF) {
        LOG.warn("Ignoring load of incompatible Java function: " +
            function.getFunctionName() + " as " + FunctionUtils.getUDFClassType(udfClass)
            + " is not a supported type. Only UDFs are supported");
        return result;
      }
      // Load each method in the UDF class and create the corresponding Impala Function
      // object.
      for (Method m: udfClass.getMethods()) {
        if (!m.getName().equals(UdfExecutor.UDF_FUNCTION_NAME)) continue;
        Function fn = ScalarFunction.fromHiveFunction(db,
            function.getFunctionName(), function.getClassName(),
            m.getParameterTypes(), m.getReturnType(), jarUri);
        if (fn == null) {
          LOG.warn("Ignoring incompatible method: " + m.toString() + " during load of " +
             "Hive UDF:" + function.getFunctionName() + " from " + udfClass);
          continue;
        }
        if (!addedSignatures.contains(fn.signatureString())) {
          result.add(fn);
          addedSignatures.add(fn.signatureString());
        }
      }
    } catch (ClassNotFoundException c) {
      String errorMsg = "Error loading Java function: " + db + "." +
          function.getFunctionName() + ". Symbol class " + udfClass +
          "not found in Jar: " + jarUri;
      LOG.error(errorMsg);
      throw new ImpalaRuntimeException(errorMsg, c);
    } catch (Exception e) {
      LOG.error("Skipping function load: " + function.getFunctionName(), e);
      throw new ImpalaRuntimeException("Error extracting functions", e);
    } catch (LinkageError e) {
      String errorMsg = "Error resolving dependencies for Java function: " + db + "." +
          function.getFunctionName();
      LOG.error(errorMsg);
      throw new ImpalaRuntimeException(errorMsg, e);
    } finally {
      if (localJarPath != null) FileSystemUtil.deleteIfExists(localJarPath);
    }
    return result;
  }

 /**
   * Extracts Impala functions stored in metastore db parameters and adds them to
   * the catalog cache.
   */
  private void loadFunctionsFromDbParams(Db db,
      org.apache.hadoop.hive.metastore.api.Database msDb) {
    if (msDb == null || msDb.getParameters() == null) return;
    LOG.info("Loading native functions for database: " + db.getName());
    TCompactProtocol.Factory protocolFactory = new TCompactProtocol.Factory();
    for (String key: msDb.getParameters().keySet()) {
      if (!key.startsWith(Db.FUNCTION_INDEX_PREFIX)) continue;
      try {
        TFunction fn = new TFunction();
        JniUtil.deserializeThrift(protocolFactory, fn,
            Base64.decodeBase64(msDb.getParameters().get(key)));
        Function addFn = Function.fromThrift(fn);
        db.addFunction(addFn, false);
        addFn.setCatalogVersion(incrementAndGetCatalogVersion());
      } catch (ImpalaException e) {
        LOG.error("Encountered an error during function load: key=" + key
            + ",continuing", e);
      }
    }
    LOG.info("Loaded native functions for database: " + db.getName());
  }

  /**
   * Loads Java functions into the catalog. For each function in "functions",
   * we extract all Impala compatible evaluate() signatures and load them
   * as separate functions in the catalog.
   */
  private void loadJavaFunctions(Db db,
      List<org.apache.hadoop.hive.metastore.api.Function> functions) {
    Preconditions.checkNotNull(functions);
    LOG.info("Loading Java functions for database: " + db.getName());
    for (org.apache.hadoop.hive.metastore.api.Function function: functions) {
      try {
        for (Function fn: extractFunctions(db.getName(), function)) {
          db.addFunction(fn);
          fn.setCatalogVersion(incrementAndGetCatalogVersion());
        }
      } catch (Exception e) {
        LOG.error("Skipping function load: " + function.getFunctionName(), e);
      }
    }
    LOG.info("Loaded Java functions for database: " + db.getName());
  }

  /**
   * Reloads function metadata for 'dbName' database. Populates the 'addedFuncs' list
   * with functions that were added as a result of this operation. Populates the
   * 'removedFuncs' list with functions that were removed.
   */
  public void refreshFunctions(MetaStoreClient msClient, String dbName,
      List<TCatalogObject> addedFuncs, List<TCatalogObject> removedFuncs)
      throws CatalogException {
    // Create a temporary database that will contain all the functions from the HMS.
    Db tmpDb;
    try {
      List<org.apache.hadoop.hive.metastore.api.Function> javaFns =
          Lists.newArrayList();
      for (String javaFn : msClient.getHiveClient().getFunctions(dbName, "*")) {
        javaFns.add(msClient.getHiveClient().getFunction(dbName, javaFn));
      }
      // Contains native functions in it's params map.
      org.apache.hadoop.hive.metastore.api.Database msDb =
          msClient.getHiveClient().getDatabase(dbName);
      tmpDb = new Db(dbName, null);
      // Load native UDFs into the temporary db.
      loadFunctionsFromDbParams(tmpDb, msDb);
      // Load Java UDFs from HMS into the temporary db.
      loadJavaFunctions(tmpDb, javaFns);

      Db db = dbCache_.get().get(dbName);
      if (db == null) {
        throw new DatabaseNotFoundException("Database does not exist: " + dbName);
      }
      // Load transient functions into the temporary db.
      for (Function fn: db.getTransientFunctions()) tmpDb.addFunction(fn);

      // Compute the removed functions and remove them from the db.
      for (Map.Entry<String, List<Function>> e: db.getAllFunctions().entrySet()) {
        for (Function fn: e.getValue()) {
          if (tmpDb.getFunction(
              fn, Function.CompareMode.IS_INDISTINGUISHABLE) == null) {
            fn.setCatalogVersion(incrementAndGetCatalogVersion());
            removedFuncs.add(fn.toTCatalogObject());
          }
        }
      }

      // We will re-add all the functions to the db because it's possible that a
      // function was dropped and a different function (for example, the binary is
      // different) with the same name and signature was re-added in Hive.
      db.removeAllFunctions();
      for (Map.Entry<String, List<Function>> e: tmpDb.getAllFunctions().entrySet()) {
        for (Function fn: e.getValue()) {
          // We do not need to increment and acquire a new catalog version for this
          // function here because this already happens when the functions are loaded
          // into tmpDb.
          db.addFunction(fn);
          addedFuncs.add(fn.toTCatalogObject());
        }
      }

    } catch (Exception e) {
      throw new CatalogException("Error refreshing functions in " + dbName + ": ", e);
    }
  }

  /**
   * Invalidates the database 'db'. This method can have potential race
   * conditions with external changes to the Hive metastore and hence any
   * conflicting changes to the objects can manifest in the form of exceptions
   * from the HMS calls which are appropriately handled. Returns the invalidated
   * 'Db' object along with list of tables to be loaded by the TableLoadingMgr.
   * Returns null if the method encounters an exception during invalidation.
   */
  private Pair<Db, List<TTableName>> invalidateDb(
      MetaStoreClient msClient, String dbName, Db existingDb) {
    try {
      List<org.apache.hadoop.hive.metastore.api.Function> javaFns =
          Lists.newArrayList();
      for (String javaFn: msClient.getHiveClient().getFunctions(dbName, "*")) {
        javaFns.add(msClient.getHiveClient().getFunction(dbName, javaFn));
      }
      org.apache.hadoop.hive.metastore.api.Database msDb =
          msClient.getHiveClient().getDatabase(dbName);
      Db newDb = new Db(dbName, msDb);
      // existingDb is usually null when the Catalog loads for the first time.
      // In that case we needn't restore any transient functions.
      if (existingDb != null) {
        // Restore UDFs that aren't persisted. They are only cleaned up on
        // Catalog restart.
        for (Function fn: existingDb.getTransientFunctions()) {
          newDb.addFunction(fn);
          fn.setCatalogVersion(incrementAndGetCatalogVersion());
        }
      }
      // Reload native UDFs.
      loadFunctionsFromDbParams(newDb, msDb);
      // Reload Java UDFs from HMS.
      loadJavaFunctions(newDb, javaFns);
      newDb.setCatalogVersion(incrementAndGetCatalogVersion());

      List<TTableName> tblsToBackgroundLoad = Lists.newArrayList();
      for (String tableName: msClient.getHiveClient().getAllTables(dbName)) {
        Table incompleteTbl = IncompleteTable.createUninitializedTable(newDb, tableName);
        incompleteTbl.setCatalogVersion(incrementAndGetCatalogVersion());
        newDb.addTable(incompleteTbl);
        if (loadInBackground_) {
          tblsToBackgroundLoad.add(new TTableName(dbName, tableName.toLowerCase()));
        }
      }

      if (existingDb != null) {
        // Identify any removed functions and add them to the delta log.
        for (Map.Entry<String, List<Function>> e:
             existingDb.getAllFunctions().entrySet()) {
          for (Function fn: e.getValue()) {
            if (newDb.getFunction(fn,
                Function.CompareMode.IS_INDISTINGUISHABLE) == null) {
              fn.setCatalogVersion(incrementAndGetCatalogVersion());
              deleteLog_.addRemovedObject(fn.toTCatalogObject());
            }
          }
        }

        // Identify any deleted tables and add them to the delta log
        Set<String> oldTableNames = Sets.newHashSet(existingDb.getAllTableNames());
        Set<String> newTableNames = Sets.newHashSet(newDb.getAllTableNames());
        oldTableNames.removeAll(newTableNames);
        for (String removedTableName: oldTableNames) {
          Table removedTable = IncompleteTable.createUninitializedTable(existingDb,
              removedTableName);
          removedTable.setCatalogVersion(incrementAndGetCatalogVersion());
          deleteLog_.addRemovedObject(removedTable.toTCatalogObject());
        }
      }
      return Pair.create(newDb, tblsToBackgroundLoad);
    } catch (Exception e) {
      LOG.warn("Encountered an exception while invalidating database: " + dbName +
          ". Ignoring further load of this db.", e);
    }
    return null;
  }

  /**
   * Resets this catalog instance by clearing all cached table and database metadata.
   * Returns the current catalog version before reset has taken any effect. The
   * requesting impalad will use that version to determine when the
   * effects of reset have been applied to its local catalog cache.
   */
  public long reset() throws CatalogException {
    long currentCatalogVersion = getCatalogVersion();
    LOG.info("Invalidating all metadata. Version: " + currentCatalogVersion);
    // First update the policy metadata.
    if (sentryProxy_ != null) {
      // Sentry Service is enabled.
      try {
        // Update the authorization policy, waiting for the result to complete.
        sentryProxy_.refresh(true);
      } catch (Exception e) {
        throw new CatalogException("Error updating authorization policy: ", e);
      }
    }

    // Update the HDFS cache pools
    CachePoolReader reader = new CachePoolReader(true);
    reader.run();

    versionLock_.writeLock().lock();
    // In case of an empty new catalog, the version should still change to reflect the
    // reset operation itself and to unblock impalads by making the catalog version >
    // INITIAL_CATALOG_VERSION. See Frontend.waitForCatalog()
    ++catalogVersion_;
    // Assign new versions to all the loaded data sources.
    for (DataSource dataSource: getDataSources()) {
      dataSource.setCatalogVersion(incrementAndGetCatalogVersion());
    }

    // Update db and table metadata
    try {
      // Not all Java UDFs are persisted to the metastore. The ones which aren't
      // should be restored once the catalog has been invalidated.
      Map<String, Db> oldDbCache = dbCache_.get();

      // Build a new DB cache, populate it, and replace the existing cache in one
      // step.
      ConcurrentHashMap<String, Db> newDbCache = new ConcurrentHashMap<String, Db>();
      List<TTableName> tblsToBackgroundLoad = Lists.newArrayList();
      try (MetaStoreClient msClient = getMetaStoreClient()) {
        for (String dbName: msClient.getHiveClient().getAllDatabases()) {
          dbName = dbName.toLowerCase();
          Db oldDb = oldDbCache.get(dbName);
          Pair<Db, List<TTableName>> invalidatedDb = invalidateDb(msClient,
              dbName, oldDb);
          if (invalidatedDb == null) continue;
          newDbCache.put(dbName, invalidatedDb.first);
          tblsToBackgroundLoad.addAll(invalidatedDb.second);
        }
      }
      dbCache_.set(newDbCache);

      // Identify any deleted databases and add them to the delta log.
      Set<String> oldDbNames = oldDbCache.keySet();
      Set<String> newDbNames = newDbCache.keySet();
      oldDbNames.removeAll(newDbNames);
      for (String dbName: oldDbNames) {
        Db removedDb = oldDbCache.get(dbName);
        updateDeleteLog(removedDb);
      }

      // Submit tables for background loading.
      for (TTableName tblName: tblsToBackgroundLoad) {
        tableLoadingMgr_.backgroundLoad(tblName);
      }
    } catch (Exception e) {
      LOG.error(e);
      throw new CatalogException("Error initializing Catalog. Catalog may be empty.", e);
    } finally {
      versionLock_.writeLock().unlock();
    }
    LOG.info("Invalidated all metadata.");
    return currentCatalogVersion;
  }

  /**
   * Adds a database name to the metadata cache and returns the database's
   * new Db object. Used by CREATE DATABASE statements.
   */
  public Db addDb(String dbName, org.apache.hadoop.hive.metastore.api.Database msDb) {
    Db newDb = new Db(dbName, msDb);
    versionLock_.writeLock().lock();
    try {
      newDb.setCatalogVersion(incrementAndGetCatalogVersion());
      addDb(newDb);
      return newDb;
    } finally {
      versionLock_.writeLock().unlock();
    }
  }

  /**
   * Removes a database from the metadata cache and returns the removed database,
   * or null if the database did not exist in the cache.
   * Used by DROP DATABASE statements.
   */
  @Override
  public Db removeDb(String dbName) {
    versionLock_.writeLock().lock();
    try {
      Db removedDb = super.removeDb(dbName);
      if (removedDb != null) updateDeleteLog(removedDb);
      return removedDb;
    } finally {
      versionLock_.writeLock().unlock();
    }
  }

  /**
   * Helper function to clean up the state associated with a removed database. It creates
   * the entries in the delete log for 'db' as well as for its tables and functions
   * (if any).
   */
  private void updateDeleteLog(Db db) {
    Preconditions.checkNotNull(db);
    Preconditions.checkState(versionLock_.isWriteLockedByCurrentThread());
    if (!db.isSystemDb()) {
      for (Table tbl: db.getTables()) {
        tbl.setCatalogVersion(incrementAndGetCatalogVersion());
        deleteLog_.addRemovedObject(tbl.toMinimalTCatalogObject());
      }
      for (Function fn: db.getFunctions(null, new PatternMatcher())) {
        fn.setCatalogVersion(incrementAndGetCatalogVersion());
        deleteLog_.addRemovedObject(fn.toTCatalogObject());
      }
    }
    db.setCatalogVersion(incrementAndGetCatalogVersion());
    deleteLog_.addRemovedObject(db.toTCatalogObject());
  }

  /**
   * Adds a table with the given name to the catalog and returns the new table,
   * loading the metadata if needed.
   */
  public Table addTable(String dbName, String tblName) {
    Db db = getDb(dbName);
    if (db == null) return null;
    Table incompleteTable = IncompleteTable.createUninitializedTable(db, tblName);
    versionLock_.writeLock().lock();
    try {
      incompleteTable.setCatalogVersion(incrementAndGetCatalogVersion());
      db.addTable(incompleteTable);
    } finally {
      versionLock_.writeLock().unlock();
    }
    return db.getTable(tblName);
  }

  /**
   * Gets the table with the given name, loading it if needed (if the existing catalog
   * object is not yet loaded). Returns the matching Table or null if no table with this
   * name exists in the catalog.
   * If the existing table is dropped or modified (indicated by the catalog version
   * changing) while the load is in progress, the loaded value will be discarded
   * and the current cached value will be returned. This may mean that a missing table
   * (not yet loaded table) will be returned.
   */
  public Table getOrLoadTable(String dbName, String tblName)
      throws CatalogException {
    TTableName tableName = new TTableName(dbName.toLowerCase(), tblName.toLowerCase());
    TableLoadingMgr.LoadRequest loadReq;

    long previousCatalogVersion;
    // Return the table if it is already loaded or submit a new load request.
    versionLock_.readLock().lock();
    try {
      Table tbl = getTable(dbName, tblName);
      if (tbl == null || tbl.isLoaded()) return tbl;
      previousCatalogVersion = tbl.getCatalogVersion();
      loadReq = tableLoadingMgr_.loadAsync(tableName);
    } finally {
      versionLock_.readLock().unlock();
    }
    Preconditions.checkNotNull(loadReq);
    try {
      // The table may have been dropped/modified while the load was in progress, so only
      // apply the update if the existing table hasn't changed.
      return replaceTableIfUnchanged(loadReq.get(), previousCatalogVersion);
    } finally {
      loadReq.close();
    }
  }

  /**
   * Replaces an existing Table with a new value if it exists and has not changed
   * (has the same catalog version as 'expectedCatalogVersion').
   */
  private Table replaceTableIfUnchanged(Table updatedTbl, long expectedCatalogVersion)
      throws DatabaseNotFoundException {
    versionLock_.writeLock().lock();
    try {
      Db db = getDb(updatedTbl.getDb().getName());
      if (db == null) {
        throw new DatabaseNotFoundException(
            "Database does not exist: " + updatedTbl.getDb().getName());
      }

      Table existingTbl = db.getTable(updatedTbl.getName());
      // The existing table does not exist or has been modified. Instead of
      // adding the loaded value, return the existing table.
      if (existingTbl == null ||
          existingTbl.getCatalogVersion() != expectedCatalogVersion) return existingTbl;

      updatedTbl.setCatalogVersion(incrementAndGetCatalogVersion());
      db.addTable(updatedTbl);
      return updatedTbl;
    } finally {
      versionLock_.writeLock().unlock();
    }
  }

  /**
   * Removes a table from the catalog and increments the catalog version.
   * Returns the removed Table, or null if the table or db does not exist.
   */
  public Table removeTable(String dbName, String tblName) {
    Db parentDb = getDb(dbName);
    if (parentDb == null) return null;
    versionLock_.writeLock().lock();
    try {
      Table removedTable = parentDb.removeTable(tblName);
      if (removedTable != null) {
        removedTable.setCatalogVersion(incrementAndGetCatalogVersion());
        deleteLog_.addRemovedObject(removedTable.toMinimalTCatalogObject());
      }
      return removedTable;
    } finally {
      versionLock_.writeLock().unlock();
    }
  }

  /**
   * Removes a function from the catalog. Increments the catalog version and returns
   * the Function object that was removed. If the function did not exist, null will
   * be returned.
   */
  @Override
  public Function removeFunction(Function desc) {
    versionLock_.writeLock().lock();
    try {
      Function removedFn = super.removeFunction(desc);
      if (removedFn != null) {
        removedFn.setCatalogVersion(incrementAndGetCatalogVersion());
        deleteLog_.addRemovedObject(removedFn.toTCatalogObject());
      }
      return removedFn;
    } finally {
      versionLock_.writeLock().unlock();
    }
  }

  /**
   * Adds a function from the catalog, incrementing the catalog version. Returns true if
   * the add was successful, false otherwise.
   */
  @Override
  public boolean addFunction(Function fn) {
    Db db = getDb(fn.getFunctionName().getDb());
    if (db == null) return false;
    versionLock_.writeLock().lock();
    try {
      if (db.addFunction(fn)) {
        fn.setCatalogVersion(incrementAndGetCatalogVersion());
        return true;
      }
    } finally {
      versionLock_.writeLock().unlock();
    }
    return false;
  }

  /**
   * Adds a data source to the catalog, incrementing the catalog version. Returns true
   * if the add was successful, false otherwise.
   */
  @Override
  public boolean addDataSource(DataSource dataSource) {
    versionLock_.writeLock().lock();
    try {
      if (dataSources_.add(dataSource)) {
        dataSource.setCatalogVersion(incrementAndGetCatalogVersion());
        return true;
      }
    } finally {
      versionLock_.writeLock().unlock();
    }
    return false;
  }

  @Override
  public DataSource removeDataSource(String dataSourceName) {
    versionLock_.writeLock().lock();
    try {
      DataSource dataSource = dataSources_.remove(dataSourceName);
      if (dataSource != null) {
        dataSource.setCatalogVersion(incrementAndGetCatalogVersion());
        deleteLog_.addRemovedObject(dataSource.toTCatalogObject());
      }
      return dataSource;
    } finally {
      versionLock_.writeLock().unlock();
    }
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
   * a pair of tables containing the removed table (or null if the table drop was not
   * successful) and the new table (or null if either the drop of the old one or the
   * add of the new table was not successful). Depending on the return value, the catalog
   * cache is in one of the following states:
   * 1. null, null: Old table was not removed and new table was not added.
   * 2. null, T_new: Invalid configuration
   * 3. T_old, null: Old table was removed but new table was not added.
   * 4. T_old, T_new: Old table was removed and new table was added.
   */
  public Pair<Table, Table> renameTable(TTableName oldTableName, TTableName newTableName)
      throws CatalogException {
    // Remove the old table name from the cache and add the new table.
    Db db = getDb(oldTableName.getDb_name());
    if (db == null) return null;
    versionLock_.writeLock().lock();
    try {
      Table oldTable =
          removeTable(oldTableName.getDb_name(), oldTableName.getTable_name());
      if (oldTable == null) return Pair.create(null, null);
      return Pair.create(oldTable,
          addTable(newTableName.getDb_name(), newTableName.getTable_name()));
    } finally {
      versionLock_.writeLock().unlock();
    }
  }

  /**
   * Reloads metadata for table 'tbl' which must not be an IncompleteTable. Updates the
   * table metadata in-place by calling load() on the given table. Returns the
   * TCatalogObject representing 'tbl'. Applies proper synchronization to protect the
   * metadata load from concurrent table modifications and assigns a new catalog version.
   * Throws a CatalogException if there is an error loading table metadata.
   */
  public TCatalogObject reloadTable(Table tbl) throws CatalogException {
    LOG.info(String.format("Refreshing table metadata: %s", tbl.getFullName()));
    Preconditions.checkState(!(tbl instanceof IncompleteTable));
    String dbName = tbl.getDb().getName();
    String tblName = tbl.getName();
    if (!tryLockTable(tbl)) {
      throw new CatalogException(String.format("Error refreshing metadata for table " +
          "%s due to lock contention", tbl.getFullName()));
    }
    final Timer.Context context =
        tbl.getMetrics().getTimer(Table.REFRESH_DURATION_METRIC).time();
    try {
      long newCatalogVersion = incrementAndGetCatalogVersion();
      versionLock_.writeLock().unlock();
      try (MetaStoreClient msClient = getMetaStoreClient()) {
        org.apache.hadoop.hive.metastore.api.Table msTbl = null;
        try {
          msTbl = msClient.getHiveClient().getTable(dbName, tblName);
        } catch (Exception e) {
          throw new TableLoadingException("Error loading metadata for table: " +
              dbName + "." + tblName, e);
        }
        tbl.load(true, msClient.getHiveClient(), msTbl);
      }
      tbl.setCatalogVersion(newCatalogVersion);
      LOG.info(String.format("Refreshed table metadata: %s", tbl.getFullName()));
      return tbl.toTCatalogObject();
    } finally {
      context.stop();
      Preconditions.checkState(!versionLock_.isWriteLockedByCurrentThread());
      tbl.getLock().unlock();
    }
  }

  /**
   * Drops the partitions specified in 'partitionSet' from 'tbl'. Throws a
   * CatalogException if 'tbl' is not an HdfsTable. Returns the target table.
   */
  public Table dropPartitions(Table tbl, List<List<TPartitionKeyValue>> partitionSet)
      throws CatalogException {
    Preconditions.checkNotNull(tbl);
    Preconditions.checkNotNull(partitionSet);
    Preconditions.checkArgument(tbl.getLock().isHeldByCurrentThread());
    if (!(tbl instanceof HdfsTable)) {
      throw new CatalogException("Table " + tbl.getFullName() + " is not an Hdfs table");
    }
    HdfsTable hdfsTable = (HdfsTable) tbl;
    List<HdfsPartition> partitions =
        hdfsTable.getPartitionsFromPartitionSet(partitionSet);
    hdfsTable.dropPartitions(partitions);
    return hdfsTable;
  }

  /**
   * Adds a partition to its HdfsTable and returns the modified table.
   */
  public Table addPartition(HdfsPartition partition) throws CatalogException {
    Preconditions.checkNotNull(partition);
    HdfsTable hdfsTable = partition.getTable();
    hdfsTable.addPartition(partition);
    return hdfsTable;
  }

  /**
   * Invalidates the table in the catalog cache, potentially adding/removing the table
   * from the cache based on whether it exists in the Hive Metastore.
   * The invalidation logic is:
   * - If the table exists in the Metastore, add it to the catalog as an uninitialized
   *   IncompleteTable (replacing any existing entry). The table metadata will be
   *   loaded lazily, on the next access. If the parent database for this table does not
   *   yet exist in Impala's cache it will also be added.
   * - If the table does not exist in the Metastore, remove it from the catalog cache.
   * - If we are unable to determine whether the table exists in the Metastore (there was
   *   an exception thrown making the RPC), invalidate any existing Table by replacing
   *   it with an uninitialized IncompleteTable.
   * Returns the thrift representation of the added/updated/removed table, or null if
   * the table was not present in the catalog cache or the Metastore.
   * Sets tblWasRemoved to true if the table was absent from the Metastore and it was
   * removed from the catalog cache.
   * Sets dbWasAdded to true if both a new database and table were added to the catalog
   * cache.
   */
  public TCatalogObject invalidateTable(TTableName tableName,
      Reference<Boolean> tblWasRemoved, Reference<Boolean> dbWasAdded) {
    tblWasRemoved.setRef(false);
    dbWasAdded.setRef(false);
    String dbName = tableName.getDb_name();
    String tblName = tableName.getTable_name();
    LOG.info(String.format("Invalidating table metadata: %s.%s", dbName, tblName));

    // Stores whether the table exists in the metastore. Can have three states:
    // 1) true - Table exists in metastore.
    // 2) false - Table does not exist in metastore.
    // 3) unknown (null) - There was exception thrown by the metastore client.
    Boolean tableExistsInMetaStore;
    Db db = null;
    try (MetaStoreClient msClient = getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Database msDb = null;
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
        Table result = removeTable(dbName, tblName);
        if (result == null) return null;
        tblWasRemoved.setRef(true);
        result.getLock().lock();
        try {
          return result.toTCatalogObject();
        } finally {
          result.getLock().unlock();
        }
      }

      db = getDb(dbName);
      if ((db == null || !db.containsTable(tblName)) && tableExistsInMetaStore == null) {
        // The table does not exist in our cache AND it is unknown whether the
        // table exists in the Metastore. Do nothing.
        return null;
      } else if (db == null && tableExistsInMetaStore) {
        // The table exists in the Metastore, but our cache does not contain the parent
        // database. A new db will be added to the cache along with the new table. msDb
        // must be valid since tableExistsInMetaStore is true.
        try {
          msDb = msClient.getHiveClient().getDatabase(dbName);
          Preconditions.checkNotNull(msDb);
          addDb(dbName, msDb);
          dbWasAdded.setRef(true);
        } catch (TException e) {
          // The Metastore database cannot be get. Log the error and return.
          LOG.error("Error executing getDatabase() metastore call: " + dbName, e);
          return null;
        }
      }
    }

    // Add a new uninitialized table to the table cache, effectively invalidating
    // any existing entry. The metadata for the table will be loaded lazily, on the
    // on the next access to the table.
    Table newTable = addTable(dbName, tblName);
    Preconditions.checkNotNull(newTable);
    if (loadInBackground_) {
      tableLoadingMgr_.backgroundLoad(new TTableName(dbName.toLowerCase(),
          tblName.toLowerCase()));
    }
    if (dbWasAdded.getRef()) {
      // The database should always have a lower catalog version than the table because
      // it needs to be created before the table can be added.
      Db addedDb = newTable.getDb();
      Preconditions.checkNotNull(addedDb);
      Preconditions.checkState(
          addedDb.getCatalogVersion() < newTable.getCatalogVersion());
    }
    return newTable.toTCatalogObject();
  }

  /**
   * Adds a new role with the given name and grant groups to the AuthorizationPolicy.
   * If a role with the same name already exists it will be overwritten.
   */
  public Role addRole(String roleName, Set<String> grantGroups) {
    versionLock_.writeLock().lock();
    try {
      Role role = new Role(roleName, grantGroups);
      role.setCatalogVersion(incrementAndGetCatalogVersion());
      authPolicy_.addRole(role);
      return role;
    } finally {
      versionLock_.writeLock().unlock();
    }
  }

  /**
   * Removes the role with the given name from the AuthorizationPolicy. Returns the
   * removed role with an incremented catalog version, or null if no role with this name
   * exists.
   */
  public Role removeRole(String roleName) {
    versionLock_.writeLock().lock();
    try {
      Role role = authPolicy_.removeRole(roleName);
      if (role == null) return null;
      for (RolePrivilege priv: role.getPrivileges()) {
        priv.setCatalogVersion(incrementAndGetCatalogVersion());
        deleteLog_.addRemovedObject(priv.toTCatalogObject());
      }
      role.setCatalogVersion(incrementAndGetCatalogVersion());
      deleteLog_.addRemovedObject(role.toTCatalogObject());
      return role;
    } finally {
      versionLock_.writeLock().unlock();
    }
  }

  /**
   * Adds a grant group to the given role name and returns the modified Role with
   * an updated catalog version. If the role does not exist a CatalogException is thrown.
   */
  public Role addRoleGrantGroup(String roleName, String groupName)
      throws CatalogException {
    versionLock_.writeLock().lock();
    try {
      Role role = authPolicy_.addGrantGroup(roleName, groupName);
      Preconditions.checkNotNull(role);
      role.setCatalogVersion(incrementAndGetCatalogVersion());
      return role;
    } finally {
      versionLock_.writeLock().unlock();
    }
  }

  /**
   * Removes a grant group from the given role name and returns the modified Role with
   * an updated catalog version. If the role does not exist a CatalogException is thrown.
   */
  public Role removeRoleGrantGroup(String roleName, String groupName)
      throws CatalogException {
    versionLock_.writeLock().lock();
    try {
      Role role = authPolicy_.removeGrantGroup(roleName, groupName);
      Preconditions.checkNotNull(role);
      role.setCatalogVersion(incrementAndGetCatalogVersion());
      return role;
    } finally {
      versionLock_.writeLock().unlock();
    }
  }

  /**
   * Adds a privilege to the given role name. Returns the new RolePrivilege and
   * increments the catalog version. If the parent role does not exist a CatalogException
   * is thrown.
   */
  public RolePrivilege addRolePrivilege(String roleName, TPrivilege thriftPriv)
      throws CatalogException {
    versionLock_.writeLock().lock();
    try {
      Role role = authPolicy_.getRole(roleName);
      if (role == null) throw new CatalogException("Role does not exist: " + roleName);
      RolePrivilege priv = RolePrivilege.fromThrift(thriftPriv);
      priv.setCatalogVersion(incrementAndGetCatalogVersion());
      authPolicy_.addPrivilege(priv);
      return priv;
    } finally {
      versionLock_.writeLock().unlock();
    }
  }

  /**
   * Removes a RolePrivilege from the given role name. Returns the removed
   * RolePrivilege with an incremented catalog version or null if no matching privilege
   * was found. Throws a CatalogException if no role exists with this name.
   */
  public RolePrivilege removeRolePrivilege(String roleName, TPrivilege thriftPriv)
      throws CatalogException {
    versionLock_.writeLock().lock();
    try {
      Role role = authPolicy_.getRole(roleName);
      if (role == null) throw new CatalogException("Role does not exist: " + roleName);
      RolePrivilege rolePrivilege =
          role.removePrivilege(thriftPriv.getPrivilege_name());
      if (rolePrivilege == null) return null;
      rolePrivilege.setCatalogVersion(incrementAndGetCatalogVersion());
      deleteLog_.addRemovedObject(rolePrivilege.toTCatalogObject());
      return rolePrivilege;
    } finally {
      versionLock_.writeLock().unlock();
    }
  }

  /**
   * Gets a RolePrivilege from the given role name. Returns the privilege if it exists,
   * or null if no privilege matching the privilege spec exist.
   * Throws a CatalogException if the role does not exist.
   */
  public RolePrivilege getRolePrivilege(String roleName, TPrivilege privSpec)
      throws CatalogException {
    versionLock_.readLock().lock();
    try {
      Role role = authPolicy_.getRole(roleName);
      if (role == null) throw new CatalogException("Role does not exist: " + roleName);
      return role.getPrivilege(privSpec.getPrivilege_name());
    } finally {
      versionLock_.readLock().unlock();
    }
  }

  /**
   * Increments the current Catalog version and returns the new value.
   */
  public long incrementAndGetCatalogVersion() {
    versionLock_.writeLock().lock();
    try {
      return ++catalogVersion_;
    } finally {
      versionLock_.writeLock().unlock();
    }
  }

  /**
   * Returns the current Catalog version.
   */
  public long getCatalogVersion() {
    versionLock_.readLock().lock();
    try {
      return catalogVersion_;
    } finally {
      versionLock_.readLock().unlock();
    }
  }

  public ReentrantReadWriteLock getLock() { return versionLock_; }
  public SentryProxy getSentryProxy() { return sentryProxy_; }
  public AuthorizationPolicy getAuthPolicy() { return authPolicy_; }

  /**
   * Reloads metadata for the partition defined by the partition spec
   * 'partitionSpec' in table 'tbl'. Returns the resulting table's TCatalogObject after
   * the partition metadata was reloaded.
   */
  public TCatalogObject reloadPartition(Table tbl, List<TPartitionKeyValue> partitionSpec)
      throws CatalogException {
    if (!tryLockTable(tbl)) {
      throw new CatalogException(String.format("Error reloading partition of table %s " +
          "due to lock contention", tbl.getFullName()));
    }
    try {
      long newCatalogVersion = incrementAndGetCatalogVersion();
      versionLock_.writeLock().unlock();
      HdfsTable hdfsTable = (HdfsTable) tbl;
      HdfsPartition hdfsPartition = hdfsTable
          .getPartitionFromThriftPartitionSpec(partitionSpec);
      // Retrieve partition name from existing partition or construct it from
      // the partition spec
      String partitionName = hdfsPartition == null
          ? HdfsTable.constructPartitionName(partitionSpec)
          : hdfsPartition.getPartitionName();
      LOG.info(String.format("Refreshing partition metadata: %s %s",
          hdfsTable.getFullName(), partitionName));
      try (MetaStoreClient msClient = getMetaStoreClient()) {
        org.apache.hadoop.hive.metastore.api.Partition hmsPartition = null;
        try {
          hmsPartition = msClient.getHiveClient().getPartition(
              hdfsTable.getDb().getName(), hdfsTable.getName(), partitionName);
        } catch (NoSuchObjectException e) {
          // If partition does not exist in Hive Metastore, remove it from the
          // catalog
          if (hdfsPartition != null) {
            hdfsTable.dropPartition(partitionSpec);
            hdfsTable.setCatalogVersion(newCatalogVersion);
          }
          return hdfsTable.toTCatalogObject();
        } catch (Exception e) {
          throw new CatalogException("Error loading metadata for partition: "
              + hdfsTable.getFullName() + " " + partitionName, e);
        }
        hdfsTable.reloadPartition(hdfsPartition, hmsPartition);
      }
      hdfsTable.setCatalogVersion(newCatalogVersion);
      LOG.info(String.format("Refreshed partition metadata: %s %s",
          hdfsTable.getFullName(), partitionName));
      return hdfsTable.toTCatalogObject();
    } finally {
      Preconditions.checkState(!versionLock_.isWriteLockedByCurrentThread());
      tbl.getLock().unlock();
    }
  }

  public CatalogDeltaLog getDeleteLog() { return deleteLog_; }

  /**
   * Returns the version of the topic update that an operation using SYNC_DDL must wait
   * for in order to ensure that its result set ('result') has been broadcast to all the
   * coordinators. For operations that don't produce a result set, e.g. INVALIDATE
   * METADATA, return the version specified in 'result.version'.
   */
  public long waitForSyncDdlVersion(TCatalogUpdateResult result) throws CatalogException {
    if (!result.isSetUpdated_catalog_objects() &&
        !result.isSetRemoved_catalog_objects()) {
      return result.getVersion();
    }
    long lastSentTopicUpdate = lastSentTopicUpdate_.get();
    // Maximum number of attempts (topic updates) to find the catalog topic version that
    // an operation using SYNC_DDL must wait for.
    long maxNumAttempts = 5;
    if (result.isSetUpdated_catalog_objects()) {
      maxNumAttempts =
          result.getUpdated_catalog_objects().size() * (MAX_NUM_SKIPPED_TOPIC_UPDATES + 1);
    }
    long numAttempts = 0;
    long begin = System.currentTimeMillis();
    long versionToWaitFor = -1;
    while (versionToWaitFor == -1) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("waitForSyncDdlVersion() attempt: " + numAttempts);
      }
      // Examine the topic update log to determine the latest topic update that
      // covers the added/modified/deleted objects in 'result'.
      long topicVersionForUpdates =
          getCoveringTopicUpdateVersion(result.getUpdated_catalog_objects());
      long topicVersionForDeletes =
          getCoveringTopicUpdateVersion(result.getRemoved_catalog_objects());
      if (topicVersionForUpdates == -1 || topicVersionForDeletes == -1) {
        // Wait for the next topic update.
        synchronized(topicUpdateLog_) {
          try {
            topicUpdateLog_.wait(TOPIC_UPDATE_WAIT_TIMEOUT_MS);
          } catch (InterruptedException e) {
            // Ignore
          }
        }
        long currentTopicUpdate = lastSentTopicUpdate_.get();
        // Don't count time-based exits from the wait() toward the maxNumAttempts
        // threshold.
        if (lastSentTopicUpdate != currentTopicUpdate) {
          ++numAttempts;
          if (numAttempts > maxNumAttempts) {
            throw new CatalogException("Couldn't retrieve the catalog topic version " +
                "for the SYNC_DDL operation after " + maxNumAttempts + " attempts." +
                "The operation has been successfully executed but its effects may have " +
                "not been broadcast to all the coordinators.");
          }
          lastSentTopicUpdate = currentTopicUpdate;
        }
      } else {
        versionToWaitFor = Math.max(topicVersionForDeletes, topicVersionForUpdates);
      }
    }
    Preconditions.checkState(versionToWaitFor >= 0);
    LOG.info("Operation using SYNC_DDL is waiting for catalog topic version: " +
        versionToWaitFor + ". Time to identify topic version (msec): " +
        (System.currentTimeMillis() - begin));
    return versionToWaitFor;
  }

  /**
   * Returns the version of the topic update that covers a set of TCatalogObjects.
   * A topic update U covers a TCatalogObject T, corresponding to a catalog object O,
   * if last_sent_version(O) >= catalog_version(T) && catalog_version(U) >=
   * last_topic_update(O). The first condition indicates that a version of O that is
   * larger or equal to the version in T has been added to a topic update. The second
   * condition indicates that U is either the update to include O or an update following
   * the one to include O. Returns -1 if there is a catalog object in 'tCatalogObjects'
   * which doesn't satisfy the above conditions.
   */
  private long getCoveringTopicUpdateVersion(List<TCatalogObject> tCatalogObjects) {
    if (tCatalogObjects == null || tCatalogObjects.isEmpty()) {
      return lastSentTopicUpdate_.get();
    }
    long versionToWaitFor = -1;
    for (TCatalogObject tCatalogObject: tCatalogObjects) {
      TopicUpdateLog.Entry topicUpdateEntry =
          topicUpdateLog_.get(Catalog.toCatalogObjectKey(tCatalogObject));
      // There are two reasons for which a topic update log entry cannot be found:
      // a) It corresponds to a new catalog object that hasn't been processed by a catalog
      // update yet.
      // b) It corresponds to a catalog object that hasn't been modified for at least
      // TOPIC_UPDATE_LOG_GC_FREQUENCY updates and hence its entry was garbage
      // collected.
      // In both cases, -1 is returned to indicate that we're waiting for the
      // entry to show up in the topic update log.
      if (topicUpdateEntry == null ||
          topicUpdateEntry.getLastSentVersion() < tCatalogObject.getCatalog_version()) {
        return -1;
      }
      versionToWaitFor =
          Math.max(versionToWaitFor, topicUpdateEntry.getLastSentCatalogUpdate());
    }
    return versionToWaitFor;
  }

  /**
   * Retrieves information about the current catalog usage including the most frequently
   * accessed tables as well as the tables with the highest memory requirements.
   */
  public TGetCatalogUsageResponse getCatalogUsage() {
    TGetCatalogUsageResponse usage = new TGetCatalogUsageResponse();
    usage.setLarge_tables(Lists.<TTableUsageMetrics>newArrayList());
    usage.setFrequently_accessed_tables(Lists.<TTableUsageMetrics>newArrayList());
    for (Table largeTable: CatalogUsageMonitor.INSTANCE.getLargestTables()) {
      TTableUsageMetrics tableUsageMetrics =
          new TTableUsageMetrics(largeTable.getTableName().toThrift());
      tableUsageMetrics.setMemory_estimate_bytes(largeTable.getEstimatedMetadataSize());
      usage.addToLarge_tables(tableUsageMetrics);
    }
    for (Table frequentTable:
        CatalogUsageMonitor.INSTANCE.getFrequentlyAccessedTables()) {
      TTableUsageMetrics tableUsageMetrics =
          new TTableUsageMetrics(frequentTable.getTableName().toThrift());
      tableUsageMetrics.setNum_metadata_operations(frequentTable.getMetadataOpsCount());
      usage.addToFrequently_accessed_tables(tableUsageMetrics);
    }
    return usage;
  }

  /**
   * Retrieves the stored metrics of the specified table and returns a pretty-printed
   * string representation. Throws an exception if table metrics were not available
   * because the table was not loaded or because another concurrent operation was holding
   * the table lock.
   */
  public String getTableMetrics(TTableName tTableName) throws CatalogException {
    String dbName = tTableName.db_name;
    String tblName = tTableName.table_name;
    Table tbl = getTable(dbName, tblName);
    if (tbl == null) {
      throw new CatalogException("Table " + dbName + "." + tblName + " was not found.");
    }
    String result;
    if (tbl instanceof IncompleteTable) {
      result = "No metrics available for table " + dbName + "." + tblName +
          ". Table not yet loaded.";
      return result;
    }
    if (!tbl.getLock().tryLock()) {
      result = "Metrics for table " + dbName + "." + tblName + "are not available " +
          "because the table is currently modified by another operation.";
      return result;
    }
    try {
      return tbl.getMetrics().toString();
    } finally {
      tbl.getLock().unlock();
    }
  }
}
