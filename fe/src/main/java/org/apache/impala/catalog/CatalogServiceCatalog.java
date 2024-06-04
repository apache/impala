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

import static org.apache.impala.catalog.monitor.TableLoadingTimeHistogram.Quantile.P100;
import static org.apache.impala.catalog.monitor.TableLoadingTimeHistogram.Quantile.P50;
import static org.apache.impala.catalog.monitor.TableLoadingTimeHistogram.Quantile.P75;
import static org.apache.impala.catalog.monitor.TableLoadingTimeHistogram.Quantile.P95;
import static org.apache.impala.catalog.monitor.TableLoadingTimeHistogram.Quantile.P99;
import static org.apache.impala.service.CatalogOpExecutor.FETCHED_HMS_DB;
import static org.apache.impala.service.CatalogOpExecutor.FETCHED_HMS_PARTITION;
import static org.apache.impala.service.CatalogOpExecutor.FETCHED_HMS_TABLE;
import static org.apache.impala.service.CatalogOpExecutor.FETCHED_LATEST_HMS_EVENT_ID;
import static org.apache.impala.service.CatalogOpExecutor.GOT_TABLE_READ_LOCK;
import static org.apache.impala.service.CatalogOpExecutor.GOT_TABLE_WRITE_LOCK;
import static org.apache.impala.thrift.TCatalogObjectType.HDFS_PARTITION;
import static org.apache.impala.thrift.TCatalogObjectType.TABLE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.impala.analysis.TableName;
import org.apache.impala.authorization.AuthorizationDelta;
import org.apache.impala.authorization.AuthorizationManager;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.catalog.FeFsTable.Utils;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.events.ExternalEventsProcessor;
import org.apache.impala.catalog.events.MetastoreEvents.EventFactoryForSyncToLatestEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventFactory;
import org.apache.impala.catalog.events.MetastoreEventsProcessor;
import org.apache.impala.catalog.events.MetastoreEventsProcessor.EventProcessorStatus;
import org.apache.impala.catalog.events.MetastoreNotificationFetchException;
import org.apache.impala.catalog.events.SelfEventContext;
import org.apache.impala.catalog.metastore.CatalogHmsUtils;
import org.apache.impala.catalog.monitor.CatalogMonitor;
import org.apache.impala.catalog.monitor.CatalogTableMetrics;
import org.apache.impala.catalog.metastore.HmsApiNameEnum;
import org.apache.impala.catalog.metastore.ICatalogMetastoreServer;
import org.apache.impala.catalog.monitor.TableLoadingTimeHistogram;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.common.Reference;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.hive.common.MutableValidWriteIdList;
import org.apache.impala.hive.executor.HiveJavaFunction;
import org.apache.impala.hive.executor.HiveJavaFunctionFactoryImpl;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.FeSupport;
import org.apache.impala.service.JniCatalog;
import org.apache.impala.thrift.CatalogLookupStatus;
import org.apache.impala.thrift.CatalogServiceConstants;
import org.apache.impala.thrift.TCatalog;
import org.apache.impala.thrift.TCatalogdHmsCacheMetrics;
import org.apache.impala.thrift.TCatalogInfoSelector;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TCatalogUpdateResult;
import org.apache.impala.thrift.TDataSource;
import org.apache.impala.thrift.TDatabase;
import org.apache.impala.thrift.TEventProcessorMetrics;
import org.apache.impala.thrift.TEventProcessorMetricsSummaryResponse;
import org.apache.impala.thrift.TFunction;
import org.apache.impala.thrift.TGetCatalogUsageResponse;
import org.apache.impala.thrift.TGetOperationUsageResponse;
import org.apache.impala.thrift.TGetPartialCatalogObjectRequest;
import org.apache.impala.thrift.TGetPartialCatalogObjectResponse;
import org.apache.impala.thrift.TGetPartitionStatsRequest;
import org.apache.impala.thrift.THdfsFileDesc;
import org.apache.impala.thrift.THdfsPartition;
import org.apache.impala.thrift.THdfsTable;
import org.apache.impala.thrift.TImpalaTableType;
import org.apache.impala.thrift.TPartialCatalogInfo;
import org.apache.impala.thrift.TPartialPartitionInfo;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TPartitionStats;
import org.apache.impala.thrift.TPrincipalType;
import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TResetMetadataRequest;
import org.apache.impala.thrift.TSystemTableName;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.thrift.TTableUsage;
import org.apache.impala.thrift.TTableUsageMetrics;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.thrift.TUpdateTableUsageRequest;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.CatalogBlacklistUtils;
import org.apache.impala.util.DebugUtils;
import org.apache.impala.util.EventSequence;
import org.apache.impala.util.FunctionUtils;
import org.apache.impala.util.NoOpEventSequence;
import org.apache.impala.util.PatternMatcher;
import org.apache.impala.util.TUniqueIdUtil;
import org.apache.impala.util.ThreadNameAnnotator;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

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
 *   garbage-collected every topicUpdateLogGcFrequency_ topic updates by the topic
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
 *   topicUpdateLogGcFrequency_ topic updates) between the time the operation was
 *   applied in the catalog cache and the time the SYNC_DDL version was checked. To reduce
 *   the probability of such an event, we set the value of the
 *   topicUpdateLogGcFrequency_ to a large value. Also, to prevent metadata operations
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
  public static final Logger LOG = LoggerFactory.getLogger(CatalogServiceCatalog.class);

  public static final String GOT_CATALOG_VERSION_READ_LOCK =
      "Got catalog version read lock";
  public static final String GOT_CATALOG_VERSION_WRITE_LOCK =
      "Got catalog version write lock";

  public static final int INITIAL_META_STORE_CLIENT_POOL_SIZE = 10;
  private static final int MAX_NUM_SKIPPED_TOPIC_UPDATES = 2;
  private final int maxSkippedUpdatesLockContention_;
  //value of timeout for the topic update thread while waiting on the table lock.
  private final long topicUpdateTblLockMaxWaitTimeMs_;
  //Default value of timeout for acquiring a table lock.
  public static final long LOCK_RETRY_TIMEOUT_MS = 7200000;
  // Time to sleep before retrying to acquire a table lock
  private static final int LOCK_RETRY_DELAY_MS = 10;
  // Threshold to add warning logs for slow lock acquiring.
  private static final long LOCK_ACQUIRING_DURATION_WARN_MS = 100;
  // default value of table id in the GetPartialCatalogObjectRequest
  public static final long TABLE_ID_UNAVAILABLE = -1;

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

  // The catalog version when we ran reset() last time. Protected by versionLock_.
  private long lastResetStartVersion_ = INITIAL_CATALOG_VERSION;

  // Manages the scheduling of background table loading.
  private final TableLoadingMgr tableLoadingMgr_;

  private final boolean loadInBackground_;

  // Periodically polls HDFS to get the latest set of known cache pools.
  private final ScheduledExecutorService cachePoolReader_ =
    Executors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder().setNameFormat("HDFSCachePoolReader").build());

  // Log of deleted catalog objects.
  private final CatalogDeltaLog deleteLog_;

  // Version of the last topic update returned to the statestore.
  // The version of a topic update is the catalog version of the CATALOG object
  // that is added to it.
  private final AtomicLong lastSentTopicUpdate_ = new AtomicLong(-1);

  // Wait time for a topic update.
  private static final long TOPIC_UPDATE_WAIT_TIMEOUT_MS = 10000;

  private final TopicUpdateLog topicUpdateLog_ = new TopicUpdateLog();

  private final String localLibraryPath_;

  private CatalogdTableInvalidator catalogdTableInvalidator_;

  // Manages the event processing from metastore for issuing invalidates on tables
  private ExternalEventsProcessor metastoreEventProcessor_;

  private ICatalogMetastoreServer catalogMetastoreServer_;

  private MetastoreEventFactory syncToLatestEventFactory_;
  /**
   * See the gflag definition in be/.../catalog-server.cc for details on these modes.
   */
  private static enum TopicMode {
    FULL,
    MIXED,
    MINIMAL
  };
  final TopicMode topicMode_;

  private final long PARTIAL_FETCH_RPC_QUEUE_TIMEOUT_S = BackendConfig.INSTANCE
      .getCatalogPartialFetchRpcQueueTimeoutS();

  private final int MAX_PARALLEL_PARTIAL_FETCH_RPC_COUNT = BackendConfig.INSTANCE
      .getCatalogMaxParallelPartialFetchRpc();

  // Controls concurrent access to doGetPartialCatalogObject() call. Limits the number
  // of parallel requests to --catalog_max_parallel_partial_fetch_rpc.
  private final Semaphore partialObjectFetchAccess_ =
      new Semaphore(MAX_PARALLEL_PARTIAL_FETCH_RPC_COUNT, /*fair =*/ true);

  private AuthorizationManager authzManager_;

  // Databases that will be skipped in loading.
  private final Set<String> blacklistedDbs_;
  // Tables that will be skipped in loading.
  private final Set<TableName> blacklistedTables_;

  // Table properties that require file metadata reload
  private final Set<String> whitelistedTblProperties_;

  // A variable to test expected failed events
  private final Set<String> failureEventsForTesting_;

  // List of event types to skip by default while fetching notification events from
  // metastore.
  private final List<String> defaultSkippedHmsEventTypes_;

  // List of common known HMS event types. Ignores those that are rare in regular (e.g.
  // daily) jobs. The list is used to generate a complement set for wanted HMS event
  // types. We need this list until HIVE-28146 is resolved. After that we can directly
  // specify what event types we want.
  private final List<String> commonHmsEventTypes_;

  // Total number of dbs, tables and functions in the catalog cache.
  // Updated in each catalog topic update (getCatalogDelta()).
  private int numDbs_ = 0;
  private int numTables_ = 0;
  private int numFunctions_ = 0;

  private final List<String> impalaSysTables;

  /**
   * Initialize the CatalogServiceCatalog using a given MetastoreClientPool impl.
   *
   * @param loadInBackground    If true, table metadata will be loaded in the background.
   * @param numLoadingThreads   Number of threads used to load table metadata.
   * @param metaStoreClientPool A pool of HMS clients backing this Catalog.
   * @throws ImpalaException
   */
  public CatalogServiceCatalog(boolean loadInBackground, int numLoadingThreads,
      String localLibraryPath, MetaStoreClientPool metaStoreClientPool)
      throws ImpalaException {
    super(metaStoreClientPool);
    blacklistedDbs_ = CatalogBlacklistUtils.parseBlacklistedDbs(
        BackendConfig.INSTANCE.getBlacklistedDbs(), LOG);
    blacklistedTables_ = CatalogBlacklistUtils.parseBlacklistedTables(
        BackendConfig.INSTANCE.getBlacklistedTables(), LOG);
    maxSkippedUpdatesLockContention_ = BackendConfig.INSTANCE
        .getBackendCfg().catalog_max_lock_skipped_topic_updates;
    Preconditions.checkState(maxSkippedUpdatesLockContention_ > 0,
        "catalog_max_lock_skipped_topic_updates must be positive");
    topicUpdateTblLockMaxWaitTimeMs_ = BackendConfig.INSTANCE
        .getBackendCfg().topic_update_tbl_max_wait_time_ms;
    Preconditions.checkState(topicUpdateTblLockMaxWaitTimeMs_ >= 0,
        "topic_update_tbl_max_wait_time_ms must be positive");
    impalaSysTables = Arrays.asList(
        BackendConfig.INSTANCE.queryLogTableName(),
        TSystemTableName.IMPALA_QUERY_LIVE.toString().toLowerCase());
    tableLoadingMgr_ = new TableLoadingMgr(this, numLoadingThreads);
    loadInBackground_ = loadInBackground;
    try {
      // We want only 'true' HDFS filesystems to poll the HDFS cache (i.e not S3,
      // local, etc.)
      if (FileSystemUtil.isDistributedFileSystem(FileSystemUtil.getDefaultFileSystem())) {
        cachePoolReader_.scheduleAtFixedRate(
            new CachePoolReader(false), 0, 1, TimeUnit.MINUTES);
      }
    } catch (IOException e) {
      LOG.error("Couldn't identify the default FS. Cache Pool reader will be disabled.");
    }
    localLibraryPath_ = localLibraryPath;
    deleteLog_ = new CatalogDeltaLog();
    topicMode_ = TopicMode.valueOf(
        BackendConfig.INSTANCE.getBackendCfg().catalog_topic_mode.toUpperCase());
    catalogdTableInvalidator_ = CatalogdTableInvalidator.create(this,
        BackendConfig.INSTANCE);
    Preconditions.checkState(PARTIAL_FETCH_RPC_QUEUE_TIMEOUT_S > 0);
    String whitelist = BackendConfig.INSTANCE.getFileMetadataReloadProperties();
    whitelistedTblProperties_ = Sets.newHashSet();
    for (String tblProps: Splitter.on(',').trimResults().omitEmptyStrings().split(
        whitelist)) {
      whitelistedTblProperties_.add(tblProps);
    }
    failureEventsForTesting_ = Sets.newHashSet();
    String failureEvents =
        BackendConfig.INSTANCE.getProcessEventFailureEventTypes().toUpperCase();
    for (String tblProps :
        Splitter.on(',').trimResults().omitEmptyStrings().split(failureEvents)) {
      failureEventsForTesting_.add(tblProps);
    }
    defaultSkippedHmsEventTypes_ = Lists.newArrayList();
    Iterable<String> eventTypes = Splitter.on(',')
        .trimResults().omitEmptyStrings()
        .split(BackendConfig.INSTANCE.getDefaultSkippedHmsEventTypes());
    for (String eventType : eventTypes) {
      defaultSkippedHmsEventTypes_.add(eventType);
    }
    LOG.info("Default skipped HMS event types: " + defaultSkippedHmsEventTypes_);
    commonHmsEventTypes_ = Lists.newArrayList();
    eventTypes = Splitter.on(',').trimResults().omitEmptyStrings()
        .split(BackendConfig.INSTANCE.getCommonHmsEventTypes());
    for (String eventType : eventTypes) {
      commonHmsEventTypes_.add(eventType);
    }
    LOG.info("Common HMS event types: " + commonHmsEventTypes_);
  }

  public void startEventsProcessor() {
    Preconditions.checkNotNull(metastoreEventProcessor_,
        "Start events processor called before initializing it");
    metastoreEventProcessor_.start();
  }


  /**
   * Initializes the Catalog using the default MetastoreClientPool impl.
   * @param initialHmsCnxnTimeoutSec Time (in seconds) CatalogServiceCatalog will wait
   * to establish an initial connection to the HMS before giving up.
   */
  public CatalogServiceCatalog(boolean loadInBackground, int numLoadingThreads,
      int initialHmsCnxnTimeoutSec, String localLibraryPath) throws ImpalaException {
    this(loadInBackground, numLoadingThreads, localLibraryPath,
        new MetaStoreClientPool(INITIAL_META_STORE_CLIENT_POOL_SIZE,
            initialHmsCnxnTimeoutSec));
  }

  /**
   * Check whether the database is in blacklist
   */
  public boolean isBlacklistedDb(String dbName) {
    Preconditions.checkNotNull(dbName);
    if (BackendConfig.INSTANCE.enableWorkloadMgmt() && dbName.equalsIgnoreCase(Db.SYS)) {
      // Override 'sys' for Impala system tables.
      return false;
    }
    return blacklistedDbs_.contains(dbName.toLowerCase());
  }

  /**
   * Check whether the table is in blacklist
   */
  public boolean isBlacklistedTable(TableName table) {
    Preconditions.checkNotNull(table);
    if (table.getDb().equalsIgnoreCase(Db.SYS) && blacklistedDbs_.contains(Db.SYS)) {
      // If we've overridden the database blacklist, only allow Impala system tables.
      return !impalaSysTables.contains(table.getTbl());
    }
    return blacklistedTables_.contains(table);
  }

  /**
   * Check whether the table is in blacklist
   */
  public boolean isBlacklistedTable(String db, String table) {
    return isBlacklistedTable(new TableName(db, table));
  }

  public void setAuthzManager(AuthorizationManager authzManager) {
    authzManager_ = Preconditions.checkNotNull(authzManager);
  }

  public ExternalEventsProcessor getMetastoreEventProcessor() {
    return metastoreEventProcessor_;
  }

  public boolean isEventProcessingActive() {
    return metastoreEventProcessor_ instanceof MetastoreEventsProcessor
        && EventProcessorStatus.ACTIVE
        .equals(((MetastoreEventsProcessor) metastoreEventProcessor_).getStatus());
  }

  /**
   * Acquires the table read lock and update the 'catalogTimeline'.
   */
  public void readLock(Table tbl, EventSequence catalogTimeline) {
    tbl.readLock().lock();
    catalogTimeline.markEvent(GOT_TABLE_READ_LOCK);
  }

  /**
   * Tries to acquire versionLock_ and the lock of 'tbl' in that order. Returns true if it
   * successfully acquires both within LOCK_RETRY_TIMEOUT_MS millisecs; both locks are
   * held when the function returns. Returns false otherwise and no lock is held in
   * this case.
   */
  public boolean tryWriteLock(Table tbl) {
    return tryLock(tbl, true, LOCK_RETRY_TIMEOUT_MS);
  }

  /**
   * Same as the above but also updates 'catalogTimeline' when the operation finishes.
   */
  public boolean tryWriteLock(Table tbl, EventSequence catalogTimeline) {
    boolean success = tryWriteLock(tbl);
    if (success) {
      catalogTimeline.markEvent(GOT_TABLE_WRITE_LOCK);
    } else {
      catalogTimeline.markEvent("Failed to acquire table write lock");
    }
    return success;
  }

  /**
   * Acquire write lock on multiple tables. If the lock couldn't be acquired on any
   * table, then release the table lock as well as version lock held due previous tables
   * and return false.
   * If write locks were acquired on all tables then release version lock
   * on all tables except last one before returning true. In case of success, it is
   * caller's responsibility to release versionLock's writeLock
   * @param tables: Catalog tables on which write lock has to be acquired
   * @return true if lock was acquired on all tables successfully. False
   *         otherwise
   */
  public boolean tryWriteLock(Table[] tables) {
    StringBuilder tableInfo = new StringBuilder();
    int numTables = tables.length;
    if (LOG.isDebugEnabled()) {
      for(int i = 0; i < numTables; i++) {
        tableInfo.append(tables[i].getFullName());
        if(i < numTables - 1) {
          tableInfo.append(", ");
        }
      }
      LOG.debug("Trying to acquire write locks for tables: " +
          tableInfo);
    }
    int tableIndex=-1, versionLockCount = 0;
    try {
      for(tableIndex = 0; tableIndex < numTables; tableIndex++) {
        Table tbl = tables[tableIndex];
        if (!tryWriteLock(tbl)) {
          LOG.debug("Could not acquire write lock on table: " + tbl.getFullName());
          return false;
        }
        versionLockCount += 1;
      }
      // in case of success, release version write lock for all tables except last
      if (tableIndex == numTables) {
        versionLockCount = versionLockCount - 1;
      }
      return true;
    } finally {
      if (tableIndex != numTables) {
        // couldn't acquire lock on all tables
        StringBuilder tablesInfo = new StringBuilder();
        for(int i = 0; i < tableIndex; i++) {
          tables[i].releaseWriteLock();
          tablesInfo.append(tables[i].getFullName() + ((i < tableIndex-1) ? ", " : ""));
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Released table write lock on tables: {}", tablesInfo);
        }
      }
      LOG.debug("Unlocking versionLock_ write lock {} number of times", versionLockCount);
      while (versionLockCount > 0) {
        versionLock_.writeLock().unlock();
        versionLockCount = versionLockCount - 1;
      }
    }
  }

  /**
   * Tries to acquire the table similar to described in
   * {@link CatalogServiceCatalog#tryWriteLock(Table)} but with a custom timeout.
   */
  public boolean tryLock(Table tbl, final boolean useWriteLock, final long timeout) {
    Preconditions.checkArgument(timeout >= 0);
    try (ThreadNameAnnotator tna = new ThreadNameAnnotator(
        String.format("Attempting to %s lock table %s with a timeout of %s ms",
            (useWriteLock ? "write" : "read"), tbl.getFullName(), timeout))) {
      long begin = System.currentTimeMillis();
      long end;
      do {
        versionLock_.writeLock().lock();
        Lock lock = useWriteLock ? tbl.writeLock() : tbl.readLock();
        try {
          //Note that we don't use the timeout directly here since the timeout
          //since we don't want to unnecessarily hold the versionLock if the table
          //cannot be acquired. Holding version lock can potentially blocks other
          //unrelated DDL operations.
          if (lock.tryLock(0, TimeUnit.SECONDS)) {
            long duration = System.currentTimeMillis() - begin;
            if (duration > LOCK_ACQUIRING_DURATION_WARN_MS) {
              LOG.warn("{} lock for table {} was acquired in {} msec",
                  useWriteLock ? "Write" : "Read", tbl.getFullName(), duration);
            }
            return true;
          }
          versionLock_.writeLock().unlock();
          // Sleep to avoid spinning and allow other operations to make progress.
          Thread.sleep(LOCK_RETRY_DELAY_MS);
        } catch (InterruptedException e) {
          // ignore
        }
        end = System.currentTimeMillis();
      } while (end - begin < timeout);
      return false;
    }
  }

  /**
   * Similar to tryLock on a table, but works on a database object instead of Table.
   * TODO: Refactor the code so that both table and db can be "lockable" using a single
   * method.
   */
  public boolean tryLockDb(Db db) {
    try (ThreadNameAnnotator tna = new ThreadNameAnnotator(
        "Attempting to lock database " + db.getName())) {
      long begin = System.currentTimeMillis();
      long end;
      do {
        versionLock_.writeLock().lock();
        if (db.getLock().tryLock()) {
          long duration = System.currentTimeMillis() - begin;
          if (duration > LOCK_ACQUIRING_DURATION_WARN_MS) {
            LOG.warn("Lock for db {} was acquired in {} msec",
                db.getName(), duration);
          }
          return true;
        }
        versionLock_.writeLock().unlock();
        try {
          // Sleep to avoid spinning and allow other operations to make progress.
          Thread.sleep(LOCK_RETRY_DELAY_MS);
        } catch (InterruptedException e) {
          // ignore
        }
        end = System.currentTimeMillis();
      } while (end - begin < LOCK_RETRY_TIMEOUT_MS);
      return false;
    }
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

    @Override
    public void run() {
      if (LOG.isTraceEnabled()) LOG.trace("Reloading cache pool names from HDFS");

      // Map of cache pool name to CachePoolInfo. Stored in a map to allow Set operations
      // to be performed on the keys.
      Map<String, CachePoolInfo> currentCachePools = new HashMap<>();
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

  public int getPartialFetchRpcQueueLength() {
    return partialObjectFetchAccess_.getQueueLength();
  }

  public int getNumAsyncWaitingTables() {
    return tableLoadingMgr_.numRemainingItems();
  }

  public int getNumAsyncLoadingTables() {
    return tableLoadingMgr_.numLoadsInProgress();
  }

  public int getNumDatabases() { return numDbs_; }
  public int getNumTables() { return numTables_; }
  public int getNumFunctions() { return numFunctions_; }

  /**
   * Adds a list of cache directive IDs for the given table name. Asynchronously
   * refreshes the table metadata once all cache directives complete.
   */
  public void watchCacheDirs(List<Long> dirIds, TTableName tblName, String reason) {
    tableLoadingMgr_.watchCacheDirs(dirIds, tblName, reason);
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
   * Retrieves TPartitionStats as a map that associates partitions with their
   * statistics. The table partitions are specified in
   * TGetPartitionStatsRequest. If statistics are not available for a partition,
   * a default TPartitionStats is used. Partitions are identified by their partitioning
   * column string values.
   */
  public Map<String, ByteBuffer> getPartitionStats(TGetPartitionStatsRequest request)
      throws CatalogException {
    Preconditions.checkState(!RuntimeEnv.INSTANCE.isTestEnv());
    TTableName tableName = request.table_name;
    LOG.info("Fetching partition statistics for: " + tableName.getDb_name() + "."
        + tableName.getTable_name());
    ValidWriteIdList writeIdList = null;
    if (request.valid_write_ids != null) {
      writeIdList = MetastoreShim.getValidWriteIdListFromThrift(
        new TableName(request.table_name.db_name, request.table_name.table_name)
          .toString(), request.valid_write_ids);
    }
    long tableId = request.getTable_id();
    Table table = getOrLoadTable(tableName.db_name, tableName.table_name,
        "needed to fetch partition stats", writeIdList, tableId,
        NoOpEventSequence.INSTANCE);

    // Table could be null if it does not exist anymore.
    if (table == null) {
      throw new CatalogException(
          "Requested partition statistics for table that does not exist: "
          + request.table_name);
    }

    // Table could be incomplete, in which case an exception should be thrown.
    if (table instanceof IncompleteTable) {
      throw new CatalogException("No statistics available for incompletely"
          + " loaded table: " + request.table_name, ((IncompleteTable) table).getCause());
    }

    // Table must be a FeFsTable type at this point.
    Preconditions.checkArgument(table instanceof HdfsTable,
        "Partition statistics can only be requested for FS tables, type is: %s",
        table.getClass().getCanonicalName());

    // Table must be loaded.
    Preconditions.checkState(table.isLoaded());

    Map<String, ByteBuffer> stats = new HashMap<>();
    HdfsTable hdfsTable = (HdfsTable) table;
    hdfsTable.takeReadLock();
    try {
      Collection<? extends PrunablePartition> partitions = hdfsTable.getPartitions();
      for (PrunablePartition partition : partitions) {
        Preconditions.checkState(partition instanceof FeFsPartition);
        FeFsPartition fsPartition = (FeFsPartition) partition;
        TPartitionStats partStats = fsPartition.getPartitionStats();
        if (partStats != null) {
          ByteBuffer compressedStats =
              ByteBuffer.wrap(fsPartition.getPartitionStatsCompressed());
          stats.put(FeCatalogUtils.getPartitionName(fsPartition), compressedStats);
        }
      }
    } finally {
      hdfsTable.releaseReadLock();
    }
    LOG.info("Fetched partition statistics for " + stats.size()
        + " partitions on: " + hdfsTable.getFullName());
    return stats;
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
    long lastResetStartVersion;
    // Total number of dbs, tables, and functions in the catalog
    int numDbs = 0;
    int numTables = 0;
    int numFunctions = 0;

    // The keys of the updated topics.
    Set<String> updatedCatalogObjects;
    TSerializer serializer;

    GetCatalogDeltaContext(long nativeCatalogServerPtr, long fromVersion, long toVersion,
        long lastResetStartVersion) throws TTransportException
    {
      this.nativeCatalogServerPtr = nativeCatalogServerPtr;
      this.fromVersion = fromVersion;
      this.toVersion = toVersion;
      this.lastResetStartVersion = lastResetStartVersion;
      updatedCatalogObjects = new HashSet<>();
      serializer = new TSerializer(new TBinaryProtocol.Factory());
    }

    void addCatalogObject(TCatalogObject obj, boolean delete) throws TException {
      addCatalogObject(obj, delete, null);
    }

    void addCatalogObject(TCatalogObject obj, boolean delete,
        PartitionMetaSummary summary) throws TException {
      String key = Catalog.toCatalogObjectKey(obj);
      if (obj.type != TCatalogObjectType.CATALOG) {
        topicUpdateLog_.add(key,
            new TopicUpdateLog.Entry(0, obj.getCatalog_version(), toVersion, 0));
        if (!delete) updatedCatalogObjects.add(key);
      }
      // TODO: TSerializer.serialize() returns a copy of the internal byte array, which
      // could be elided.
      if (topicMode_ == TopicMode.FULL || topicMode_ == TopicMode.MIXED) {
        String v1Key = CatalogServiceConstants.CATALOG_TOPIC_V1_PREFIX + key;
        byte[] data = serializer.serialize(obj);
        int actualSize = FeSupport.NativeAddPendingTopicItem(nativeCatalogServerPtr,
            v1Key, obj.catalog_version, data, delete);
        if (actualSize < 0) {
          LOG.error("NativeAddPendingTopicItem failed in BE. key=" + v1Key + ", delete="
              + delete + ", data_size=" + data.length);
        } else if (summary != null && obj.type == HDFS_PARTITION) {
          summary.update(true, delete, obj.hdfs_partition.partition_name,
              obj.catalog_version, data.length, actualSize);
        }
      }

      if (topicMode_ == TopicMode.MINIMAL || topicMode_ == TopicMode.MIXED) {
        // Serialize a minimal version of the object that can be used by impalads
        // that are running in 'local-catalog' mode. This is used by those impalads
        // to invalidate their local cache.
        TCatalogObject minimalObject = getMinimalObjectForV2(obj);
        if (minimalObject != null) {
          byte[] data = serializer.serialize(minimalObject);
          String v2Key = CatalogServiceConstants.CATALOG_TOPIC_V2_PREFIX + key;
          int actualSize = FeSupport.NativeAddPendingTopicItem(nativeCatalogServerPtr,
              v2Key, obj.catalog_version, data, delete);
          if (actualSize < 0) {
            LOG.error("NativeAddPendingTopicItem failed in BE. key=" + v2Key + ", delete="
                + delete + ", data_size=" + data.length);
          } else if (summary != null && obj.type == HDFS_PARTITION) {
            summary.update(false, delete, obj.hdfs_partition.partition_name,
                obj.catalog_version, data.length, actualSize);
          }
        }
      }
    }

    private TCatalogObject getMinimalObjectForV2(TCatalogObject obj) {
      Preconditions.checkState(topicMode_ == TopicMode.MINIMAL ||
          topicMode_ == TopicMode.MIXED);
      TCatalogObject min = new TCatalogObject(obj.type, obj.catalog_version);
      switch (obj.type) {
      case DATABASE:
        min.setDb(new TDatabase(obj.db.db_name));
        break;
      case TABLE:
      case VIEW:
        min.setTable(new TTable(obj.table.db_name, obj.table.tbl_name));
        break;
      case HDFS_PARTITION:
        // LocalCatalog caches the partition meta only using the partition id as the key.
        // But we still need to pass the db and table name for generating the topic entry
        // key.
        THdfsPartition partObject = new THdfsPartition();
        partObject.setDb_name(obj.hdfs_partition.db_name);
        partObject.setTbl_name(obj.hdfs_partition.tbl_name);
        partObject.setPartition_name(obj.hdfs_partition.partition_name);
        partObject.setId(obj.hdfs_partition.id);
        if (obj.hdfs_partition.isSetPrev_id()) {
          Preconditions.checkState(
              obj.hdfs_partition.prev_id != HdfsPartition.INITIAL_PARTITION_ID - 1,
              "Invalid partition id");
          // For updates, coordinators can invalidate the old partition instance.
          partObject.setPrev_id(obj.hdfs_partition.prev_id);
        }
        min.setHdfs_partition(partObject);
        break;
      case CATALOG:
        // Sending the top-level catalog version is important for implementing SYNC_DDL.
        // This also allows impalads to detect a catalogd restart and invalidate the
        // whole catalog.
        // TODO(todd) ensure that the impalad does this invalidation as required.
        return obj;
      case PRIVILEGE:
      case PRINCIPAL:
      case AUTHZ_CACHE_INVALIDATION:
        // The caching of this data on the impalad side is somewhat complex
        // and this code is also under some churn at the moment. So, we'll just publish
        // the full information rather than doing fetch-on-demand.
        return obj;
      case FUNCTION:
        TFunction fnObject = new TFunction(obj.fn.getName());
        // IMPALA-8486: add the hdfs location so coordinators can mark their libCache
        // entry for this function to be stale.
        if (obj.fn.hdfs_location != null) fnObject.setHdfs_location(obj.fn.hdfs_location);
        min.setFn(fnObject);
        break;
      case DATA_SOURCE:
        min.setData_source(new TDataSource(obj.data_source));
        break;
      case HDFS_CACHE_POOL:
        // HdfsCachePools just contain the name strings. Publish them as minimal objects.
        return obj;
      default:
        throw new AssertionError("Unexpected catalog object type: " + obj.type);
      }
      return min;
    }

    boolean versionNotInRange(long version) {
      return version <= fromVersion || version > toVersion;
    }

    boolean isFullUpdate() { return fromVersion == 0; }
  }

  /**
   * Creates a partition meta summary for the given table name.
   */
  private PartitionMetaSummary createPartitionMetaSummary(String tableName) {
    return new PartitionMetaSummary(tableName, /*inCatalogd*/true,
        topicMode_ == TopicMode.FULL || topicMode_ == TopicMode.MIXED,
        topicMode_ == TopicMode.MINIMAL || topicMode_ == TopicMode.MIXED);
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
    GetCatalogDeltaContext ctx;
    // Get lock to read catalogVersion_ and lastResetStartVersion_
    versionLock_.readLock().lock();
    try {
      ctx = new GetCatalogDeltaContext(nativeCatalogServerPtr, fromVersion,
          catalogVersion_, lastResetStartVersion_);
    } finally {
      versionLock_.readLock().unlock();
    }
    for (Db db: getAllDbs()) {
      ctx.numDbs++;
      addDatabaseToCatalogDelta(db, ctx);
    }
    for (DataSource dataSource: getAllDataSources()) {
      addDataSourceToCatalogDelta(dataSource, ctx);
    }
    for (HdfsCachePool cachePool: getAllHdfsCachePools()) {
      addHdfsCachePoolToCatalogDelta(cachePool, ctx);
    }
    for (Role role: getAllRoles()) {
      addPrincipalToCatalogDelta(role, ctx);
    }
    for (User user: getAllUsers()) {
      addPrincipalToCatalogDelta(user, ctx);
    }
    for (AuthzCacheInvalidation authzCacheInvalidation: getAllAuthzCacheInvalidation()) {
      addAuthzCacheInvalidationToCatalogDelta(authzCacheInvalidation, ctx);
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
      collectPartitionDeletion(ctx, removedObject);
    }
    // Each topic update should contain a single "TCatalog" object which is used to
    // pass overall state on the catalog, such as the current version and the
    // catalog service id. By setting the catalog version to the latest catalog
    // version at this point, it ensures impalads will always bump their versions,
    // even in the case where an object has been dropped. Also pass the catalog version
    // when we reset the entire catalog last time. So coordinators in local catalog mode
    // can safely forward their min catalog version.
    TCatalogObject catalog =
        new TCatalogObject(TCatalogObjectType.CATALOG, ctx.toVersion);
    catalog.setCatalog(
        new TCatalog(JniCatalog.getServiceId(), ctx.lastResetStartVersion));
    ctx.addCatalogObject(catalog, false);
    // Garbage collect the delete and topic update log.
    deleteLog_.garbageCollect(ctx.toVersion);
    topicUpdateLog_.garbageCollectUpdateLogEntries(ctx.toVersion);
    lastSentTopicUpdate_.set(ctx.toVersion);
    // Notify any operation that is waiting on the next topic update.
    synchronized (topicUpdateLog_) {
      topicUpdateLog_.notifyAll();
    }
    numDbs_ = ctx.numDbs;
    numTables_ = ctx.numTables;
    numFunctions_ = ctx.numFunctions;
    return ctx.toVersion;
  }

  /**
   * Collects partition deletion from removed HdfsTable objects.
   */
  private void collectPartitionDeletion(GetCatalogDeltaContext ctx,
      TCatalogObject removedObject) throws TException {
    // If this is a HdfsTable and incremental metadata updates are enabled, make sure we
    // send deletes for removed partitions. So we won't leak partition topic entries in
    // the statestored catalog topic. Partitions are only included as objects in topic
    // updates if incremental metadata updates are enabled. Don't need this if
    // incremental metadata updates are disabled, because in this case the table
    // snapshot will be sent as a complete object. See more details in
    // addTableToCatalogDeltaHelper().
    if (!BackendConfig.INSTANCE.isIncrementalMetadataUpdatesEnabled()
        || removedObject.type != TCatalogObjectType.TABLE
        || removedObject.getTable().getTable_type() != TTableType.HDFS_TABLE) {
      return;
    }
    THdfsTable hdfsTable = removedObject.getTable().getHdfs_table();
    Preconditions.checkState(
        !hdfsTable.has_full_partitions && hdfsTable.has_partition_names,
        /*errorMessage*/hdfsTable);
    String tblName = removedObject.getTable().db_name + "."
        + removedObject.getTable().tbl_name;
    PartitionMetaSummary deleteSummary = createPartitionMetaSummary(tblName);
    Set<String> collectedPartNames = new HashSet<>();
    for (THdfsPartition part : hdfsTable.partitions.values()) {
      Preconditions.checkState(part.id >= HdfsPartition.INITIAL_PARTITION_ID
          && part.db_name != null
          && part.tbl_name != null
          && part.partition_name != null, /*errorMessage*/part);
      TCatalogObject removedPart = new TCatalogObject(HDFS_PARTITION,
          removedObject.getCatalog_version());
      removedPart.setHdfs_partition(part);
      String partObjKey = Catalog.toCatalogObjectKey(removedPart);
      boolean collected = false;
      if (!ctx.updatedCatalogObjects.contains(partObjKey)) {
        ctx.addCatalogObject(removedPart, true, deleteSummary);
        collectedPartNames.add(part.partition_name);
        collected = true;
      }
      LOG.trace("{} deletion of {} id={} from the active partition set " +
              "of a removed/invalidated table (version={})",
          collected ? "Collected" : "Skipped", partObjKey, part.id,
          removedObject.getCatalog_version());
    }
    // Adds the recently dropped partitions that are not yet synced to the catalog
    // topic.
    if (hdfsTable.isSetDropped_partitions()) {
      for (THdfsPartition part : hdfsTable.dropped_partitions) {
        // If a partition is dropped and then re-added, the old instance is added to
        // droppedPartitions and the new instance is in partitionMap of HdfsTable.
        // So partitions collected in the above loop could have the same name here.
        if (collectedPartNames.contains(part.partition_name)) continue;
        TCatalogObject removedPart = new TCatalogObject(HDFS_PARTITION,
            removedObject.getCatalog_version());
        removedPart.setHdfs_partition(part);
        String partObjKey = Catalog.toCatalogObjectKey(removedPart);
        // Skip if there is an update of the partition collected. It could be
        // collected from a new version of the HdfsTable and this comes from an
        // invalidated HdfsTable.
        boolean collected = false;
        if (!ctx.updatedCatalogObjects.contains(partObjKey)) {
          ctx.addCatalogObject(removedPart, true, deleteSummary);
          collected = true;
        }
        LOG.trace("{} deletion of {} id={} from the dropped partition set " +
                "of a removed/invalidated table (version={})",
            collected ? "Collected" : "Skipped", partObjKey, part.id,
            removedObject.getCatalog_version());
      }
    }
    if (deleteSummary.hasUpdates()) LOG.info(deleteSummary.toString());
  }

  /**
   * Evaluates if the information from an event (serviceId and versionNumber) matches to
   * the catalog object. If there is match, the in-flight version for that object is
   * removed and method returns true. If it does not match, returns false

   * @param ctx self context which provides all the information needed to
   * evaluate if this is a self-event or not
   * @return true if given event information evaluates to a self-event, false otherwise
   */
  public boolean evaluateSelfEvent(SelfEventContext ctx) throws CatalogException {
    Preconditions.checkState(isEventProcessingActive(),
        "Event processing should be enabled when calling this method");
    boolean isInsertEvent = ctx.isInsertEventContext();
    long versionNumber =
        isInsertEvent ? ctx.getInsertEventId(0) : ctx.getVersionNumberFromEvent();
    String serviceIdFromEvent = ctx.getServiceIdFromEvent();

    if (!isInsertEvent) {
      // no version info or service id in the event
      if (versionNumber == -1 || serviceIdFromEvent.isEmpty()) {
        LOG.debug("Not a self-event since the given version is {} and service id is {}",
            versionNumber, serviceIdFromEvent.isEmpty() ? "empty" : serviceIdFromEvent);
        return false;
      }
      // if the service id from event doesn't match with our service id this is not a
      // self-event
      if (!getCatalogServiceId().equals(serviceIdFromEvent)) {
        LOG.debug("Not a self-event because service id of this catalog {} does not match "
                + "with one in event {}.",
            getCatalogServiceId(), serviceIdFromEvent);
        return false;
      }
    } else if (versionNumber == -1) {
      // if insert event, we only compare eventId
      LOG.debug("Not a self-event because eventId is {}", versionNumber);
      return false;
    }
    Db db = getDb(ctx.getDbName()); // Uses thread-safe hashmap.
    if (db == null) {
      throw new DatabaseNotFoundException("Database " + ctx.getDbName() + " not found");
    }
    if (ctx.getTblName() == null) {
      // Not taking lock, rely on Db's internal locking.
      return evaluateSelfEventForDb(db, versionNumber);
    }

    Table tbl = db.getTable(ctx.getTblName()); // Uses thread-safe hashmap.
    if (tbl == null) {
      throw new TableNotFoundException(
          String.format("Table %s.%s not found", ctx.getDbName(), ctx.getTblName()));
    }

    if (ctx.getPartitionKeyValues() == null) {
      // Not taking lock, rely on Table's internal locking.
      return evaluateSelfEventForTable(tbl, isInsertEvent, versionNumber);
    }

    // TODO: Could be a Precondition? If partitionKeyValues != null, this should be
    // a HDFS table.
    if (!(tbl instanceof HdfsTable)) return false;

    // We should acquire the table lock so that we wait for any other updates
    // happening to this table at the same time, as they could affect the list of
    // partitions.
    // TODO: add more fine grained locking to protect partitions without taking
    //       longly held locks (IMPALA-12461)
    // Increasing the timeout would not help if the table lock is held by
    // a concurrent DDL as we would need the lock during the actual processing of the
    // event.
    if (!tryWriteLock(tbl)) {
      throw new CatalogException(String.format("Error during self-event evaluation "
          + "for table %s due to lock contention", tbl.getFullName()));
    }
    versionLock_.writeLock().unlock();
    try {
      return evaluateSelfEventForPartition(
          ctx, (HdfsTable)tbl, isInsertEvent, versionNumber);
    } finally {
      tbl.releaseWriteLock();
    }
  }

  private boolean evaluateSelfEventForDb(Db db, long versionNumber)
      throws CatalogException {
    boolean removed = db.removeFromVersionsForInflightEvents(versionNumber);
    if (!removed) {
      LOG.debug("Could not find version {} in the in-flight event list of database "
          + "{}", versionNumber, db.getName());
    }
    return removed;
  }

  private boolean evaluateSelfEventForTable(Table tbl,
      boolean isInsertEvent, long versionNumber) throws CatalogException {
    boolean removed =
        tbl.removeFromVersionsForInflightEvents(isInsertEvent, versionNumber);
    if (!removed) {
      LOG.debug("Could not find {} {} in in-flight event list of table {}",
          isInsertEvent ? "eventId" : "version", versionNumber, tbl.getFullName());
    }
    return removed;
  }

  private boolean evaluateSelfEventForPartition(SelfEventContext ctx, HdfsTable tbl,
      boolean isInsertEvent, long versionNumber) throws CatalogException {
    List<List<TPartitionKeyValue>> partitionKeyValues = ctx.getPartitionKeyValues();
    List<String> failingPartitions = new ArrayList<>();
    int len = partitionKeyValues.size();
    for (int i=0; i<len; ++i) {
      List<TPartitionKeyValue> partitionKeyValue = partitionKeyValues.get(i);
      versionNumber = isInsertEvent ? ctx.getInsertEventId(i) : versionNumber;
      HdfsPartition hdfsPartition =
          tbl.getPartitionFromThriftPartitionSpec(partitionKeyValue);
      if (hdfsPartition == null
          || !hdfsPartition.removeFromVersionsForInflightEvents(isInsertEvent,
              versionNumber)) {
        // even if this is an error condition we should not bail out early since we
        // should clean up the self-event state on the rest of the partitions
        String partName = HdfsTable.constructPartitionName(partitionKeyValue);
        if (hdfsPartition == null) {
          LOG.debug("Partition {} not found during self-event "
              + "evaluation for the table {}", partName, tbl.getFullName());
        } else {
          LOG.trace("Could not find {} in in-flight event list of the partition {} "
              + "of table {}", versionNumber, partName, tbl.getFullName());
        }
        failingPartitions.add(partName);
      }
    }
    return failingPartitions.isEmpty();
  }

  /**
   * Adds a given version number from the catalog table's list of versions for in-flight
   * events. Applicable only when external event processing is enabled.
   * @param isInsertEvent if false add versionNumber for DDL Event, otherwise add eventId
   * for Insert Event.
   * @param tbl Catalog table
   * @param versionNumber when isInsertEventContext is true, it is eventId to add
   * when isInsertEventContext is false, it is version number to add
   * @return true if versionNumber is added to in-flight list. Otherwise, return false.
   */
  public boolean addVersionsForInflightEvents(
      boolean isInsertEvent, Table tbl, long versionNumber) {
    if (!isEventProcessingActive()) return false;
    boolean added = tbl.addToVersionsForInflightEvents(isInsertEvent, versionNumber);
    if (added) {
      LOG.info("Added {} {} in table's {} in-flight events",
          isInsertEvent ? "eventId" : "catalog version", versionNumber,
          tbl.getFullName());
    }
    return added;
  }

  /**
   * Adds a given version number from the catalog database's list of versions for
   * in-flight events. Applicable only when external event processing is enabled.
   *
   * @param db Catalog database
   * @param versionNumber version number to be added
   * @return true if versionNumber is added to in-flight list. Otherwise, return false.
   */
  public boolean addVersionsForInflightEvents(Db db, long versionNumber) {
    if (!isEventProcessingActive()) return false;
    boolean added = db.addToVersionsForInflightEvents(versionNumber);
    if (added) {
      LOG.info("Added catalog version {} in database's {} in-flight events",
          versionNumber, db.getName());
    }
    return added;
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
  List<Db> getAllDbs() {
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
   * Get a snapshot view of all the users in the catalog.
   */
  private List<User> getAllUsers() {
    versionLock_.readLock().lock();
    try {
      return ImmutableList.copyOf(authPolicy_.getAllUsers());
    } finally {
      versionLock_.readLock().unlock();
    }
  }

  /**
   * Get a snapshot view of all authz cache invalidation markers in the catalog.
   */
  private List<AuthzCacheInvalidation> getAllAuthzCacheInvalidation() {
    versionLock_.readLock().lock();
    try {
      return ImmutableList.copyOf(authzCacheInvalidation_);
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
      ctx.numTables++;
      addTableToCatalogDelta(tbl, ctx);
    }
    for (Function fn: getAllFunctions(db)) {
      ctx.numFunctions++;
      addFunctionToCatalogDelta(fn, ctx);
    }
  }

  /**
   * Get a snapshot view of all the tables in a database.
   */
  List<Table> getAllTables(Db db) {
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
   * Given a database name and a property key returns the value of the key from the
   * parameters map of the HMS db object
   * @param dbName name of the database
   * @param propertyKey property key
   * @return value of key from the db parameter. returns null if Db is not found or key
   * does not exist in the parameters
   */
  public String getDbProperty(String dbName, String propertyKey) {
    Preconditions.checkNotNull(dbName);
    Preconditions.checkNotNull(propertyKey);
    versionLock_.readLock().lock();
    try {
      Db db = getDb(dbName);
      if (db == null) return null;
      if (!db.getMetaStoreDb().isSetParameters()) return null;
      return db.getMetaStoreDb().getParameters().get(propertyKey);
    } finally {
      versionLock_.readLock().unlock();
    }
  }

  /**
   * Given a dbname, table name and a key returns the value of the key from the cached
   * Table object's parameters
   * @return Value of the parameter which maps to property key, null if the table
   * doesn't exist, if it is a incomplete table or if the parameter is not found
   */
  public List<String> getTableProperties(
      String dbName, String tblName, List<String> propertyKeys) {
    Preconditions.checkNotNull(dbName);
    Preconditions.checkNotNull(tblName);
    Preconditions.checkNotNull(propertyKeys);
    versionLock_.readLock().lock();
    try {
      Db db = getDb(dbName);
      if (db == null) return null;
      Table tbl = db.getTable(tblName);
      if (tbl == null || tbl instanceof IncompleteTable) return null;
      if (!tbl.getMetaStoreTable().isSetParameters()) return null;
      List<String> propertyValues = new ArrayList<>(propertyKeys.size());
      for (String propertyKey : propertyKeys) {
        propertyValues.add(tbl.getMetaStoreTable().getParameters().get(propertyKey));
      }
      return propertyValues;
    } finally {
      versionLock_.readLock().unlock();
    }
  }

  /**
   * Updates the Db with the given metastore database object. Useful to doing in-place
   * updates to the HMS db like in case of changing owner, adding comment or setting
   * certain properties
   * @param msDb The HMS database object to be used to update
   * @return The updated Db object
   * @throws DatabaseNotFoundException if Db with the name provided by given Database
   * is not found in Catalog
   */
  public Db updateDb(Database msDb) throws DatabaseNotFoundException {
    Preconditions.checkNotNull(msDb);
    Preconditions.checkNotNull(msDb.getName());
    versionLock_.writeLock().lock();
    try {
      Db db = getDb(msDb.getName());
      if (db == null) {
        throw new DatabaseNotFoundException("Database " + msDb.getName() + " not found");
      }
      db.setMetastoreDb(msDb.getName(), msDb);
      db.setCatalogVersion(incrementAndGetCatalogVersion());
      return db;
    } finally {
      versionLock_.writeLock().unlock();
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
    TopicUpdateLog.Entry topicUpdateEntry =
        topicUpdateLog_.getOrCreateLogEntry(tbl.getUniqueName());
    Preconditions.checkNotNull(topicUpdateEntry);
    // it is important to get the table version once and then use it for following logic
    // since we don't have the table lock yet and the table could be changed during the
    // execution below.
    final long tblVersion = tbl.getCatalogVersion();
    if (tblVersion <= ctx.toVersion) {
      // if we have already skipped this table due to lock contention
      // maxSkippedUpdatesLockContention number of times, we must block until we add it
      // to the topic updates. Otherwise, we can attempt to take a lock with a timeout.
      // if the topicUpdateTblLockMaxWaitTimeMs is set to 0, it means the lock timeout
      // is disabled and we should just block here until lock is acquired.
      boolean lockWithTimeout = topicUpdateTblLockMaxWaitTimeMs_ > 0
          && topicUpdateEntry.getNumSkippedUpdatesLockContention()
          < maxSkippedUpdatesLockContention_;
      if (topicUpdateTblLockMaxWaitTimeMs_ > 0 && !lockWithTimeout) {
        LOG.warn("Topic update thread blocking until lock is acquired for table {}",
            tbl.getFullName());
      }
      lockTableAndAddToCatalogDelta(tblVersion, tbl, ctx, lockWithTimeout);
    } else {
      // tbl is outside the current topic update window. For a fast changing table, it is
      // possible that this tbl is starved and never added to topic updates. Hence we
      // see how many times the table was skipped before.
      if (topicUpdateEntry.getNumSkippedTopicUpdates() == MAX_NUM_SKIPPED_TOPIC_UPDATES) {
        LOG.warn("Topic update thread blocking until lock is acquired for table {} "
                + "since the table was already skipped {} number of times",
            tbl.getFullName(), MAX_NUM_SKIPPED_TOPIC_UPDATES);
        lockTableAndAddToCatalogDelta(tblVersion, tbl, ctx, false);
      } else {
        LOG.info("Table {} (version={}) is skipping topic update ({}, {}]",
            tbl.getFullName(), tblVersion, ctx.fromVersion, ctx.toVersion);
        topicUpdateLog_.add(tbl.getUniqueName(),
            new TopicUpdateLog.Entry(
                topicUpdateEntry.getNumSkippedTopicUpdates() + 1,
                topicUpdateEntry.getLastSentVersion(),
                topicUpdateEntry.getLastSentCatalogUpdate(),
                topicUpdateEntry.getNumSkippedUpdatesLockContention()));
      }
    }
  }

  /**
   * This method takes a lock on the table and adds it to the
   * {@link GetCatalogDeltaContext} which is eventually sent via the topic updates. A lock
   * on table essentially blocks other concurrent catalog operations on the table. Also,
   * if the table is a {@link HdfsTable} and it is already locked, this method may or may
   * not block until the table lock is acquired depending on whether lockWithTimeout
   * parameter is false or not.
   * When the lockWithTimeout is true this method attempts to acquire a lock with a
   * timeout specified by {@code topicUpdateTblLockMaxWaitTimeMs}. If the lock is acquired
   * within the timeout, it continues ahead and adds the table to the ctx. However, if
   * the lock is not acquired within the timeout, it skips the table and increments
   * the counter in {@link TopicUpdateLog.Entry}.
   * @param tblVersion The table version at the time when topic update evaluates if the
   *                   table needs to be in this update or not.
   * @param tbl The table object which needs to added to the topic update.
   * @param ctx GetCatalogDeltaContext where the table is add if the lock is acquired.
   * @param lockWithTimeout If this is true, the method attempts to lock with a timeout
   *                        else, it blocks until lock is acquired.
   */
  private void lockTableAndAddToCatalogDelta(final long tblVersion, Table tbl,
      GetCatalogDeltaContext ctx, boolean lockWithTimeout) throws TException {
    Stopwatch sw = Stopwatch.createStarted();
    if (tbl instanceof HdfsTable && lockWithTimeout) {
      if (!lockHdfsTblWithTimeout(tblVersion, (HdfsTable) tbl, ctx)) return;
    } else {
      // this is not HdfsTable or lockWithTimeout is false.
      // We block until table read lock is acquired.
      tbl.takeReadLock();
    }
    long elapsedTime = sw.stop().elapsed(TimeUnit.MILLISECONDS);
    if (elapsedTime > 2000) {
      LOG.debug("Time taken to acquire read lock on table {} for topic update {} ms",
          tbl.getFullName(), elapsedTime);
    }
    try {
      addTableToCatalogDeltaHelper(tbl, ctx);
    } finally {
      tbl.releaseReadLock();
    }
  }

  /**
   * Attempts to take a read lock on the give HdfsTable within a configurable timeout
   * of {@code topicUpdateTblLockMaxWaitTimeMs}.
   * @param tblVersion The version of the table when topic update thread inspects the
   *                   table to be added to the catalog topic updates.
   * @param hdfsTable The table to be read locked.
   * @param ctx The current {@link GetCatalogDeltaContext} for this topic update.
   * @return true if the table was successfully read-locked, false otherwise.
   */
  private boolean lockHdfsTblWithTimeout(long tblVersion, HdfsTable hdfsTable,
      GetCatalogDeltaContext ctx) {
    // see the comment below on why we need 2 attempts.
    final int maxAttempts = 2;
    int attemptCount = 0;
    boolean lockAcquired;
    do {
      attemptCount++;
      // topicUpdateTblLockMaxWaitTimeMs indicates the total amount of time we are willing
      // to wait to acquire the table lock. We make 2 attempts and hence the timeout here
      // is topicUpdateTblLockMaxWaitTimeMs/2 so that overall the method waits for
      // maximum of topicUpdateTblLockMaxWaitTimeMs for the lock.
      long timeoutMs = topicUpdateTblLockMaxWaitTimeMs_ /maxAttempts;
      lockAcquired = tryLock(hdfsTable, false, timeoutMs);
      if (lockAcquired) {
        // table lock was successfully acquired. We can now release the versionLock.
        versionLock_.writeLock().unlock();
        return true;
      }
      // If we reach here, the topic update thread could not take a lock on this table.
      // We should bump up the pending table version so that this gets included in the
      // next topic update. However, there is a race condition which needs to be handled
      // here. If we update the pending version here
      // after hdfsTable.setCatalogVersion() is called from CatalogOpExecutor, the bump up
      // of pendingVersion takes no effect. Hence we detect such case by comparing the
      // tbl version with the expected tblVersion. if the tblVersion does not match, it
      // means that the setCatalogVersion has already been called and there is no point
      // in bumping up the pending version now. We retry to take a lock on the tbl again
      // in such case. If we cannot get a read lock in attempt 2, it means that we were
      // unlucky again and some other write operation has acquired the tbl lock. In such
      // a case it is guaranteed that the tbl version will be updated outside current
      // window of the topic updates and it is safe to skip the table.
      boolean pendingVersionUpdated = hdfsTable
          .updatePendingVersion(tblVersion, incrementAndGetCatalogVersion());
      if (pendingVersionUpdated) break;
      // if pendingVersionUpdated is false it means that tblVersion has been changed
      // and hence we didn't update the pendingVersion. We retry once to acquire a read
      // lock.
    } while (attemptCount != maxAttempts);
    // lock could not be acquired, we update the skip count in the topicUpdate entry
    // if applicable.
    TopicUpdateLog.Entry topicUpdateEntry = topicUpdateLog_
        .getOrCreateLogEntry(hdfsTable.getUniqueName());
    LOG.info(
        "Table {} (version={}, lastSeen={}) is skipping topic update ({}, {}] "
            + "due to lock contention", hdfsTable.getFullName(), tblVersion,
        hdfsTable.getLastVersionSeenByTopicUpdate(), ctx.fromVersion, ctx.toVersion);
    if (hdfsTable.getLastVersionSeenByTopicUpdate() != tblVersion) {
      // if the last version skipped by topic update is not same as the last version
      // sent, it means the table was updated and topic update thread is lagging
      // behind.
      topicUpdateLog_.add(hdfsTable.getUniqueName(),
          new TopicUpdateLog.Entry(
              topicUpdateEntry.getNumSkippedTopicUpdates(),
              topicUpdateEntry.getLastSentVersion(),
              topicUpdateEntry.getLastSentCatalogUpdate(),
              topicUpdateEntry.getNumSkippedUpdatesLockContention() + 1));
      // we keep track of the table version when topic update thread had to skip the
      // table from updates so that next iteration can determine if we need to
      // increment the lock contention counter in topic update entry again.
      hdfsTable.setLastVersionSeenByTopicUpdate(tblVersion);
    }
    return false;
  }

  /**
   * Helper function that tries to add a table in a topic update. It acquires table's
   * lock and checks if its version is in the ('ctx.fromVersion', 'ctx.toVersion'] range
   * and how many consecutive times (if any) has the table skipped a topic update.
   */
  private void addTableToCatalogDeltaHelper(Table tbl, GetCatalogDeltaContext ctx)
      throws TException {
    Preconditions.checkState(tbl.isReadLockedByCurrentThread(),
        "Topic update thread does not hold a lock on table " + tbl.getFullName()
            + " while generating catalog delta");
    TCatalogObject catalogTbl =
        new TCatalogObject(TABLE, Catalog.INITIAL_CATALOG_VERSION);
    long tblVersion = tbl.getCatalogVersion();
    if (tblVersion <= ctx.fromVersion) {
      LOG.trace("Table {} version {} skipping the update ({}, {}]",
          tbl.getFullName(), tbl.getCatalogVersion(), ctx.fromVersion, ctx.toVersion);
      return;
    }
    String tableUniqueName = tbl.getUniqueName();
    TopicUpdateLog.Entry topicUpdateEntry =
        topicUpdateLog_.getOrCreateLogEntry(tableUniqueName);
    if (tblVersion > ctx.toVersion &&
        topicUpdateEntry.getNumSkippedTopicUpdates() < MAX_NUM_SKIPPED_TOPIC_UPDATES) {
      LOG.info("Table " + tbl.getFullName() + " is skipping topic update " +
          ctx.toVersion);
      topicUpdateLog_.add(tableUniqueName,
          new TopicUpdateLog.Entry(
              topicUpdateEntry.getNumSkippedTopicUpdates() + 1,
              topicUpdateEntry.getLastSentVersion(),
              topicUpdateEntry.getLastSentCatalogUpdate(),
              topicUpdateEntry.getNumSkippedUpdatesLockContention()));
      return;
    }
    try {
      if (BackendConfig.INSTANCE.isIncrementalMetadataUpdatesEnabled()
          && tbl instanceof HdfsTable) {
        catalogTbl.setTable(((HdfsTable) tbl).toThriftWithMinimalPartitions());
        addHdfsPartitionsToCatalogDelta((HdfsTable) tbl, ctx);
      } else {
        catalogTbl.setTable(tbl.toThrift());
      }
      // Update catalog object type from TABLE to VIEW if it's indeed a view.
      if (TImpalaTableType.VIEW == tbl.getTableType()) {
        catalogTbl.setType(TCatalogObjectType.VIEW);
      }
    } catch (Exception e) {
      LOG.error(String.format("Error calling toThrift() on table %s: %s",
          tbl.getFullName(), e.getMessage()), e);
      return;
    }
    catalogTbl.setCatalog_version(tbl.getCatalogVersion());
    ctx.addCatalogObject(catalogTbl, false);
  }

  private void addHdfsPartitionsToCatalogDelta(HdfsTable hdfsTable,
      GetCatalogDeltaContext ctx) throws TException {
    // Reset the max sent partition id if we are collecting a full update (e.g. due to
    // statestored restarts).
    if (ctx.isFullUpdate()) hdfsTable.resetMaxSentPartitionId();

    PartitionMetaSummary updateSummary = createPartitionMetaSummary(
        hdfsTable.getFullName());

    // Add updates for new partitions.
    long maxSentId = hdfsTable.getMaxSentPartitionId();
    for (TCatalogObject catalogPart : hdfsTable.getNewPartitionsSinceLastUpdate()) {
      maxSentId = Math.max(maxSentId, catalogPart.getHdfs_partition().getId());
      ctx.addCatalogObject(catalogPart, false, updateSummary);
    }
    hdfsTable.setMaxSentPartitionId(maxSentId);

    for (HdfsPartition part : hdfsTable.getDroppedPartitions()) {
      TCatalogObject removedPart = part.toMinimalTCatalogObject();
      if (!ctx.updatedCatalogObjects.contains(
          Catalog.toCatalogObjectKey(removedPart))) {
        ctx.addCatalogObject(removedPart, true, updateSummary);
      }
    }
    hdfsTable.resetDroppedPartitions();

    if (updateSummary.hasUpdates()) LOG.info(updateSummary.toString());
  }

  /**
   * Adds a function to the topic update if its version is in the range
   * ('ctx.fromVersion', 'ctx.toVersion'].
   */
  private void addFunctionToCatalogDelta(Function fn, GetCatalogDeltaContext ctx)
      throws TException {
    long fnVersion = fn.getCatalogVersion();
    if (ctx.versionNotInRange(fnVersion)) return;
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
    if (ctx.versionNotInRange(dsVersion)) return;
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
    if (ctx.versionNotInRange(cpVersion)) return;
    TCatalogObject pool =
        new TCatalogObject(TCatalogObjectType.HDFS_CACHE_POOL, cpVersion);
    pool.setCache_pool(cachePool.toThrift());
    ctx.addCatalogObject(pool, false);
  }


  /**
   * Adds a principal to the topic update if its version is in the range
   * ('ctx.fromVersion', 'ctx.toVersion']. It iterates through all the privileges of
   * this principal to determine if they can be inserted in the topic update.
   */
  private void addPrincipalToCatalogDelta(Principal principal, GetCatalogDeltaContext ctx)
      throws TException {
    long principalVersion = principal.getCatalogVersion();
    if (!ctx.versionNotInRange(principalVersion)) {
      TCatalogObject thriftPrincipal =
          new TCatalogObject(TCatalogObjectType.PRINCIPAL, principalVersion);
      thriftPrincipal.setPrincipal(principal.toThrift());
      ctx.addCatalogObject(thriftPrincipal, false);
    }
    for (PrincipalPrivilege p: getAllPrivileges(principal)) {
      addPrincipalPrivilegeToCatalogDelta(p, ctx);
    }
  }

  /**
   * Get a snapshot view of all the privileges in a principal.
   */
  private List<PrincipalPrivilege> getAllPrivileges(Principal principal) {
    Preconditions.checkNotNull(principal);
    versionLock_.readLock().lock();
    try {
      return ImmutableList.copyOf(principal.getPrivileges());
    } finally {
      versionLock_.readLock().unlock();
    }
  }

  /**
   * Adds a principal privilege to the topic update if its version is in the range
   * ('ctx.fromVersion', 'ctx.toVersion'].
   */
  private void addPrincipalPrivilegeToCatalogDelta(PrincipalPrivilege priv,
      GetCatalogDeltaContext ctx) throws TException  {
    long privVersion = priv.getCatalogVersion();
    if (ctx.versionNotInRange(privVersion)) return;
    TCatalogObject privilege =
        new TCatalogObject(TCatalogObjectType.PRIVILEGE, privVersion);
    privilege.setPrivilege(priv.toThrift());
    ctx.addCatalogObject(privilege, false);
  }

  /**
   * Adds an authz cache invalidation to the topic update if its version is in the range
   * ('ctx.fromVersion', 'ctx.toVersion'].
   */
  private void addAuthzCacheInvalidationToCatalogDelta(
      AuthzCacheInvalidation authzCacheInvalidation, GetCatalogDeltaContext ctx)
      throws TException  {
    long authzCacheInvalidationVersion = authzCacheInvalidation.getCatalogVersion();
    if (ctx.versionNotInRange(authzCacheInvalidationVersion)) return;
    TCatalogObject catalogObj = new TCatalogObject(
        TCatalogObjectType.AUTHZ_CACHE_INVALIDATION, authzCacheInvalidationVersion);
    catalogObj.setAuthz_cache_invalidation(authzCacheInvalidation.toThrift());
    ctx.addCatalogObject(catalogObj, false);
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
    Map<String, List<Function>> dbFns = db.getAllFunctions();
    List<Function> fns = new ArrayList<>(dbFns.size());
    for (List<Function> fnOverloads: dbFns.values()) {
      for (Function fn: fnOverloads) {
        fns.add(fn);
      }
    }
    return fns;
  }

  /**
   * Extracts Impala functions stored in metastore db parameters and adds them to
   * the catalog cache.
   */
  private void loadFunctionsFromDbParams(Db db,
      org.apache.hadoop.hive.metastore.api.Database msDb) {
    if (msDb == null || msDb.getParameters() == null) return;
    LOG.info("Loading native functions for database: " + db.getName());
    List<Function> funcs = FunctionUtils.deserializeNativeFunctionsFromDbParams(
        msDb.getParameters());
    for (Function f : funcs) {
      db.addFunction(f, false);
      f.setCatalogVersion(incrementAndGetCatalogVersion());
    }
    LOG.info("Loaded {} native functions for database: {}", funcs.size(), db.getName());
  }

  /**
   * Loads Java functions into the catalog. For each function in "functions",
   * we extract all Impala compatible evaluate() signatures and load them
   * as separate functions in the catalog.
   */
  private void loadJavaFunctions(Db db,
      List<org.apache.hadoop.hive.metastore.api.Function> functions) {
    Preconditions.checkNotNull(functions);
    if (BackendConfig.INSTANCE.disableCatalogDataOpsDebugOnly()) {
      LOG.info("Skip loading Java functions: catalog data ops disabled.");
      return;
    }
    LOG.info("Loading Java functions for database: " + db.getName());
    int numFuncs = 0;
    for (org.apache.hadoop.hive.metastore.api.Function function: functions) {
      try {
        HiveJavaFunctionFactoryImpl factory =
            new HiveJavaFunctionFactoryImpl(localLibraryPath_);
        HiveJavaFunction javaFunction = factory.create(function);
        for (Function fn: javaFunction.extract()) {
          db.addFunction(fn);
          fn.setCatalogVersion(incrementAndGetCatalogVersion());
          ++numFuncs;
        }
      } catch (Exception | LinkageError e) {
        LOG.error("Skipping function load: " + function.getFunctionName(), e);
      }
    }
    LOG.info("Loaded {} Java functions for database: {}", numFuncs, db.getName());
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
          new ArrayList<>();
      for (String javaFn : msClient.getHiveClient().getFunctions(dbName, "*")) {
        javaFns.add(msClient.getHiveClient().getFunction(dbName, javaFn));
      }
      // Contains native functions in it's params map.
      org.apache.hadoop.hive.metastore.api.Database msDb =
          msClient.getHiveClient().getDatabase(dbName);
      tmpDb = new Db(dbName, msDb);
      // Load native UDFs into the temporary db.
      loadFunctionsFromDbParams(tmpDb, msDb);
      // Load Java UDFs from HMS into the temporary db.
      loadJavaFunctions(tmpDb, javaFns);

      Db db = getDb(dbName);
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
   * Loads DataSource objects into the catalog and assigns new versions to all the
   * loaded DataSource objects.
   */
  public void refreshDataSources() throws TException {
    Map<String, DataSource> newDataSrcs = null;
    try (MetaStoreClient msClient = getMetaStoreClient()) {
      // Load all DataSource objects from HMS DataConnector objects.
      newDataSrcs = MetastoreShim.loadAllDataSources(msClient.getHiveClient());
      if (newDataSrcs == null) return;
    }
    Set<String> oldDataSrcNames = dataSources_.keySet();
    Set<String> newDataSrcNames = newDataSrcs.keySet();
    oldDataSrcNames.removeAll(newDataSrcNames);
    int removedDataSrcNum = 0;
    for (String dataSrcName: oldDataSrcNames) {
      // Add removed DataSource objects to deleteLog_.
      DataSource dataSrc = dataSources_.remove(dataSrcName);
      if (dataSrc != null) {
        dataSrc.setCatalogVersion(incrementAndGetCatalogVersion());
        deleteLog_.addRemovedObject(dataSrc.toTCatalogObject());
        removedDataSrcNum++;
      }
    }
    for (DataSource dataSrc: newDataSrcs.values()) {
      dataSrc.setCatalogVersion(incrementAndGetCatalogVersion());
      dataSources_.add(dataSrc);
    }
    LOG.info("Finished refreshing DataSources. Added {}. Removed {}.",
        newDataSrcs.size(), removedDataSrcNum);
  }

  /**
   * Load the list of TableMeta from Hive. If pull_table_types_and_comments=true, the list
   * will contain the table types and comments. Otherwise, we just fetch the table names
   * and set nulls on the types and comments.
   *
   * @param msClient HMS client for fetching metadata
   * @param dbName Database name of the required tables. Should not be null.
   * @param tblName Nullable table name. If it's null, all tables of the required database
   *               will be fetched. If it's not null, the list will contain only one item.
   */
  private List<TableMeta> getTableMetaFromHive(MetaStoreClient msClient, String dbName,
      @Nullable String tblName) throws TException {
    // Load the exact TableMeta list if pull_table_types_and_comments is set to true.
    if (BackendConfig.INSTANCE.pullTableTypesAndComments()) {
      return msClient.getHiveClient().getTableMeta(dbName, tblName, /*tableTypes*/ null);
    }
    // Unloaded tables are all treated as TABLEs. Simply set its type and comment to null.
    // It doesn't matter actually. The real table type will be refreshed after the table
    // is loaded.
    List<TableMeta> res = Lists.newArrayList();
    if (tblName == null) {
      // request for getting all table names
      for (String tableName : msClient.getHiveClient().getAllTables(dbName)) {
        res.add(new TableMeta(dbName, tableName, /*tableType*/ null));
      }
    } else if (msClient.getHiveClient().tableExists(dbName, tblName)) {
      res.add(new TableMeta(dbName, tblName, /*tableType*/ null));
    }
    return res;
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
      MetaStoreClient msClient, String dbName, Db existingDb,
      EventSequence catalogTimeline) {
    try {
      List<org.apache.hadoop.hive.metastore.api.Function> javaFns =
          new ArrayList<>();
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
      catalogTimeline.markEvent("Loaded functions of " + dbName);

      LOG.info("Loading table list for database: {}", dbName);
      int numTables = 0;
      List<TTableName> tblsToBackgroundLoad = new ArrayList<>();
      for (TableMeta tblMeta: getTableMetaFromHive(msClient, dbName, /*tblName*/null)) {
        String tableName = tblMeta.getTableName().toLowerCase();
        if (isBlacklistedTable(dbName, tableName)) {
          LOG.info("skip blacklisted table: " + dbName + "." + tableName);
          continue;
        }
        LOG.trace("Get {}", tblMeta);
        Table incompleteTbl = IncompleteTable.createUninitializedTable(newDb, tableName,
            MetastoreShim.mapToInternalTableType(tblMeta.getTableType()),
            tblMeta.getComments());
        incompleteTbl.setCatalogVersion(incrementAndGetCatalogVersion());
        newDb.addTable(incompleteTbl);
        ++numTables;
        if (loadInBackground_) {
          tblsToBackgroundLoad.add(new TTableName(dbName, tableName));
        }
      }
      catalogTimeline.markEvent(String.format(
          "Loaded %d table names of database %s", numTables, dbName));
      LOG.info("Loaded table list for database: {}. Number of tables: {}",
          dbName, numTables);

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
          Table removedTable = IncompleteTable.createUninitializedTableForRemove(
              existingDb, removedTableName);
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
   * Refreshes authorization metadata. When authorization is not enabled, this
   * method is a no-op.
   */
  public AuthorizationDelta refreshAuthorization(boolean resetVersions)
      throws CatalogException {
    Preconditions.checkState(authzManager_ != null);
    try {
      return authzManager_.refreshAuthorization(resetVersions);
    } catch (Exception e) {
      throw new CatalogException("Error refreshing authorization policy: ", e);
    }
  }

  /**
   * Resets this catalog instance by clearing all cached table and database metadata.
   * Returns the current catalog version before reset has taken any effect. The
   * requesting impalad will use that version to determine when the
   * effects of reset have been applied to its local catalog cache.
   */
  public long reset(EventSequence catalogTimeline) throws CatalogException {
    long startVersion = getCatalogVersion();
    LOG.info("Invalidating all metadata. Version: " + startVersion);
    // First update the policy metadata.
    refreshAuthorization(true);

    // Even though we get the current notification event id before stopping the event
    // processing here there is a small window of time where we could re-process some of
    // the event ids, if there is external DDL activity on metastore during reset.
    // Unfortunately, there is no good way to avoid this since HMS does not provide
    // APIs which can fetch all the tables/databases at a given id. It is OKAY to
    // re-process some of these events since event processor relies on creation eventId
    // to uniquely determine if the table was created/dropped by catalogd. In case of
    // alter events, however it is likely that some tables would be unnecessarily
    // refreshed. That would happen when during reset, there were external alter events
    // and by the time we processed them, catalog had already loaded them.
    long currentEventId;
    try {
      currentEventId = metastoreEventProcessor_.getCurrentEventId();
    } catch (MetastoreNotificationFetchException e) {
      throw new CatalogException("Failed to fetch current event id", e);
    }
    // pause the event processing since the cache is anyways being cleared
    metastoreEventProcessor_.pause();
    // Update the HDFS cache pools
    try {
      // We want only 'true' HDFS filesystems to poll the HDFS cache (i.e not S3,
      // local, etc.)
      if (FileSystemUtil.isDistributedFileSystem(FileSystemUtil.getDefaultFileSystem())) {
        CachePoolReader reader = new CachePoolReader(true);
        reader.run();
      }
    } catch (IOException e) {
      LOG.error("Couldn't identify the default FS. Cache Pool reader will be disabled.");
    }
    versionLock_.writeLock().lock();
    catalogTimeline.markEvent(GOT_CATALOG_VERSION_WRITE_LOCK);
    // In case of an empty new catalog, the version should still change to reflect the
    // reset operation itself and to unblock impalads by making the catalog version >
    // INITIAL_CATALOG_VERSION. See Frontend.waitForCatalog()
    ++catalogVersion_;

    // Update data source, db and table metadata
    try {
      // Refresh DataSource objects from HMS and assign new versions.
      refreshDataSources();

      // Not all Java UDFs are persisted to the metastore. The ones which aren't
      // should be restored once the catalog has been invalidated.
      Map<String, Db> oldDbCache = dbCache_.get();

      // Build a new DB cache, populate it, and replace the existing cache in one
      // step.
      Map<String, Db> newDbCache = new ConcurrentHashMap<String, Db>();
      List<TTableName> tblsToBackgroundLoad = new ArrayList<>();
      try (MetaStoreClient msClient = getMetaStoreClient(catalogTimeline)) {
        List<String> allDbs = msClient.getHiveClient().getAllDatabases();
        catalogTimeline.markEvent("Got database list");
        int numComplete = 0;
        for (String dbName: allDbs) {
          if (isBlacklistedDb(dbName)) {
            LOG.info("skip blacklisted db: " + dbName);
            continue;
          }
          String annotation = String.format("invalidating metadata - %s/%s dbs complete",
              numComplete++, allDbs.size());
          try (ThreadNameAnnotator tna = new ThreadNameAnnotator(annotation)) {
            dbName = dbName.toLowerCase();
            Db oldDb = oldDbCache.get(dbName);
            Pair<Db, List<TTableName>> invalidatedDb = invalidateDb(msClient,
                dbName, oldDb, catalogTimeline);
            if (invalidatedDb == null) continue;
            newDbCache.put(dbName, invalidatedDb.first);
            tblsToBackgroundLoad.addAll(invalidatedDb.second);
          }
        }
      }
      dbCache_.set(newDbCache);
      catalogTimeline.markEvent("Updated catalog cache");

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
      LOG.error("Error initializing Catalog", e);
      throw new CatalogException("Error initializing Catalog. Catalog may be empty.", e);
    } finally {
      // It's possible that concurrent reset() gets a startVersion later than us but
      // acquires the version lock before us so the lastResetStartVersion_ is already
      // bumped. Don't need to update it in this case.
      if (lastResetStartVersion_ < startVersion) lastResetStartVersion_ = startVersion;
      versionLock_.writeLock().unlock();
      // clear all txn to write ids mapping so that there is no memory leak for previous
      // events
      clearWriteIds();
      // restart the event processing for id just before the reset
      metastoreEventProcessor_.start(currentEventId);
    }
    LOG.info("Invalidated all metadata.");
    return startVersion;
  }

  public Db addDb(String dbName, org.apache.hadoop.hive.metastore.api.Database msDb) {
    return addDb(dbName, msDb, -1);
  }

  /**
   * Adds a database name to the metadata cache and returns the database's
   * new Db object. Used by CREATE DATABASE statements.
   */
  public Db addDb(String dbName, org.apache.hadoop.hive.metastore.api.Database msDb,
      long eventId) {
    Db newDb = new Db(dbName, msDb);
    versionLock_.writeLock().lock();
    try {
      newDb.setCatalogVersion(incrementAndGetCatalogVersion());
      newDb.setCreateEventId(eventId);
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

  public Table addIncompleteTable(String dbName, String tblName, TImpalaTableType tblType,
      String tblComment) {
    return addIncompleteTable(dbName, tblName, tblType, tblComment, -1L);
  }

  /**
   * Adds a table with the given name to the catalog and returns the new table.
   */
  public Table addIncompleteTable(String dbName, String tblName, TImpalaTableType tblType,
      String tblComment, long createEventId) {
    versionLock_.writeLock().lock();
    try {
      // IMPALA-9211: get db object after holding the writeLock in case of getting stale
      // db object due to concurrent INVALIDATE METADATA
      Db db = getDb(dbName);
      if (db == null) return null;
      Table existingTbl = db.getTable(tblName);
      if (existingTbl instanceof HdfsTable) {
        // Add the old instance to the deleteLog_ so we can send isDeleted updates for
        // its partitions.
        existingTbl.setCatalogVersion(incrementAndGetCatalogVersion());
        deleteLog_.addRemovedObject(existingTbl.toMinimalTCatalogObject());
      }
      Table incompleteTable = IncompleteTable.createUninitializedTable(
          db, tblName, tblType, tblComment);
      incompleteTable.setCatalogVersion(incrementAndGetCatalogVersion());
      incompleteTable.setCreateEventId(createEventId);
      db.addTable(incompleteTable);
      return db.getTable(tblName);
    } finally {
      versionLock_.writeLock().unlock();
    }
  }

  /**
   * Adds a table 'table' to the database 'db' and returns the table that was added.
   */
  public Table addTable(Db db, Table table) {
    versionLock_.writeLock().lock();
    try {
      Preconditions.checkNotNull(db).addTable(Preconditions.checkNotNull(table));
    } finally {
      versionLock_.writeLock().unlock();
    }
    return table;
  }

  public Table getOrLoadTable(String dbName, String tblName, String reason,
      ValidWriteIdList validWriteIdList)
      throws CatalogException {
    return getOrLoadTable(dbName, tblName, reason, validWriteIdList,
        TABLE_ID_UNAVAILABLE, NoOpEventSequence.INSTANCE);
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
  public Table getOrLoadTable(String dbName, String tblName, String reason,
      ValidWriteIdList validWriteIdList, long tableId, EventSequence catalogTimeline)
      throws CatalogException {
    TTableName tableName = new TTableName(dbName.toLowerCase(), tblName.toLowerCase());
    Table tbl;
    TableLoadingMgr.LoadRequest loadReq = null;
    List<HdfsPartition.Builder> partsToBeRefreshed = Collections.emptyList();

    long previousCatalogVersion = -1;
    // Return the table if it is already loaded or submit a new load request.
    acquireVersionReadLock(catalogTimeline);
    try {
      tbl = getTable(dbName, tblName);
      // tbl doesn't exist in the catalog
      if (tbl == null) return null;
      LOG.trace("table {} exits in cache, last synced id {}", tbl.getFullName(),
          tbl.getLastSyncedEventId());
      boolean isLoaded = tbl.isLoaded();
      if (isLoaded && tbl instanceof IncompleteTable
          && ((IncompleteTable) tbl).isLoadFailedByRecoverableError()) {
        // If the previous load of incomplete table had failed due to recoverable errors,
        // try loading again instead of returning the existing table
        isLoaded = false;
      }
      // if no validWriteIdList is provided, we return the tbl if its loaded
      // In the external front end use case it is possible that an external table might
      // have validWriteIdList, so we can simply ignore this value if table is external
      if (isLoaded
          && (validWriteIdList == null
                 || (!AcidUtils.isTransactionalTable(
                        tbl.getMetaStoreTable().getParameters())))) {
        incrementCatalogDCacheHitMetric(reason);
        LOG.trace("returning already loaded table {}", tbl.getFullName());
        return tbl;
      }
      // if a validWriteIdList is provided, we see if the cached table can provided a
      // consistent view of the given validWriteIdList. If yes, we can return the table
      // otherwise we reload the table. It is possible that the cached table is stale
      // even if the ValidWriteIdList matches (eg. out-of-band drop and recreate of
      // table) Hence we should make sure that we are comparing
      // the ValidWriteIdList only when the table id matches.
      if (tbl instanceof HdfsTable
          && AcidUtils.compare((HdfsTable) tbl, validWriteIdList, tableId) >= 0) {
        incrementCatalogDCacheHitMetric(reason);
        // Check if any partition of the table has a newly compacted file.
        // We just take the read lock here so that we don't serialize all the getTable
        // calls for the same table. If there are concurrent calls, it is possible we
        // refresh a partition for multiple times but that doesn't break the table's
        // consistency because we refresh the file metadata based on the same writeIdList
        Preconditions.checkState(
            AcidUtils.isTransactionalTable(tbl.getMetaStoreTable().getParameters()),
            "Compaction id check cannot be done for non-transactional table "
                + tbl.getFullName());
        readLock(tbl, catalogTimeline);
        try {
          partsToBeRefreshed =
              AcidUtils.getPartitionsForRefreshingFileMetadata(this, (HdfsTable) tbl);
        } finally {
          tbl.readLock().unlock();
        }
        // If all the partitions don't have a newly compacted file, return the table
        if (partsToBeRefreshed.isEmpty()) return tbl;
      } else {
        CatalogMonitor.INSTANCE.getCatalogdHmsCacheMetrics()
            .getCounter(CatalogHmsUtils.CATALOGD_CACHE_MISS_METRIC)
            .inc();
        // Update the cache stats for a HMS API from which the current method got invoked.
        if (HmsApiNameEnum.contains(reason)) {
          // Update the cache miss metric, as the valid write id list did not match and we
          // have to reload the table.
          CatalogMonitor.INSTANCE.getCatalogdHmsCacheMetrics()
              .getCounter(String.format(
                  CatalogHmsUtils.CATALOGD_CACHE_API_MISS_METRIC, reason))
              .inc();
        }
        previousCatalogVersion = tbl.getCatalogVersion();
        LOG.trace("Loading full table {}", tbl.getFullName());
        loadReq = tableLoadingMgr_.loadAsync(tableName, tbl.getCreateEventId(), reason,
            catalogTimeline);
      }
    } finally {
      versionLock_.readLock().unlock();
    }
    if (!partsToBeRefreshed.isEmpty()) {
      return refreshFileMetadata((HdfsTable) tbl, partsToBeRefreshed, catalogTimeline);
    }
    Preconditions.checkNotNull(loadReq);
    try {
      Table t = loadReq.get();
      catalogTimeline.markEvent("Async loaded table");
      // The table may have been dropped/modified while the load was in progress, so only
      // apply the update if the existing table hasn't changed.
      return replaceTableIfUnchanged(t, previousCatalogVersion, tableId);
    } finally {
      loadReq.close();
    }
  }

  /**
   * Increments catalogD's cache hit metrics
   * @param reason
   */
  private void incrementCatalogDCacheHitMetric(String reason) {
    CatalogMonitor.INSTANCE.getCatalogdHmsCacheMetrics()
        .getCounter(CatalogHmsUtils.CATALOGD_CACHE_HIT_METRIC)
        .inc();
    // Update the cache stats for a HMS API from which the current method got invoked.
    if (HmsApiNameEnum.contains(reason)) {
      CatalogMonitor.INSTANCE.getCatalogdHmsCacheMetrics()
          .getCounter(String
              .format(CatalogHmsUtils.CATALOGD_CACHE_API_HIT_METRIC, reason))
          .inc();
    }
  }

  /**
   * Replaces an existing Table with a new value if it exists and has not changed
   * (has the same catalog version as 'expectedCatalogVersion'). There is one exception
   * for transactional tables, we still replace the existing table if the updatedTbl has
   * more recent writeIdList than the existing table.
   */
  private Table replaceTableIfUnchanged(Table updatedTbl, long expectedCatalogVersion,
      long tableId) throws DatabaseNotFoundException {
    versionLock_.writeLock().lock();
    try {
      Db db = getDb(updatedTbl.getDb().getName());
      if (db == null) {
        throw new DatabaseNotFoundException(
            "Database does not exist: " + updatedTbl.getDb().getName());
      }

      Table existingTbl = db.getTable(updatedTbl.getName());
      // The existing table does not exist or has been modified. Instead of
      // adding the loaded value, return the existing table. For ACID tables,
      // we also compare the writeIdList with existing table's and return existing table
      // when its writeIdList is more recent. The check is needed in addition to the
      // catalogVersion check because it is possible that when table's files are
      // refreshed to account for compaction, the table's version number is higher but
      // ValidWriteIdList is still the same as before refresh. This could cause this
      // method to not update the table when the updatedTbl has a higher ValidWriteIdList
      // if we just rely on catalog version comparison which would break the logic to
      // reload on stale ValidWriteIdList logic.
      if (existingTbl == null
          || (existingTbl.getCatalogVersion() != expectedCatalogVersion
                 && (!(existingTbl instanceof HdfsTable)
                        || AcidUtils.compare((HdfsTable) existingTbl,
                               updatedTbl.getValidWriteIds(), tableId)
                            >= 0))) {
        LOG.trace("returning existing table {} with last synced id: ",
            existingTbl.getFullName(), existingTbl.getLastSyncedEventId());
        return existingTbl;
      }


      if (existingTbl instanceof HdfsTable) {
        // Add the old instance to the deleteLog_ so we can send isDeleted updates for
        // its stale partitions.
        existingTbl.setCatalogVersion(incrementAndGetCatalogVersion());
        deleteLog_.addRemovedObject(existingTbl.toMinimalTCatalogObject());
      }
      updatedTbl.setCatalogVersion(incrementAndGetCatalogVersion());
      // note that we update the db with a new instance of table. In such case
      // we may lose some temporary state stored in the old table like pendingVersion
      // This is okay since the table version is bumped up to the next topic update window
      // and eventually the table will need to added to the topic update if hits the
      // MAX_NUM_SKIPPED_TOPIC_UPDATES limit.
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
      if (removedTable != null && !removedTable.isStoredInImpaladCatalogCache()) {
        CatalogMonitor.INSTANCE.getCatalogTableMetrics().removeTable(removedTable);
      }
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
  public Pair<Table, Table> renameTable(
      TTableName oldTableName, TTableName newTableName) {
    // Remove the old table name from the cache and add the new table.
    Db db = getDb(oldTableName.getDb_name());
    if (db == null) return null;
    versionLock_.writeLock().lock();
    try {
      Table oldTable =
          removeTable(oldTableName.getDb_name(), oldTableName.getTable_name());
      if (oldTable == null) return Pair.create(null, null);
      return Pair.create(oldTable,
          addIncompleteTable(newTableName.getDb_name(), newTableName.getTable_name(),
              oldTable.getTableType(), oldTable.getTableComment(),
              oldTable.getCreateEventId()));
    } finally {
      versionLock_.writeLock().unlock();
    }
  }

  /**
   * Wrapper around {@link #reloadTable(Table, String, long, boolean, EventSequence)}
   * which passes false for {@code isSkipFileMetadataReload} argument
   * and ignore the result.
   */
  public void reloadTable(Table tbl, String reason, EventSequence catalogTimeline)
      throws CatalogException {
    reloadTable(tbl, reason, -1, false, catalogTimeline);
  }

  /**
   * Wrapper around {@link #reloadTable(Table, TResetMetadataRequest,
   * CatalogObject.ThriftObjectType, String, long, boolean, EventSequence)} which passes
   * an empty TResetMetadataRequest and NONE for {@code resultType} argument, ignores the
   * result.
   * eventId: HMS event id which triggered reload
   */
  public void reloadTable(Table tbl, String reason, long eventId,
      boolean isSkipFileMetadataReload, EventSequence catalogTimeline)
      throws CatalogException {
    reloadTable(tbl, new TResetMetadataRequest(), CatalogObject.ThriftObjectType.NONE,
        reason, eventId, isSkipFileMetadataReload, catalogTimeline);
  }

  /**
   * Wrapper around {@link #reloadTable(Table, TResetMetadataRequest,
   * CatalogObject.ThriftObjectType, String, long, boolean, EventSequence)} which passes
   * false for {@code isSkipFileMetadataReload} argument.
   * eventId: HMS event id which triggered reload
   */
  public TCatalogObject reloadTable(Table tbl, TResetMetadataRequest request,
      CatalogObject.ThriftObjectType resultType, String reason, long eventId,
      EventSequence catalogTimeline) throws CatalogException {
    return reloadTable(tbl, request, resultType, reason, eventId,
        /*isSkipFileMetadataReload*/false, catalogTimeline);
  }

  /**
   * Reloads metadata for table 'tbl' which must not be an IncompleteTable. Updates the
   * table metadata in-place by calling load() on the given table. Returns the
   * TCatalogObject representing 'tbl'. Applies proper synchronization to protect the
   * metadata load from concurrent table modifications and assigns a new catalog version.
   * Throws a CatalogException if there is an error loading table metadata.
   * If this reload is triggered while processing some HMS event (for example from
   * MetastoreEventProcessor), then eventId passed would be > -1. In that case
   * check table's last synced event id and if it is >= eventId then skip reloading
   * table. Otherwise, set the eventId as table's last synced id after reload is done.
   */
  public TCatalogObject reloadTable(Table tbl, TResetMetadataRequest request,
      CatalogObject.ThriftObjectType resultType, String reason, long eventId,
      boolean isSkipFileMetadataReload, EventSequence catalogTimeline)
      throws CatalogException {
    LOG.info(String.format("Refreshing table metadata: %s", tbl.getFullName()));
    Preconditions.checkState(!(tbl instanceof IncompleteTable));
    String dbName = tbl.getDb().getName();
    String tblName = tbl.getName();
    if (!tryWriteLock(tbl, catalogTimeline)) {
      throw new CatalogException(String.format("Error refreshing metadata for table " +
          "%s due to lock contention", tbl.getFullName()));
    }
    long newCatalogVersion = incrementAndGetCatalogVersion();
    versionLock_.writeLock().unlock();
    boolean syncToLatestEventId =
        BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls();
    final Timer.Context context =
        tbl.getMetrics().getTimer(Table.REFRESH_DURATION_METRIC).time();
    try {
      if (eventId != -1 && tbl.getLastSyncedEventId() != -1 &&
          tbl.getLastSyncedEventId() >= eventId) {
        LOG.info("Not reloading table for event id: {} since table is already synced "
            + "till event id: {}", eventId, tbl.getLastSyncedEventId());
        return tbl.toTCatalogObject(resultType);
      }
      long currentHmsEventId = -1;
      try (MetaStoreClient msClient = getMetaStoreClient(catalogTimeline)) {
        if (syncToLatestEventId) {
          try {
            currentHmsEventId = msClient.getHiveClient().getCurrentNotificationEventId()
                .getEventId();
            catalogTimeline.markEvent(FETCHED_LATEST_HMS_EVENT_ID + currentHmsEventId);
          } catch (TException e) {
            throw new CatalogException("Failed to reload table: " + tbl.getFullName() +
                " as there was an error in fetching current event id from HMS", e);
          }
        }
        org.apache.hadoop.hive.metastore.api.Table msTbl = null;
        try {
          msTbl = msClient.getHiveClient().getTable(dbName, tblName);
          catalogTimeline.markEvent(FETCHED_HMS_TABLE);
        } catch (Exception e) {
          throw new TableLoadingException("Error loading metadata for table: " +
              dbName + "." + tblName, e);
        }
        if (tbl instanceof HdfsTable) {
          ((HdfsTable) tbl)
              .load(true, msClient.getHiveClient(), msTbl, !isSkipFileMetadataReload,
                  /* loadTableSchema*/true, request.refresh_updated_hms_partitions,
                  /* partitionsToUpdate*/null, request.debug_action,
                  /*partitionToEventId*/null, reason, catalogTimeline);
        } else {
          tbl.load(true, msClient.getHiveClient(), msTbl, reason, catalogTimeline);
        }
        catalogTimeline.markEvent("Loaded table");
      }
      boolean isFullReloadOnTable = !isSkipFileMetadataReload;
      if (currentHmsEventId != -1 && syncToLatestEventId) {
        // fetch latest event id from HMS before starting table load and set that event
        // id as table's last synced id. It may happen that while the table was being
        // reloaded, more events were generated for the table and the table reload would
        // already have reflected those changes. In such scenario, we will again be
        // replaying those events next time the table is synced to the latest event id.
        // We are not replaying new events for the table in the end because certain
        // events like ALTER_TABLE in MetastoreEventProcessor call this method which
        // might trigger an endless loop cycle
        if (isFullReloadOnTable) {
          tbl.setLastSyncedEventId(currentHmsEventId);
        } else {
          tbl.setLastSyncedEventId(eventId);
        }
      }
      tbl.setCatalogVersion(newCatalogVersion);
      LOG.info(String.format("Refreshed table metadata: %s", tbl.getFullName()));
      // Set the last refresh event id as current HMS event id since all the metadata
      // until the current HMS event id is refreshed at this point.
      if (currentHmsEventId > eventId && isFullReloadOnTable) {
        tbl.setLastRefreshEventId(currentHmsEventId, isFullReloadOnTable);
      } else {
        tbl.setLastRefreshEventId(eventId, isFullReloadOnTable);
      }
      return tbl.toTCatalogObject(resultType);
    } finally {
      context.stop();
      UnlockWriteLockIfErroneouslyLocked();
      tbl.releaseWriteLock();
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
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
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

  public TCatalogObject invalidateTable(TTableName tableName,
      Reference<Boolean> tblWasRemoved, Reference<Boolean> dbWasAdded,
      EventSequence catalogTimeline) {
    return invalidateTable(tableName, tblWasRemoved, dbWasAdded, catalogTimeline, -1L);
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
      Reference<Boolean> tblWasRemoved, Reference<Boolean> dbWasAdded,
      EventSequence catalogTimeline, long eventId) {
    tblWasRemoved.setRef(false);
    dbWasAdded.setRef(false);
    String dbName = tableName.getDb_name();
    String tblName = tableName.getTable_name();
    if (isBlacklistedTable(dbName, tblName)) {
      LOG.info("Skip invalidating blacklisted table: " + tableName);
      return null;
    }
    LOG.info(String.format("Invalidating table metadata: %s.%s", dbName, tblName));

    // Stores whether the table exists in the metastore. Can have three states:
    // 1) Non empty - Table exists in metastore.
    // 2) Empty - Table does not exist in metastore.
    // 3) unknown (null) - There was exception thrown by the metastore client.
    List<TableMeta> metaRes = null;
    Db db = null;
    try (MetaStoreClient msClient = getMetaStoreClient(catalogTimeline)) {
      org.apache.hadoop.hive.metastore.api.Database msDb = null;
      try {
        metaRes = getTableMetaFromHive(msClient, dbName, tblName);
        catalogTimeline.markEvent(FETCHED_HMS_TABLE);
      } catch (UnknownDBException | NoSuchObjectException e) {
        // The parent database does not exist in the metastore. Treat this the same
        // as if the table does not exist.
        metaRes = Collections.emptyList();
      } catch (TException e) {
        LOG.error("Error executing tableExists() metastore call: " + tblName, e);
      }

      if (metaRes != null && metaRes.isEmpty()) {
        Table result = removeTable(dbName, tblName);
        catalogTimeline.markEvent("Removed table in catalog cache");
        if (result == null) return null;
        tblWasRemoved.setRef(true);
        result.takeReadLock();
        try {
          return result.toTCatalogObject();
        } finally {
          result.releaseReadLock();
        }
      }

      db = getDb(dbName);
      if ((db == null || !db.containsTable(tblName)) && metaRes == null) {
        // The table does not exist in our cache AND it is unknown whether the
        // table exists in the Metastore. Do nothing.
        return null;
      } else if (db == null && !metaRes.isEmpty()) {
        // The table exists in the Metastore, but our cache does not contain the parent
        // database. A new db will be added to the cache along with the new table. msDb
        // must be valid since tableExistsInMetaStore is true.
        try {
          msDb = msClient.getHiveClient().getDatabase(dbName);
          catalogTimeline.markEvent(FETCHED_HMS_DB);
          Preconditions.checkNotNull(msDb);
          addDb(dbName, msDb);
          catalogTimeline.markEvent("Added database in catalog cache");
          dbWasAdded.setRef(true);
        } catch (TException e) {
          // The Metastore database cannot be get. Log the error and return.
          LOG.error("Error executing getDatabase() metastore call: " + dbName, e);
          return null;
        }
      }
    }
    Preconditions.checkState(metaRes != null);
    Preconditions.checkState(metaRes.size() == 1);
    TableMeta tblMeta = metaRes.get(0);

    // Add a new uninitialized table to the table cache, effectively invalidating
    // any existing entry. The metadata for the table will be loaded lazily, on the
    // on the next access to the table.
    Table newTable = addIncompleteTable(dbName, tblName,
        MetastoreShim.mapToInternalTableType(tblMeta.getTableType()),
        tblMeta.getComments(), eventId);
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
   * Invalidate the table if it exists by overwriting existing entry by a Incomplete
   * Table.
   * @return null if the table does not exist else return the invalidated table
   */
  public Table invalidateTableIfExists(String dbName, String tblName) {
    Table incompleteTable;
    versionLock_.writeLock().lock();
    try {
      Db db = getDb(dbName);
      if (db == null) return null;
      Table existingTbl = db.getTable(tblName);
      if (existingTbl == null) return null;
      incompleteTable = IncompleteTable.createUninitializedTable(db, tblName,
          existingTbl.getTableType(), existingTbl.getTableComment());
      incompleteTable.setCatalogVersion(incrementAndGetCatalogVersion());
      incompleteTable.setCreateEventId(existingTbl.getCreateEventId());
      db.addTable(incompleteTable);
    } finally {
      versionLock_.writeLock().unlock();
    }
    if (loadInBackground_) {
      tableLoadingMgr_.backgroundLoad(
          new TTableName(dbName.toLowerCase(), tblName.toLowerCase()));
    }
    return incompleteTable;
  }

  /**
   * Refresh table if exists. Returns true if reloadTable() succeeds, false
   * otherwise.
   */
  public boolean reloadTableIfExists(String dbName, String tblName, String reason,
      long eventId, boolean isSkipFileMetadataReload) throws CatalogException {
    try {
      Table table = getTable(dbName, tblName);
      if (table == null || table instanceof IncompleteTable) return false;
      if (eventId > 0 && eventId <= table.getCreateEventId()) {
        LOG.debug("Not reloading the table {}.{} for event {} since it is recreated at "
            + "event {}.", dbName, tblName, eventId, table.getCreateEventId());
        return false;
      }
      reloadTable(table, reason, eventId, isSkipFileMetadataReload,
          NoOpEventSequence.INSTANCE);
    } catch (DatabaseNotFoundException | TableLoadingException e) {
      LOG.info(String.format("Reload table if exists failed with: %s", e.getMessage()));
      return false;
    }
    return true;
  }

  /**
   * Update DB if it exists in catalog. Returns true if updateDb() succeeds, false
   * otherwise.
   */
  public boolean updateDbIfExists(Database msdb) {
    try {
      updateDb(msdb);
    } catch (DatabaseNotFoundException e) {
      return false;
    }
    return true;
  }

  /**
   * Adds a new role with the given name and grant groups to the AuthorizationPolicy.
   * If a role with the same name already exists it will be overwritten.
   */
  public Role addRole(String roleName, Set<String> grantGroups) {
    Principal role = addPrincipal(roleName, grantGroups, TPrincipalType.ROLE);
    Preconditions.checkState(role instanceof Role);
    return (Role) role;
  }

  /**
   * Adds a new user with the given name to the AuthorizationPolicy.
   * If a user with the same name already exists it will be overwritten.
   */
  public User addUser(String userName) {
    Principal user = addPrincipal(userName, new HashSet<>(),
        TPrincipalType.USER);
    Preconditions.checkState(user instanceof User);
    return (User) user;
  }

  /**
   * Add a user to the catalog if it doesn't exist. This is necessary so privileges
   * can be added for a user. example: owner privileges.
   */
  public User addUserIfNotExists(String owner, Reference<Boolean> existingUser) {
    versionLock_.writeLock().lock();
    try {
      User user = getAuthPolicy().getUser(owner);
      existingUser.setRef(Boolean.TRUE);
      if (user == null) {
        user = addUser(owner);
        existingUser.setRef(Boolean.FALSE);
      }
      return user;
    } finally {
      versionLock_.writeLock().unlock();
    }
  }

  private Principal addPrincipal(String principalName, Set<String> grantGroups,
      TPrincipalType type) {
    versionLock_.writeLock().lock();
    try {
      Principal principal = Principal.newInstance(principalName, type, grantGroups);
      principal.setCatalogVersion(incrementAndGetCatalogVersion());
      authPolicy_.addPrincipal(principal);
      return principal;
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
    Principal role = removePrincipal(roleName, TPrincipalType.ROLE);
    if (role == null) return null;
    Preconditions.checkState(role instanceof Role);
    return (Role) role;
  }

  /**
   * Removes the user with the given name from the AuthorizationPolicy. Returns the
   * removed user with an incremented catalog version, or null if no user with this name
   * exists.
   */
  public User removeUser(String userName) {
    Principal user = removePrincipal(userName, TPrincipalType.USER);
    if (user == null) return null;
    Preconditions.checkState(user instanceof User);
    return (User) user;
  }

  private Principal removePrincipal(String principalName, TPrincipalType type) {
    versionLock_.writeLock().lock();
    try {
      Principal principal = authPolicy_.removePrincipal(principalName, type);
      // TODO(todd): does this end up leaking the privileges associated
      // with this principal into the CatalogObjectVersionSet on the catalogd?
      if (principal == null) return null;
      for (PrincipalPrivilege priv: principal.getPrivileges()) {
        priv.setCatalogVersion(incrementAndGetCatalogVersion());
        deleteLog_.addRemovedObject(priv.toTCatalogObject());
      }
      principal.setCatalogVersion(incrementAndGetCatalogVersion());
      deleteLog_.addRemovedObject(principal.toTCatalogObject());
      return principal;
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
      Role role = authPolicy_.addRoleGrantGroup(roleName, groupName);
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
      Role role = authPolicy_.removeRoleGrantGroup(roleName, groupName);
      Preconditions.checkNotNull(role);
      role.setCatalogVersion(incrementAndGetCatalogVersion());
      return role;
    } finally {
      versionLock_.writeLock().unlock();
    }
  }

  /**
   * Adds a privilege to the given role name. Returns the new PrincipalPrivilege and
   * increments the catalog version. If the parent role does not exist a CatalogException
   * is thrown.
   */
  public PrincipalPrivilege addRolePrivilege(String roleName, TPrivilege thriftPriv)
      throws CatalogException {
    Preconditions.checkArgument(thriftPriv.getPrincipal_type() == TPrincipalType.ROLE);
    return addPrincipalPrivilege(roleName, thriftPriv, TPrincipalType.ROLE);
  }

  /**
   * Adds a privilege to the given user name. Returns the new PrincipalPrivilege and
   * increments the catalog version. If the user does not exist a CatalogException is
   * thrown.
   */
  public PrincipalPrivilege addUserPrivilege(String userName, TPrivilege thriftPriv)
      throws CatalogException {
    Preconditions.checkArgument(thriftPriv.getPrincipal_type() == TPrincipalType.USER);
    return addPrincipalPrivilege(userName, thriftPriv, TPrincipalType.USER);
  }

  private PrincipalPrivilege addPrincipalPrivilege(String principalName,
      TPrivilege thriftPriv, TPrincipalType type) throws CatalogException {
    versionLock_.writeLock().lock();
    try {
      Principal principal = authPolicy_.getPrincipal(principalName, type);
      if (principal == null) {
        throw new CatalogException(String.format("%s does not exist: %s",
            Principal.toString(type), principalName));
      }
      PrincipalPrivilege priv = PrincipalPrivilege.fromThrift(thriftPriv);
      priv.setCatalogVersion(incrementAndGetCatalogVersion());
      authPolicy_.addPrivilege(priv);
      return priv;
    } finally {
      versionLock_.writeLock().unlock();
    }
  }

  /**
   * Removes a PrincipalPrivilege from the given role name and privilege name. Returns
   * the removed PrincipalPrivilege with an incremented catalog version or null if no
   * matching privilege was found. Throws a CatalogException if no role exists with this
   * name.
   */
  public PrincipalPrivilege removeRolePrivilege(String roleName, String privilegeName)
      throws CatalogException {
    return removePrincipalPrivilege(roleName, privilegeName, TPrincipalType.ROLE);
  }

  /**
   * Removes a PrincipalPrivilege from the given user name and privilege name. Returns
   * the removed PrincipalPrivilege with an incremented catalog version or null if no
   * matching privilege was found. Throws a CatalogException if no user exists with this
   * name.
   */
  public PrincipalPrivilege removeUserPrivilege(String userName, String privilegeName)
      throws CatalogException {
    return removePrincipalPrivilege(userName, privilegeName, TPrincipalType.USER);
  }

  private PrincipalPrivilege removePrincipalPrivilege(String principalName,
      String privilegeName, TPrincipalType type) throws CatalogException {
    versionLock_.writeLock().lock();
    try {
      Principal principal = authPolicy_.getPrincipal(principalName, type);
      if (principal == null) {
        throw new CatalogException(String.format("%s does not exist: %s",
            Principal.toString(type), principalName));
      }
      PrincipalPrivilege principalPrivilege = principal.removePrivilege(privilegeName);
      if (principalPrivilege == null) return null;
      principalPrivilege.setCatalogVersion(incrementAndGetCatalogVersion());
      deleteLog_.addRemovedObject(principalPrivilege.toTCatalogObject());
      return principalPrivilege;
    } finally {
      versionLock_.writeLock().unlock();
    }
  }

  /**
   * Gets a PrincipalPrivilege from the given principal name. Returns the privilege
   * if it exists, or null if no privilege matching the privilege spec exist.
   * Throws a CatalogException if the principal does not exist.
   */
  public PrincipalPrivilege getPrincipalPrivilege(String principalName,
      TPrivilege privSpec) throws CatalogException {
    String privilegeName = PrincipalPrivilege.buildPrivilegeName(privSpec);
    versionLock_.readLock().lock();
    try {
      Principal principal = authPolicy_.getPrincipal(principalName,
          privSpec.getPrincipal_type());
      if (principal == null) {
        throw new CatalogException(Principal.toString(privSpec.getPrincipal_type()) +
            " does not exist: " + principalName);
      }
      return principal.getPrivilege(privilegeName);
    } finally {
      versionLock_.readLock().unlock();
    }
  }

  @Override
  public AuthzCacheInvalidation getAuthzCacheInvalidation(String markerName) {
    versionLock_.readLock().lock();
    try {
      return authzCacheInvalidation_.get(markerName);
    } finally {
      versionLock_.readLock().unlock();;
    }
  }

  /**
   * Gets the {@link AuthzCacheInvalidation} for a given marker name or creates a new
   * {@link AuthzCacheInvalidation} if it does not exist and increment the catalog
   * version of {@link AuthzCacheInvalidation}. A catalog version update indicates a
   * an authorization cache invalidation notification.
   *
   * @param markerName the authorization cache invalidation marker name
   * @return the updated {@link AuthzCacheInvalidation} instance
   */
  public AuthzCacheInvalidation incrementAuthzCacheInvalidationVersion(
      String markerName) {
    versionLock_.writeLock().lock();
    try {
      AuthzCacheInvalidation authzCacheInvalidation = getAuthzCacheInvalidation(
          markerName);
      if (authzCacheInvalidation == null) {
        authzCacheInvalidation = new AuthzCacheInvalidation(markerName);
        authzCacheInvalidation_.add(authzCacheInvalidation);
      }
      authzCacheInvalidation.setCatalogVersion(incrementAndGetCatalogVersion());
      return authzCacheInvalidation;
    } finally {
      versionLock_.writeLock().unlock();
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

  private void acquireVersionReadLock(EventSequence catalogTimeline) {
    versionLock_.readLock().lock();
    catalogTimeline.markEvent(GOT_CATALOG_VERSION_READ_LOCK);
  }

  public ReentrantReadWriteLock getLock() { return versionLock_; }
  public AuthorizationPolicy getAuthPolicy() { return authPolicy_; }

  /**
   * Reloads metadata for the partition defined by the partition spec
   * 'partitionSpec' in table 'tbl'. Returns the resulting table's TCatalogObject after
   * the partition metadata was reloaded.
   */
  public TCatalogObject reloadPartition(Table tbl, List<TPartitionKeyValue> partitionSpec,
      Reference<Boolean> wasPartitionReloaded, CatalogObject.ThriftObjectType resultType,
      String reason, EventSequence catalogTimeline) throws CatalogException {
    if (!tryWriteLock(tbl, catalogTimeline)) {
      throw new CatalogException(String.format("Error reloading partition of table %s " +
          "due to lock contention", tbl.getFullName()));
    }
    try {
      long newCatalogVersion = incrementAndGetCatalogVersion();
      versionLock_.writeLock().unlock();
      HdfsTable hdfsTable = (HdfsTable) tbl;
      wasPartitionReloaded.setRef(false);
      HdfsPartition hdfsPartition = hdfsTable
          .getPartitionFromThriftPartitionSpec(partitionSpec);
      // Retrieve partition name from existing partition or construct it from
      // the partition spec
      String partitionName = hdfsPartition == null
          ? HdfsTable.constructPartitionName(partitionSpec)
          : hdfsPartition.getPartitionName();
      return reloadHdfsPartition(hdfsTable, partitionName, wasPartitionReloaded,
          resultType, reason, newCatalogVersion, hdfsPartition, catalogTimeline);
    } finally {
      UnlockWriteLockIfErroneouslyLocked();
      tbl.releaseWriteLock();
    }
  }

  /**
   * Reloads the HdfsPartition identified by the partitionName and returns Table's
   * TCatalogObject. The returned TCatalogObject is populated based on the value of
   * resultType. This method expects that the table lock has been taken already by the
   * caller. If the partition does not exist in HMS and hdfsPartition is not null, this
   * method will remove the HdfsPartition from the table.
   */
  public TCatalogObject reloadHdfsPartition(HdfsTable hdfsTable, String partitionName,
      Reference<Boolean> wasPartitionReloaded, CatalogObject.ThriftObjectType resultType,
      String reason, long newCatalogVersion, @Nullable HdfsPartition hdfsPartition,
      EventSequence catalogTimeline) throws CatalogException {
    Preconditions.checkState(hdfsTable.isWriteLockedByCurrentThread());
    LOG.info(String.format("Refreshing partition metadata: %s %s (%s)",
        hdfsTable.getFullName(), partitionName, reason));
    try (MetaStoreClient msClient = getMetaStoreClient(catalogTimeline)) {
      org.apache.hadoop.hive.metastore.api.Partition hmsPartition = null;
      try {
        hmsPartition = msClient.getHiveClient().getPartition(
            hdfsTable.getDb().getName(), hdfsTable.getName(), partitionName);
        catalogTimeline.markEvent(FETCHED_HMS_PARTITION);
      } catch (NoSuchObjectException e) {
        // If partition does not exist in Hive Metastore, remove it from the
        // catalog
        if (hdfsPartition != null) {
          hdfsTable.dropPartition(hdfsPartition);
          hdfsTable.setCatalogVersion(newCatalogVersion);
          // non-existing partition was dropped from catalog, so we mark it as refreshed
          wasPartitionReloaded.setRef(true);
        } else {
          LOG.info(String.format("Partition metadata for %s was not refreshed since "
                  + "it does not exist in metastore anymore",
              hdfsTable.getFullName() + " " + partitionName));
        }
        return hdfsTable.toTCatalogObject(resultType);
      } catch (Exception e) {
        throw new CatalogException("Error loading metadata for partition: "
            + hdfsTable.getFullName() + " " + partitionName, e);
      }
      Map<Partition, HdfsPartition> hmsPartToHdfsPart = new HashMap<>();
      // note that hdfsPartition can be null here which is a valid input argument
      // in such a case a new hdfsPartition is added and nothing is removed.
      hmsPartToHdfsPart.put(hmsPartition, hdfsPartition);
      hdfsTable.reloadPartitions(msClient.getHiveClient(), hmsPartToHdfsPart, true,
          catalogTimeline);
    }
    hdfsTable.setCatalogVersion(newCatalogVersion);
    wasPartitionReloaded.setRef(true);
    LOG.info(String.format("Refreshed partition metadata: %s %s",
        hdfsTable.getFullName(), partitionName));
    return hdfsTable.toTCatalogObject(resultType);
  }

  public CatalogDeltaLog getDeleteLog() { return deleteLog_; }

  /**
   * Returns the catalog version of the topic update that an operation using SYNC_DDL
   * must wait for in order to ensure that its result set ('result') has been broadcast
   * to all the coordinators. For operations that don't produce a result set,
   * e.g. INVALIDATE METADATA, return the version specified in 'result.version'.
   */
  public long waitForSyncDdlVersion(TCatalogUpdateResult result) throws CatalogException {
    if (result.getVersion() <= topicUpdateLog_.getOldestTopicUpdateToGc()
        || (!result.isSetUpdated_catalog_objects() &&
        !result.isSetRemoved_catalog_objects())) {
      return result.getVersion();
    }
    long lastSentTopicUpdate = lastSentTopicUpdate_.get();
    // Maximum number of attempts (topic updates) to find the catalog version that
    // an operation using SYNC_DDL must wait for.
    // this maximum attempt limit is only applicable when topicUpdateTblLockMaxWaitTimeMs_
    // is disabled (set to 0). This is due to the fact that the topic update thread will
    // not block until it add the table to the topics when this configuration set to a
    // non-zero value. In such a case we don't know how many topic updates would be
    // required before the required table version is added to the topics.
    long maxNumAttempts = 5;
    if (result.isSetUpdated_catalog_objects()) {
      maxNumAttempts = Math.max(maxNumAttempts,
          result.getUpdated_catalog_objects().size() *
              (MAX_NUM_SKIPPED_TOPIC_UPDATES + 1));
    }
    long numAttempts = 0;
    long begin = System.currentTimeMillis();
    long versionToWaitFor = -1;
    TUniqueId serviceId = JniCatalog.getServiceId();
    while (versionToWaitFor == -1) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("waitForSyncDdlVersion() attempt: " + numAttempts);
      }
      if (BackendConfig.INSTANCE.isCatalogdHAEnabled()) {
        // Catalog serviceId is changed when the HA role of the catalog instance is
        // changed from active to standby, or from standby to active. Inactive catalogd
        // does not receive catalog topic updates from the statestore. To avoid waiting
        // indefinitely, throw exception if its service id has been changed.
        if (!Strings.isNullOrEmpty(BackendConfig.INSTANCE.debugActions())) {
          DebugUtils.executeDebugAction(
              BackendConfig.INSTANCE.debugActions(), DebugUtils.WAIT_SYNC_DDL_VER_DELAY);
        }
        if (!serviceId.equals(JniCatalog.getServiceId())) {
          String errorMsg = "Couldn't retrieve the catalog topic update for the " +
              "SYNC_DDL operation since HA role of this catalog instance has been " +
              "changed. The operation has been successfully executed but its effects " +
              "may have not been broadcast to all the coordinators.";
          LOG.error(errorMsg);
          throw new CatalogException(errorMsg);
        }
      }
      // Examine the topic update log to determine the latest topic update that
      // covers the added/modified/deleted objects in 'result'.
      long topicVersionForUpdates =
          getCoveringTopicUpdateVersion(result.getUpdated_catalog_objects());
      long topicVersionForDeletes =
          getCoveringTopicUpdateVersion(result.getRemoved_catalog_objects());
      if (topicVersionForUpdates == -1 || topicVersionForDeletes == -1) {
        LOG.info("Topic update for {} not found yet. Last sent catalog version: {}. " +
                "Updated objects: {}, deleted objects: {}",
            topicVersionForUpdates == -1 ? "updates" : "deletes",
            lastSentTopicUpdate_.get(),
            FeCatalogUtils.debugString(result.updated_catalog_objects),
            FeCatalogUtils.debugString(result.removed_catalog_objects));
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
          if (shouldTimeOut(numAttempts, maxNumAttempts, begin)) {
            LOG.error(String.format("Couldn't retrieve the covering topic update for "
                    + "catalog objects. Updated objects: %s, deleted objects: %s",
                FeCatalogUtils.debugString(result.updated_catalog_objects),
                FeCatalogUtils.debugString(result.removed_catalog_objects)));
            throw new CatalogException("Couldn't retrieve the catalog topic update " +
                "for the SYNC_DDL operation after " + numAttempts + " attempts and " +
                "elapsed time of " + (System.currentTimeMillis() - begin) + " msec. " +
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
    LOG.info("Operation using SYNC_DDL is waiting for catalog version {} " +
            "to be sent. Time to identify topic update (msec): {}.",
        versionToWaitFor, (System.currentTimeMillis() - begin));
    return versionToWaitFor;
  }

  /**
   * This util method determines if the sync ddl operation which is waiting for the
   * topic update to be available should timeout or not based on given number of attempts
   * and startTime. If {@code max_wait_time_for_sync_ddl_s} flag is set, it
   * checks for the time elapsed since the startTime otherwise checks if the numAttempts
   * is greater than maxAttempts.
   * @return true if the operation should timeout else false if it needs to wait more.
   */
  private boolean shouldTimeOut(long numAttempts, long maxNumAttempts, long startTime) {
    int timeoutSecs = BackendConfig.INSTANCE.getMaxWaitTimeForSyncDdlSecs();
    if (topicUpdateTblLockMaxWaitTimeMs_ > 0) {
      if (timeoutSecs <= 0) return false;
      return (System.currentTimeMillis() - startTime) > timeoutSecs * 1000L;
    } else {
      return numAttempts > maxNumAttempts;
    }
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
      String key = Catalog.toCatalogObjectKey(tCatalogObject);
      TopicUpdateLog.Entry topicUpdateEntry = topicUpdateLog_.get(key);
      // There are two reasons for which a topic update log entry cannot be found:
      // a) It corresponds to a new catalog object that hasn't been processed by a catalog
      // update yet.
      // b) It corresponds to a catalog object that hasn't been modified for at least
      // topicUpdateLogGcFrequency_ updates and hence its entry was garbage
      // collected.
      // In both cases, -1 is returned to indicate that we're waiting for the
      // entry to show up in the topic update log.
      if (topicUpdateEntry == null) return -1;
      if (topicUpdateEntry.getLastSentVersion() < tCatalogObject.getCatalog_version()) {
        // TODO: This may be too verbose. Remove this after IMPALA-9135 is fixed.
        LOG.info("Should wait for next update for {}: older version {} is sent. " +
                "Expects a version >= {}.", key,
            topicUpdateEntry.getLastSentVersion(), tCatalogObject.getCatalog_version());
        return -1;
      }
      versionToWaitFor =
          Math.max(versionToWaitFor, topicUpdateEntry.getLastSentCatalogUpdate());
    }
    return versionToWaitFor;
  }

  /**
   * Retrieves information about the current catalog usage including:
   * 1. the tables with the most frequently accessed.
   * 2. the tables with the highest memory requirements.
   * 3. the tables with the highest file counts.
   * 4. the tables with the longest table loading time.
   */
  public TGetCatalogUsageResponse getCatalogUsage() {
    TGetCatalogUsageResponse usage = new TGetCatalogUsageResponse();
    CatalogTableMetrics catalogTableMetrics =
        CatalogMonitor.INSTANCE.getCatalogTableMetrics();
    usage.setLarge_tables(new ArrayList<>());
    usage.setFrequently_accessed_tables(new ArrayList<>());
    usage.setHigh_file_count_tables(new ArrayList<>());
    usage.setLong_metadata_loading_tables(new ArrayList<>());
    for (Pair<TTableName, Long> largeTable : catalogTableMetrics.getLargestTables()) {
      TTableUsageMetrics tableUsageMetrics =
          new TTableUsageMetrics(largeTable.getFirst());
      tableUsageMetrics.setMemory_estimate_bytes(largeTable.getSecond());
      usage.addToLarge_tables(tableUsageMetrics);
    }
    for (Pair<TTableName, Long> frequentTable :
            catalogTableMetrics.getFrequentlyAccessedTables()) {
      TTableUsageMetrics tableUsageMetrics =
          new TTableUsageMetrics(frequentTable.getFirst());
      tableUsageMetrics.setNum_metadata_operations(frequentTable.getSecond());
      usage.addToFrequently_accessed_tables(tableUsageMetrics);
    }
    for (Pair<TTableName, Long> mostFilesTable :
            catalogTableMetrics.getHighFileCountTables()) {
      TTableUsageMetrics tableUsageMetrics =
          new TTableUsageMetrics(mostFilesTable.getFirst());
      tableUsageMetrics.setNum_files(mostFilesTable.getSecond());
      usage.addToHigh_file_count_tables(tableUsageMetrics);
    }
    for (Pair<TTableName, TableLoadingTimeHistogram> longestLoadingTable :
            catalogTableMetrics.getLongMetadataLoadingTables()) {
      TTableUsageMetrics tableUsageMetrics =
          new TTableUsageMetrics(longestLoadingTable.getFirst());
      tableUsageMetrics.setMedian_table_loading_ns(
          longestLoadingTable.getSecond().getQuantile(P50));
      tableUsageMetrics.setMax_table_loading_ns(
          longestLoadingTable.getSecond().getQuantile(P100));
      tableUsageMetrics.setP75_loading_time_ns(
          longestLoadingTable.getSecond().getQuantile(P75));
      tableUsageMetrics.setP95_loading_time_ns(
          longestLoadingTable.getSecond().getQuantile(P95));
      tableUsageMetrics.setP99_loading_time_ns(
          longestLoadingTable.getSecond().getQuantile(P99));
      tableUsageMetrics.setNum_table_loading(
          longestLoadingTable.getSecond().getCount());
      usage.addToLong_metadata_loading_tables(tableUsageMetrics);
    }
    return usage;
  }

  /**
   * Retrieves information about the current catalog on-going operations.
   */
  public TGetOperationUsageResponse getOperationUsage() {
    return CatalogMonitor.INSTANCE.getCatalogOperationTracker().getOperationMetrics();
  }

  /**
   * Gets the events processor metrics. Used for publishing metrics on the webUI
   */
  public TEventProcessorMetrics getEventProcessorMetrics() {
    return metastoreEventProcessor_.getEventProcessorMetrics();
  }

  /**
   * Gets the Catalogd HMS cache metrics. Used for publishing metrics on the webUI.
   */
  public TCatalogdHmsCacheMetrics getCatalogdHmsCacheMetrics() {
    return catalogMetastoreServer_.getCatalogdHmsCacheMetrics();
  }

  /**
   * Gets the events processor summary. Used for populating the contents of the events
   * processor detailed view page
   */
  public TEventProcessorMetricsSummaryResponse getEventProcessorSummary() {
    return metastoreEventProcessor_.getEventProcessorSummary();
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
    if (!tbl.tryReadLock()) {
      result = "Metrics for table " + dbName + "." + tblName + "are not available " +
          "because the table is currently modified by another operation.";
      return result;
    }
    try {
      return tbl.getMetrics().toString();
    } finally {
      tbl.releaseReadLock();
    }
  }

  /**
   * A wrapper around doGetPartialCatalogObject() that controls the number of concurrent
   * invocations.
   */
  public TGetPartialCatalogObjectResponse getPartialCatalogObject(
      TGetPartialCatalogObjectRequest req) throws CatalogException {
    return getPartialCatalogObject(req, "needed by coordinator");
  }

  /**
   * A wrapper around doGetPartialCatalogObject() that controls the number of concurrent
   * invocations.
   */
  public TGetPartialCatalogObjectResponse getPartialCatalogObject(
      TGetPartialCatalogObjectRequest req, String reason) throws CatalogException {
    Preconditions.checkNotNull(reason);
    try {
      if (!partialObjectFetchAccess_.tryAcquire(1,
          PARTIAL_FETCH_RPC_QUEUE_TIMEOUT_S, TimeUnit.SECONDS)) {
        // Timed out trying to acquire the semaphore permit.
        throw new CatalogException("Timed out while fetching partial object metadata. " +
            "Please check the metric 'catalog.partial-fetch-rpc.queue-len' for the " +
            "current queue length and consider increasing " +
            "'catalog_partial_fetch_rpc_queue_timeout_s' and/or " +
            "'catalog_max_parallel_partial_fetch_rpc'");
      }
      // Acquired the permit at this point, should be released before we exit out of
      // this method.
      //
      // Is there a chance that this thread can get interrupted at this point before it
      // enters the try block, eventually leading to the semaphore permit not
      // getting released? It can probably happen if the JVM is already in a bad shape.
      // In the worst case, every permit is blocked and the subsequent requests throw
      // the timeout exception and the user can monitor the queue metric to see that it
      // is full, so the issue should be easy to diagnose.
      // TODO: Figure out if such a race is possible.
      try (ThreadNameAnnotator tna = new ThreadNameAnnotator(
            "Get Partial Catalog Object - " +
            Catalog.toCatalogObjectKey(req.object_desc))) {
        return doGetPartialCatalogObject(req, reason);
      } finally {
        partialObjectFetchAccess_.release();
      }
    } catch (InterruptedException e) {
      throw new CatalogException("Error running getPartialCatalogObject(): ", e);
    }
  }

  @Override
  public String getAcidUserId() {
    return String.format("CatalogD %s", getCatalogServiceId());
  }

  /**
   * Gets the id for this catalog service
   */
  public String getCatalogServiceId() {
    return TUniqueIdUtil.PrintId(JniCatalog.getServiceId()).intern();
  }

  /**
   * Returns the number of currently running partial RPCs.
   */
  @VisibleForTesting
  public int getConcurrentPartialRpcReqCount() {
    // Calculated based on number of currently available semaphore permits.
    return MAX_PARALLEL_PARTIAL_FETCH_RPC_COUNT - partialObjectFetchAccess_
        .availablePermits();
  }

  /**
   * Return a partial view of information about a given catalog object. This services
   * the CatalogdMetaProvider running on impalads when they are configured in
   * "local-catalog" mode. If required objects are not present, for example, the database
   * from which a table is requested, the types of the missing objects will be set in the
   * response's lookup_status.
   */
  private TGetPartialCatalogObjectResponse doGetPartialCatalogObject(
      TGetPartialCatalogObjectRequest req, String tableLoadReason)
      throws CatalogException {
    TCatalogObject objectDesc = Preconditions.checkNotNull(req.object_desc,
        "missing object_desc");
    switch (objectDesc.type) {
    case CATALOG:
      return getPartialCatalogInfo(req);
    case DATABASE:
      TDatabase dbDesc = Preconditions.checkNotNull(req.object_desc.db);
      versionLock_.readLock().lock();
      try {
        Db db = getDb(dbDesc.getDb_name());
        if (db == null) {
          return createGetPartialCatalogObjectError(req,
              CatalogLookupStatus.DB_NOT_FOUND);
        }

        return db.getPartialInfo(req);
      } finally {
        versionLock_.readLock().unlock();
      }
    case TABLE:
    case VIEW: {
      Table table;
      ValidWriteIdList writeIdList = null;
      try {
        long tableId = TABLE_ID_UNAVAILABLE;
        if (req.table_info_selector.valid_write_ids != null) {
          Preconditions.checkState(objectDesc.type.equals(TABLE));
          String dbName = objectDesc.getTable().db_name == null ? Catalog.DEFAULT_DB
            : objectDesc.getTable().db_name;
          String tblName = objectDesc.getTable().tbl_name;
          writeIdList = MetastoreShim.getValidWriteIdListFromThrift(
              dbName + "." + tblName, req.table_info_selector.valid_write_ids);
          tableId = req.table_info_selector.getTable_id();
        }
        table = getOrLoadTable(
            objectDesc.getTable().getDb_name(), objectDesc.getTable().getTbl_name(),
            tableLoadReason, writeIdList, tableId, NoOpEventSequence.INSTANCE);
      } catch (DatabaseNotFoundException e) {
        return createGetPartialCatalogObjectError(req, CatalogLookupStatus.DB_NOT_FOUND);
      }
      if (table == null) {
        return createGetPartialCatalogObjectError(req,
            CatalogLookupStatus.TABLE_NOT_FOUND);
      } else if (!table.isLoaded()) {
        // Table can still remain in an incomplete state if there was a concurrent
        // invalidate request.
        return createGetPartialCatalogObjectError(req,
            CatalogLookupStatus.TABLE_NOT_LOADED);
      }
      Map<HdfsPartition, TPartialPartitionInfo> missingPartialInfos;
      TGetPartialCatalogObjectResponse resp;
      table.takeReadLock();
      try {
        if (table instanceof HdfsTable) {
          HdfsTable hdfsTable = (HdfsTable)table;
          missingPartialInfos = Maps.newHashMap();
          resp = hdfsTable.getPartialInfo(req, missingPartialInfos);
          if (missingPartialInfos.isEmpty()) return resp;
          // there were some partialPartitionInfos which don't have file-descriptors
          // for the requested writeIdList
          setFileMetadataFromFS(hdfsTable, writeIdList, missingPartialInfos);
          return resp;
        } else {
          return table.getPartialInfo(req);
        }
      } finally {
        table.releaseReadLock();
      }
    }
    case FUNCTION: {
      versionLock_.readLock().lock();
      try {
        Db db = getDb(objectDesc.fn.name.db_name);
        if (db == null) {
          return createGetPartialCatalogObjectError(req,
              CatalogLookupStatus.DB_NOT_FOUND);
        }

        List<Function> funcs = db.getFunctions(objectDesc.fn.name.function_name);
        if (funcs.isEmpty()) {
          return createGetPartialCatalogObjectError(req,
              CatalogLookupStatus.FUNCTION_NOT_FOUND);
        }
        TGetPartialCatalogObjectResponse resp = new TGetPartialCatalogObjectResponse();
        List<TFunction> thriftFuncs = Lists.newArrayListWithCapacity(funcs.size());
        for (Function f : funcs) thriftFuncs.add(f.toThrift());
        resp.setFunctions(thriftFuncs);
        return resp;
      } finally {
        versionLock_.readLock().unlock();
      }
    }
    case DATA_SOURCE: {
      TDataSource dsDesc = Preconditions.checkNotNull(req.object_desc.data_source);
      versionLock_.readLock().lock();
      try {
        TGetPartialCatalogObjectResponse resp = new TGetPartialCatalogObjectResponse();
        if (dsDesc.getName() == null || dsDesc.getName().isEmpty()) {
          // Return all DataSource objects.
          List<DataSource> data_srcs = getDataSources();
          if (data_srcs == null || data_srcs.isEmpty()) {
            resp.setData_srcs(Collections.emptyList());
          } else {
            List<TDataSource> thriftDataSrcs =
                Lists.newArrayListWithCapacity(data_srcs.size());
            for (DataSource ds : data_srcs) thriftDataSrcs.add(ds.toThrift());
            resp.setData_srcs(thriftDataSrcs);
          }
        } else {
          // Return the DataSource for the given name.
          DataSource ds = getDataSource(dsDesc.getName());
          if (ds == null) {
            resp.setData_srcs(Collections.emptyList());
          } else {
            List<TDataSource> thriftDataSrcs = Lists.newArrayListWithCapacity(1);
            thriftDataSrcs.add(ds.toThrift());
            resp.setData_srcs(thriftDataSrcs);
          }
        }
        return resp;
      } finally {
        versionLock_.readLock().unlock();
      }
    }
    default:
      throw new CatalogException("Unable to fetch partial info for type: " +
          req.object_desc.type);
    }
  }

  /**
   * Helper method which gets the filemetadata for given HdfsPartitions from FileSystem
   * with respect to the given ValidWriteIdList. Additionally, it sets it
   * in the corresponding {@link TPartialPartitionInfo} object.
   */
  private void setFileMetadataFromFS(HdfsTable table, ValidWriteIdList reqWriteIdList,
      Map<HdfsPartition, TPartialPartitionInfo> partToPartialInfoMap)
      throws CatalogException {
    Preconditions.checkNotNull(reqWriteIdList);
    Preconditions.checkState(MapUtils.isNotEmpty(partToPartialInfoMap));
    Stopwatch timer = Stopwatch.createStarted();
    try {
      String logPrefix = String.format(
          "Fetching file and block metadata for %s paths for table %s for "
              + "validWriteIdList %s", partToPartialInfoMap.size(), table.getFullName(),
          reqWriteIdList);
      ValidTxnList validTxnList;
      try (MetaStoreClient client = getMetaStoreClient()) {
        validTxnList = MetastoreShim.getValidTxns(client.getHiveClient());
      } catch (TException ex) {
        throw new CatalogException(
            "Unable to fetch valid transaction ids while loading file metadata for table "
                + table.getFullName(), ex);
      }
      List<HdfsPartition.Builder> partBuilders = partToPartialInfoMap.keySet().stream()
          .map(HdfsPartition.Builder::new)
          .collect(Collectors.toList());
      new ParallelFileMetadataLoader(
          table.getFileSystem(), partBuilders, reqWriteIdList,
          validTxnList, Utils.shouldRecursivelyListPartitions(table),
          table.getHostIndex(), null, logPrefix)
          .load();
      for (HdfsPartition.Builder builder : partBuilders) {
        // Let's retrieve the original partition instance from builder because this is
        // stored in the keys of 'partToPartialInfoMap'.
        HdfsPartition part = builder.getOldInstance();
        TPartialPartitionInfo partitionInfo = partToPartialInfoMap.get(part);
        partitionInfo.setFile_descriptors(transformFds(
            builder.getFileDescriptors()));
        partitionInfo.setInsert_file_descriptors(transformFds(
            builder.getInsertFileDescriptors()));
        partitionInfo.setDelete_file_descriptors(transformFds(
            builder.getDeleteFileDescriptors()));
      }
    } finally {
      LOG.info(
          "Time taken to load file metadata for table {} from filesystem for writeIdList"
              + " {}: {} msec.", table.getFullName(), reqWriteIdList,
          timer.stop().elapsed(TimeUnit.MILLISECONDS));
    }
  }

  /**
   * This helper function reloads file metadata for partitions passed in and updates the
   * table in place. It doesn't reload validWriteIds so the table will be consistent
   * even this function is called for many times.
   *
   * @return the updated table
   */
  private Table refreshFileMetadata(HdfsTable hdfsTable,
      List<HdfsPartition.Builder> partBuilders, EventSequence catalogTimeline)
      throws CatalogException {
    Preconditions.checkState(!partBuilders.isEmpty());

    if (!tryWriteLock(hdfsTable, catalogTimeline)) {
      throw new CatalogException(String.format(
          "Error during refreshing file metadata for table %s due to lock contention",
          hdfsTable.getFullName()));
    }
    long newVersion = incrementAndGetCatalogVersion();
    versionLock_.writeLock().unlock();
    try {
      try (MetaStoreClient client = getMetaStoreClient(catalogTimeline)) {
        hdfsTable.loadFileMetadataForPartitions(
            client.getHiveClient(), partBuilders, true, catalogTimeline);
      }
      hdfsTable.updatePartitions(partBuilders);
      hdfsTable.setCatalogVersion(newVersion);
    } finally {
      hdfsTable.writeLock().unlock();
    }
    LOG.debug("Refreshed file metadata for table {}", hdfsTable.getFullName());
    return hdfsTable;
  }

  private static List<THdfsFileDesc> transformFds(List<FileDescriptor> fds) {
    List<THdfsFileDesc> ret = Lists.newArrayListWithCapacity(fds.size());
    for (FileDescriptor fd : fds) {
      ret.add(fd.toThrift());
    }
    return ret;
  }

  private static TGetPartialCatalogObjectResponse createGetPartialCatalogObjectError(
      TGetPartialCatalogObjectRequest req, CatalogLookupStatus status) {
    TGetPartialCatalogObjectResponse resp = new TGetPartialCatalogObjectResponse();
    resp.setLookup_status(status);
    LOG.warn("Fetching {} failed: {}. Could not find {}", req.object_desc.type,
        status, req.object_desc);
    return resp;
  }

  /**
   * Return a partial view of information about global parts of the catalog (eg
   * the list of database names, etc).
   */
  private TGetPartialCatalogObjectResponse getPartialCatalogInfo(
      TGetPartialCatalogObjectRequest req) {
    TGetPartialCatalogObjectResponse resp = new TGetPartialCatalogObjectResponse();
    resp.catalog_info = new TPartialCatalogInfo();
    TCatalogInfoSelector sel = Preconditions.checkNotNull(req.catalog_info_selector,
        "no catalog_info_selector in request");
    if (sel.want_db_names) {
      resp.catalog_info.db_names = ImmutableList.copyOf(dbCache_.get().keySet());
    }
    // TODO(todd) implement data sources and other global information.
    return resp;
  }

  /**
   * Set the last used time of specified tables to now.
   * TODO: Make use of TTableUsage.num_usages.
   */
  public void updateTableUsage(TUpdateTableUsageRequest req) {
    for (TTableUsage usage : req.usages) {
      Table table = null;
      try {
        table = getTable(usage.table_name.db_name, usage.table_name.table_name);
      } catch (DatabaseNotFoundException e) {
        // do nothing
      }
      if (table != null) table.refreshLastUsedTime();
    }
  }

  /**
   * Marks write ids with corresponding status for the table if it is loaded HdfsTable.
   */
  public void addWriteIdsToTable(String dbName, String tblName, long eventId,
      List<Long> writeIds, MutableValidWriteIdList.WriteIdStatus status)
    throws CatalogException {
    Table tbl;
    try {
      tbl = getTable(dbName, tblName);
    } catch (DatabaseNotFoundException e) {
      LOG.debug("Not adding write ids to table {}.{} for event {} " +
          "since database was not found", dbName, tblName, eventId);
      return;
    }
    if (tbl == null) {
      LOG.debug("Not adding write ids to table {}.{} for event {} since it was not found",
          dbName, tblName, eventId);
      return;
    }
    if (!tbl.isLoaded()) {
      LOG.debug("Not adding write ids to table {}.{} for event {} " +
          "since it was not loaded" , dbName, tblName, eventId);
      return;
    }
    if (!(tbl instanceof HdfsTable)) {
      LOG.debug("Not adding write ids to table {}.{} for event {} " +
          "since it is not HdfsTable", dbName, tblName, eventId);
      return;
    }
    if (eventId > 0 && eventId <= tbl.getCreateEventId()) {
      LOG.debug("Not adding write ids to table {}.{} for event {} since it is recreated.",
          dbName, tblName, eventId);
      return;
    }
    if (tbl.getNumClusteringCols() == 0) {
      // For non-partitioned tables, we just reload the whole table without keeping
      // track of write ids.
      LOG.debug("Not adding write ids to table {}.{} for event {} since it is "
              + "a non-partitioned table",
          dbName, tblName, eventId);
      return;
    }
    if (!tryWriteLock(tbl)) {
      throw new CatalogException(String.format(
          "Error locking table %s for event %d", tbl.getFullName(), eventId));
    }
    try {
      boolean syncToLatestEvent =
          BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls();
      long newCatalogVersion = incrementAndGetCatalogVersion();
      versionLock_.writeLock().unlock();
      HdfsTable hdfsTable = (HdfsTable) tbl;
      // Note: Not doing >= check in below condition because a single event
      // may invoke multiple method calls and each method might be setting
      // last synced event id for the table. To ensure that subsequent method calls
      // don't skip processing the table, hence > check
      if (hdfsTable.getLastSyncedEventId() > eventId) {
        LOG.info("EventId: {}, skipping adding writeIds {} with status {} to table {} "
                + "since it is already synced till event id: {}", eventId, writeIds,
            status, hdfsTable.getFullName(), hdfsTable.getLastSyncedEventId());
        return;
      }
      // A non-acid table could be upgraded to an acid table, and its valid write id list
      // is not yet be loaded. In this case, we just do nothing. The table should be
      // reloaded for the AlterTable event that sets the table as transactional.
      if (hdfsTable.getValidWriteIds() == null) {
        LOG.info("Not adding write ids to table {}.{} for event {} since it was just "
            + "upgraded to an acid table and it's valid write id list is not loaded",
            dbName, tblName, eventId);
      }
      if (hdfsTable.getValidWriteIds() != null &&
          hdfsTable.addWriteIds(writeIds, status)) {
        tbl.setCatalogVersion(newCatalogVersion);
        LOG.info("Added {} writeId to table {}: {} for event {}", status,
            tbl.getFullName(), writeIds, eventId);
      }
      if (syncToLatestEvent) {
        hdfsTable.setLastSyncedEventId(eventId);
      }
    } finally {
      UnlockWriteLockIfErroneouslyLocked();
      tbl.releaseWriteLock();
    }
  }

  /**
   * This method checks if the version lock is unlocked. If it's still locked then it
   * logs an error and unlocks it.
   */
  private void UnlockWriteLockIfErroneouslyLocked() {
    if (versionLock_.isWriteLockedByCurrentThread()) {
      LOG.error("Write lock should have been released.");
      versionLock_.writeLock().unlock();
    }
  }

  CatalogdTableInvalidator getCatalogdTableInvalidator() {
    return catalogdTableInvalidator_;
  }

  @VisibleForTesting
  void setCatalogdTableInvalidator(CatalogdTableInvalidator cleaner) {
    catalogdTableInvalidator_ = cleaner;
  }

  @VisibleForTesting
  public void setMetastoreEventProcessor(
      ExternalEventsProcessor metastoreEventProcessor) {
    this.metastoreEventProcessor_ = metastoreEventProcessor;
  }

  public void setCatalogMetastoreServer(ICatalogMetastoreServer catalogMetastoreServer) {
    this.catalogMetastoreServer_ = catalogMetastoreServer;
  }

  public void setEventFactoryForSyncToLatestEvent(MetastoreEventFactory factory) {
    Preconditions.checkArgument(factory != null, "factory is null");
    Preconditions.checkArgument(factory instanceof EventFactoryForSyncToLatestEvent,
        "factory is not an instance of EventFactorySyncToLatestEvent");
    this.syncToLatestEventFactory_ = factory;
  }

  public MetastoreEventFactory getEventFactoryForSyncToLatestEvent() {
    return syncToLatestEventFactory_;
  }

  public boolean isPartitionLoadedAfterEvent(String dbName, String tableName,
      Partition msPartition, long eventId) {
    try {
      HdfsPartition hdfsPartition = getHdfsPartition(dbName, tableName, msPartition);
      LOG.info("Partition {} of table {}.{} has last refresh id as {}. " +
          "Comparing it with {}.", hdfsPartition.getPartitionName(), dbName, tableName,
          hdfsPartition.getLastRefreshEventId(), eventId);
      if (hdfsPartition.getLastRefreshEventId() >= eventId) return true;
    } catch (CatalogException ex) {
      LOG.warn("Encountered an exception while the partition's last refresh event id: "
          + dbName + "." + tableName + ". Ignoring further processing and try to " +
          "reload the partition.", ex);
    }
    return false;
  }

  public Set<String> getWhitelistedTblProperties() {
    return whitelistedTblProperties_;
  }

  public Set<String> getFailureEventsForTesting() { return failureEventsForTesting_; }

  public List<String> getDefaultSkippedHmsEventTypes() {
    return defaultSkippedHmsEventTypes_;
  }

  public List<String> getCommonHmsEventTypes() { return commonHmsEventTypes_; }
}
