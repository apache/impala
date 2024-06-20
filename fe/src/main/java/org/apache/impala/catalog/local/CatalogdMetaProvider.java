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

package org.apache.impala.catalog.local;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.impala.authorization.AuthorizationChecker;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.catalog.AuthzCacheInvalidation;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.CatalogDeltaLog;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogObjectCache;
import org.apache.impala.catalog.DataSource;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.HdfsCachePool;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.HdfsPartitionLocationCompressor;
import org.apache.impala.catalog.HdfsStorageDescriptor;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.ImpaladCatalog.ObjectUpdateSequencer;
import org.apache.impala.catalog.Principal;
import org.apache.impala.catalog.PrincipalPrivilege;
import org.apache.impala.catalog.SqlConstraints;
import org.apache.impala.catalog.VirtualColumn;
import org.apache.impala.catalog.local.LocalIcebergTable.TableParams;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.Pair;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.FeSupport;
import org.apache.impala.service.FrontendProfile;
import org.apache.impala.service.MetadataOp;
import org.apache.impala.thrift.CatalogLookupStatus;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TBriefTableMeta;
import org.apache.impala.thrift.TCatalogInfoSelector;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TDataSource;
import org.apache.impala.thrift.TDatabase;
import org.apache.impala.thrift.TDbInfoSelector;
import org.apache.impala.thrift.TErrorCode;
import org.apache.impala.thrift.TFunction;
import org.apache.impala.thrift.TFunctionName;
import org.apache.impala.thrift.TGetLatestCompactionsRequest;
import org.apache.impala.thrift.TGetLatestCompactionsResponse;
import org.apache.impala.thrift.TGetPartialCatalogObjectRequest;
import org.apache.impala.thrift.TGetPartialCatalogObjectResponse;
import org.apache.impala.thrift.THdfsFileDesc;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartialPartitionInfo;
import org.apache.impala.thrift.TPartialTableInfo;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableInfoSelector;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.thrift.TUnit;
import org.apache.impala.thrift.TUpdateCatalogCacheRequest;
import org.apache.impala.thrift.TUpdateCatalogCacheResponse;
import org.apache.impala.thrift.TValidWriteIdList;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.IcebergUtil;
import org.apache.impala.util.ListMap;
import org.apache.impala.util.TByteBuffer;
import org.apache.impala.util.ThreadNameAnnotator;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.ehcache.sizeof.SizeOf;
import org.github.jamm.CannotAccessFieldException;
import org.github.jamm.MemoryMeter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.GuardedBy;

/**
 * MetaProvider which fetches metadata in a granular fashion from the catalogd.
 *
 * When pieces of metadata are requested, a local LRU cache is first queried. If the
 * metadata is present in the cache, it will be returned. Otherwise, an RPC is made
 * to the catalogd to fetch the metadata and the metadata is written back into the cache
 * before being returned.
 *
 * The implementation subscribes to a "minimal" subset of the Catalog statestore topic
 * in order to be notified about changes to objects stored within the catalogd. We
 * use the notifications from this topic to perform cache invalidation. The invalidation
 * strategy is slightly different depending on the specific piece of metadata:
 *
 * Strategy 1): small objects, cached coarse-grained
 * ---------------------------
 * For objects which we know to be small (eg database metadata, lists of table names,
 * etc) we simply invalidate our local cache whenever we see any change that indicates
 * our data might be stale. For example, when we see any change of a table, we invalidate
 * the list of tables in the associated database. This is simpler than tracking whether
 * the table is an addition or just an updated version of an already-existing entity,
 * and in most cases we aim for simplicity even if it means occasional over-invalidation.
 *
 * As another example, when we see a new version of a database object, we invalidate
 * the list of databases as well as the cached information for that specific database.
 *
 * Strategy 2): granular information about tables
 * -----------------------------------------------
 * The metadata associated with tables is large enough that, instead of caching it as
 * a single coarse-grained entry, we use separate cache entries for more fine-grained
 * pieces of metadata (eg lists of partitions, individual partitions, etc). Thus, when
 * we see a notification indicating that a table has changed versions in the catalogd,
 * it would be difficult to evict all of the associated items from the cache. The cache
 * implementation does not support "prefix search" or "tag-based invalidation" of any
 * kind, so we would need to scan through all of the cached items in order to invalidate
 * all of the data referring to a table.
 *
 * Instead, we use a different strategy: all of the granular metadata associated with a
 * particular version of a table includes the _version number_ of the table as part of
 * its cache key. When we are notified that the table has changed version numbers, we
 * simply invalidate the top-level table entry, and allow other information to remain
 * in the cache. When we next load this table, we will load a new version of the top-level
 * table entry, including its new version number. Thus, the requests for granular
 * information pertaining to the new version will not include the old version number
 * in cache keys anymore. The old metadata is essentially invalidated by the fact that
 * it is no longer "linked". Over time, the old entries will naturally age out of the
 * cache.
 *
 *
 * Metadata that is _not_ fetched on demand
 * ================================================
 * This implementation does not fetch _all_ metadata on demand. In fact, some pieces of
 * metadata are currently provided in the same manner as "legacy" coordinators: the
 * full metadata objects are published by the catalog daemon into the statestore, and
 * we keep a full "replica" of that information. In particular, we currently use this
 * strategy for Sentry metadata (roles and privileges) since the caching of this data
 * is relatively more complex. Given that this data is typically quite small relative
 * to the table metadata, it's not too expensive to maintain the full replica.
 *
 *
 * TODO(todd): expose statistics on a per-query and per-daemon level about cache
 * hit rates, number of outbound RPCs, etc.
 * TODO(todd): handle retry/backoff to ride over short catalog interruptions
 */
public class CatalogdMetaProvider implements MetaProvider {

  private final static Logger LOG = LoggerFactory.getLogger(CatalogdMetaProvider.class);

  /**
   * Sentinel value used as a negative cache entry for column statistics.
   * Some columns (e.g. partitioning columns )do not have statistics in the catalog
   * and won't be returned when we ask it for stats. It's important to cache negative
   * entries for those or else we would require a round-trip every time the table
   * is loaded.
   *
   * This special sentinel value is stored in the cache to indicate such a "negative
   * cache" entry. It is always compared by reference equality.
   */
  private static final ColumnStatisticsObj NEGATIVE_COLUMN_STATS_SENTINEL =
      new ColumnStatisticsObj();

  /**
   * Used as a cache key for caching the "null partition key value", which is a global
   * Hive configuration. Value is a String.
   */
  private static final Object NULL_PARTITION_KEY_VALUE_CACHE_KEY = new Object();

  /**
   * Used as a cache key for caching the global list of database names. Value is
   * an ImmutableList<String>.
   */
  private static final Object DB_LIST_CACHE_KEY = new Object();

  private static final String CATALOG_FETCH_PREFIX = "CatalogFetch";
  private static final String DB_LIST_STATS_CATEGORY = "DatabaseList";
  private static final String DB_METADATA_STATS_CATEGORY = "Databases";
  private static final String TABLE_LIST_STATS_CATEGORY = "TableList";
  private static final String TABLE_METADATA_CACHE_CATEGORY = "Tables";
  private static final String PARTITION_LIST_STATS_CATEGORY = "PartitionLists";
  private static final String PARTITIONS_STATS_CATEGORY = "Partitions";
  private static final String COLUMN_STATS_STATS_CATEGORY = "ColumnStats";
  private static final String GLOBAL_CONFIGURATION_STATS_CATEGORY = "Config";
  private static final String FUNCTION_LIST_STATS_CATEGORY = "FunctionLists";
  private static final String FUNCTIONS_STATS_CATEGORY = "Functions";
  private static final String RPC_STATS_CATEGORY = "RPCs";
  private static final String STORAGE_METADATA_LOAD_CATEGORY = "StorageLoad";
  private static final String DATA_SOURCE_LIST_STATS_CATEGORY = "DataSourceLists";
  private static final Object DS_OBJ_LIST_CACHE_KEY = new Object();
  private static final String RPC_REQUESTS =
      CATALOG_FETCH_PREFIX + "." + RPC_STATS_CATEGORY + ".Requests";
  private static final String RPC_BYTES =
      CATALOG_FETCH_PREFIX + "." + RPC_STATS_CATEGORY + ".Bytes";
  private static final String RPC_TIME =
      CATALOG_FETCH_PREFIX + "." + RPC_STATS_CATEGORY + ".Time";

  /**
   * File descriptors store replicas using a compressed format that references hosts
   * by index in a "host index" list rather than by their full addresses. Since we cache
   * partition metadata including file descriptors across many queries, we can't rely on
   * callers to provide a consistent host index. Instead, cached file descriptors are
   * always relative to this global host index.
   *
   * Note that we never evict entries from this host index. We rely on the fact that,
   * in a given storage cluster, the number of hosts is bounded, and "leaking" the unique
   * network addresses won't cause a problem over time.
   */
  private final ListMap<TNetworkAddress> cacheHostIndex_ =
      new ListMap<TNetworkAddress>();

  /**
   * Number of requests which piggy-backed on a concurrent request for the same key,
   * and resulted in success. Used only for test assertions.
   */
  @VisibleForTesting
  final AtomicInteger piggybackSuccessCountForTests = new AtomicInteger();

  /**
   * Number of requests which piggy-backed on a concurrent request for the same key,
   * and resulted in an exception. Used only for test assertions.
   */
  @VisibleForTesting
  final AtomicInteger piggybackExceptionCountForTests = new AtomicInteger();

  /**
   * The underlying cache.
   *
   * The keys in this cache are various types of objects (strings, DbCacheKey, etc).
   * The values are also variant depending on the type of cache key. While any key
   * is being loaded, it is a Future<T>, which gets replaced with a non-wrapped object
   * once it is successfully loaded (see {@link #getIfPresent(Object)} for a convenient
   * wrapper).
   *
   * For details of the usage of Futures within the cache, see
   * {@link #loadWithCaching(String, String, Object, Callable).
   */
  final Cache<Object,Object> cache_;

  private final Histogram cacheEntrySize_ = new Histogram(new UniformReservoir());

  /**
   * The last catalog version seen in an update from the catalogd.
   *
   * This is used to implement SYNC_DDL: when a SYNC_DDL operation is done, the catalog
   * responds to the DDL with the version of the catalog at which the DDL has been
   * applied. The backend then waits until this 'lastSeenCatalogVersion' advances past
   * the version where the DDL was applied, and correlates that with the corresponding
   * statestore topic version. It then waits until the statestore reports that this topic
   * version has been distributed to all coordinators before proceeding.
   */
  private final AtomicLong lastSeenCatalogVersion_ = new AtomicLong(
      Catalog.INITIAL_CATALOG_VERSION);

  /**
   * The catalog version at last time when catalogd starts resetting the entire catalog.
   *
   * This is used to implement global INVALIDATE METADATA: Catalogd saves its catalog
   * version to this when it starts to reset the entire catalog. After the reset is done,
   * all valid catalog objects should have a catalog version larger than this. Thus, we
   * can safely advance the catalog version lower bound to this version + 1.
   * The coordinator first gets this value in the RPC response from Catalogd and starts
   * to wait until the catalog version lower bound becomes larger than it. Once we
   * receive the update from statestored and update our catalog cache accordingly, the
   * catalog version lower bound is set to lastResetCatalogVersion_ + 1 (returned by
   * updateCatalogCache()).
   */
  private final AtomicLong lastResetCatalogVersion_ = new AtomicLong(-1);

  /**
   * Tracks objects that have been deleted in response to a DDL issued from this
   * coordinator.
   */
  CatalogDeltaLog deletedObjectsLog_ = new CatalogDeltaLog();

  /**
   * The last known Catalog Service ID. If the ID changes, it indicates the CatalogServer
   * has restarted.
   */
  @GuardedBy("catalogServiceIdLock_")
  private TUniqueId catalogServiceId_ = Catalog.INITIAL_CATALOG_SERVICE_ID;
  private final Object catalogServiceIdLock_ = new Object();


  /**
   * Cache of authorization policy metadata. Populated from data pushed from the
   * StateStore. Currently this is _not_ "fetch-on-demand".
   */
  private final AuthorizationPolicy authPolicy_ = new AuthorizationPolicy();
  // Cache of authorization refresh markers.
  private final CatalogObjectCache<AuthzCacheInvalidation> authzCacheInvalidation_ =
      new CatalogObjectCache<>();
  private AtomicReference<? extends AuthorizationChecker> authzChecker_;

  // Cache of known HDFS cache pools. Allows for checking the existence
  // of pools without hitting HDFS. This is _not_ "fetch-on-demand".
  private final CatalogObjectCache<HdfsCachePool> hdfsCachePools_ =
      new CatalogObjectCache<>(false);

  public CatalogdMetaProvider(TBackendGflags flags) {
    Preconditions.checkArgument(flags.isSetLocal_catalog_cache_expiration_s());
    Preconditions.checkArgument(flags.isSetLocal_catalog_cache_mb());

    long cacheSizeBytes;
    if (flags.local_catalog_cache_mb < 0) {
      long maxHeapBytes = ManagementFactory.getMemoryMXBean()
          .getHeapMemoryUsage().getMax();
      cacheSizeBytes = (long)(maxHeapBytes * 0.6);
    } else {
      cacheSizeBytes = flags.local_catalog_cache_mb * 1024 * 1024;
    }
    int expirationSecs = flags.local_catalog_cache_expiration_s;
    int concurrencyLevel = flags.local_catalog_cache_concurrency_level;
    if (concurrencyLevel <= 0) {
      // 4 is the default value of the local cache
      concurrencyLevel = 4;
    }
    LOG.info("Metadata cache configuration: capacity={} MB, expiration={} sec, " +
        "concurrencyLevel={}", cacheSizeBytes/1024/1024, expirationSecs,
        concurrencyLevel);

    // TODO(todd) add end-to-end test cases which stress cache eviction (both time
    // and size-triggered) and make sure results are still correct.
    cache_ = CacheBuilder.newBuilder()
        .concurrencyLevel(concurrencyLevel)
        .maximumWeight(cacheSizeBytes)
        .expireAfterAccess(expirationSecs, TimeUnit.SECONDS)
        .weigher(new SizeOfWeigher(
            BackendConfig.INSTANCE.useJammWeigher(), cacheEntrySize_))
        .recordStats()
        .build();
  }

  public CacheStats getCacheStats() {
    return cache_.stats();
  }

  public Snapshot getCacheEntrySize() {
    return cacheEntrySize_.getSnapshot();
  }

  @Override
  public Iterable<HdfsCachePool> getHdfsCachePools() {
    return hdfsCachePools_;
  }

  @Override
  public AuthorizationPolicy getAuthPolicy() {
    return authPolicy_;
  }

  @Override
  public boolean isReady() {
    return lastSeenCatalogVersion_.get() > Catalog.INITIAL_CATALOG_VERSION;
  }

  public void setAuthzChecker(
      AtomicReference<? extends AuthorizationChecker> authzChecker) {
    authzChecker_ = authzChecker;
  }

  /**
   * Send a GetPartialCatalogObject request to catalogd. This handles converting
   * non-OK status responses back to exceptions, performing various generic sanity
   * checks, etc.
   */
  private TGetPartialCatalogObjectResponse sendRequest(
      TGetPartialCatalogObjectRequest req)
      throws TException {
    TGetPartialCatalogObjectResponse resp;
    byte[] ret = null;
    Stopwatch sw = Stopwatch.createStarted();
    try {
      ret = FeSupport.GetPartialCatalogObject(new TSerializer().serialize(req));
    } catch (InternalException e) {
      throw new TException(e);
    } finally {
      sw.stop();
      FrontendProfile profile = FrontendProfile.getCurrentOrNull();
      if (profile != null) {
        profile.addToCounter(RPC_REQUESTS, TUnit.NONE, 1);
        profile.addToCounter(RPC_BYTES, TUnit.BYTES, ret == null ? 0 : ret.length);
        profile.addToCounter(RPC_TIME, TUnit.TIME_MS, sw.elapsed(TimeUnit.MILLISECONDS));
      }
    }
    resp = new TGetPartialCatalogObjectResponse();
    new TDeserializer().deserialize(resp, ret);
    if (resp.status.status_code != TErrorCode.OK) {
      // TODO(todd) do reasonable error handling
      throw new TException(resp.toString());
    }

    // If we get a "not found" response, then we assume that this was a case of an
    // inconsistent cache. For example, we might have cached the list of tables within
    // a database, but the table we're trying to load was dropped just prior to us
    // trying to load it. In these cases, we need to invalidate whatever cache items
    // might have led us to the dropped object and throw an exception so that we
    // can retry with a reloaded cache.
    switch (resp.lookup_status) {
      case DB_NOT_FOUND:
      case FUNCTION_NOT_FOUND:
      case TABLE_NOT_FOUND:
      case TABLE_NOT_LOADED:
      case PARTITION_NOT_FOUND:
      case DATA_SOURCE_NOT_FOUND:
        invalidateCacheForObject(req.object_desc);
        throw new InconsistentMetadataFetchException(resp.lookup_status,
            String.format("Fetching %s failed: %s. Could not find %s",
                req.object_desc.type, resp.lookup_status, req.object_desc));
      default: break;
    }
    Preconditions.checkState(resp.lookup_status == CatalogLookupStatus.OK);

    // If we requested information about a particular version of an object, but
    // got back a response for a different version, then we have a case of "read skew".
    // For example, we may have fetched the partition list of a table, performed pruning,
    // and then tried to fetch the specific partitions needed for a query, while some
    // concurrent DDL modified the set of partitions. This could result in an unexpected
    // result which violates the snapshot consistency guarantees expected by users.
    if (req.object_desc.isSetCatalog_version() &&
        resp.isSetObject_version_number() &&
        req.object_desc.catalog_version != resp.object_version_number) {
      invalidateCacheForObject(req.object_desc);
      LOG.warn("Catalog object {} changed version from {} to {} while fetching metadata",
          req.object_desc.toString(), req.object_desc.catalog_version,
          resp.object_version_number);
      throw new InconsistentMetadataFetchException(CatalogLookupStatus.VERSION_MISMATCH,
          String.format("Catalog object %s changed version between accesses.",
              req.object_desc.toString()));
    }
    return resp;
  }

  @SuppressWarnings("unchecked")
  private <CacheKeyType, ValueType> ValueType loadWithCaching(String itemString,
      String statsCategory, CacheKeyType key,
      final Callable<ValueType> loadCallable) throws TException {

    // We cache Futures during loading to deal with a particularly troublesome race
    // around invalidation (IMPALA-7534). Namely, we have the following interleaving to
    // worry about:
    //
    //  Thread 1: loadTableList() misses and sends a request to fetch table names
    //  Catalogd: sends a response with table list ['foo']
    //  Thread 2:    creates a table 'bar'
    //  Catalogd:    returns an invalidation for the table name list
    //  Thread 2:    invalidates the table list
    //  Thread 1: response arrives with ['foo'], which is stored in the cache
    //
    // In this case, we've "missed" an invalidation because it arrived concurrently
    // with the loading of a value in the cache. This is a well-known issue with
    // Guava:
    //
    //    https://softwaremill.com/race-condition-cache-guava-caffeine/
    //
    // In order to avoid this issue, if we don't find an element in the cache, we insert
    // a Future while we load the value. Essentially, an entry can be in one of the
    // following states:
    //
    // Missing (no entry in the cache):
    //   invalidate would be ignored, but that's OK, because any future read would fetch
    //   new data from the catalogd, and see a version newer than the invalidate
    //
    // Loading (a Future<> in the cache):
    //    invalidate removes the future. When loading completes, its attempt to swap
    //    in the value will fail. Any request after the invalidate will cause a second
    //    load to be triggered, which sees the post-invalidated data in catalogd.
    //
    //    Any concurrent *read* of the cache (with no invalidation or prior to an
    //    invalidation) will piggy-back on the e same Future and return its result when
    //    it completes.
    //
    // Cached (non-Future in the cache):
    //    no interesting race: an invalidation ensures that any future load will miss
    //    and fetch a new value
    //
    // NOTE: we don't need to perform this dance for cache keys which embed a version
    // number, because invalidation is not handled by removing cache entries, but
    // rather by bumping top-level version numbers.
    Stopwatch sw = Stopwatch.createStarted();
    boolean hit = false;
    boolean isPiggybacked = false;
    try (ThreadNameAnnotator tna = new ThreadNameAnnotator(
        "LoadWithCaching for " + itemString)) {
      CompletableFuture<Object> f = new CompletableFuture<Object>();
      // NOTE: the Cache ensures that this is an atomic operation of either returning
      // an existing value or inserting our own. Only one thread can think it is the
      // "loader" at a time.
      Object inCache = cache_.get(key, () -> f);
      if (!(inCache instanceof Future)) {
        hit = true;
        return (ValueType)inCache;
      }

      if (inCache != f) {
        isPiggybacked = true;
        Future<ValueType> existing = (Future<ValueType>)inCache;
        ValueType ret = Uninterruptibles.getUninterruptibly(existing);
        piggybackSuccessCountForTests.incrementAndGet();
        return ret;
      }

      // No other thread was loading this value, so we need to fetch it ourselves.
      try {
        f.complete(loadCallable.call());
        // Assuming we were able to load the value, store it back into the map
        // as a plain-old object. This is important to get the proper weight in the
        // map. If someone invalidated this load concurrently, this 'replace' will
        // fail because 'f' will not be the current value.
        cache_.asMap().replace(key, f, f.get());
      } catch (Exception e) {
        // If there was an exception, remove it from the map so that any later loads
        // retry.
        cache_.asMap().remove(key, f);
        // Ensure any piggy-backed loaders get the exception. 'f.get()' below will
        // throw to this caller.
        f.completeExceptionally(e);
      }
      return (ValueType) Uninterruptibles.getUninterruptibly(f);
    } catch (ExecutionException | UncheckedExecutionException e) {
      if (isPiggybacked) {
        piggybackExceptionCountForTests.incrementAndGet();
      }

      Throwables.propagateIfPossible(e.getCause(), TException.class);
      // Since the loading code should only throw TException, we shouldn't get
      // any other exceptions here. If for some reason we do, just rethrow as RTE.
      throw new RuntimeException(e);
    } finally {
      sw.stop();
      addStatsToProfile(statsCategory, /*numHits=*/hit ? 1 : 0,
          /*numMisses=*/hit ? 0 : 1, sw);
      LOG.trace("Request for {}: {}{}", itemString, isPiggybacked ? "piggy-backed " : "",
          hit ? "hit" : "miss");
    }
  }

  /**
   * Adds basic statistics to the query's profile when accessing cache entries.
   * For each cache request, the number of hits, misses, and elapsed time is aggregated.
   * Cache requests for different types of cache entries, such as function names vs.
   * table names, are differentiated by a 'statsCategory'.
   */
  private void addStatsToProfile(String statsCategory, int numHits, int numMisses,
      Stopwatch stopwatch) {
    FrontendProfile profile = FrontendProfile.getCurrentOrNull();
    if (profile == null) return;
    final String prefix = CATALOG_FETCH_PREFIX + "." +
        Preconditions.checkNotNull(statsCategory) + ".";
    profile.addToCounter(prefix + "Requests", TUnit.NONE, numHits + numMisses);
    profile.addToCounter(prefix + "Time", TUnit.TIME_MS,
        stopwatch.elapsed(TimeUnit.MILLISECONDS));
    profile.addToCounter(prefix + "Hits", TUnit.NONE, numHits);
    profile.addToCounter(prefix + "Misses", TUnit.NONE, numMisses);
  }

  /**
   * Adds tables metadata storage access time to query's profile.
   * The access time is aggregated for the tables which need to be loaded.
   */
  private void addTableMetadatStorageLoadTimeToProfile(long storageLoadTimeNano) {
    FrontendProfile profile = FrontendProfile.getCurrentOrNull();
    if (profile == null) return;
    // Storage-load-time for the table and its partitions
    final String storageAccessTimeCounter = CATALOG_FETCH_PREFIX + "." +
         STORAGE_METADATA_LOAD_CATEGORY + "." + "Time";
    profile.addToCounter(storageAccessTimeCounter, TUnit.TIME_MS,
       TimeUnit.MILLISECONDS.convert(storageLoadTimeNano, TimeUnit.NANOSECONDS));
  }

  @Override
  public ImmutableList<String> loadDbList() throws TException {
    return loadWithCaching("database list", DB_LIST_STATS_CATEGORY, DB_LIST_CACHE_KEY,
        new Callable<ImmutableList<String>>() {
          @Override
          public ImmutableList<String> call() throws Exception {
            TGetPartialCatalogObjectRequest req = newReqForCatalog();
            req.catalog_info_selector.want_db_names = true;
            TGetPartialCatalogObjectResponse resp = sendRequest(req);
            checkResponse(resp.catalog_info != null && resp.catalog_info.db_names != null,
                req, "missing database names");
            return ImmutableList.copyOf(resp.catalog_info.db_names);
          }
    });
  }

  private TGetPartialCatalogObjectRequest newReqForCatalog() {
    TGetPartialCatalogObjectRequest req = new TGetPartialCatalogObjectRequest();
    req.object_desc = new TCatalogObject();
    req.object_desc.setType(TCatalogObjectType.CATALOG);
    req.catalog_info_selector = new TCatalogInfoSelector();
    return req;
  }

  private TGetPartialCatalogObjectRequest newReqForDb(String dbName) {
    TGetPartialCatalogObjectRequest req = new TGetPartialCatalogObjectRequest();
    req.object_desc = new TCatalogObject();
    req.object_desc.setType(TCatalogObjectType.DATABASE);
    req.object_desc.db = new TDatabase(dbName);
    req.db_info_selector = new TDbInfoSelector();
    return req;
  }

  private TGetPartialCatalogObjectRequest newReqForFunction(String dbName,
      String funcName) {
    TGetPartialCatalogObjectRequest req = new TGetPartialCatalogObjectRequest();
    req.object_desc = new TCatalogObject();
    req.object_desc.setType(TCatalogObjectType.FUNCTION);
    req.object_desc.fn = new TFunction();
    req.object_desc.fn.name = new TFunctionName();
    req.object_desc.fn.name.db_name = dbName;
    req.object_desc.fn.name.function_name = funcName;
    return req;
  }

  private TGetPartialCatalogObjectRequest newReqForDataSource(String dsName) {
    TGetPartialCatalogObjectRequest req = new TGetPartialCatalogObjectRequest();
    req.object_desc = new TCatalogObject();
    req.object_desc.setType(TCatalogObjectType.DATA_SOURCE);
    if (dsName == null) dsName = "";
    req.object_desc.setData_source(new TDataSource(dsName, "", "", ""));
    return req;
  }

  @Override
  public Database loadDb(final String dbName) throws TException {
    return loadWithCaching("database metadata for " + dbName,
        DB_METADATA_STATS_CATEGORY,
        new DbCacheKey(dbName.toLowerCase(), DbCacheKey.DbInfoType.HMS_METADATA),
        new Callable<Database>() {
          @Override
          public Database call() throws Exception {
            TGetPartialCatalogObjectRequest req = newReqForDb(dbName);
            req.db_info_selector.want_hms_database = true;
            TGetPartialCatalogObjectResponse resp = sendRequest(req);
            checkResponse(resp.db_info != null && resp.db_info.hms_database != null,
                req, "missing expected HMS database");
            return resp.db_info.hms_database;
          }
      });
  }

  @Override
  public ImmutableCollection<TBriefTableMeta> loadTableList(final String dbName)
      throws MetaException, UnknownDBException, TException {
    ImmutableMap<String, TBriefTableMeta> res = loadWithCaching(
        "table list of database " + dbName, TABLE_LIST_STATS_CATEGORY,
        new DbCacheKey(dbName.toLowerCase(), DbCacheKey.DbInfoType.TABLE_LIST),
        new Callable<ImmutableMap<String, TBriefTableMeta>>() {
          @Override
          public ImmutableMap<String, TBriefTableMeta> call() throws Exception {
            TGetPartialCatalogObjectRequest req = newReqForDb(dbName);
            req.db_info_selector.want_brief_meta_of_tables = true;
            TGetPartialCatalogObjectResponse resp = sendRequest(req);
            checkResponse(
                resp.db_info != null && resp.db_info.brief_meta_of_tables != null,
                req, "missing expected table names");
            ImmutableMap.Builder<String, TBriefTableMeta> map = ImmutableMap.builder();
            for (TBriefTableMeta meta : resp.db_info.brief_meta_of_tables) {
              map.put(meta.getName(), meta);
            }
            return map.build();
          }
      });
    return res.values();
  }

  private TGetPartialCatalogObjectRequest newReqForTable(String dbName,
      String tableName) {
    TGetPartialCatalogObjectRequest req = new TGetPartialCatalogObjectRequest();
    req.object_desc = new TCatalogObject();
    req.object_desc.setType(TCatalogObjectType.TABLE);
    req.object_desc.table = new TTable(dbName, tableName);
    req.table_info_selector = new TTableInfoSelector();
    return req;
  }

  private TGetPartialCatalogObjectRequest newReqForTable(TableMetaRef table) {
    Preconditions.checkArgument(table instanceof TableMetaRefImpl,
        "table ref %s was not created by CatalogdMetaProvider", table);
    TGetPartialCatalogObjectRequest req = newReqForTable(
        ((TableMetaRefImpl)table).dbName_,
        ((TableMetaRefImpl)table).tableName_);
    req.object_desc.setCatalog_version(((TableMetaRefImpl)table).catalogVersion_);
    return req;
  }

  @Override
  public Pair<Table, TableMetaRef> getTableIfPresent(String dbName, String tblName) {
    TableCacheKey cacheKey = new TableCacheKey(dbName.toLowerCase(),
        tblName.toLowerCase());
    try {
      Object value = getIfPresent(cacheKey);
      if (value == null) return null;
      TableMetaRefImpl ref = (TableMetaRefImpl) value;
      return Pair.create(ref.msTable_, ref);
    } catch (TException e) {
      return null;
    }
  }

  @Override
  public Pair<Table, TableMetaRef> loadTable(final String dbName, final String tableName)
      throws NoSuchObjectException, MetaException, TException {
    TableCacheKey cacheKey = new TableCacheKey(dbName.toLowerCase(),
        tableName.toLowerCase());
    TableMetaRefImpl ref = loadWithCaching(
        "table metadata for " + dbName + "." + tableName,
        TABLE_METADATA_CACHE_CATEGORY,
        cacheKey,
        new Callable<TableMetaRefImpl>() {
          @Override
          public TableMetaRefImpl call() throws Exception {
            TGetPartialCatalogObjectRequest req = newReqForTable(dbName, tableName);
            req.table_info_selector.want_hms_table = true;
            // To be consistent with implementation in legacy catalog mode, we eagerly
            // load constraint information whenever a table is loaded.
            req.table_info_selector.want_table_constraints = true;
            TGetPartialCatalogObjectResponse resp = sendRequest(req);
            checkResponse(resp.table_info != null && resp.table_info.hms_table != null,
                req, "missing expected HMS table");
            addTableMetadatStorageLoadTimeToProfile(
                resp.table_info.storage_metadata_load_time_ns);
            List<SQLPrimaryKey> primaryKeys = resp.table_info.sql_constraints == null ?
                new ArrayList<>() : resp.table_info.sql_constraints.getPrimary_keys();
            List<SQLForeignKey> foreignKeys = resp.table_info.sql_constraints == null ?
                new ArrayList<>() : resp.table_info.sql_constraints.getForeign_keys();
            return new TableMetaRefImpl(
                dbName, tableName, resp.table_info.hms_table, resp.object_version_number,
                new SqlConstraints(primaryKeys, foreignKeys),
                resp.table_info.valid_write_ids, resp.table_info.is_marked_cached,
                resp.table_info.partition_prefixes, resp.table_info.virtual_columns);
           }
      });
    // The table list is populated based on tables in a given Db in catalogd. If a table
    // is not loaded at the time in catalogd, we assume that the table type is "TABLE" and
    // comment is empty (See MetadataOp.getTableType and MetadataOp.getTableComment()).
    // However, after issuing this load request, if we find that our assumption was
    // incorrect, we need to invalidate the table list immediately. The table list gets
    // invalidated automatically when we receive the catalog topic-update but until then,
    // all the getTables calls will show incorrect table type and comment for this table.
    // TODO: consider removing this after IMPALA-9670.
    invalidateStaleTableList(dbName.toLowerCase(), ref);
    return Pair.create(ref.msTable_, (TableMetaRef)ref);
  }

  /**
   * Invalidate table list of the given DB if it's loaded and it contains stale
   * type/comment comparing to the newly loaded msTable.
   */
  private void invalidateStaleTableList(final String dbName,
      final TableMetaRefImpl latestMeta) {
    Preconditions.checkNotNull(latestMeta.msTable_, "loaded table should have a msTable");
    DbCacheKey dbKey = new DbCacheKey(dbName, DbCacheKey.DbInfoType.TABLE_LIST);
    Object dbValue = cache_.getIfPresent(dbKey);
    if (dbValue instanceof ImmutableMap) {
      TBriefTableMeta cachedMeta = ((ImmutableMap<String, TBriefTableMeta>) dbValue).get(
          latestMeta.tableName_);
      String latestComment = MetadataOp.getTableComment(latestMeta.msTable_);
      boolean hasStaleComment = !StringUtils.equals(latestComment, cachedMeta.comment);
      // It's possible that the cached type is null (due to previously unloaded) and the
      // latest msType we get is a table type (e.g. MANAGED_TABLE, EXTERNAL_TABLE).
      // This is the most usual case and we don't consider the cached type is stale since
      // they have the same result after applying MetadataOp.getImpalaTableType().
      boolean hasStaleType =
          MetadataOp.getImpalaTableType(cachedMeta.msType) !=
          MetadataOp.getImpalaTableType(latestMeta.msTable_.getTableType());
      if (hasStaleType || hasStaleComment) {
        List<String> invalidated = new ArrayList<>();
        invalidateCacheForDb(dbName, ImmutableList.of(DbCacheKey.DbInfoType.TABLE_LIST),
            invalidated);
        if (!invalidated.isEmpty()) {
          Preconditions.checkState(invalidated.size() == 1);
          LOG.debug("Invalidated stale {} after loading table {}: hasStaleType={}, " +
                  "hasStaleComment={}",
              invalidated.get(0), latestMeta.tableName_, hasStaleType, hasStaleComment);
        }
      }
    }
  }

  @Override
  public List<ColumnStatisticsObj> loadTableColumnStatistics(final TableMetaRef table,
      List<String> colNames) throws TException {
    Stopwatch sw = Stopwatch.createStarted();
    List<ColumnStatisticsObj> ret = Lists.newArrayListWithCapacity(colNames.size());
    // Look up in cache first, keeping track of which ones are missing.
    // We can't use 'loadWithCaching' since we need to fetch several entries batched
    // in a single RPC to the catalog.
    int negativeHitCount = 0;
    List<String> missingCols = Lists.newArrayListWithCapacity(colNames.size());
    for (String colName: colNames) {
      ColStatsCacheKey cacheKey = new ColStatsCacheKey((TableMetaRefImpl)table, colName);
      ColumnStatisticsObj val = (ColumnStatisticsObj) getIfPresent(cacheKey);
      if (val == null) {
        missingCols.add(colName);
      } else if (val == NEGATIVE_COLUMN_STATS_SENTINEL) {
        negativeHitCount++;
      } else {
        ret.add(val);
      }
    }
    int hitCount = ret.size();

    // Fetch and re-add those missing ones.
    if (!missingCols.isEmpty()) {
      TGetPartialCatalogObjectRequest req = newReqForTable(table);
      req.table_info_selector.want_stats_for_column_names = missingCols;
      TGetPartialCatalogObjectResponse resp = sendRequest(req);
      checkResponse(resp.table_info != null && resp.table_info.column_stats != null,
          req, "missing column stats");

      Set<String> colsWithoutStats = new HashSet<>(missingCols);
      for (ColumnStatisticsObj stats: resp.table_info.column_stats) {
        cache_.put(new ColStatsCacheKey((TableMetaRefImpl)table, stats.getColName()),
            stats);
        ret.add(stats);
        colsWithoutStats.remove(stats.getColName());
      }

      // Cache negative entries for any that were not returned.
      for (String missingColName: colsWithoutStats) {
        cache_.put(new ColStatsCacheKey((TableMetaRefImpl)table, missingColName),
            NEGATIVE_COLUMN_STATS_SENTINEL);
      }
    }
    sw.stop();
    addStatsToProfile(COLUMN_STATS_STATS_CATEGORY,
        hitCount + negativeHitCount, missingCols.size(), sw);
    LOG.trace("Request for column stats of {}: hit {}/ neg hit {} / miss {}",
        table, hitCount, negativeHitCount, missingCols.size());
    return ret;
  }

  @SuppressWarnings("unchecked")
  private Object getIfPresent(Object cacheKey) throws TException {
    Object existing = cache_.getIfPresent(cacheKey);
    if (existing == null) return null;
    if (!(existing instanceof Future)) return existing;
    try {
      return Uninterruptibles.getUninterruptibly((Future<Object>) existing);
    } catch (ExecutionException e) {
      // Try throwing the cause which is the error in the loading thread.
      // This is consistent with the error handling in loadWithCaching().
      // So failures like InconsistentMetadataFetchException can be caught
      // and retry in Frontend#getTExecRequest().
      Throwables.propagateIfPossible(e.getCause(), TException.class);
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<PartitionRef> loadPartitionList(final TableMetaRef table)
      throws TException {
    PartitionListCacheKey key = new PartitionListCacheKey((TableMetaRefImpl) table);
    return (List<PartitionRef>) loadWithCaching("partition list for " + table,
        PARTITION_LIST_STATS_CATEGORY, key, new Callable<List<PartitionRef>>() {
          /** Called to load cache for cache misses */
          @Override
          public List<PartitionRef> call() throws Exception {
            TGetPartialCatalogObjectRequest req = newReqForTable(table);
            req.table_info_selector.want_partition_names = true;
            TGetPartialCatalogObjectResponse resp = sendRequest(req);
            checkResponse(resp.table_info != null && resp.table_info.partitions != null,
                req, "missing partition list result");
            List<PartitionRef> partitionRefs =
                Lists.newArrayListWithCapacity(resp.table_info.partitions.size());
            for (TPartialPartitionInfo p : resp.table_info.partitions) {
              checkResponse(
                  p.isSetId(), req, "response missing partition IDs for partition %s", p);
              partitionRefs.add(new PartitionRefImpl(p));
            }
            return partitionRefs;
          }
        });
  }

  @Override
  public SqlConstraints loadConstraints(
      final TableMetaRef table, Table msTbl) {
    return ((TableMetaRefImpl) table).sqlConstraints_;
  }

  @Override
  public Map<String, PartitionMetadata> loadPartitionsByRefs(TableMetaRef table,
      List<String> partitionColumnNames,
      ListMap<TNetworkAddress> hostIndex,
      List<PartitionRef> partitionRefs)
      throws MetaException, TException {
    Preconditions.checkArgument(table instanceof TableMetaRefImpl);
    TableMetaRefImpl refImpl = (TableMetaRefImpl)table;
    Stopwatch sw = Stopwatch.createStarted();
    // Load what we can from the cache.
    Map<PartitionRef, PartitionMetadata> refToMeta = loadPartitionsFromCache(refImpl,
        hostIndex, partitionRefs);
    if (BackendConfig.INSTANCE.isAutoCheckCompaction()) {
      // If any partitions are stale after compaction, they will be removed from refToMeta
      List<PartitionRef> stalePartitions =
          checkLatestCompaction(refImpl.dbName_, refImpl.tableName_, refImpl, refToMeta);
      cache_.invalidateAll(stalePartitions.stream()
          .map(PartitionRefImpl.class::cast)
          .map(PartitionRefImpl::getId)
          .map(PartitionCacheKey::new)
          .collect(Collectors.toList()));
      LOG.debug("Checked the latest compaction id for {}.{}", refImpl.dbName_,
          refImpl.tableName_);
    }

    final int numHits = refToMeta.size();
    final int numMisses = partitionRefs.size() - numHits;

    // Load the remainder from the catalogd.
    List<PartitionRef> missingRefs = new ArrayList<>();
    for (PartitionRef ref: partitionRefs) {
      if (!refToMeta.containsKey(ref)) missingRefs.add(ref);
    }
    if (!missingRefs.isEmpty()) {
      Map<PartitionRef, PartitionMetadata> fromCatalogd = loadPartitionsFromCatalogd(
          refImpl, hostIndex, missingRefs);
      refToMeta.putAll(fromCatalogd);
      // Write back to the cache.
      storePartitionsInCache(hostIndex, fromCatalogd);
    }
    sw.stop();
    addStatsToProfile(PARTITIONS_STATS_CATEGORY, numHits, numMisses, sw);
    LOG.trace("Request for partitions of {}: hit {}/{}", table, numHits,
        partitionRefs.size());

    // Convert the returned map to be by-name instead of by-ref.
    Map<String, PartitionMetadata> nameToMeta = Maps.newHashMapWithExpectedSize(
        refToMeta.size());
    for (Map.Entry<PartitionRef, PartitionMetadata> e: refToMeta.entrySet()) {
      nameToMeta.put(e.getKey().getName(), e.getValue());
    }
    return nameToMeta;
  }

  /**
   * Fetches the latest compactions from catalogd and compares with partition metadata in
   * cache. If a partition is stale due to compaction, removes it from metas. And return
   * the stale partitions.
   */
  private List<PartitionRef> checkLatestCompaction(String dbName, String tableName,
      TableMetaRef table, Map<PartitionRef, PartitionMetadata> metas) throws TException {
    Preconditions.checkNotNull(table, "TableMetaRef must be non-null");
    Preconditions.checkNotNull(metas, "Partition map must be non-null");
    if (!table.isTransactional() || metas.isEmpty()) return Collections.emptyList();
    Stopwatch sw = Stopwatch.createStarted();
    TGetLatestCompactionsRequest req = new TGetLatestCompactionsRequest();
    req.db_name = dbName;
    req.table_name = tableName;
    req.non_parition_name = HdfsTable.DEFAULT_PARTITION_NAME;
    if (table.isPartitioned()) {
      req.partition_names =
          metas.keySet().stream().map(PartitionRef::getName).collect(Collectors.toList());
    }
    req.last_compaction_id = metas.values()
                                 .stream()
                                 .mapToLong(PartitionMetadata::getLastCompactionId)
                                 .max()
                                 .orElse(-1);
    byte[] ret = FeSupport.GetLatestCompactions(
        new TSerializer(new TBinaryProtocol.Factory()).serialize(req));
    TGetLatestCompactionsResponse resp = new TGetLatestCompactionsResponse();
    new TDeserializer(new TBinaryProtocol.Factory()).deserialize(resp, ret);
    if (resp.status.status_code != TErrorCode.OK) {
      throw new TException(Joiner.on("\n").join(resp.status.getError_msgs()));
    }
    Map<String, Long> partNameToCompactionId = resp.partition_to_compaction_id;
    Preconditions.checkNotNull(
        partNameToCompactionId, "Partition name to compaction id map must be non-null");
    List<PartitionRef> stalePartitions = new ArrayList<>();
    Iterator<Map.Entry<PartitionRef, PartitionMetadata>> iter =
        metas.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<PartitionRef, PartitionMetadata> entry = iter.next();
      if (partNameToCompactionId.containsKey(entry.getKey().getName())) {
        stalePartitions.add(entry.getKey());
        iter.remove();
      }
    }
    LOG.info("Checked the latest compaction info for {}.{}. Time taken: {}", dbName,
        tableName, PrintUtils.printTimeMs(sw.stop().elapsed(TimeUnit.MILLISECONDS)));
    return stalePartitions;
  }

  /**
   * Load the specified partitions 'prefs' from catalogd. The partitions are made
   * relative to the given 'hostIndex' before being returned.
   */
  private Map<PartitionRef, PartitionMetadata> loadPartitionsFromCatalogd(
      TableMetaRefImpl table, ListMap<TNetworkAddress> hostIndex,
      List<PartitionRef> partRefs) throws TException {
    List<Long> ids = Lists.newArrayListWithCapacity(partRefs.size());
    for (PartitionRef partRef: partRefs) {
      ids.add(((PartitionRefImpl)partRef).getId());
    }

    TGetPartialCatalogObjectRequest req = newReqForTable(table);
    req.table_info_selector.partition_ids = ids;
    req.table_info_selector.want_partition_metadata = true;
    req.table_info_selector.want_partition_files = true;
    if (BackendConfig.INSTANCE.isAutoCheckCompaction()) {
      req.table_info_selector.valid_write_ids = table.validWriteIds_;
    }
    // TODO(todd): fetch incremental stats on-demand for compute-incremental-stats.
    req.table_info_selector.want_partition_stats = true;
    TGetPartialCatalogObjectResponse resp = sendRequest(req);
    checkResponse(resp.table_info != null && resp.table_info.partitions != null,
        req, "missing partition list result");
    checkResponse(resp.table_info.network_addresses != null,
        req, "missing network addresses");
    checkResponse(resp.table_info.partitions.size() == ids.size(),
        req, "returned %d partitions instead of expected %d",
        resp.table_info.partitions.size(), ids.size());
    addTableMetadatStorageLoadTimeToProfile(
        resp.table_info.storage_metadata_load_time_ns);
    Map<PartitionRef, PartitionMetadata> ret = new HashMap<>();
    for (int i = 0; i < ids.size(); i++) {
      PartitionRef partRef = partRefs.get(i);
      TPartialPartitionInfo part = resp.table_info.partitions.get(i);
      HdfsStorageDescriptor hdfsStorageDescriptor = null;
      HdfsPartitionLocationCompressor.Location location;
      if (part.isSetHdfs_storage_descriptor()) {
        Preconditions.checkNotNull(part.location, "location should not be null");
        hdfsStorageDescriptor = HdfsStorageDescriptor.fromThrift(
            part.hdfs_storage_descriptor, table.tableName_);
        location = table.getPartitionLocationCompressor().new Location(part.location);
      } else {
        checkResponse(table.msTable_.getPartitionKeysSize() == 0, req,
            "Should not return a partition with missing partition meta unless " +
            "the table is unpartitioned");
        // For the only partition of a nonpartitioned table, reuse table-level metadata.
        try {
          hdfsStorageDescriptor = HdfsStorageDescriptor.fromStorageDescriptor(
              table.tableName_, table.msTable_.getSd());
        } catch (HdfsStorageDescriptor.InvalidStorageDescriptorException e) {
          Preconditions.checkState(false, "Failed to create HdfsStorageDescriptor " +
              "using sd of table");
        }
        location = table.getPartitionLocationCompressor().new Location(
            table.msTable_.getSd().getLocation());
        part.setHms_parameters(table.msTable_.getParameters());
      }

      // Transform the file descriptors to the caller's index.
      checkResponse(part.file_descriptors != null, req, "missing file descriptors");
      ImmutableList<FileDescriptor> fds = convertThriftFdList(part.file_descriptors,
          resp.table_info.network_addresses, hostIndex);
      ImmutableList<FileDescriptor> insertFds = convertThriftFdList(
          part.insert_file_descriptors, resp.table_info.network_addresses, hostIndex);
      ImmutableList<FileDescriptor> deleteFds = convertThriftFdList(
          part.delete_file_descriptors, resp.table_info.network_addresses, hostIndex);
      PartitionMetadataImpl metaImpl = new PartitionMetadataImpl(part.getHms_parameters(),
          part.write_id, hdfsStorageDescriptor,
          fds, insertFds, deleteFds, part.getPartition_stats(),
          part.has_incremental_stats, part.is_marked_cached, location,
          part.last_compaction_id);

      checkResponse(partRef != null, req, "returned unexpected partition id %s", part.id);

      PartitionMetadata oldVal = ret.put(partRef, metaImpl);
      if (oldVal != null) {
        throw new RuntimeException("catalogd returned partition " + part.id +
            " multiple times");
      }
    }
    return ret;
  }

  @Override
  public TPartialTableInfo loadIcebergTable(final TableMetaRef table) throws TException {
    Preconditions.checkArgument(table instanceof TableMetaRefImpl);
    TableMetaRefImpl tableRef = (TableMetaRefImpl)table;
    String itemStr = "iceberg metadata for " + tableRef.dbName_ + "."
        + tableRef.tableName_;
    IcebergMetaCacheKey cacheKey = new IcebergMetaCacheKey(tableRef);
    return loadWithCaching(itemStr, TABLE_METADATA_CACHE_CATEGORY, cacheKey,
        new Callable<TPartialTableInfo>() {
          @Override
          public TPartialTableInfo call() throws Exception {
            TGetPartialCatalogObjectRequest req = newReqForTable(table);
            req.table_info_selector.want_iceberg_table = true;
            TGetPartialCatalogObjectResponse resp = sendRequest(req);
            checkResponse(resp.table_info != null &&
                resp.table_info.iceberg_table != null, req,
                "missing Iceberg table metadata");
            return resp.getTable_info();
          }
    });
  }

  @Override
  public org.apache.iceberg.Table loadIcebergApiTable(final TableMetaRef table,
      TableParams params, Table msTable) throws TException {
    Preconditions.checkArgument(table instanceof TableMetaRefImpl);
    TableMetaRefImpl tableRef = (TableMetaRefImpl)table;
    String itemStr = "iceberg api table for " + tableRef.dbName_ + "."
        + tableRef.tableName_;
    IcebergApiTableCacheKey cacheKey = new IcebergApiTableCacheKey(tableRef);
    return loadWithCaching(itemStr, TABLE_METADATA_CACHE_CATEGORY, cacheKey,
        new Callable<org.apache.iceberg.Table>() {
          @Override
          public org.apache.iceberg.Table call() throws Exception {
            return IcebergUtil.loadTable(
                params.getIcebergCatalog(),
                IcebergUtil.getIcebergTableIdentifier(msTable),
                params.getIcebergCatalogLocation(),
                msTable.getParameters());
          }
        });
    }

  private ImmutableList<FileDescriptor> convertThriftFdList(List<THdfsFileDesc> thriftFds,
      List<TNetworkAddress> networkAddresses, ListMap<TNetworkAddress> hostIndex) {
    List<FileDescriptor> fds = Lists.newArrayListWithCapacity(thriftFds.size());
    for (THdfsFileDesc thriftFd: thriftFds) {
      FileDescriptor fd = FileDescriptor.fromThrift(thriftFd);
      // The file descriptors returned via the RPC use host indexes that reference
      // the 'network_addresses' list in the RPC. However, the caller may have already
      // loaded some addresses into 'hostIndex'. So, the returned FDs need to be
      // remapped to point to the caller's 'hostIndex' instead of the list in the
      // RPC response.
      fds.add(fd.cloneWithNewHostIndex(networkAddresses, hostIndex));
    }
    return ImmutableList.copyOf(fds);
  }

  /**
   * Load all partitions from 'partitionRefs' that are currently present in the cache.
   * Any partitions that miss the cache are left unset in the resulting map.
   *
   * The FileDescriptors of the resulting partitions are copied and made relative to
   * the provided hostIndex.
   */
  private Map<PartitionRef, PartitionMetadata> loadPartitionsFromCache(
      TableMetaRefImpl table, ListMap<TNetworkAddress> hostIndex,
      List<PartitionRef> partitionRefs) throws TException {

    Map<PartitionRef, PartitionMetadata> ret = Maps.newHashMapWithExpectedSize(
        partitionRefs.size());
    for (PartitionRef ref: partitionRefs) {
      PartitionRefImpl prefImpl = (PartitionRefImpl)ref;
      PartitionCacheKey cacheKey = new PartitionCacheKey(prefImpl.getId());
      PartitionMetadataImpl val = (PartitionMetadataImpl)getIfPresent(cacheKey);
      if (val == null) continue;

      // The entry in the cache has file descriptors that are relative to the cache's
      // host index, rather than the caller's host index. So, we need to transform them.
      ret.put(ref, val.cloneRelativeToHostIndex(cacheHostIndex_, hostIndex));
    }
    return ret;
  }


  /**
   * Write back the partitions in 'metas' into the cache. The file descriptors in these
   * partitions must be relative to the 'hostIndex'.
   */
  private void storePartitionsInCache(ListMap<TNetworkAddress> hostIndex,
      Map<PartitionRef, PartitionMetadata> metas) {
    for (Map.Entry<PartitionRef, PartitionMetadata> e: metas.entrySet()) {
      PartitionRefImpl prefImpl = (PartitionRefImpl)e.getKey();
      PartitionMetadataImpl metaImpl = (PartitionMetadataImpl)e.getValue();
      PartitionCacheKey cacheKey = new PartitionCacheKey(prefImpl.getId());
      PartitionMetadataImpl cacheVal = metaImpl.cloneRelativeToHostIndex(hostIndex,
          cacheHostIndex_);
      cache_.put(cacheKey, cacheVal);
    }
  }

  private static void checkResponse(boolean condition,
      TGetPartialCatalogObjectRequest req, String msg, Object... args) throws TException {
    if (condition) return;
    throw new TException(String.format("Invalid response from catalogd for request " +
        req.toString() + ": " + msg, args));
  }

  @Override
  public String loadNullPartitionKeyValue() throws MetaException, TException {
    return (String) loadWithCaching("null partition key value",
        GLOBAL_CONFIGURATION_STATS_CATEGORY,
        NULL_PARTITION_KEY_VALUE_CACHE_KEY,
        new Callable<String>() {
          /** Called to load cache for cache misses */
          @Override
          public String call() throws Exception {
            return FeSupport.GetNullPartitionName();
          }
        });
  }

  @Override
  public List<String> loadFunctionNames(final String dbName) throws TException {
    return loadWithCaching("function names for database " + dbName,
        FUNCTION_LIST_STATS_CATEGORY,
        new DbCacheKey(dbName, DbCacheKey.DbInfoType.FUNCTION_NAMES),
        new Callable<ImmutableList<String>>() {
          @Override
          public ImmutableList<String> call() throws Exception {
            TGetPartialCatalogObjectRequest req = newReqForDb(dbName);
            req.db_info_selector.want_function_names = true;
            TGetPartialCatalogObjectResponse resp = sendRequest(req);
            checkResponse(resp.db_info != null && resp.db_info.function_names != null,
                req, "missing expected function names");
            return ImmutableList.copyOf(resp.db_info.function_names);
          }
      });
  }

  @Override
  public ImmutableList<Function> loadFunction(final String dbName,
      final String functionName) throws TException {
    ImmutableList<TFunction> thriftFuncs = loadWithCaching(
        "function " + dbName + "." + functionName,
        FUNCTIONS_STATS_CATEGORY,
        new FunctionsCacheKey(dbName, functionName),
        new Callable<ImmutableList<TFunction>>() {
          @Override
          public ImmutableList<TFunction> call() throws Exception {
            TGetPartialCatalogObjectRequest req = newReqForFunction(dbName, functionName);
            TGetPartialCatalogObjectResponse resp = sendRequest(req);
            checkResponse(resp.functions != null, req, "missing expected function");
            return ImmutableList.copyOf(resp.functions);
          }
      });
    // It may seem wasteful to cache the thrift function objects and then always
    // convert them back to non-Thrift 'Functions' on every load. However, loading
    // functions is rare enough and this ensures we don't accidentally leak something
    // mutable back to the catalog layer. If this turns out to be a problem we can
    // consider wrapping Function with an immutable 'FeFunction' interface.
    ImmutableList.Builder<Function> funcs = ImmutableList.builder();
    for (TFunction thriftFunc : thriftFuncs) {
      funcs.add(Function.fromThrift(thriftFunc));
    }
    return funcs.build();
  }

  /**
   * Get all DataSource objects from catalogd.
   */
  public ImmutableList<DataSource> loadDataSources() throws TException {
    ImmutableList<TDataSource> thriftDataSrcs = loadWithCaching(
        "DataSource object list", DATA_SOURCE_LIST_STATS_CATEGORY,
        DS_OBJ_LIST_CACHE_KEY, new Callable<ImmutableList<TDataSource>>() {
          @Override
          public ImmutableList<TDataSource> call() throws Exception {
            TGetPartialCatalogObjectRequest req = newReqForDataSource(null);
            TGetPartialCatalogObjectResponse resp = sendRequest(req);
            checkResponse(resp.data_srcs != null, req, "no existing DataSource");
            return ImmutableList.copyOf(resp.data_srcs);
          }
      });
    ImmutableList.Builder<DataSource> dataSrcBld = ImmutableList.builder();
    for (TDataSource thriftDataSrc : thriftDataSrcs) {
      dataSrcBld.add(DataSource.fromThrift(thriftDataSrc));
    }
    return dataSrcBld.build();
  }

  /**
   * Get the DataSource object from catalogd for the given DataSource name.
   */
  public DataSource loadDataSource(String dsName) throws TException {
    Preconditions.checkState(dsName != null && !dsName.isEmpty());
    return loadWithCaching("DataSource object for " + dsName,
        DATA_SOURCE_LIST_STATS_CATEGORY,
        new DataSourceCacheKey(dsName.toLowerCase()), new Callable<DataSource>() {
          @Override
          public DataSource call() throws Exception {
            TGetPartialCatalogObjectRequest req = newReqForDataSource(dsName);
            TGetPartialCatalogObjectResponse resp = sendRequest(req);
            checkResponse(resp.data_srcs != null, req, "missing expected DataSource");
            if (resp.data_srcs.size() == 1) {
              return DataSource.fromThrift(resp.data_srcs.get(0));
            } else {
              Preconditions.checkState(resp.data_srcs.size() == 0);
              return null;
            }
          }
      });
  }

  /**
   * Invalidate portions of the cache as indicated by the provided request.
   *
   * This is called in two scenarios:
   *
   * 1) after a user DDL, the catalog returns a list of updated objects. This ensures
   * that the impalad that issued the DDL can immediately invalidate its cache for the
   * modified objects and will see the effects immediately.
   *
   * 2) catalog topic updates are received via the statestore. These topic updates
   * indicate that the catalogd representation of the object has changed and therefore
   * needs to be invalidated in the impalad.
   */
  public synchronized TUpdateCatalogCacheResponse updateCatalogCache(
      TUpdateCatalogCacheRequest req) {
    if (req.isSetCatalog_service_id()) {
      // Requests from processing statestore updates won't have this. Only requests from
      // DDLs will set this. See more details in ImpalaServer::ProcessCatalogUpdateResult.
      witnessCatalogServiceId(req.catalog_service_id);
    }

    // We might see a top-level catalog version number while visiting the objects. If so,
    // we'll capture it here and process it down at the end after applying all other
    // objects.
    Long nextCatalogVersion = null;

    ObjectUpdateSequencer authObjectSequencer = new ObjectUpdateSequencer();
    ObjectUpdateSequencer hdfsCachePoolSequencer = new ObjectUpdateSequencer();

    Pair<Boolean, ByteBuffer> update;
    int maxMessageSize = BackendConfig.INSTANCE.getThriftRpcMaxMessageSize();
    final TConfiguration config = new TConfiguration(maxMessageSize,
        TConfiguration.DEFAULT_MAX_FRAME_SIZE, TConfiguration.DEFAULT_RECURSION_DEPTH);
    while ((update = FeSupport.NativeGetNextCatalogObjectUpdate(req.native_iterator_ptr))
        != null) {
      boolean isDelete = update.first;
      TCatalogObject obj = new TCatalogObject();
      try {
        obj.read(new TBinaryProtocol(new TByteBuffer(config, update.second)));
      } catch (TException e) {
        // TODO(todd) include the bad key here! currently the JNI bridge doesn't expose
        // the key in any way.
        LOG.warn("Unable to deserialize updated catalog info. Skipping cache " +
            "invalidation which may result in stale metadata being used at this " +
            "coordinator.", e);
        continue;
      }
      if (isDelete) {
        deletedObjectsLog_.addRemovedObject(obj);
      } else if (deletedObjectsLog_.wasObjectRemovedAfter(obj)) {
        LOG.trace("Skipping update because a matching object was removed " +
            "in a later catalog version: {}", obj);
        continue;
      }

      if (!isDelete && obj.type == TCatalogObjectType.HDFS_PARTITION) {
        // Skip if this is the update for a new partition.
        if (!obj.hdfs_partition.isSetPrev_id()) continue;
        // This is an update from an existing partiton. Invalidate the previous partition
        // instance by resetting the id to the previous one.
        obj.hdfs_partition.setId(obj.hdfs_partition.prev_id);
        obj.hdfs_partition.unsetPrev_id();
      }
      invalidateCacheForObject(obj);

      if (obj.type == TCatalogObjectType.HDFS_CACHE_POOL) {
        // HdfsCachePools have no dependency to each other. But we can't update
        // hdfsCachePools_ directly since a new catalog service id (due to catalogd
        // restart) will cause hdfsCachePools_ to be clear (see mode details in
        // witnessCatalogServiceId()). So we have to deal with HDFS_CACHE_POOL objects
        // after the CATALOG object.
        hdfsCachePoolSequencer.add(obj, isDelete);
        continue;
      }

      // The sequencing of updates to authorization objects is important since they
      // may be cross-referential. So, just add them to the sequencer which ensures
      // we handle them in the right order later.
      if (obj.type == TCatalogObjectType.PRINCIPAL ||
          obj.type == TCatalogObjectType.PRIVILEGE ||
          obj.type == TCatalogObjectType.AUTHZ_CACHE_INVALIDATION) {
        authObjectSequencer.add(obj, isDelete);
        continue;
      }

      // Handle CATALOG objects. These are sent only via the updates published via
      // the statestore topic, and not via the synchronous updates returned from DDLs.
      if (obj.type == TCatalogObjectType.CATALOG) {
        // The top-level CATALOG object version is used to implement SYNC_DDL. We need
        // to keep track of this and pass it back to the C++ code in the return value
        // of this call. This is also used to know when the catalog is ready at
        // startup.
        nextCatalogVersion = obj.catalog_version;
        witnessCatalogServiceId(obj.catalog.catalog_service_id);
        long resetStartVersion = obj.catalog.last_reset_catalog_version;
        if (lastResetCatalogVersion_.getAndSet(resetStartVersion) != resetStartVersion) {
          // Detected a new reset() finishes in Catalogd, clear the cache in case some
          // tables are skipped in this topic update.
          cache_.invalidateAll();
          // Don't need to clear hdfsCachePools_ if this comes from a catalogd restart,
          // because we already clear it in witnessCatalogServiceId().
          // Shouldn't clear hdfsCachePools_ if this comes from a global invalidation,
          // because HdfsCachePool updates may already come in the previous statestore
          // update (see how the version lock is held in CatalogServiceCatalog.reset()).
          // Clear hdfsCachePools_ will also clear them.
        }
      }
    }

    for (TCatalogObject obj : hdfsCachePoolSequencer.getUpdatedObjects()) {
      updateHdfsCachePools(obj, /*isDelete=*/false);
    }
    for (TCatalogObject obj : hdfsCachePoolSequencer.getDeletedObjects()) {
      updateHdfsCachePools(obj, /*isDelete=*/true);
    }

    for (TCatalogObject obj : authObjectSequencer.getUpdatedObjects()) {
      updateAuthPolicy(obj, /*isDelete=*/false);
    }
    for (TCatalogObject obj : authObjectSequencer.getDeletedObjects()) {
      updateAuthPolicy(obj, /*isDelete=*/true);
    }

    deletedObjectsLog_.garbageCollect(lastSeenCatalogVersion_.get());

    // NOTE: it's important to defer setting the new catalog version until the
    // end of the loop, since the CATALOG object might be one of the first objects
    // processed, and we don't want to prematurely indicate that we are done processing
    // the update.
    if (nextCatalogVersion != null) {
      lastSeenCatalogVersion_.set(nextCatalogVersion);
    }

    // NOTE: the return value is ignored when this function is called by a DDL
    // operation.
    synchronized (catalogServiceIdLock_) {
      // Set catalog_object_version_lower_bound to lastResetCatalogVersion_ + 1. All
      // catalog objects with catalog version <= lastResetCatalogVersion_ should have
      // been invalidated. See more comments above the definition of
      // lastResetCatalogVersion_.
      return new TUpdateCatalogCacheResponse(catalogServiceId_,
          lastResetCatalogVersion_.get() + 1, lastSeenCatalogVersion_.get());
    }
  }

  private void updateHdfsCachePools(TCatalogObject obj, boolean isDelete) {
    Preconditions.checkState(obj.type == TCatalogObjectType.HDFS_CACHE_POOL);
    String poolName = obj.getCache_pool().getPool_name();
    if (isDelete) {
      HdfsCachePool existingItem = hdfsCachePools_.get(poolName);
      if (existingItem != null
          && existingItem.getCatalogVersion() <= obj.getCatalog_version()) {
        hdfsCachePools_.remove(poolName);
        LOG.trace("Removed HdfsCachePool {}", poolName);
      }
      return;
    }
    HdfsCachePool cachePool = new HdfsCachePool(obj.getCache_pool());
    cachePool.setCatalogVersion(obj.getCatalog_version());
    if (hdfsCachePools_.add(cachePool)) {
      LOG.trace("Added HdfsCachePool name={}, version={}", poolName,
          obj.getCatalog_version());
      return;
    }
    LOG.warn("Ignored stale HdfsCachePool update: name={}, version={}", poolName,
        obj.getCatalog_version());
  }

  private void updateAuthPolicy(TCatalogObject obj, boolean isDelete) {
    LOG.trace("Updating authorization policy: {} isDelete={}", obj, isDelete);
    switch (obj.type) {
    case PRINCIPAL:
      if (!isDelete) {
        Principal principal = Principal.fromThrift(obj.getPrincipal());
        principal.setCatalogVersion(obj.getCatalog_version());
        authPolicy_.addPrincipal(principal);
      } else {
        authPolicy_.removePrincipalIfLowerVersion(obj.getPrincipal(),
            obj.getCatalog_version());
      }
      break;
    case PRIVILEGE:
      if (!isDelete) {
        // TODO(todd): duplicate code from ImpaladCatalog.
        PrincipalPrivilege privilege =
            PrincipalPrivilege.fromThrift(obj.getPrivilege());
        privilege.setCatalogVersion(obj.getCatalog_version());
        try {
          authPolicy_.addPrivilege(privilege);
        } catch (CatalogException e) {
          // TODO(todd) it's odd that we swallow this error, both here and in
          // the original code in ImpaladCatalog.
          LOG.error("Error adding privilege: ", e);
        }
      } else {
        authPolicy_.removePrivilegeIfLowerVersion(obj.getPrivilege(),
            obj.getCatalog_version());
      }
      break;
      case AUTHZ_CACHE_INVALIDATION:
      if (!isDelete) {
        AuthzCacheInvalidation authzCacheInvalidation = new AuthzCacheInvalidation(
            obj.getAuthz_cache_invalidation());
        authzCacheInvalidation.setCatalogVersion(obj.getCatalog_version());
        authzCacheInvalidation_.add(authzCacheInvalidation);
        Preconditions.checkState(authzChecker_ != null);
        authzChecker_.get().invalidateAuthorizationCache();
      } else {
        authzCacheInvalidation_.remove(obj.getAuthz_cache_invalidation()
            .getMarker_name());
      }
      break;
    default:
        throw new IllegalArgumentException("invalid type: " + obj.type);
    }
  }

  /**
   * Witness a service ID received from the catalog. We can see the service IDs
   * either from a DDL response (in which case the service ID is part of the RPC
   * response object) or from a statestore topic update (in which case the service ID
   * is part of the published CATALOG object).
   *
   * If we notice the service ID changed, we need to invalidate our cache.
   */
  private void witnessCatalogServiceId(TUniqueId serviceId) {
    synchronized (catalogServiceIdLock_) {
      if (!catalogServiceId_.equals(serviceId)) {
        if (!catalogServiceId_.equals(Catalog.INITIAL_CATALOG_SERVICE_ID)) {
          LOG.warn("Detected catalog service restart: service ID changed from " +
              "{} to {}. Invalidating all cached metadata on this coordinator.",
              catalogServiceId_, serviceId);
        }
        catalogServiceId_ = serviceId;
        cache_.invalidateAll();
        // Clear cached items from the previous catalogd instance. Otherwise, we'll
        // ignore new updates from the new catalogd instance since they have lower
        // versions.
        hdfsCachePools_.clear();
        // TODO(todd): we probably need to invalidate the auth policy too.
        // we are probably better off detecting this at a higher level and
        // reinstantiating the metaprovider entirely, similar to how ImpaladCatalog
        // handles this.

        // TODO(todd): slight race here: a concurrent request from the old catalog
        // could theoretically be just about to write something back into the cache
        // after we do the above invalidate. Maybe we would be better off replacing
        // the whole cache object, or doing a soft barrier here to wait for any
        // concurrent cache accessors to cycle out. Another option is to associate
        // the catalog service ID as part of all of the cache keys.
        //
        // This is quite unlikely to be an issue in practice, so deferring it to later
        // clean-up.
      }
    }
  }

  /**
   * Invalidate items from the cache in response to seeing an updated catalog object
   * from the catalogd or getting an error response from another request that indicates
   * that the object has been removed.
   */
  @VisibleForTesting
  void invalidateCacheForObject(TCatalogObject obj) {
    List<String> invalidated = new ArrayList<>();
    switch (obj.type) {
    case TABLE:
    case VIEW:
      invalidateCacheForTable(obj.table.db_name, obj.table.tbl_name, invalidated);

      // Currently adding or dropping a table doesn't send an invalidation for the
      // DB, so we'll be coarse-grained here and invalidate the DB table list when
      // any table change happens. It's relatively cheap to re-fetch this.
      invalidateCacheForDb(obj.table.db_name,
          ImmutableList.of(DbCacheKey.DbInfoType.TABLE_LIST),
          invalidated);
      break;
    case HDFS_PARTITION:
      invalidateCacheForPartition(obj.hdfs_partition.db_name, obj.hdfs_partition.tbl_name,
          obj.hdfs_partition.partition_name, obj.hdfs_partition.id, invalidated);
      break;
    case FUNCTION:
      // Same as above: if we see a function, it might be new or deleted and we should
      // refresh the list of functions in the DB to be safe.
      invalidateCacheForDb(obj.fn.name.db_name,
          ImmutableList.of(DbCacheKey.DbInfoType.FUNCTION_NAMES),
          invalidated);
      invalidateCacheForFunction(obj.fn.name.db_name, obj.fn.name.function_name,
          invalidated);
      if (obj.fn.hdfs_location != null) {
        // After the coordinator creates a function, it will also receive an invalidation
        // update for this function from the statestored's broadcast. We shouldn't remove
        // the libcache entry for this case, just mark it as needs refresh. LibCache will
        // refresh the cached file if its mtime changes in HDFS.
        FeSupport.NativeLibCacheSetNeedsRefresh(obj.fn.hdfs_location);
      }
      break;
    case DATABASE:
      if (cache_.asMap().remove(DB_LIST_CACHE_KEY) != null) {
        invalidated.add("list of database names");
      }
      invalidateCacheForDb(obj.db.db_name, ImmutableList.of(
          DbCacheKey.DbInfoType.TABLE_LIST,
          DbCacheKey.DbInfoType.HMS_METADATA,
          DbCacheKey.DbInfoType.FUNCTION_NAMES), invalidated);
      break;
    case DATA_SOURCE:
      if (cache_.asMap().remove(DS_OBJ_LIST_CACHE_KEY) != null) {
        invalidated.add("list of DataSource objects");
      }
      invalidateCacheForDataSource(obj.data_source.name, invalidated);
      break;
    default:
      break;
    }
    if (!invalidated.isEmpty()) {
      LOG.debug("Invalidated objects in cache: {}", invalidated);
    }
  }

  /**
   * Invalidate cached metadata of the given types for the given database. If anything
   * was invalidated, adds a human-readable string to 'invalidated' indicating the
   * invalidated metadata.
   */
  private void invalidateCacheForDb(String dbName, Iterable<DbCacheKey.DbInfoType> types,
      List<String> invalidated) {
    for (DbCacheKey.DbInfoType type: types) {
      DbCacheKey key = new DbCacheKey(dbName.toLowerCase(), type);
      if (cache_.asMap().remove(key) != null) {
        invalidated.add(type + " for DB " + dbName);
      }
    }
  }

  /**
   * Invalidate cached metadata for the given table. If anything was invalidated, adds
   * a human-readable string to 'invalidated' indicating the invalidated metadata.
   */
  private void invalidateCacheForTable(String dbName, String tblName,
      List<String> invalidated) {
    TableCacheKey key = new TableCacheKey(dbName.toLowerCase(), tblName.toLowerCase());
    if (cache_.asMap().remove(key) != null) {
      invalidated.add("table " + dbName + "." + tblName);
    }
  }

  /**
   * Invalidate cached metadata for the given partition. If anything was invalidated, adds
   * a human-readable string to 'invalidated' indicating the invalidated metadata.
   */
  private void invalidateCacheForPartition(String dbName, String tblName, String partName,
      long partitionId, List<String> invalidated) {
    PartitionCacheKey key = new PartitionCacheKey(partitionId);
    if (cache_.asMap().remove(key) != null) {
      invalidated.add(String.format("partition %s.%s:%s (id=%d)",
          dbName, tblName, partName, partitionId));
    }
  }

  /**
   * Invalidate cached metadata for the given function. If anything was invalidated, adds
   * a human-readable string to 'invalidated' indicating the invalidated metadata.
   */
  private void invalidateCacheForFunction(String dbName, String functionName,
      List<String> invalidated) {
    // TODO(todd) check whether we need to lower-case/canonicalize names?
    FunctionsCacheKey key = new FunctionsCacheKey(dbName, functionName);
    if (cache_.asMap().remove(key) != null) {
      invalidated.add("function " + dbName + "." + functionName);
    }
  }

  /**
   * Invalidate cached DataSource for the given DataSource name. If anything was
   * invalidated, adds a human-readable string to 'invalidated' indicating the
   * invalidated DataSource.
   */
  private void invalidateCacheForDataSource(String dsName, List<String> invalidated) {
    DataSourceCacheKey key = new DataSourceCacheKey(dsName.toLowerCase());
    if (cache_.asMap().remove(key) != null) {
      invalidated.add("DataSource " + dsName);
    }
  }

  /**
   * Reference to a partition within a table. We remember the partition's ID and pass
   * that back to the catalog in subsequent requests back to fetch the details of the
   * partition, since the ID is smaller than the name and provides a unique (not-reused)
   * identifier.
   */
  @Immutable
  private static class PartitionRefImpl implements PartitionRef {
    @SuppressWarnings("Immutable") // Thrift objects are mutable, but we won't mutate it.
    private final TPartialPartitionInfo info_;

    public PartitionRefImpl(TPartialPartitionInfo p) {
      this.info_ = Preconditions.checkNotNull(p);
    }

    @Override
    public String getName() {
      return info_.getName();
    }

    private long getId() {
      return info_.id;
    }
  }

  public static class PartitionMetadataImpl implements PartitionMetadata {
    private final Map<String, String> hmsParameters_;
    private final long writeId_;
    private final HdfsStorageDescriptor hdfsStorageDescriptor_;
    // Prefix-compressed location. The corresponding prefix map, i.e. the
    // HdfsPartitionLocationCompressor object is in the corresponding TableMetaRefImpl
    // object. TableMetaRefImpl may be evicted. But when this partition is accessed,
    // we can assure that the table meta is already loaded, so the prefix map won't be
    // missing.
    private final HdfsPartitionLocationCompressor.Location location_;
    private final ImmutableList<FileDescriptor> fds_;
    private final ImmutableList<FileDescriptor> insertFds_;
    private final ImmutableList<FileDescriptor> deleteFds_;
    private final byte[] partitionStats_;
    private final boolean hasIncrementalStats_;
    private final boolean isMarkedCached_;
    private final long lastCompactionId_;

    public PartitionMetadataImpl(Map<String, String> hmsParameters, long writeId,
        HdfsStorageDescriptor hdfsStorageDescriptor, ImmutableList<FileDescriptor> fds,
        ImmutableList<FileDescriptor> insertFds, ImmutableList<FileDescriptor> deleteFds,
        byte[] partitionStats, boolean hasIncrementalStats, boolean isMarkedCached,
        HdfsPartitionLocationCompressor.Location location, long lastCompactionId) {
      this.hmsParameters_ = hmsParameters;
      this.writeId_ = writeId;
      this.hdfsStorageDescriptor_ = hdfsStorageDescriptor;
      this.location_ = location;
      this.fds_ = fds;
      this.insertFds_ = insertFds;
      this.deleteFds_ = deleteFds;
      this.partitionStats_ = partitionStats;
      this.hasIncrementalStats_ = hasIncrementalStats;
      this.isMarkedCached_ = isMarkedCached;
      this.lastCompactionId_ = lastCompactionId;
    }

    /**
     * Clone this metadata object, but make it relative to 'dstIndex' instead of
     * 'origIndex'.
     */
    public PartitionMetadataImpl cloneRelativeToHostIndex(
        ListMap<TNetworkAddress> origIndex,
        ListMap<TNetworkAddress> dstIndex) {
      ImmutableList<FileDescriptor> fds = cloneFdsRelativeToHostIndex(
          fds_, origIndex, dstIndex);
      ImmutableList<FileDescriptor> insertFds = cloneFdsRelativeToHostIndex(
          insertFds_, origIndex, dstIndex);
      ImmutableList<FileDescriptor> deleteFds = cloneFdsRelativeToHostIndex(
          deleteFds_, origIndex, dstIndex);
      return new PartitionMetadataImpl(hmsParameters_, writeId_, hdfsStorageDescriptor_,
          fds, insertFds, deleteFds, partitionStats_, hasIncrementalStats_,
          isMarkedCached_, location_, lastCompactionId_);
    }

    private static ImmutableList<FileDescriptor> cloneFdsRelativeToHostIndex(
        ImmutableList<FileDescriptor> fds, ListMap<TNetworkAddress> origIndex,
        ListMap<TNetworkAddress> dstIndex) {
      List<FileDescriptor> ret = Lists.newArrayListWithCapacity(fds.size());
      for (FileDescriptor fd: fds) {
        ret.add(fd.cloneWithNewHostIndex(origIndex.getList(), dstIndex));
      }
      return ImmutableList.copyOf(ret);
    }

    @Override
    public Map<String, String> getHmsParameters() { return hmsParameters_; }

    @Override
    public long getWriteId() { return writeId_; }

    @Override
    public HdfsStorageDescriptor getInputFormatDescriptor() {
      return hdfsStorageDescriptor_;
    }

    @Override
    public HdfsPartitionLocationCompressor.Location getLocation() { return location_; }

    @Override
    public ImmutableList<FileDescriptor> getFileDescriptors() {
      if (insertFds_.isEmpty()) return fds_;
      List<FileDescriptor> ret = Lists.newArrayListWithCapacity(
          insertFds_.size() + deleteFds_.size());
      ret.addAll(insertFds_);
      ret.addAll(deleteFds_);
      return ImmutableList.copyOf(ret);
    }

    @Override
    public ImmutableList<FileDescriptor> getInsertFileDescriptors() {
      return insertFds_;
    }

    @Override
    public ImmutableList<FileDescriptor> getDeleteFileDescriptors() {
      return deleteFds_;
    }

    @Override
    public byte[] getPartitionStats() { return partitionStats_; }

    @Override
    public boolean hasIncrementalStats() { return hasIncrementalStats_; }

    @Override
    public boolean isMarkedCached() { return isMarkedCached_; }

    @Override
    public long getLastCompactionId() { return lastCompactionId_; }
  }

  /**
   * A reference to a table that has been looked up, allowing callers to fetch further
   * detailed information. This is is more extensive than just the table name so that
   * we can provide a consistency check that the catalog version doesn't change in
   * between calls.
   */
  private static class TableMetaRefImpl implements TableMetaRef {
    private final String dbName_;
    private final String tableName_;
    // SQL constraints for the table, populated during loadTable().
    private final SqlConstraints sqlConstraints_;

    /**
     * Stash the HMS Table object since we need this in order to handle some strange
     * behavior whereby the catalogd returns a Partition with no HMS partition object
     * in the case of unpartitioned tables.
     */
    private final Table msTable_;

    /**
     * List of virtual columns of this table.
     */
    List<VirtualColumn> virtualColumns_ = new ArrayList<>();

    /**
     * The version of the table when we first loaded it. Subsequent requests about
     * the table are verified against this version.
     */
    private final long catalogVersion_;

    /**
     * Valid write id list of ACID tables.
     */
    private final TValidWriteIdList validWriteIds_;

    /**
     * True if this table's is marked as cached by hdfs caching. See comments in
     * LocalFsTable.
     */
    private final boolean isMarkedCached_;

    private final HdfsPartitionLocationCompressor partitionLocationCompressor_;

    public TableMetaRefImpl(String dbName, String tableName,
        Table msTable, long catalogVersion, SqlConstraints sqlConstraints,
        TValidWriteIdList validWriteIds, boolean isMarkedCached,
        List<String> locationPrefixes, List<TColumn> tvirtCols) {
      this.dbName_ = dbName;
      this.tableName_ = tableName;
      this.msTable_ = msTable;
      this.catalogVersion_ = catalogVersion;
      this.sqlConstraints_ = sqlConstraints;
      this.validWriteIds_ = validWriteIds;
      this.isMarkedCached_ = isMarkedCached;
      // Non-hdfs tables will have null locationPrefixes.
      this.partitionLocationCompressor_ = (locationPrefixes == null) ? null :
          new HdfsPartitionLocationCompressor(
              msTable.getPartitionKeysSize(), locationPrefixes);
      for (TColumn tvCol : tvirtCols) {
        virtualColumns_.add(VirtualColumn.fromThrift(tvCol));
      }
    }

    @Override
    public String toString() {
      return String.format("TableMetaRef %s.%s@%d", dbName_, tableName_, catalogVersion_);
    }

    @Override
    public boolean isPartitioned() {
      return msTable_.getPartitionKeysSize() != 0;
    }

    @Override
    public boolean isMarkedCached() { return isMarkedCached_; }

    public HdfsPartitionLocationCompressor getPartitionLocationCompressor() {
      return partitionLocationCompressor_;
    }

    public List<String> getPartitionPrefixes() {
      return partitionLocationCompressor_.getPrefixes();
    }

    @Override
    public boolean isTransactional() {
      return AcidUtils.isTransactionalTable(msTable_.getParameters());
    }

    @Override
    public List<VirtualColumn> getVirtualColumns() {
      return virtualColumns_;
    }
  }

  /**
   * Base class for cache key for a named item within a database (eg table or function).
   */
  private static class DbChildCacheKey {
    final String dbName_;
    final String childName_;

    protected DbChildCacheKey(String dbName, String childName) {
      this.dbName_ = dbName;
      this.childName_ = childName;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(dbName_, childName_, getClass());
    }
    @Override
    public boolean equals(Object obj) {
      if (obj == null || !obj.getClass().equals(getClass())) {
        return false;
      }
      DbChildCacheKey other = (DbChildCacheKey)obj;
      return childName_.equals(other.childName_) &&
          dbName_.equals(other.dbName_);
    }
  }

  /**
   * Base class for cache keys related to a specific table. Such keys are explicitly
   * invalidated by 'invalidateCacheForTable' above.
   */
  private static class TableCacheKey extends DbChildCacheKey {
    TableCacheKey(String dbName, String tableName) {
      super(dbName, tableName);
    }
  }

  /**
   * Cache key for a the set of overloads of a function given a name.
   * Invalidated by 'invalidateCacheForFunction' above.
   * Values are ImmutableList<TFunction>.
   */
  private static class FunctionsCacheKey extends DbChildCacheKey {
    FunctionsCacheKey(String dbName, String funcName) {
      super(dbName, funcName);
    }
  }

  /**
   * Base class for cache keys that are tied to a particular version of a table.
   * These keys are never explicitly invalidated. Instead, we rely on the fact that,
   * when the table is updated, it has a new version number. This results in making
   * previous entries for earlier versions of the table essentially unreachable in the
   * cache. They will age out over time as no queries access them.
   */
  private static class VersionedTableCacheKey extends TableCacheKey {
    /**
     * The catalog version number of the Table object. Including the version number in
     * the cache key ensures that, if the version number changes, any dependent entities
     * are "automatically" invalidated.
     */
    final long version_;

    VersionedTableCacheKey(TableMetaRefImpl table) {
      super(table.dbName_, table.tableName_);
      version_ = table.catalogVersion_;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(super.hashCode(), version_);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj.getClass().equals(getClass()))) {
        return false;
      }
      VersionedTableCacheKey other = (VersionedTableCacheKey)obj;
      return super.equals(obj) && version_ == other.version_;
    }
  }

  /**
   * Cache key for an entry storing column statistics.
   *
   * Values for these keys are 'ColumnStatisticsObj' objects.
   */
  private static class ColStatsCacheKey extends VersionedTableCacheKey {
    private final String colName_;

    public ColStatsCacheKey(TableMetaRefImpl table, String colName) {
      super(table);
      colName_ = colName;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(super.hashCode(), colName_);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ColStatsCacheKey)) return false;
      ColStatsCacheKey other = (ColStatsCacheKey)obj;
      return super.equals(obj) && colName_.equals(other.colName_);
    }
  }

  /**
   * Cache key for the partition list of a table.
   *
   * Values for these keys are 'List<PartitionRefImpl>'.
   */
  private static class PartitionListCacheKey extends VersionedTableCacheKey {
    PartitionListCacheKey(TableMetaRefImpl table) {
      super(table);
    }
  }

  /**
   * Key for caching information about a single partition.
   *
   * Values for these keys are 'PartitionMetadataImpl' objects.
   */
  @Immutable
  private static class PartitionCacheKey {
    private final long partId_;

    PartitionCacheKey(long partId) {
      partId_ = partId;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(partId_, getClass());
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof PartitionCacheKey)) return false;
      PartitionCacheKey other = (PartitionCacheKey)obj;
      return partId_ == other.partId_;
    }
  }

  /**
   * Cache key for metadata about databases.
   */
  private static class DbCacheKey {
    static enum DbInfoType {
      /** Cache the HMS Database object */
      HMS_METADATA,
      /** Cache an ImmutableList<BriefTableMeta> for table names within the DB */
      TABLE_LIST,
      /** Cache an ImmutableList<String> for function names within the DB */
      FUNCTION_NAMES
    }
    private final String dbName_;
    private final DbInfoType type_;

    DbCacheKey(String dbName, DbInfoType type) {
      dbName_ = Preconditions.checkNotNull(dbName);
      type_ = Preconditions.checkNotNull(type);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(getClass(), dbName_, type_);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      DbCacheKey other = (DbCacheKey) obj;
      return dbName_.equals(other.dbName_) && type_ == other.type_;
    }
  }

  /**
   * Cache key for an entry storing Iceberg table metadata.
   *
   * Values for these keys are 'TPartialTableInfo' objects.
   */
  private static class IcebergMetaCacheKey extends VersionedTableCacheKey {

    public IcebergMetaCacheKey(TableMetaRefImpl table) {
      super(table);
    }
  }

  /**
   * Cache key for an entry storing Iceberg API metadata.
   *
   * Values for these keys are 'org.apache.iceberg.Table' objects.
   */
  private static class IcebergApiTableCacheKey extends VersionedTableCacheKey {

    public IcebergApiTableCacheKey(TableMetaRefImpl table) {
      super(table);
    }
  }

  /**
   * Cache key for a DataSource object.
   */
  @Immutable
  private static class DataSourceCacheKey {
    private final String dsName_;

    DataSourceCacheKey(String dsName) {
      dsName_ = dsName;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(dsName_, getClass());
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof DataSourceCacheKey)) return false;
      DataSourceCacheKey other = (DataSourceCacheKey)obj;
      return dsName_.equals(other.dsName_);
    }
  }

  @VisibleForTesting
  static class SizeOfWeigher implements Weigher<Object, Object> {
    // Bypass flyweight objects like small boxed integers, Boolean.TRUE, enums, etc.
    private static final boolean BYPASS_FLYWEIGHT = true;
    // Cache the reflected sizes of classes seen.
    private static final boolean CACHE_SIZES = true;

    private static final int BYTES_PER_WORD = 8; // Assume 64-bit VM.
    // Guava cache overhead based on:
    // http://code-o-matic.blogspot.com/2012/02/updated-memory-cost-per-javaguava.html
    private static final int OVERHEAD_PER_ENTRY =
        12 * BYTES_PER_WORD + // base cost per entry
        4 * BYTES_PER_WORD;  // for use of 'maximumSize()'

    // Retain Ehcache while Jamm is thoroughly tested. We only expect to make one
    // CatalogdMetaProvider and thus one SizeOfWeigher.
    private final SizeOf SIZEOF;
    private final MemoryMeter METER;

    private final boolean useJamm_;
    private final Histogram entrySize_;

    public SizeOfWeigher(boolean useJamm, Histogram entrySize) {
      if (useJamm) {
        METER = MemoryMeter.builder().build();
        SIZEOF = null;
      } else {
        SIZEOF = SizeOf.newInstance(BYPASS_FLYWEIGHT, CACHE_SIZES);
        METER = null;
      }
      useJamm_ = useJamm;
      entrySize_ = entrySize;
    }

    public SizeOfWeigher() {
      this(true, null);
    }

    @Override
    public int weigh(Object key, Object value) {
      long size = OVERHEAD_PER_ENTRY;
      try {
        if (useJamm_) {
          size += METER.measureDeep(key);
          size += METER.measureDeep(value);
        } else {
          size += SIZEOF.deepSizeOf(key, value);
        }
      } catch (CannotAccessFieldException e) {
        // This may happen if we miss an add-opens call for lambdas in Java 17.
        LOG.warn("Unable to weigh cache entry, additional add-opens needed", e);
      } catch (UnsupportedOperationException e) {
        // Thrown by ehcache for lambdas in Java 17.
        LOG.warn("Unable to weigh cache entry", e);
      }

      if (entrySize_ != null) {
        entrySize_.update(size);
      }
      if (size > Integer.MAX_VALUE) {
        return Integer.MAX_VALUE;
      }
      return (int)size;
    }
  }

  @Override
  public TValidWriteIdList getValidWriteIdList(TableMetaRef ref) {
    Preconditions.checkArgument(ref instanceof TableMetaRefImpl,
        "table ref %s was not created by CatalogdMetaProvider", ref);
    return ((TableMetaRefImpl)ref).validWriteIds_;
  }
}
