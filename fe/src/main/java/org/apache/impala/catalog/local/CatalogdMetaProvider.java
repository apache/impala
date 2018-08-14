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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.Pair;
import org.apache.impala.common.Reference;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TCatalogInfoSelector;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TDatabase;
import org.apache.impala.thrift.TDbInfoSelector;
import org.apache.impala.thrift.TErrorCode;
import org.apache.impala.thrift.TGetPartialCatalogObjectRequest;
import org.apache.impala.thrift.TGetPartialCatalogObjectResponse;
import org.apache.impala.thrift.THdfsFileDesc;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartialPartitionInfo;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableInfoSelector;
import org.apache.impala.util.ListMap;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.ehcache.sizeof.SizeOf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.errorprone.annotations.Immutable;

/**
 * MetaProvider which fetches metadata in a granular fashion from the catalogd.
 *
 * Automatically partially caches metadata to avoid fetching too much data from the
 * catalogd on every query.
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
   * Hive configuration.
   */
  private static final Object NULL_PARTITION_KEY_VALUE_CACHE_KEY = new Object();

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

  // TODO(todd): currently we haven't implemented catalogd thrift APIs for all pieces
  // of metadata. In order to incrementally build this out, we delegate various calls
  // to the "direct" provider for now and circumvent catalogd.
  private DirectMetaProvider directProvider_ = new DirectMetaProvider();


  final Cache<Object,Object> cache_;

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
    LOG.info("Metadata cache configuration: capacity={} MB, expiration={} sec",
        cacheSizeBytes/1024/1024, expirationSecs);

    // TODO(todd) add end-to-end test cases which stress cache eviction (both time
    // and size-triggered) and make sure results are still correct.
    cache_ = CacheBuilder.newBuilder()
        .maximumWeight(cacheSizeBytes)
        .expireAfterAccess(expirationSecs, TimeUnit.SECONDS)
        .weigher(new SizeOfWeigher())
        .recordStats()
        .build();
  }

  public CacheStats getCacheStats() {
    return cache_.stats();
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
    byte[] ret;
    try {
      ret = FeSupport.GetPartialCatalogObject(new TSerializer().serialize(req));
    } catch (InternalException e) {
      throw new TException(e);
    }
    resp = new TGetPartialCatalogObjectResponse();
    new TDeserializer().deserialize(resp, ret);
    if (resp.status.status_code != TErrorCode.OK) {
      // TODO(todd) do reasonable error handling
      throw new TException(resp.toString());
    }

    // If we requested information about a particular version of an object, but
    // got back a response for a different version, then we have a case of "read skew".
    // For example, we may have fetched the partition list of a table, performed pruning,
    // and then tried to fetch the specific partitions needed for a query, while some
    // concurrent DDL modified the set of partitions. This could result in an unexpected
    // result which violates the snapshot consistency guarantees expected by users.
    if (req.object_desc.isSetCatalog_version() &&
        resp.isSetObject_version_number() &&
        req.object_desc.catalog_version != resp.object_version_number) {
      throw new InconsistentMetadataFetchException(String.format(
          "Catalog object %s changed version from %s to %s while fetching metadata",
          req.object_desc.toString(),
          req.object_desc.catalog_version,
          resp.object_version_number));
    }
    return resp;
  }

  @Override
  public ImmutableList<String> loadDbList() throws TException {
    TGetPartialCatalogObjectRequest req = newReqForCatalog();
    req.catalog_info_selector.want_db_names = true;
    TGetPartialCatalogObjectResponse resp = sendRequest(req);
    checkResponse(resp.catalog_info != null && resp.catalog_info.db_names != null, req,
        "missing table names");
    return ImmutableList.copyOf(resp.catalog_info.db_names);
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

  @Override
  public Database loadDb(String dbName) throws TException {
    TGetPartialCatalogObjectRequest req = newReqForDb(dbName);
    req.db_info_selector.want_hms_database = true;
    TGetPartialCatalogObjectResponse resp = sendRequest(req);
    checkResponse(resp.db_info != null && resp.db_info.hms_database != null,
        req, "missing expected HMS database");
    return resp.db_info.hms_database;
  }

  @Override
  public ImmutableList<String> loadTableNames(String dbName)
      throws MetaException, UnknownDBException, TException {
    // TODO(todd): do we ever need to fetch the DB without the table names
    // or vice versa?
    TGetPartialCatalogObjectRequest req = newReqForDb(dbName);
    req.db_info_selector.want_table_names = true;
    TGetPartialCatalogObjectResponse resp = sendRequest(req);
    checkResponse(resp.db_info != null && resp.db_info.table_names != null,
        req, "missing expected HMS table");
    return ImmutableList.copyOf(resp.db_info.table_names);
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
  public Pair<Table, TableMetaRef> loadTable(String dbName, String tableName)
      throws NoSuchObjectException, MetaException, TException {
    TGetPartialCatalogObjectRequest req = newReqForTable(dbName, tableName);
    req.table_info_selector.want_hms_table = true;
    TGetPartialCatalogObjectResponse resp = sendRequest(req);
    checkResponse(resp.table_info != null && resp.table_info.hms_table != null,
        req, "missing expected HMS table");
    TableMetaRef ref = new TableMetaRefImpl(dbName, tableName, resp.table_info.hms_table,
        resp.object_version_number);
    return Pair.create(resp.table_info.hms_table, ref);
  }

  @Override
  public List<ColumnStatisticsObj> loadTableColumnStatistics(final TableMetaRef table,
      List<String> colNames) throws TException {
    List<ColumnStatisticsObj> ret = Lists.newArrayListWithCapacity(colNames.size());

    // Look up in cache first, keeping track of which ones are missing.
    int negativeHitCount = 0;
    List<String> missingCols = Lists.newArrayListWithCapacity(colNames.size());
    for (String colName: colNames) {
      ColStatsCacheKey cacheKey = new ColStatsCacheKey((TableMetaRefImpl)table, colName);
      ColumnStatisticsObj val = (ColumnStatisticsObj)cache_.getIfPresent(cacheKey);
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
    LOG.trace("Request for column stats of {}: hit {}/ neg hit {} / miss {}",
        table, hitCount, negativeHitCount, missingCols.size());
    return ret;
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<PartitionRef> loadPartitionList(final TableMetaRef table)
      throws TException {
    final Reference<Boolean> hitCache = new Reference<>(true);
    try {
      PartitionListCacheKey key = new PartitionListCacheKey((TableMetaRefImpl)table);
      return (List<PartitionRef>) cache_.get(key,
          new Callable<List<PartitionRef>>() {
            /** Called to load cache for cache misses */
            @Override
            public List<PartitionRef> call() throws Exception {
              hitCache.setRef(false);
              TGetPartialCatalogObjectRequest req = newReqForTable(table);
              req.table_info_selector.want_partition_names = true;
              TGetPartialCatalogObjectResponse resp = sendRequest(req);
              checkResponse(resp.table_info != null && resp.table_info.partitions != null,
                  req, "missing partition list result");
              List<PartitionRef> partitionRefs = Lists.newArrayListWithCapacity(
                  resp.table_info.partitions.size());
              for (TPartialPartitionInfo p: resp.table_info.partitions) {
                checkResponse(p.isSetId(), req,
                    "response missing partition IDs for partition %s", p);
                partitionRefs.add(new PartitionRefImpl(p));
              }
              return partitionRefs;
            }
          });
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), TException.class);
      throw new RuntimeException(e);
    } finally {
      LOG.trace("Request for partition list of {}: {}", table,
          hitCache.getRef() ? "hit":"miss");
    }
  }

  @Override
  public Map<String, PartitionMetadata> loadPartitionsByRefs(TableMetaRef table,
      List<String> partitionColumnNames,
      ListMap<TNetworkAddress> hostIndex,
      List<PartitionRef> partitionRefs)
      throws MetaException, TException {
    Preconditions.checkArgument(table instanceof TableMetaRefImpl);
    TableMetaRefImpl refImpl = (TableMetaRefImpl)table;

    // Load what we can from the cache.
    Map<PartitionRef, PartitionMetadata> refToMeta = loadPartitionsFromCache(refImpl,
        hostIndex, partitionRefs);

    LOG.trace("Request for partitions of {}: hit {}/{}", table, refToMeta.size(),
        partitionRefs.size());

    // Load the remainder from the catalogd.
    List<PartitionRef> missingRefs = Lists.newArrayList();
    for (PartitionRef ref: partitionRefs) {
      if (!refToMeta.containsKey(ref)) missingRefs.add(ref);
    }
    if (!missingRefs.isEmpty()) {
      Map<PartitionRef, PartitionMetadata> fromCatalogd = loadPartitionsFromCatalogd(
          refImpl, hostIndex, missingRefs);
      refToMeta.putAll(fromCatalogd);
      // Write back to the cache.
      storePartitionsInCache(refImpl, hostIndex, fromCatalogd);
    }

    // Convert the returned map to be by-name instead of by-ref.
    Map<String, PartitionMetadata> nameToMeta = Maps.newHashMapWithExpectedSize(
        refToMeta.size());
    for (Map.Entry<PartitionRef, PartitionMetadata> e: refToMeta.entrySet()) {
      nameToMeta.put(e.getKey().getName(), e.getValue());
    }
    return nameToMeta;
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
    TGetPartialCatalogObjectResponse resp = sendRequest(req);
    checkResponse(resp.table_info != null && resp.table_info.partitions != null,
        req, "missing partition list result");
    checkResponse(resp.table_info.network_addresses != null,
        req, "missing network addresses");
    checkResponse(resp.table_info.partitions.size() == ids.size(),
        req, "returned %d partitions instead of expected %d",
        resp.table_info.partitions.size(), ids.size());

    Map<PartitionRef, PartitionMetadata> ret = Maps.newHashMap();
    for (int i = 0; i < ids.size(); i++) {
      PartitionRef partRef = partRefs.get(i);
      TPartialPartitionInfo part = resp.table_info.partitions.get(i);
      Partition msPart = part.getHms_partition();
      if (msPart == null) {
        checkResponse(table.msTable_.getPartitionKeysSize() == 0, req,
            "Should not return a partition with missing HMS partition unless " +
            "the table is unpartitioned");
        msPart = DirectMetaProvider.msTableToPartition(table.msTable_);
      }

      // Transform the file descriptors to the caller's index.
      checkResponse(part.file_descriptors != null, req, "missing file descriptors");
      List<FileDescriptor> fds = Lists.newArrayListWithCapacity(
          part.file_descriptors.size());
      for (THdfsFileDesc thriftFd: part.file_descriptors) {
        FileDescriptor fd = FileDescriptor.fromThrift(thriftFd);
        // The file descriptors returned via the RPC use host indexes that reference
        // the 'network_addresses' list in the RPC. However, the caller may have already
        // loaded some addresses into 'hostIndex'. So, the returned FDs need to be
        // remapped to point to the caller's 'hostIndex' instead of the list in the
        // RPC response.
        fds.add(fd.cloneWithNewHostIndex(resp.table_info.network_addresses, hostIndex));
      }
      PartitionMetadataImpl metaImpl = new PartitionMetadataImpl(msPart,
          ImmutableList.copyOf(fds));

      checkResponse(partRef != null, req, "returned unexpected partition id %s", part.id);

      PartitionMetadata oldVal = ret.put(partRef, metaImpl);
      if (oldVal != null) {
        throw new RuntimeException("catalogd returned partition " + part.id +
            " multiple times");
      }
    }
    return ret;
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
      List<PartitionRef> partitionRefs) {

    Map<PartitionRef, PartitionMetadata> ret = Maps.newHashMapWithExpectedSize(
        partitionRefs.size());
    for (PartitionRef ref: partitionRefs) {
      PartitionRefImpl prefImpl = (PartitionRefImpl)ref;
      PartitionCacheKey cacheKey = new PartitionCacheKey(table, prefImpl.getId());
      PartitionMetadataImpl val = (PartitionMetadataImpl)cache_.getIfPresent(cacheKey);
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
  private void storePartitionsInCache(TableMetaRefImpl table,
      ListMap<TNetworkAddress> hostIndex, Map<PartitionRef, PartitionMetadata> metas) {
    for (Map.Entry<PartitionRef, PartitionMetadata> e: metas.entrySet()) {
      PartitionRefImpl prefImpl = (PartitionRefImpl)e.getKey();
      PartitionMetadataImpl metaImpl = (PartitionMetadataImpl)e.getValue();
      PartitionCacheKey cacheKey = new PartitionCacheKey(table, prefImpl.getId());
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
    try {
      return (String) cache_.get(NULL_PARTITION_KEY_VALUE_CACHE_KEY,
          new Callable<String>() {
            /** Called to load cache for cache misses */
            @Override
            public String call() throws Exception {
              return directProvider_.loadNullPartitionKeyValue();
            }
          });
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), TException.class);
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<String> loadFunctionNames(String dbName) throws MetaException, TException {
    return directProvider_.loadFunctionNames(dbName);
  }

  @Override
  public Function getFunction(String dbName, String functionName)
      throws MetaException, TException {
    return directProvider_.getFunction(dbName, functionName);
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
    private final Partition msPartition_;
    private final ImmutableList<FileDescriptor> fds_;

    public PartitionMetadataImpl(Partition msPartition,
        ImmutableList<FileDescriptor> fds) {
      this.msPartition_ = Preconditions.checkNotNull(msPartition);
      this.fds_ = fds;
    }

    /**
     * Clone this metadata object, but make it relative to 'dstIndex' instead of
     * 'origIndex'.
     */
    public PartitionMetadataImpl cloneRelativeToHostIndex(
        ListMap<TNetworkAddress> origIndex,
        ListMap<TNetworkAddress> dstIndex) {
      List<FileDescriptor> fds = Lists.newArrayListWithCapacity(fds_.size());
      for (FileDescriptor fd: fds_) {
        fds.add(fd.cloneWithNewHostIndex(origIndex.getList(), dstIndex));
      }
      return new PartitionMetadataImpl(msPartition_, ImmutableList.copyOf(fds));
    }

    @Override
    public Partition getHmsPartition() {
      return msPartition_;
    }

    @Override
    public ImmutableList<FileDescriptor> getFileDescriptors() {
      return fds_;
    }
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

    /**
     * Stash the HMS Table object since we need this in order to handle some strange
     * behavior whereby the catalogd returns a Partition with no HMS partition object
     * in the case of unpartitioned tables.
     */
    private final Table msTable_;

    /**
     * The version of the table when we first loaded it. Subsequent requests about
     * the table are verified against this version.
     */
    private final long catalogVersion_;

    public TableMetaRefImpl(String dbName, String tableName,
        Table msTable, long catalogVersion) {
      this.dbName_ = dbName;
      this.tableName_ = tableName;
      this.msTable_ = msTable;
      this.catalogVersion_ = catalogVersion;
    }

    @Override
    public String toString() {
      return String.format("TableMetaRef %s.%s@%d", dbName_, tableName_, catalogVersion_);
    }
  }

  /**
   * Base class for cache keys related to a specific table.
   */
  private static class TableCacheKey {
    final String dbName_;
    final String tableName_;
    /**
     * The catalog version number of the Table object. Including the version number in
     * the cache key ensures that, if the version number changes, any dependent entities
     * are "automatically" invalidated.
     *
     * TODO(todd): need to handle the case of a catalogd restarting and reloading metadata
     * for a table reusing a previously-used version number.
     */
    final long version_;

    TableCacheKey(TableMetaRefImpl table) {
      dbName_ = table.dbName_;
      tableName_ = table.tableName_;
      version_ = table.catalogVersion_;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(dbName_, tableName_, version_, getClass());
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !obj.getClass().equals(getClass())) {
        return false;
      }
      TableCacheKey other = (TableCacheKey)obj;
      return version_ == other.version_ &&
          tableName_.equals(other.tableName_) &&
          dbName_.equals(other.dbName_);
    }
  }

  /**
   * Cache key for an entry storing column statistics.
   *
   * Values for these keys are 'ColumnStatisticsObj' objects.
   */
  private static class ColStatsCacheKey extends TableCacheKey {
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
      if (obj == null || !(obj instanceof ColStatsCacheKey)) {
        return false;
      }
      ColStatsCacheKey other = (ColStatsCacheKey)obj;
      return super.equals(obj) && colName_.equals(other.colName_);
    }
  }

  /**
   * Cache key for the partition list of a table.
   *
   * Values for these keys are 'List<PartitionRefImpl>'.
   */
  private static class PartitionListCacheKey extends TableCacheKey {
    PartitionListCacheKey(TableMetaRefImpl table) {
      super(table);
    }
  }

  /**
   * Key for caching information about a single partition.
   *
   * TODO(todd): currently this inherits from TableCacheKey. This means that, if a
   * table's version number changes, all of its partitions must be reloaded. However,
   * since partition IDs are globally unique within a catalogd instance, we could
   * optimize this to just key based on the partition ID. However, currently, there are
   * some cases where partitions are mutated in place rather than replaced with a new ID.
   * We need to eliminate those or add a partition sequence number before we can make
   * this optimization.
   */
  private static class PartitionCacheKey extends TableCacheKey {
    private final long partId_;

    PartitionCacheKey(TableMetaRefImpl table, long partId) {
      super(table);
      partId_ = partId;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(super.hashCode(), partId_);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof PartitionCacheKey)) {
        return false;
      }
      PartitionCacheKey other = (PartitionCacheKey)obj;
      return super.equals(obj) && partId_ == other.partId_;
    }
  }

  @VisibleForTesting
  static class SizeOfWeigher implements Weigher<Object, Object> {
    // Bypass flyweight objects like small boxed integers, Boolean.TRUE, enums, etc.
    private static final boolean BYPASS_FLYWEIGHT = true;
    // Cache the reflected sizes of classes seen.
    private static final boolean CACHE_SIZES = true;

    private static SizeOf SIZEOF = SizeOf.newInstance(BYPASS_FLYWEIGHT, CACHE_SIZES);

    private static final int BYTES_PER_WORD = 8; // Assume 64-bit VM.
    // Guava cache overhead based on:
    // http://code-o-matic.blogspot.com/2012/02/updated-memory-cost-per-javaguava.html
    private static final int OVERHEAD_PER_ENTRY =
        12 * BYTES_PER_WORD + // base cost per entry
        4 * BYTES_PER_WORD;  // for use of 'maximumSize()'

    @Override
    public int weigh(Object key, Object value) {
      long size = SIZEOF.deepSizeOf(key, value) + OVERHEAD_PER_ENTRY;
      if (size > Integer.MAX_VALUE) {
        return Integer.MAX_VALUE;
      }
      return (int)size;
    }
  }
}
