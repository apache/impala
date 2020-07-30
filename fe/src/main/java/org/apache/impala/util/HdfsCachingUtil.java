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

package org.apache.impala.util;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.log4j.Logger;

import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.JniCatalogConstants;
import org.apache.impala.thrift.THdfsCachingOp;
import com.google.common.base.Preconditions;

/**
 * Utility class for submitting and dropping HDFS cache requests.
 */
public class HdfsCachingUtil {
  private static final Logger LOG = Logger.getLogger(HdfsCachingUtil.class);

  // The key name used to save cache directive IDs in table/partition properties.
  public final static String CACHE_DIR_ID_PROP_NAME = "cache_directive_id";

  // The key name used to store the replication factor for cached files
  public final static String CACHE_DIR_REPLICATION_PROP_NAME = "cache_replication";

  // The number of caching refresh intervals that can go by when waiting for data to
  // become cached before assuming no more progress is being made.
  private final static int MAX_UNCHANGED_CACHING_REFRESH_INTERVALS = 5;

  private static DistributedFileSystem dfs = null;

  /**
   * Returns the dfs singleton object.
   */
  private static DistributedFileSystem getDfs() throws ImpalaRuntimeException {
    if (dfs == null) {
      try {
        dfs = FileSystemUtil.getDistributedFileSystem();
      } catch (IOException e) {
        throw new ImpalaRuntimeException("HdfsCachingUtil failed to initialize the " +
            "DistributedFileSystem: ", e);
      }
    }
    return dfs;
  }

  /**
   * Caches the location of the given Hive Metastore Table and updates the
   * table's properties with the submitted cache directive ID. The caller is
   * responsible for not caching the same table twice, as HDFS will create a second
   * cache directive even if it is similar to an already existing one.
   *
   * Returns the ID of the submitted cache directive and throws if there is an error
   * submitting.
   */
  public static long submitCacheTblDirective(
      org.apache.hadoop.hive.metastore.api.Table table,
      String poolName, short replication) throws ImpalaRuntimeException {
    long id = HdfsCachingUtil.submitDirective(new Path(table.getSd().getLocation()),
        poolName, replication);
    table.putToParameters(CACHE_DIR_ID_PROP_NAME, Long.toString(id));
    table.putToParameters(CACHE_DIR_REPLICATION_PROP_NAME, Long.toString(replication));
    return id;
  }

  /**
   * Caches the location of the given partition and updates the
   * partitions's properties with the submitted cache directive ID. The caller is
   * responsible for not caching the same partition twice, as HDFS will create a second
   * cache directive even if it is similar to an already existing one.
   *
   * Returns the ID of the submitted cache directive and throws if there is an error
   * submitting the directive.
   */
  public static long submitCachePartitionDirective(HdfsPartition.Builder part,
      String poolName, short replication) throws ImpalaRuntimeException {
    long id = HdfsCachingUtil.submitDirective(new Path(part.getLocation()),
        poolName, replication);
    part.putToParameters(CACHE_DIR_ID_PROP_NAME, Long.toString(id));
    part.putToParameters(CACHE_DIR_REPLICATION_PROP_NAME, Long.toString(replication));
    return id;
  }

  /**
   * Convenience method for working directly on a metastore partition. See
   * submitCachePartitionDirective(HdfsPartition, String, short) for more details.
   */
  public static long submitCachePartitionDirective(
      org.apache.hadoop.hive.metastore.api.Partition part,
      String poolName, short replication) throws ImpalaRuntimeException {
    long id = HdfsCachingUtil.submitDirective(new Path(part.getSd().getLocation()),
        poolName, replication);
    part.putToParameters(CACHE_DIR_ID_PROP_NAME, Long.toString(id));
    part.putToParameters(CACHE_DIR_REPLICATION_PROP_NAME, Long.toString(replication));
    return id;
  }

  /**
   * Removes the cache directive associated with the table from HDFS, uncaching all
   * data. Also updates the table's metadata. No-op if the table is not cached.
   */
  public static void removeTblCacheDirective(
      org.apache.hadoop.hive.metastore.api.Table table) throws ImpalaRuntimeException {
    Preconditions.checkNotNull(table);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Uncaching table: " + table.getDbName() + "." + table.getTableName());
    }
    Map<String, String> parameters = table.getParameters();
    if (parameters == null) {
      LOG.warn("removePartitionCacheDirective(): table " + table.getTableName() +
          "has a null parameter map.");
    }
    Long id = getCacheDirectiveId(parameters);
    if (id == null) {
      LOG.warn("removePartitionCacheDirective(): table " + table.getTableName() +
          "doesn't have a cache directive id.");
      return;
    }
    HdfsCachingUtil.removeDirective(id);
    table.getParameters().remove(CACHE_DIR_ID_PROP_NAME);
    table.getParameters().remove(CACHE_DIR_REPLICATION_PROP_NAME);
  }

  /**
   * Removes the cache directive associated with the partition from HDFS, uncaching all
   * data. Also updates the partition's metadata to remove the cache directive ID.
   * No-op if the table is not cached.
   */
  public static void removePartitionCacheDirective(HdfsPartition.Builder part)
      throws ImpalaException {
    Preconditions.checkNotNull(part);
    Map<String, String> parameters = part.getParameters();
    if (parameters == null) {
      LOG.warn("removePartitionCacheDirective(): partition " + part.getPartitionName() +
          "has a null parameter map.");
    }
    Long id = getCacheDirectiveId(parameters);
    if (id == null) {
      LOG.warn("removePartitionCacheDirective(): partition " + part.getPartitionName() +
          "doesn't have a cache directive id.");
      return;
    }
    HdfsCachingUtil.removeDirective(id);
    part.getParameters().remove(CACHE_DIR_ID_PROP_NAME);
    part.getParameters().remove(CACHE_DIR_REPLICATION_PROP_NAME);
  }

  /**
   * Convenience method for working directly on a metastore partition params map. See
   * removePartitionCacheDirective(HdfsPartition.Builder) for more details.
   */
  public static void removePartitionCacheDirective(
      Map<String, String> partitionParams) throws ImpalaException {
    Long id = getCacheDirectiveId(partitionParams);
    if (id == null) return;
    HdfsCachingUtil.removeDirective(id);
    partitionParams.remove(CACHE_DIR_ID_PROP_NAME);
    partitionParams.remove(CACHE_DIR_REPLICATION_PROP_NAME);
  }

  /**
   * Returns the cache directive ID from the given table/partition parameter
   * map. Returns null if the CACHE_DIR_ID_PROP_NAME key was not set or if
   * there was an error parsing the associated ID.
   */
  public static Long getCacheDirectiveId(Map<String, String> params) {
    if (params == null) return null;
    String idStr = params.get(CACHE_DIR_ID_PROP_NAME);
    if (idStr == null) return null;
    try {
      return Long.parseLong(idStr);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /**
   * Given a cache directive ID, returns the pool the directive is cached in.
   * Returns null if no outstanding cache directive match this ID.
   */
  public static String getCachePool(long directiveId)
      throws ImpalaRuntimeException {
    CacheDirectiveEntry entry = getDirective(directiveId);
    return entry == null ? null : entry.getInfo().getPool();
  }

  /**
   * Given a cache directive ID, returns the replication factor for the directive.
   * Returns null if no outstanding cache directives match this ID.
   */
  public static Short getCacheReplication(long directiveId)
      throws ImpalaRuntimeException {
    CacheDirectiveEntry entry = getDirective(directiveId);
    return entry != null ? entry.getInfo().getReplication() : null;
  }

  /**
   * Returns the cache replication value from the parameters map. We assume that only
   * cached table parameters are used and the property is always present.
   */
  public static Short getCachedCacheReplication(Map<String, String> params) {
    Preconditions.checkNotNull(params);
    String replication = params.get(CACHE_DIR_REPLICATION_PROP_NAME);
    if (replication == null) {
      return JniCatalogConstants.HDFS_DEFAULT_CACHE_REPLICATION_FACTOR;
    }
    try {
      return Short.parseShort(replication);
    } catch (NumberFormatException e) {
      return JniCatalogConstants.HDFS_DEFAULT_CACHE_REPLICATION_FACTOR;
    }
  }

  /**
   * Waits on a cache directive to either complete or stop making progress. Progress is
   * checked by polling the HDFS caching stats every
   * DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS. We verify the request's
   * "currentBytesCached" is increasing compared to "bytesNeeded".
   * If "currentBytesCached" == "bytesNeeded" or if no progress is made for a
   * MAX_UNCHANGED_CACHING_REFRESH_INTERVALS, this function returns.
   */
  public static void waitForDirective(long directiveId)
      throws ImpalaRuntimeException  {
    long bytesNeeded = 0L;
    long currentBytesCached = 0L;
    CacheDirectiveEntry cacheDir = getDirective(directiveId);
    if (cacheDir == null) return;

    bytesNeeded = cacheDir.getStats().getBytesNeeded();
    currentBytesCached = cacheDir.getStats().getBytesCached();
    if (LOG.isTraceEnabled()) {
      LOG.trace(String.format("Waiting on cache directive id: %d. Bytes " +
          "cached (%d) / needed (%d)", directiveId, currentBytesCached, bytesNeeded));
    }
    // All the bytes are cached, just return.
    if (bytesNeeded == currentBytesCached) return;

    // The refresh interval is how often HDFS will update cache directive stats. We use
    // this value to determine how frequently we should poll for changes.
    // The key dfs.namenode.path.based.cache.refresh.interval.ms is copied from the string
    // DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS in DFSConfigKeys.java from the
    // hadoop-hdfs jar.
    long hdfsRefreshIntervalMs = getDfs().getConf().getLong(
        "dfs.namenode.path.based.cache.refresh.interval.ms", 30000L);
    Preconditions.checkState(hdfsRefreshIntervalMs > 0);

    // Loop until either MAX_UNCHANGED_CACHING_REFRESH_INTERVALS have passed with no
    // changes or all required data is cached.
    int unchangedCounter = 0;
    while (unchangedCounter < MAX_UNCHANGED_CACHING_REFRESH_INTERVALS) {
      long previousBytesCached = currentBytesCached;
      cacheDir = getDirective(directiveId);
      if (cacheDir == null) return;
      currentBytesCached = cacheDir.getStats().getBytesCached();
      bytesNeeded = cacheDir.getStats().getBytesNeeded();
      if (currentBytesCached == bytesNeeded) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(String.format("Cache directive id: %d has completed." +
              "Bytes cached (%d) / needed (%d)", directiveId, currentBytesCached,
              bytesNeeded));
        }
        return;
      }

      if (currentBytesCached == previousBytesCached) {
        ++unchangedCounter;
      } else {
        unchangedCounter = 0;
      }
      try {
        // Sleep for the refresh interval + a little bit more to ensure a full interval
        // has completed. A value of 25% the refresh interval was arbitrarily chosen.
        Thread.sleep((long) (hdfsRefreshIntervalMs * 1.25));
      } catch (InterruptedException e) { /* ignore */ }
    }
    LOG.warn(String.format("No changes in cached bytes in: %d(ms). All data may not " +
        "be cached. Final stats for cache directive id: %d. Bytes cached (%d)/needed " +
        "(%d)", hdfsRefreshIntervalMs * MAX_UNCHANGED_CACHING_REFRESH_INTERVALS,
        directiveId, currentBytesCached, bytesNeeded));
  }

  /**
   * Submits a new caching directive for the specified cache pool name, path and
   * replication. Returns the directive ID if the submission was successful or an
   * ImpalaRuntimeException if the submission fails.
   */
  private static long submitDirective(Path path, String poolName, short replication)
      throws ImpalaRuntimeException {
    Preconditions.checkNotNull(path);
    Preconditions.checkState(poolName != null && !poolName.isEmpty());
    CacheDirectiveInfo info = new CacheDirectiveInfo.Builder()
        .setPool(poolName)
        .setReplication(replication)
        .setPath(path).build();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Submitting cache directive: " + info.toString());
    }
    try {
      return getDfs().addCacheDirective(info);
    } catch (IOException e) {
      throw new ImpalaRuntimeException(e.getMessage(), e);
    }
  }

  /**
   * Update cache directive for a table and updates the metastore parameters.
   * Returns the cache directive ID
   */
  public static long modifyCacheDirective(Long id,
      org.apache.hadoop.hive.metastore.api.Table table,
      String poolName, short replication) throws ImpalaRuntimeException {
    Preconditions.checkNotNull(id);
    HdfsCachingUtil.modifyCacheDirective(id, new Path(table.getSd().getLocation()),
        poolName, replication);
    table.putToParameters(CACHE_DIR_ID_PROP_NAME, Long.toString(id));
    table.putToParameters(CACHE_DIR_REPLICATION_PROP_NAME, Long.toString(replication));
    return id;
  }

  /**
   * Update cache directive for a partition and update the metastore parameters.
   * Returns the cache directive ID
   */
  public static long modifyCacheDirective(Long id, HdfsPartition.Builder part,
      String poolName, short replication) throws ImpalaRuntimeException {
    Preconditions.checkNotNull(id);
    HdfsCachingUtil.modifyCacheDirective(id, new Path(part.getLocation()),
        poolName, replication);
    part.putToParameters(CACHE_DIR_ID_PROP_NAME, Long.toString(id));
    part.putToParameters(CACHE_DIR_REPLICATION_PROP_NAME, Long.toString(replication));
    return id;
  }

  /**
   * Update an existing cache directive to avoid having the same entry multiple
   * times
   */
  private static void modifyCacheDirective(Long id, Path path, String poolName,
      short replication) throws ImpalaRuntimeException {
    Preconditions.checkNotNull(path);
    Preconditions.checkNotNull(id);
    Preconditions.checkState(poolName != null && !poolName.isEmpty());
    CacheDirectiveInfo info = new CacheDirectiveInfo.Builder()
        .setId(id)
        .setPool(poolName)
        .setReplication(replication)
        .setPath(path).build();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Modifying cache directive: " + info.toString());
    }
    try {
      getDfs().modifyCacheDirective(info);
    } catch (IOException e) {
      throw new ImpalaRuntimeException(e.getMessage(), e);
    }
  }

  /**
   * Removes the given cache directive if it exists, uncaching the data. If the
   * cache request does not exist in HDFS no error is returned.
   * Throws an ImpalaRuntimeException if there was any problem removing the
   * directive.
   */
  private static void removeDirective(long directiveId) throws ImpalaRuntimeException {
    if (LOG.isTraceEnabled()) LOG.trace("Removing cache directive id: " + directiveId);
    try {
      getDfs().removeCacheDirective(directiveId);
    } catch (IOException e) {
      // There is no special exception type for the case where a directive ID does not
      // exist so we must inspect the error message.
      if (e.getMessage().contains("No directive with ID")) return;
      throw new ImpalaRuntimeException(e.getMessage(), e);
    }
  }

  /**
   * Gets the cache directive matching the given ID. Returns null if no matching
   * directives were found.
   */
  private static CacheDirectiveEntry getDirective(long directiveId)
      throws ImpalaRuntimeException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Getting cache directive id: " + directiveId);
    }
    CacheDirectiveInfo filter = new CacheDirectiveInfo.Builder()
        .setId(directiveId)
        .build();
    try {
      RemoteIterator<CacheDirectiveEntry> itr = getDfs().listCacheDirectives(filter);
      if (itr.hasNext()) return itr.next();
    } catch (IOException e) {
      // Handle connection issues with e.g. HDFS and possible not found errors
      throw new ImpalaRuntimeException(e.getMessage(), e);
    }
    throw new ImpalaRuntimeException(
        "HDFS cache directive filter returned empty result. This must not happen");
  }

  /**
   * Helper method for frequent lookup of replication factor in the thrift caching
   * structure.
   */
  public static short getReplicationOrDefault(THdfsCachingOp op) {
    return op.isSetReplication() ? op.getReplication() :
      JniCatalogConstants.HDFS_DEFAULT_CACHE_REPLICATION_FACTOR;
  }

  /**
   * Returns a boolean indicating if the given thrift caching operation would perform an
   * update on an already existing cache directive.
   */
  public static boolean isUpdateOp(THdfsCachingOp op, Map<String, String> params)
      throws ImpalaRuntimeException {

    Long directiveId = Long.parseLong(params.get(CACHE_DIR_ID_PROP_NAME));
    CacheDirectiveEntry entry = getDirective(directiveId);
    Preconditions.checkNotNull(entry);

    // Verify cache pool
    if (!op.getCache_pool_name().equals(entry.getInfo().getPool())) {
      return false;
    }

    // Check cache replication factor
    if ((op.isSetReplication() && op.getReplication() !=
        entry.getInfo().getReplication()) || ( !op.isSetReplication() &&
        entry.getInfo().getReplication() !=
        JniCatalogConstants.HDFS_DEFAULT_CACHE_REPLICATION_FACTOR)) {
      return true;
    }
    return false;
  }

  /**
   * Validates the properties of the chosen cache pool. Throws on error.
   */
  public static void validateCachePool(THdfsCachingOp op, Long directiveId,
      TableName table, HdfsPartition partition)
      throws ImpalaRuntimeException {

    CacheDirectiveEntry entry = getDirective(directiveId);
    Preconditions.checkNotNull(entry);

    if (!op.getCache_pool_name().equals(entry.getInfo().getPool())) {
      throw new ImpalaRuntimeException(String.format("Cannot cache partition in " +
          "pool '%s' because it is already cached in '%s'. To change the cache " +
          "pool for this partition, first uncache using: ALTER TABLE %s.%s " +
          "%sSET UNCACHED", op.getCache_pool_name(),
          entry.getInfo().getPool(), table.getDb(), table,
          // Insert partition string if partition non null
          partition != null ? String.format(" PARTITION(%s) ",
          partition.getPartitionName().replaceAll("/", ", ")) : ""));
    }
  }

  /**
   * Validates the properties of the chosen cache pool. Throws on error.
   */
  public static void validateCachePool(THdfsCachingOp op, Long directiveId,
      TableName table) throws ImpalaRuntimeException {
    validateCachePool(op, directiveId, table, null);
  }

  /**
   * Validates and returns true if a parameter map contains a cache directive ID and
   * validates it against the NameNode to make sure it exists. If the cache
   * directive ID does not exist, we remove the value from the parameter map,
   * issue a log message and return false. As the value is not written back to the
   * Hive MS from this method, the result will be only valid until the next metadata
   * fetch. Lastly, we update the cache replication factor in the parameters with the
   * value read from HDFS.
   */
  public static boolean validateCacheParams(Map<String, String> params) {
    Long directiveId = getCacheDirectiveId(params);
    if (directiveId == null) return false;

    CacheDirectiveEntry entry = null;
    try {
      entry = getDirective(directiveId);
    } catch (ImpalaRuntimeException e) {
      if (e.getCause() != null && e.getCause() instanceof RemoteException) {
        // This exception signals that the cache directive no longer exists.
        LOG.error("Cache directive does not exist", e);
        params.remove(CACHE_DIR_ID_PROP_NAME);
        params.remove(CACHE_DIR_REPLICATION_PROP_NAME);
      } else {
        // This exception signals that there was a connection problem with HDFS.
        LOG.error("IO Exception, possible connectivity issues with HDFS", e);
      }
      return false;
    }
    Preconditions.checkNotNull(entry);

    // On the upgrade path the property might not exist, if it exists
    // and is different from the one from the meta store, issue a warning.
    String replicationFactor = params.get(CACHE_DIR_REPLICATION_PROP_NAME);
    if (replicationFactor != null &&
        Short.parseShort(replicationFactor) != entry.getInfo().getReplication()) {
      LOG.info("Replication factor for entry in HDFS differs from value in Hive MS: " +
          entry.getInfo().getPath().toString() + " " +
          entry.getInfo().getReplication().toString() + " != " +
          params.get(CACHE_DIR_REPLICATION_PROP_NAME));
    }
    params.put(CACHE_DIR_REPLICATION_PROP_NAME,
        String.valueOf(entry.getInfo().getReplication()));
    return true;
  }
}
