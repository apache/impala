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

package com.cloudera.impala.util;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo.Expiration;
import org.apache.log4j.Logger;

import com.cloudera.impala.common.FileSystemUtil;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.ImpalaRuntimeException;
import com.google.common.base.Preconditions;

/**
 * Utility class for submitting and dropping HDFS cache requests.
 */
public class HdfsCachingUtil {
  private static final Logger LOG = Logger.getLogger(HdfsCachingUtil.class);

  // The key name used to save cache directive IDs in table/partition properties.
  private final static String CACHE_DIR_ID_PROP_NAME = "cache_directive_id";

  // The number of caching refresh intervals that can go by when waiting for data to
  // become cached before assuming no more progress is being made.
  private final static int MAX_UNCHANGED_CACHING_REFRESH_INTERVALS = 5;

  private final static DistributedFileSystem dfs;
  static {
    try {
      dfs = FileSystemUtil.getDistributedFileSystem();
    } catch (IOException e) {
      throw new RuntimeException("HdfsCachingUtil failed to initialize the " +
          "DistributedFileSystem: ", e);
    }
  }

  /**
   * Caches the location of the given Hive Metastore Table and updates the
   * table's properties with the submitted cache directive ID.
   * Returns the ID of the submitted cache directive and throws if there is an error
   * submitting the directive or if the table was already cached.
   */
  public static long submitCacheTblDirective(
      org.apache.hadoop.hive.metastore.api.Table table,
      String poolName) throws ImpalaRuntimeException {
    if (table.getParameters().get(CACHE_DIR_ID_PROP_NAME) != null) {
      throw new ImpalaRuntimeException(String.format(
          "Table is already cached: %s.%s", table.getDbName(), table.getTableName()));
    }
    long id = HdfsCachingUtil.submitDirective(new Path(table.getSd().getLocation()),
        poolName);
    table.putToParameters(CACHE_DIR_ID_PROP_NAME, Long.toString(id));
    return id;
  }

  /**
   * Caches the location of the given Hive Metastore Partition and updates the
   * partitions's properties with the submitted cache directive ID.
   * Returns the ID of the submitted cache directive and throws if there is an error
   * submitting the directive.
   */
  public static long submitCachePartitionDirective(
      org.apache.hadoop.hive.metastore.api.Partition part,
      String poolName) throws ImpalaRuntimeException {
    if (part.getParameters().get(CACHE_DIR_ID_PROP_NAME) != null) {
      throw new ImpalaRuntimeException(String.format(
          "Partition is already cached: %s.%s/%s", part.getDbName(), part.getTableName(),
          part.getValues()));
    }
    long id = HdfsCachingUtil.submitDirective(new Path(part.getSd().getLocation()),
        poolName);
    part.putToParameters(CACHE_DIR_ID_PROP_NAME, Long.toString(id));
    return id;
  }

  /**
   * Removes the cache directive associated with the table from HDFS, uncaching all
   * data. Also updates the table's metadata. No-op if the table is not cached.
   */
  public static void uncacheTbl(org.apache.hadoop.hive.metastore.api.Table table)
      throws ImpalaRuntimeException {
    Preconditions.checkNotNull(table);
    LOG.debug("Uncaching table: " + table.getDbName() + "." + table.getTableName());
    Long id = getCacheDirIdFromParams(table.getParameters());
    if (id == null) return;
    HdfsCachingUtil.removeDirective(id);
    table.getParameters().remove(CACHE_DIR_ID_PROP_NAME);
  }

  /**
   * Removes the cache directive associated with the partition from HDFS, uncaching all
   * data. Also updates the partition's metadata to remove the cache directive ID.
   * No-op if the table is not cached.
   */
  public static void uncachePartition(
      org.apache.hadoop.hive.metastore.api.Partition part) throws ImpalaException {
    Preconditions.checkNotNull(part);
    Long id = getCacheDirIdFromParams(part.getParameters());
    if (id == null) return;
    HdfsCachingUtil.removeDirective(id);
    part.getParameters().remove(CACHE_DIR_ID_PROP_NAME);
  }

  /**
   * Returns the cache directive ID from the given table/partition parameter
   * map. Returns null if the CACHE_DIR_ID_PROP_NAME key was not set or if
   * there was an error parsing the associated ID.
   */
  public static Long getCacheDirIdFromParams(Map<String, String> params) {
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
  public static String getCachePool(long requestId) throws ImpalaRuntimeException {
    CacheDirectiveEntry entry = getDirective(requestId);
    return entry == null ? null : entry.getInfo().getPool();
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
    LOG.debug(String.format("Waiting on cache directive id: %d. Bytes " +
        "cached (%d) / needed (%d)", directiveId, currentBytesCached, bytesNeeded));
    // All the bytes are cached, just return.
    if (bytesNeeded == currentBytesCached) return;

    // The refresh interval is how often HDFS will update cache directive stats. We use
    // this value to determine how frequently we should poll for changes.
    long hdfsRefreshIntervalMs = dfs.getConf().getLong(
        DFSConfigKeys.DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS,
        DFSConfigKeys.DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS_DEFAULT);
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
        LOG.debug(String.format("Cache directive id: %d has completed." +
            "Bytes cached (%d) / needed (%d)", directiveId, currentBytesCached,
            bytesNeeded));
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
   * Submits a new caching directive for the specified cache pool name and path.
   * Returns the directive ID if the submission was successful or an
   * ImpalaRuntimeException if the submission fails.
   */
  private static long submitDirective(Path path, String poolName)
      throws ImpalaRuntimeException {
    Preconditions.checkNotNull(path);
    Preconditions.checkState(poolName != null && !poolName.isEmpty());
    CacheDirectiveInfo info = new CacheDirectiveInfo.Builder()
        .setExpiration(Expiration.NEVER)
        .setPool(poolName)
        .setPath(path).build();
    LOG.debug("Submitting cache directive: " + info.toString());
    try {
      return dfs.addCacheDirective(info);
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
    LOG.debug("Removing cache directive id: " + directiveId);
    try {
      dfs.removeCacheDirective(directiveId);
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
    LOG.trace("Getting cache directive id: " + directiveId);
    CacheDirectiveInfo filter = new CacheDirectiveInfo.Builder()
        .setId(directiveId)
        .build();
    try {
      RemoteIterator<CacheDirectiveEntry> itr = dfs.listCacheDirectives(filter);
      while (itr.hasNext()) {
        CacheDirectiveEntry entry = itr.next();
        if (entry.getInfo().getId() == directiveId) return entry;
      }
    } catch (IOException e) {
      throw new ImpalaRuntimeException(e.getMessage(), e);
    }
    return null;
  }
}