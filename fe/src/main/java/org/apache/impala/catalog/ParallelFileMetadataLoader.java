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

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.impala.catalog.HdfsPartition.Builder;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.Pair;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.util.ListMap;
import org.apache.impala.util.ThreadNameAnnotator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;


/**
 * Utility to coordinate the issuing of parallel metadata loading requests
 * on a thread pool.
 *
 * This may safely be used even to load a single path: if only one path is to
 * be loaded, this avoids creating any extra threads and uses the current thread
 * instead.
 */
public class ParallelFileMetadataLoader {
  private final static Logger LOG = LoggerFactory.getLogger(
      ParallelFileMetadataLoader.class);

  private static final int MAX_HDFS_PARTITIONS_PARALLEL_LOAD =
      BackendConfig.INSTANCE.maxHdfsPartsParallelLoad();
  private static final int MAX_NON_HDFS_PARTITIONS_PARALLEL_LOAD =
      BackendConfig.INSTANCE.maxNonHdfsPartsParallelLoad();

  public static final AtomicInteger TOTAL_THREADS = new AtomicInteger(0);
  public static final AtomicInteger TOTAL_TABLES = new AtomicInteger(0);

  // Maximum number of errors logged when loading partitioned tables.
  private static final int MAX_PATH_METADATA_LOADING_ERRORS_TO_LOG = 100;

  private final String logPrefix_;
  private final FileSystem fs_;

  // Loaders run in parallel and return a result Object.
  private final List<Pair<String, Callable<Object>>> loaders_;
  // Updaters are run sequentially after the loaders finish. They process the result
  // Object returned by the corresponding loader.
  private final List<Consumer<Object>> updaters_;

  /**
   * Constructs a loader for file descriptors and metadata stats.
   */
  public ParallelFileMetadataLoader(FileSystem fs,
      Collection<Builder> partBuilders,
      ValidWriteIdList writeIdList, ValidTxnList validTxnList, boolean isRecursive,
      @Nullable ListMap<TNetworkAddress> hostIndex, String debugAction,
      String logPrefix) {
    fs_ = fs;
    logPrefix_ = logPrefix;
    if (writeIdList != null || validTxnList != null) {
      // make sure that both either both writeIdList and validTxnList are set or both
      // of them are not.
      Preconditions.checkState(writeIdList != null && validTxnList != null);
    }
    // Group the partitions by their path (multiple partitions may point to the same
    // path).
    Map<String, List<HdfsPartition.Builder>> partsByPath = Maps.newHashMap();
    for (HdfsPartition.Builder p : partBuilders) {
      partsByPath.computeIfAbsent(p.getLocation(), (path) -> new ArrayList<>()).add(p);
    }
    // Create a loader (FileMetadataLoader) and updater for each path.
    loaders_ = new ArrayList<>(partsByPath.size());
    updaters_ = new ArrayList<>(partsByPath.size());
    for (Map.Entry<String, List<HdfsPartition.Builder>> e : partsByPath.entrySet()) {
      List<HdfsPartition.Builder> builders = e.getValue();
      List<FileDescriptor> oldFds = builders.get(0).getFileDescriptors();
      HdfsFileFormat format = builders.get(0).getFileFormat();
      Preconditions.checkState(!HdfsFileFormat.ICEBERG.equals(format));
      FileMetadataLoader loader = new FileMetadataLoader(e.getKey(), isRecursive, oldFds,
          hostIndex, validTxnList, writeIdList, format);
      // If there is a cached partition mapped to this path, we recompute the block
      // locations even if the underlying files have not changed.
      // This is done to keep the cached block metadata up to date.
      boolean hasCachedPartition = Iterables.any(builders,
          HdfsPartition.Builder::isMarkedCached);
      loader.setForceRefreshBlockLocations(hasCachedPartition);
      loader.setDebugAction(debugAction);

      loaders_.add(new Pair<>(e.getKey(), () -> { loader.load(); return loader; }));

      updaters_.add((result) ->
          updatePartBuilders((FileMetadataLoader) result, builders));
    }
  }

  /**
   * Store the loaded FDs from loader into the partitions via builders.
   */
  private static void updatePartBuilders(FileMetadataLoader loader,
      List<HdfsPartition.Builder> builders) {
    for (HdfsPartition.Builder partBuilder : builders) {
      // Checks if we can reuse the old file descriptors. Partition builders in the
      // list may have different file descriptors. We need to verify them one by one.
      if ((!loader.hasFilesChangedCompareTo(partBuilder.getFileDescriptors()))) {
        LOG.trace("Detected files unchanged on partition {}",
            partBuilder.getPartitionName());
        continue;
      }
      partBuilder.clearFileDescriptors();
      List<FileDescriptor> deleteDescriptors = loader.getLoadedDeleteDeltaFds();
      if (deleteDescriptors != null && !deleteDescriptors.isEmpty()) {
        partBuilder.setInsertFileDescriptors(loader.getLoadedInsertDeltaFds());
        partBuilder.setDeleteFileDescriptors(loader.getLoadedDeleteDeltaFds());
      } else {
        partBuilder.setFileDescriptors(loader.getLoadedFds());
      }
      partBuilder.setFileMetadataStats(loader.getFileMetadataStats());
    }
  }

  /**
   * Constructs a loader from provided loaders and updaters.
   */
  public ParallelFileMetadataLoader(FileSystem fs, String logPrefix,
      List<Pair<String, Callable<Object>>> loaders,
      List<Consumer<Object>> updaters) {
    Preconditions.checkNotNull(loaders);
    Preconditions.checkNotNull(updaters);
    Preconditions.checkArgument(loaders.size() == updaters.size());
    fs_ = fs;
    logPrefix_ = logPrefix;
    loaders_ = loaders;
    updaters_ = updaters;
  }

  /**
   * Loads the file metadata for the given list of Partitions in the constructor. If the
   * load is successful also set the fileDescriptors in the HdfsPartition.Builders. Any
   * successful loaders are guaranteed to complete before any exception is thrown.
   * @throws TableLoadingException
   */
  public void load() throws TableLoadingException {
    if (loaders_.isEmpty()) return;

    int failedLoadTasks = 0;
    int poolSize = getPoolSize(loaders_.size(), fs_);
    ExecutorService pool = createPool(poolSize, logPrefix_);
    TOTAL_THREADS.addAndGet(poolSize);
    try (ThreadNameAnnotator tna = new ThreadNameAnnotator(logPrefix_)) {
      TOTAL_TABLES.incrementAndGet();
      List<Pair<String, Future<Object>>> futures = new ArrayList<>(loaders_.size());
      for (Pair<String, Callable<Object>> loader : loaders_) {
        futures.add(new Pair<>(loader.first, pool.submit(loader.second)));
      }

      // Wait for the loaders to finish.
      Preconditions.checkState(futures.size() == updaters_.size());
      for (int i = 0; i < futures.size(); i++) {
        Pair<String, Future<Object>> future = futures.get(i);
        try {
          Object result = future.second.get();
          updaters_.get(i).accept(result);
        } catch (ExecutionException | InterruptedException e) {
          if (++failedLoadTasks <= MAX_PATH_METADATA_LOADING_ERRORS_TO_LOG) {
            LOG.error("{} encountered an error loading data for path {}",
                logPrefix_, future.first, e);
          }
        }
      }
    } finally {
      pool.shutdown();
      TOTAL_THREADS.addAndGet(-poolSize);
      TOTAL_TABLES.addAndGet(-1);
    }
    if (failedLoadTasks > 0) {
      int errorsNotLogged = failedLoadTasks - MAX_PATH_METADATA_LOADING_ERRORS_TO_LOG;
      if (errorsNotLogged > 0) {
        LOG.error("{} error loading {} paths. Only the first {} errors were logged",
            logPrefix_, failedLoadTasks, MAX_PATH_METADATA_LOADING_ERRORS_TO_LOG);
      }
      throw new TableLoadingException(logPrefix_ + ": failed to load " + failedLoadTasks
          + " paths. Check the catalog server log for more details.");
    }
  }

  /**
   * Returns the thread pool to load the file metadata. Callers should use
   * {@link #getPoolSize(int, FileSystem)} to get a correct pool size.
   */
  public static ExecutorService createPool(int poolSize, String logPrefix) {
    Preconditions.checkState(poolSize > 0, "Illegal poolSize: {}", poolSize);
    if (poolSize == 1) {
      return MoreExecutors.newDirectExecutorService();
    } else {
      LOG.info("{} using a thread pool of size {}", logPrefix, poolSize);
      return Executors.newFixedThreadPool(poolSize);
    }
  }

  /**
   * Returns the thread pool size to load the file metadata.
   *
   * We use different thread pool sizes for HDFS and non-HDFS tables since the latter
   * supports much higher throughput of RPC calls for listStatus/listFiles. For
   * simplicity, the filesystem type is determined based on the table's root path and
   * not for each partition individually. Based on our experiments, S3 showed a linear
   * speed up (up to ~100x) with increasing number of loading threads where as the HDFS
   * throughput was limited to ~5x in un-secure clusters and up to ~3.7x in secure
   * clusters. We narrowed it down to scalability bottlenecks in HDFS RPC implementation
   * (HADOOP-14558) on both the server and the client side.
   */
  public static int getPoolSize(int numLoaders, FileSystem fs) {
    int poolSize = FileSystemUtil.supportsStorageIds(fs) ?
        MAX_HDFS_PARTITIONS_PARALLEL_LOAD :
        MAX_NON_HDFS_PARTITIONS_PARALLEL_LOAD;
    // Thread pool size need not exceed the number of paths to be loaded.
    poolSize = Math.min(numLoaders, poolSize);
    return poolSize;
  }
}
