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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.Nullable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.impala.catalog.FeFsTable.Utils;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
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

  // Maximum number of errors logged when loading partitioned tables.
  private static final int MAX_PATH_METADATA_LOADING_ERRORS_TO_LOG = 100;

  private final String logPrefix_;
  private final Map<Path, FileMetadataLoader> loaders_;
  private final Map<Path, List<HdfsPartition.Builder>> partsByPath_;
  private final FileSystem fs_;

  public ParallelFileMetadataLoader(FileSystem fs,
      Collection<HdfsPartition.Builder> partBuilders,
      ValidWriteIdList writeIdList, ValidTxnList validTxnList, boolean isRecursive,
      @Nullable ListMap<TNetworkAddress> hostIndex, String debugAction, String logPrefix)
      throws CatalogException {
    if (writeIdList != null || validTxnList != null) {
      // make sure that both either both writeIdList and validTxnList are set or both
      // of them are not.
      Preconditions.checkState(writeIdList != null && validTxnList != null);
    }
    // Group the partitions by their path (multiple partitions may point to the same
    // path).
    partsByPath_ = Maps.newHashMap();
    for (HdfsPartition.Builder p : partBuilders) {
      Path partPath = FileSystemUtil.createFullyQualifiedPath(new Path(p.getLocation()));
      partsByPath_.computeIfAbsent(partPath, (path) -> new ArrayList<>())
          .add(p);
    }
    // Create a FileMetadataLoader for each path.
    loaders_ = Maps.newHashMap();
    for (Map.Entry<Path, List<HdfsPartition.Builder>> e : partsByPath_.entrySet()) {
      List<FileDescriptor> oldFds = e.getValue().get(0).getFileDescriptors();
      FileMetadataLoader loader = new FileMetadataLoader(e.getKey(),
          isRecursive, oldFds, hostIndex, validTxnList, writeIdList,
          e.getValue().get(0).getFileFormat());
      // If there is a cached partition mapped to this path, we recompute the block
      // locations even if the underlying files have not changed.
      // This is done to keep the cached block metadata up to date.
      boolean hasCachedPartition = Iterables.any(e.getValue(),
          HdfsPartition.Builder::isMarkedCached);
      loader.setForceRefreshBlockLocations(hasCachedPartition);
      loader.setDebugAction(debugAction);
      loaders_.put(e.getKey(), loader);
    }
    this.logPrefix_ = logPrefix;
    this.fs_ = fs;
  }

  /**
   * Loads the file metadata for the given list of Partitions in the constructor. If the
   * load is successful also set the fileDescriptors in the HdfsPartition.Builders.
   * @throws TableLoadingException
   */
  void load() throws TableLoadingException {
    loadInternal();

    // Store the loaded FDs into the partitions.
    for (Map.Entry<Path, List<HdfsPartition.Builder>> e : partsByPath_.entrySet()) {
      Path p = e.getKey();
      FileMetadataLoader loader = loaders_.get(p);

      for (HdfsPartition.Builder partBuilder : e.getValue()) {
        // Checks if we can reuse the old file descriptors. Partition builders in the list
        // may have different old file descriptors. We need to verify them one by one.
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
      }
    }
  }

  /**
   * Call 'load()' in parallel on all of the loaders. If any loaders fail, throws
   * an exception. However, any successful loaders are guaranteed to complete
   * before any exception is thrown.
   */
  private void loadInternal() throws TableLoadingException {
    if (loaders_.isEmpty()) return;

    int failedLoadTasks = 0;
    ExecutorService pool = createPool();
    try (ThreadNameAnnotator tna = new ThreadNameAnnotator(logPrefix_)) {
      List<Pair<FileMetadataLoader, Future<Void>>> futures =
          new ArrayList<>(loaders_.size());
      for (FileMetadataLoader loader : loaders_.values()) {
        futures.add(new Pair<>(
            loader, pool.submit(() -> { loader.load(); return null; })));
      }

      // Wait for the loaders to finish.
      for (int i = 0; i < futures.size(); i++) {
        try {
          futures.get(i).second.get();
        } catch (ExecutionException | InterruptedException e) {
          if (++failedLoadTasks <= MAX_PATH_METADATA_LOADING_ERRORS_TO_LOG) {
            LOG.error(logPrefix_ + " encountered an error loading data for path " +
                futures.get(i).first.getPartDir(), e);
          }
        }
      }
    } finally {
      pool.shutdown();
    }
    if (failedLoadTasks > 0) {
      int errorsNotLogged = failedLoadTasks - MAX_PATH_METADATA_LOADING_ERRORS_TO_LOG;
      if (errorsNotLogged > 0) {
        LOG.error(logPrefix_ + " error loading {} paths. Only the first {} errors " +
            "were logged", failedLoadTasks, MAX_PATH_METADATA_LOADING_ERRORS_TO_LOG);
      }
      throw new TableLoadingException(logPrefix_ + ": failed to load " + failedLoadTasks
          + " paths. Check the catalog server log for more details.");
    }
  }

  /**
   * Returns the thread pool to load the file metadata.
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
  private ExecutorService createPool() {
    int numLoaders = loaders_.size();
    Preconditions.checkState(numLoaders > 0);
    int poolSize = FileSystemUtil.supportsStorageIds(fs_) ?
        MAX_HDFS_PARTITIONS_PARALLEL_LOAD : MAX_NON_HDFS_PARTITIONS_PARALLEL_LOAD;
    // Thread pool size need not exceed the number of paths to be loaded.
    poolSize = Math.min(numLoaders, poolSize);

    if (poolSize == 1) {
      return MoreExecutors.newDirectExecutorService();
    } else {
      LOG.info(logPrefix_ + " using a thread pool of size {}", poolSize);
      return Executors.newFixedThreadPool(poolSize);
    }
  }
}
