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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.fs.FileSystem;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.util.ThreadNameAnnotator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
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
  private List<FileMetadataLoader> loaders_;
  private final FileSystem fs_;

  /**
   * @param logPrefix informational prefix for log messages
   * @param fs the filesystem to load from (used to determine appropriate parallelism)
   * @param loaders the metadata loaders to execute in parallel.
   */
  public ParallelFileMetadataLoader(String logPrefix, FileSystem fs,
      Collection<FileMetadataLoader> loaders) {
    logPrefix_ = logPrefix;
    loaders_ = ImmutableList.copyOf(loaders);

    // TODO(todd) in actuality, different partitions could be on different file systems.
    // We probably should create one pool per filesystem type, and size each of those
    // pools based on that particular filesystem, so if there's a mixed S3+HDFS table
    // we do the right thing.
    fs_ = fs;
  }

  /**
   * Call 'load()' in parallel on all of the loaders. If any loaders fail, throws
   * an exception. However, any successful loaders are guaranteed to complete
   * before any exception is thrown.
   */
  void load() throws TableLoadingException {
    if (loaders_.isEmpty()) return;

    int failedLoadTasks = 0;
    ExecutorService pool = createPool();
    try (ThreadNameAnnotator tna = new ThreadNameAnnotator(logPrefix_)) {
      List<Future<Void>> futures = new ArrayList<>(loaders_.size());
      for (FileMetadataLoader loader : loaders_) {
        futures.add(pool.submit(() -> { loader.load(); return null; }));
      }

      // Wait for the loaders to finish.
      for (int i = 0; i < futures.size(); i++) {
        try {
          futures.get(i).get();
        } catch (ExecutionException | InterruptedException e) {
          if (++failedLoadTasks <= MAX_PATH_METADATA_LOADING_ERRORS_TO_LOG) {
            LOG.error(logPrefix_ + " encountered an error loading data for path " +
                loaders_.get(i).getPartDir(), e);
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
