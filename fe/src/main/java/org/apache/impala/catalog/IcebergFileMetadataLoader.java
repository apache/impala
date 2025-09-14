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

import static org.apache.impala.catalog.ParallelFileMetadataLoader.
    MAX_HDFS_PARTITIONS_PARALLEL_LOAD;
import static org.apache.impala.catalog.ParallelFileMetadataLoader.TOTAL_THREADS;
import static org.apache.impala.catalog.ParallelFileMetadataLoader.createPool;

import com.codahale.metrics.Clock;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.ContentFile;
import org.apache.impala.catalog.FeFsTable.FileMetadataStats;
import org.apache.impala.catalog.FeIcebergTable.Utils;
import org.apache.impala.catalog.iceberg.GroupedContentFiles;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TIcebergPartition;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.util.IcebergUtil;
import org.apache.impala.util.ListMap;
import org.apache.impala.util.ThreadNameAnnotator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility for loading the content files metadata of the Iceberg tables.
 */
public class IcebergFileMetadataLoader extends FileMetadataLoader {
  private final static Logger LOG = LoggerFactory.getLogger(
      IcebergFileMetadataLoader.class);

  private final org.apache.iceberg.Table iceTbl_;
  private final Path tablePath_;
  private final GroupedContentFiles icebergFiles_;
  private final List<TIcebergPartition> oldIcebergPartitions_;
  private AtomicInteger nextPartitionId_ = new AtomicInteger(0);
  // Map of the freshly loaded Iceberg partitions and their corresponding ids.
  private ConcurrentHashMap<TIcebergPartition, Integer> loadedIcebergPartitions_;
  private final boolean requiresDataFilesInTableLocation_;

  public IcebergFileMetadataLoader(org.apache.iceberg.Table iceTbl,
        Iterable<IcebergFileDescriptor> oldFds, ListMap<TNetworkAddress> hostIndex,
        GroupedContentFiles icebergFiles, List<TIcebergPartition> partitions,
        boolean requiresDataFilesInTableLocation) {
    super(iceTbl.location(), true, oldFds, hostIndex, null, null,
        HdfsFileFormat.ICEBERG);
    iceTbl_ = iceTbl;
    tablePath_ = FileSystemUtil.createFullyQualifiedPath(new Path(iceTbl.location()));
    icebergFiles_ = icebergFiles;
    oldIcebergPartitions_ = partitions;
    requiresDataFilesInTableLocation_ = requiresDataFilesInTableLocation;
  }

  @Override
  public void load() throws CatalogException, IOException {
    String msg = String.format("Refreshing Iceberg file metadata from path %s", partDir_);
    LOG.trace(msg);
    try (ThreadNameAnnotator tna = new ThreadNameAnnotator(msg)) {
      loadInternal();
    } finally {
      FileMetadataLoader.TOTAL_TASKS.decrementAndGet();
    }
  }

  public List<IcebergFileDescriptor> getLoadedIcebergFds() {
    Preconditions.checkState(loadedFds_ != null,
        "Must have successfully loaded first");
    return loadedFds_.stream()
        .map(fd -> (IcebergFileDescriptor)fd)
        .collect(Collectors.toList());
  }

  public Map<TIcebergPartition, Integer> getIcebergPartitions() {
    Preconditions.checkNotNull(loadedIcebergPartitions_);
    return Collections.unmodifiableMap(loadedIcebergPartitions_);
  }

  public List<TIcebergPartition> getIcebergPartitionList() {
    return IcebergContentFileStore.convertPartitionMapToList(getIcebergPartitions());
  }

  private void loadInternal() throws CatalogException, IOException {
    loadedFds_ = new ArrayList<>();
    loadedIcebergPartitions_ = new ConcurrentHashMap<>();
    loadStats_ = new LoadStats(partDir_);
    fileMetadataStats_ = new FileMetadataStats();

    // Process the existing Fd ContentFile and return the newly added ContentFile
    Iterable<ContentFile<?>> newContentFiles = loadContentFilesWithOldFds(tablePath_);
    // Iterate through all the newContentFiles, determine if StorageIds are supported,
    // and use different handling methods accordingly.
    // This considers that different ContentFiles are on different FileSystems
    List<ContentFile<?>> filesSupportsStorageIds = Lists.newArrayList();
    FileSystem fsForTable = FileSystemUtil.getFileSystemForPath(tablePath_);
    FileSystem defaultFs = FileSystemUtil.getDefaultFileSystem();
    AtomicLong numUnknownDiskIds = new AtomicLong();
    for (ContentFile<?> contentFile : newContentFiles) {
      FileSystem fsForPath = fsForTable;
      // If requiresDataFilesInTableLocation_ is true, we assume that the file system
      // for all ContentFiles is the same as fsForTable
      if (!requiresDataFilesInTableLocation_) {
        Path path = new Path(contentFile.path().toString());
        fsForPath = path.toUri().getScheme() != null ?
            FileSystemUtil.getFileSystemForPath(path) : defaultFs;
      }
      // If the specific fs does not support StorageIds, then
      // we create FileDescriptor directly
      if (FileSystemUtil.supportsStorageIds(fsForPath)) {
        filesSupportsStorageIds.add(contentFile);
      } else {
        IcebergFileDescriptor fd =
            createNonLocatedFd(fsForPath, contentFile, tablePath_, numUnknownDiskIds);
        registerNewlyLoadedFd(fd);
      }
    }
    List<IcebergFileDescriptor> newFds = parallelListing(filesSupportsStorageIds,
        numUnknownDiskIds);
    for (IcebergFileDescriptor fd : newFds) {
      registerNewlyLoadedFd(fd);
    }
    loadStats_.unknownDiskIds += numUnknownDiskIds.get();
    if (LOG.isTraceEnabled()) {
      LOG.trace(loadStats_.debugString());
    }
  }

  private void registerNewlyLoadedFd(IcebergFileDescriptor fd) {
    loadedFds_.add(fd);
    fileMetadataStats_.accumulate(fd);
    ++loadStats_.loadedFiles;
  }

  /**
   *  Iceberg tables are a collection of immutable, uniquely identifiable data files,
   *  which means we can safely reuse the old FDs.
   */
  private Iterable<ContentFile<?>> loadContentFilesWithOldFds(Path partPath)
      throws TableLoadingException {
    if (forceRefreshLocations || oldFdsByPath_.isEmpty()) {
      return icebergFiles_.getAllContentFiles();
    }
    List<ContentFile<?>> newContentFiles = Lists.newArrayList();
    for (ContentFile<?> contentFile : icebergFiles_.getAllContentFiles()) {
      IcebergFileDescriptor fd = getOldFd(contentFile, partPath);
      if (fd == null) {
        newContentFiles.add(contentFile);
      } else {
        int oldPartId = fd.getFbFileMetadata().icebergMetadata().partId();
        TIcebergPartition partition = oldIcebergPartitions_.get(oldPartId);
        Integer newPartId = loadedIcebergPartitions_.computeIfAbsent(
            partition, k -> nextPartitionId_.getAndIncrement());
        // Look up the partition info in this old file descriptor from the partition list.
        // Put the partition info in the new partitions map and write the new partition id
        // to the file metadata of the fd.
        if (!fd.getFbFileMetadata().icebergMetadata().mutatePartId(newPartId)) {
          throw new TableLoadingException("Error modifying the Iceberg file descriptor.");
        }
        ++loadStats_.skippedFiles;
        loadedFds_.add(fd);
        fileMetadataStats_.accumulate(fd);
      }
    }
    return newContentFiles;
  }

  private IcebergFileDescriptor createNonLocatedFd(FileSystem fs,
      ContentFile<?> contentFile, Path partPath, AtomicLong numUnknownDiskIds)
      throws CatalogException, IOException {
    Path fileLoc = FileSystemUtil.createFullyQualifiedPath(
        new Path(contentFile.path().toString()));
    // For OSS service (e.g. S3A, COS, OSS, etc), we create FileStatus ourselves.
    FileStatus stat = Utils.createFileStatus(contentFile, fileLoc);

    Pair<String, String> absPathRelPath = getAbsPathRelPath(partPath, stat);
    String absPath = absPathRelPath.first;
    String relPath = absPathRelPath.second;
    int partitionId = addPartitionInfo(contentFile);

    return IcebergFileDescriptor.cloneWithFileMetadata(
        createFd(fs, stat, relPath, numUnknownDiskIds, absPath),
        IcebergUtil.createIcebergMetadata(iceTbl_, contentFile, partitionId));
  }

  private IcebergFileDescriptor createLocatedFd(FileSystem fs, ContentFile<?> contentFile,
      FileStatus stat, Path partPath, AtomicLong numUnknownDiskIds)
      throws CatalogException, IOException {
    Preconditions.checkState(stat instanceof LocatedFileStatus);

    Pair<String, String> absPathRelPath = getAbsPathRelPath(partPath, stat);
    String absPath = absPathRelPath.first;
    String relPath = absPathRelPath.second;
    int partitionId = addPartitionInfo(contentFile);

    return IcebergFileDescriptor.cloneWithFileMetadata(
        createFd(fs, stat, relPath, numUnknownDiskIds, absPath),
        IcebergUtil.createIcebergMetadata(iceTbl_, contentFile, partitionId));
  }

  private int addPartitionInfo(ContentFile<?> contentFile) {
    TIcebergPartition partition =
        IcebergUtil.createIcebergPartitionInfo(iceTbl_, contentFile);
    return loadedIcebergPartitions_.computeIfAbsent(
        partition, k -> nextPartitionId_.getAndIncrement());
  }

  Pair<String, String> getAbsPathRelPath(Path partPath, FileStatus stat)
      throws TableLoadingException {
    String absPath = null;
    String relPath = FileSystemUtil.relativizePathNoThrow(stat.getPath(), partPath);
    if (relPath == null) {
      if (requiresDataFilesInTableLocation_) {
        throw new TableLoadingException(String.format("Failed to load Iceberg datafile " +
            "%s, because it's outside of the table location", stat.getPath().toUri()));
      } else {
        absPath = stat.getPath().toString();
      }
    }
    return new Pair<>(absPath, relPath);
  }

  /**
   * Using a thread pool to perform parallel List operations on the FileSystem, this takes
   * into account the situation where multiple FileSystems exist within the ContentFiles.
   */
  private List<IcebergFileDescriptor> parallelListing(
      List<ContentFile<?>> contentFiles,
      AtomicLong numUnknownDiskIds) throws IOException {
    final Map<Path, List<ContentFile<?>>> partitionPaths =
        collectPartitionPaths(contentFiles);
    if (partitionPaths.isEmpty()) return Collections.emptyList();
    List<IcebergFileDescriptor> ret = new ArrayList<>();
    String logPrefix = "Parallel Iceberg file metadata listing";
    int poolSize = getPoolSize(partitionPaths.size());
    ExecutorService pool = createPool(poolSize, logPrefix);
    TOTAL_THREADS.addAndGet(poolSize);
    try (ThreadNameAnnotator tna = new ThreadNameAnnotator(logPrefix)) {
      TOTAL_TASKS.addAndGet(partitionPaths.size());
      List<Future<List<IcebergFileDescriptor>>> tasks =
          partitionPaths.entrySet().stream()
              .map(entry -> pool.submit(() -> {
                try {
                  return createFdsForPartition(entry.getKey(), entry.getValue(),
                      numUnknownDiskIds);
                } finally {
                  TOTAL_TASKS.decrementAndGet();
                }
              }))
              .collect(Collectors.toList());
      for (Future<List<IcebergFileDescriptor>> task : tasks) {
        ret.addAll(task.get());
      }
    } catch (ExecutionException | InterruptedException e) {
      throw new IOException(String.format("%s: failed to load paths.", logPrefix), e);
    } finally {
      TOTAL_THREADS.addAndGet(-poolSize);
      pool.shutdown();
    }
    return ret;
  }

  private Map<Path, List<ContentFile<?>>> collectPartitionPaths(
      List<ContentFile<?>> contentFiles) {
    final Clock clock = Clock.defaultClock();
    long startTime = clock.getTick();
    Map<Path, List<ContentFile<?>>> ret = contentFiles.stream()
        .collect(Collectors.groupingBy(
            cf -> new Path(String.valueOf(cf.path())).getParent(),
            HashMap::new,
            Collectors.toList()
        ));
    long duration = clock.getTick() - startTime;
    LOG.info("Collected {} Iceberg content files into {} partitions. Duration: {}",
        contentFiles.size(), ret.size(), PrintUtils.printTimeNs(duration));
    return ret;
  }

  /**
   * Returns thread pool size for listing files in parallel from storage systems that
   * provide block location information.
   */
  private static int getPoolSize(int numLoaders) {
    return Math.min(numLoaders, MAX_HDFS_PARTITIONS_PARALLEL_LOAD);
  }

  private List<IcebergFileDescriptor> createFdsForPartition(Path partitionPath,
      List<ContentFile<?>> contentFiles, AtomicLong numUnknownDiskIds)
      throws IOException, CatalogException {
    FileSystem fs = FileSystemUtil.getFileSystemForPath(partitionPath);
    RemoteIterator<? extends FileStatus> remoteIterator =
        FileSystemUtil.listFiles(fs, partitionPath, recursive_, debugAction_);
    Map<Path, FileStatus> pathToFileStatus = new HashMap<>();
    while (remoteIterator.hasNext()) {
      FileStatus status = remoteIterator.next();
      pathToFileStatus.put(status.getPath(), status);
    }
    List<IcebergFileDescriptor> ret = new ArrayList<>();
    for (ContentFile<?> contentFile : contentFiles) {
      Path path = FileSystemUtil.createFullyQualifiedPath(
          new Path(contentFile.path().toString()));
      FileStatus stat = pathToFileStatus.get(path);
      if (stat == null) {
        LOG.warn(String.format(
            "Failed to load Iceberg content file: '%s', Not found on storage",
            contentFile.path().toString()));
        continue;
      }
      ret.add(createLocatedFd(fs, contentFile, stat, tablePath_, numUnknownDiskIds));
    }
    return ret;
  }

  IcebergFileDescriptor getOldFd(ContentFile<?> contentFile, Path partPath)
      throws TableLoadingException {
    Path contentFilePath = FileSystemUtil.createFullyQualifiedPath(
        new Path(contentFile.path().toString()));
    String lookupPath = FileSystemUtil.relativizePathNoThrow(contentFilePath, partPath);
    if (lookupPath == null) {
      if (requiresDataFilesInTableLocation_) {
        throw new TableLoadingException(String.format("Failed to load Iceberg datafile " +
            "%s, because it's outside of the table location", contentFilePath));
      } else {
        lookupPath = contentFilePath.toString();
      }
    }
    FileDescriptor fd = oldFdsByPath_.get(lookupPath);
    if (fd == null) return null;

    Preconditions.checkState(fd instanceof IcebergFileDescriptor);
    return (IcebergFileDescriptor) fd;
  }
}
