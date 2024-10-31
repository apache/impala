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

import static org.apache.impala.catalog.ParallelFileMetadataLoader.TOTAL_THREADS;
import static org.apache.impala.catalog.ParallelFileMetadataLoader.createPool;
import static org.apache.impala.catalog.ParallelFileMetadataLoader.getPoolSize;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.iceberg.ContentFile;
import org.apache.impala.catalog.FeIcebergTable.Utils;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.HdfsTable.FileMetadataStats;
import org.apache.impala.catalog.iceberg.GroupedContentFiles;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.Reference;
import org.apache.impala.common.Pair;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TNetworkAddress;
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

  // Default value of 'newFilesThreshold_' if the given parameter or startup flag have
  // invalid value.
  private final int NEW_FILES_THRESHOLD_DEFAULT = 100;

  // If there are more new files than 'newFilesThreshold_', we should fall back
  // to regular file metadata loading.
  private final int newFilesThreshold_;

  private final GroupedContentFiles icebergFiles_;
  private final boolean canDataBeOutsideOfTableLocation_;
  private boolean useParallelListing_;

  public IcebergFileMetadataLoader(Path partDir, boolean recursive,
      List<FileDescriptor> oldFds, ListMap<TNetworkAddress> hostIndex,
      ValidTxnList validTxnList, ValidWriteIdList writeIds,
      GroupedContentFiles icebergFiles, boolean canDataBeOutsideOfTableLocation) {
    this(partDir, recursive, oldFds, hostIndex, validTxnList, writeIds,
        icebergFiles, canDataBeOutsideOfTableLocation,
        BackendConfig.INSTANCE.icebergReloadNewFilesThreshold());
  }

  public IcebergFileMetadataLoader(Path partDir, boolean recursive,
      List<FileDescriptor> oldFds, ListMap<TNetworkAddress> hostIndex,
      ValidTxnList validTxnList, ValidWriteIdList writeIds,
      GroupedContentFiles icebergFiles, boolean canDataBeOutsideOfTableLocation,
      int newFilesThresholdParam) {
    super(partDir, recursive, oldFds, hostIndex, validTxnList, writeIds,
        HdfsFileFormat.ICEBERG);
    icebergFiles_ = icebergFiles;
    canDataBeOutsideOfTableLocation_ = canDataBeOutsideOfTableLocation;
    if (newFilesThresholdParam >= 0) {
      newFilesThreshold_ = newFilesThresholdParam;
    } else {
      newFilesThreshold_ = NEW_FILES_THRESHOLD_DEFAULT;
      LOG.warn("Ignoring invalid new files threshold: {} " +
          "using value: {}", newFilesThresholdParam, newFilesThreshold_);
    }
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

  private void loadInternal() throws CatalogException, IOException {
    loadedFds_ = new ArrayList<>();
    loadStats_ = new LoadStats(partDir_);
    fileMetadataStats_ = new FileMetadataStats();

    // Process the existing Fd ContentFile and return the newly added ContentFile
    Iterable<ContentFile<?>> newContentFiles = loadContentFilesWithOldFds();
    // Iterate through all the newContentFiles, determine if StorageIds are supported,
    // and use different handling methods accordingly.
    // This considers that different ContentFiles are on different FileSystems
    List<Pair<FileSystem, ContentFile<?>>> filesSupportsStorageIds = Lists.newArrayList();
    FileSystem fsForTable = FileSystemUtil.getFileSystemForPath(partDir_);
    FileSystem defaultFs = FileSystemUtil.getDefaultFileSystem();
    for (ContentFile<?> contentFile : newContentFiles) {
      FileSystem fsForPath = fsForTable;
      // If canDataBeOutsideOfTableLocation is not true, we assume that the file system
      // for all ContentFiles is the same as fsForTable
      if (canDataBeOutsideOfTableLocation_) {
        Path path = new Path(contentFile.path().toString());
        fsForPath = path.toUri().getScheme() != null ?
            FileSystemUtil.getFileSystemForPath(path) : defaultFs;
      }
      // If the specific fs does not support StorageIds, then
      // we create FileDescriptor directly
      if (FileSystemUtil.supportsStorageIds(fsForPath)) {
        filesSupportsStorageIds.add(Pair.create(fsForPath, contentFile));
      } else {
        FileDescriptor fd = createFd(fsForPath, contentFile, null, null);
        loadedFds_.add(fd);
        fileMetadataStats_.accumulate(fd);
        ++loadStats_.loadedFiles;
      }
    }
    // If the number of filesSupportsStorageIds are greater than newFilesThreshold,
    // we will use a recursive file listing to load file metadata. If number of new
    // files are less or equal to this, we will load the metadata of the newly added
    // files one by one
    useParallelListing_ = filesSupportsStorageIds.size() > newFilesThreshold_;
    Reference<Long> numUnknownDiskIds = new Reference<>(0L);
    Map<Path, FileStatus> nameToFileStatus = Collections.emptyMap();
    if (useParallelListing_) {
      nameToFileStatus = parallelListing(filesSupportsStorageIds);
    }
    for (Pair<FileSystem, ContentFile<?>> contentFileInfo : filesSupportsStorageIds) {
      Path path = FileSystemUtil.createFullyQualifiedPath(
          new Path(contentFileInfo.getSecond().path().toString()));
      FileStatus stat = nameToFileStatus.get(path);
      FileDescriptor fd = createFd(contentFileInfo.getFirst(),
          contentFileInfo.getSecond(), stat, numUnknownDiskIds);
      loadedFds_.add(fd);
      fileMetadataStats_.accumulate(fd);
    }
    loadStats_.loadedFiles += filesSupportsStorageIds.size();
    loadStats_.unknownDiskIds += numUnknownDiskIds.getRef();
    if (LOG.isTraceEnabled()) {
      LOG.trace(loadStats_.debugString());
    }
  }

  @VisibleForTesting
  boolean useParallelListing() {
    return useParallelListing_;
  }

  /**
   *  Iceberg tables are a collection of immutable, uniquely identifiable data files,
   *  which means we can safely reuse the old FDs.
   */
  private Iterable<ContentFile<?>> loadContentFilesWithOldFds() throws IOException {
    if (forceRefreshLocations || oldFdsByPath_.isEmpty()) {
      return icebergFiles_.getAllContentFiles();
    }
    List<ContentFile<?>> newContentFiles = Lists.newArrayList();
    for (ContentFile<?> contentFile : icebergFiles_.getAllContentFiles()) {
      FileDescriptor fd = getOldFd(contentFile);
      if (fd == null) {
        newContentFiles.add(contentFile);
      } else {
        ++loadStats_.skippedFiles;
        loadedFds_.add(fd);
        fileMetadataStats_.accumulate(fd);
      }
    }
    return newContentFiles;
  }

  private FileDescriptor createFd(FileSystem fs, ContentFile<?> contentFile,
      FileStatus stat, Reference<Long> numUnknownDiskIds) throws IOException {
    if (stat == null) {
      Path fileLoc = FileSystemUtil.createFullyQualifiedPath(
          new Path(contentFile.path().toString()));
      if (FileSystemUtil.supportsStorageIds(fs)) {
        stat = Utils.createLocatedFileStatus(fileLoc, fs);
      } else {
        // For OSS service (e.g. S3A, COS, OSS, etc), we create FileStatus ourselves.
        stat = Utils.createFileStatus(contentFile, fileLoc);
      }
    }

    String absPath = null;
    String relPath = FileSystemUtil.relativizePathNoThrow(stat.getPath(), partDir_);
    if (relPath == null) {
      if (canDataBeOutsideOfTableLocation_) {
        absPath = stat.getPath().toString();
      } else {
        throw new IOException(String.format("Failed to load Iceberg datafile %s, because "
            + "it's outside of the table location", stat.getPath().toUri()));
      }
    }
    return createFd(fs, stat, relPath, numUnknownDiskIds, absPath);
  }

  /**
   * Using a thread pool to perform parallel List operations on the FileSystem, this takes
   * into account the situation where multiple FileSystems exist within the ContentFiles.
   */
  private Map<Path, FileStatus> parallelListing(
      Iterable<Pair<FileSystem, ContentFile<?>>> contentFiles) throws IOException {
    final Set<Path> partitionPaths = collectPartitionPaths(contentFiles);
    if (partitionPaths.size() == 0) return Collections.emptyMap();
    String logPrefix = "Parallel Iceberg file metadata listing";
    // Use the file system type of the table's root path as
    // the basis for determining the pool size.
    int poolSize = getPoolSize(partitionPaths.size(),
        FileSystemUtil.getFileSystemForPath(partDir_));
    ExecutorService pool = createPool(poolSize, logPrefix);
    TOTAL_THREADS.addAndGet(poolSize);
    Map<Path, FileStatus> nameToFileStatus = Maps.newConcurrentMap();
    try (ThreadNameAnnotator tna = new ThreadNameAnnotator(logPrefix)) {
      TOTAL_TASKS.addAndGet(partitionPaths.size());
      List<Future<Void>> tasks =
          partitionPaths.stream()
              .map(path -> pool.submit(() -> {
                try {
                  return listingTask(path, nameToFileStatus);
                } finally {
                  TOTAL_TASKS.decrementAndGet();
                }
              }))
              .collect(Collectors.toList());
      for (Future<Void> task : tasks) { task.get(); }
    } catch (ExecutionException | InterruptedException e) {
      throw new IOException(String.format("%s: failed to load paths.", logPrefix), e);
    } finally {
      TOTAL_THREADS.addAndGet(-poolSize);
      pool.shutdown();
    }
    return nameToFileStatus;
  }

  private Set<Path> collectPartitionPaths(
      Iterable<Pair<FileSystem, ContentFile<?>>> contentFiles) {
    return StreamSupport.stream(contentFiles.spliterator(), false)
        .map(contentFile ->
            new Path(String.valueOf(contentFile.getSecond().path())).getParent())
        .collect(Collectors.toSet());
  }

  private Void listingTask(Path partitionPath,
      Map<Path, FileStatus> nameToFileStatus) throws IOException {
    FileSystem fs = FileSystemUtil.getFileSystemForPath(partitionPath);
    RemoteIterator<? extends FileStatus> remoteIterator =
        FileSystemUtil.listFiles(fs, partitionPath, recursive_, debugAction_);
    Map<Path, FileStatus> perThreadMapping = new HashMap<>();
    while (remoteIterator.hasNext()) {
      FileStatus status = remoteIterator.next();
      perThreadMapping.put(status.getPath(), status);
    }
    nameToFileStatus.putAll(perThreadMapping);
    return null;
  }

  FileDescriptor getOldFd(ContentFile<?> contentFile) throws IOException {
    Path contentFilePath = FileSystemUtil.createFullyQualifiedPath(
        new Path(contentFile.path().toString()));
    String lookupPath = FileSystemUtil.relativizePathNoThrow(contentFilePath, partDir_);
    if (lookupPath == null) {
      if (canDataBeOutsideOfTableLocation_) {
        lookupPath = contentFilePath.toString();
      } else {
        throw new IOException(String.format("Failed to load Iceberg datafile %s, because "
            + "it's outside of the table location", contentFilePath));
      }
    }
    return oldFdsByPath_.get(lookupPath);
  }
}
