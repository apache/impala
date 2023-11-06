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
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.impala.catalog.iceberg.GroupedContentFiles;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.Reference;
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

  private static final Configuration CONF = new Configuration();

  // Default value of 'newFilesThreshold_' if the given parameter or startup flag have
  // invalid value.
  private final int NEW_FILES_THRESHOLD_DEFAULT = 100;

  // If there are more new files than 'newFilesThreshold_', we should fall back
  // to regular file metadata loading.
  private final int newFilesThreshold_;

  private final GroupedContentFiles icebergFiles_;
  private final boolean canDataBeOutsideOfTableLocation_;

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
    if (!shouldReuseOldFds()) {
      super.load();
    } else {
      try {
        reloadWithOldFds();
      } finally {
        FileMetadataLoader.TOTAL_TASKS.decrementAndGet();
      }
    }
  }

  /**
   *  Iceberg tables are a collection of immutable, uniquely identifiable data files,
   *  which means we can safely reuse the old FDs.
   */
  private void reloadWithOldFds() throws IOException {
    loadStats_ = new LoadStats(partDir_);
    FileSystem fs = partDir_.getFileSystem(CONF);

    String msg = String.format("Refreshing Iceberg file metadata from path %s " +
        "while reusing old file descriptors", partDir_);
    LOG.trace(msg);
    try (ThreadNameAnnotator tna = new ThreadNameAnnotator(msg)) {
      loadedFds_ = new ArrayList<>();
      Reference<Long> numUnknownDiskIds = new Reference<>(0L);
      for (ContentFile<?> contentFile : icebergFiles_.getAllContentFiles()) {
        FileDescriptor fd = getOldFd(contentFile);
        if (fd == null) {
          fd = getFileDescriptor(fs, contentFile, numUnknownDiskIds);
        } else {
          ++loadStats_.skippedFiles;
        }
        loadedFds_.add(Preconditions.checkNotNull(fd));
      }
      Preconditions.checkState(loadStats_.loadedFiles <= newFilesThreshold_);
      loadStats_.unknownDiskIds += numUnknownDiskIds.getRef();
      if (LOG.isTraceEnabled()) {
        LOG.trace(loadStats_.debugString());
      }
    }
  }

  private FileDescriptor getFileDescriptor(FileSystem fs, ContentFile<?> contentFile,
        Reference<Long> numUnknownDiskIds) throws IOException {
    Path fileLoc = FileSystemUtil.createFullyQualifiedPath(
        new Path(contentFile.path().toString()));
    FileStatus stat;
    if (FileSystemUtil.supportsStorageIds(fs)) {
      stat = Utils.createLocatedFileStatus(fileLoc, fs);
    } else {
      // For OSS service (e.g. S3A, COS, OSS, etc), we create FileStatus ourselves.
      stat = Utils.createFileStatus(contentFile, fileLoc);
    }
    return getFileDescriptor(fs, FileSystemUtil.supportsStorageIds(fs),
        numUnknownDiskIds, stat);
  }

  /**
   * Throw exception if the path fails to relativize based on the location of the Iceberg
   * tables, and files is not allowed outside the table location.
   */
  @Override
  protected FileDescriptor getFileDescriptor(FileSystem fs, boolean listWithLocations,
      Reference<Long> numUnknownDiskIds, FileStatus fileStatus) throws IOException {
    String absPath = null;
    String relPath = FileSystemUtil.relativizePathNoThrow(fileStatus.getPath(), partDir_);
    if (relPath == null) {
      if (canDataBeOutsideOfTableLocation_) {
        absPath = fileStatus.getPath().toString();
      } else {
        throw new IOException(String.format("Failed to load Iceberg datafile %s, because "
            + "it's outside of the table location", fileStatus.getPath().toUri()));
      }
    }

    String path = Strings.isNullOrEmpty(relPath) ? absPath : relPath;
    FileDescriptor fd = oldFdsByPath_.get(path);
    if (listWithLocations || forceRefreshLocations || fd == null ||
        fd.isChanged(fileStatus)) {
      fd = createFd(fs, fileStatus, relPath, numUnknownDiskIds, absPath);
      ++loadStats_.loadedFiles;
    } else {
      ++loadStats_.skippedFiles;
    }
    return fd;
  }

  /**
   * Return file status list based on the data and delete files of the Iceberg tables.
   */
  @Override
  protected List<FileStatus> getFileStatuses(FileSystem fs, boolean listWithLocations)
      throws IOException {
    if (icebergFiles_.isEmpty()) return null;
    // For the FSs in 'FileSystemUtil#SCHEME_SUPPORT_STORAGE_IDS' (e.g. HDFS, Ozone,
    // Alluxio, etc.) we ensure the file with block location information, so we're going
    // to get the block information through 'FileSystemUtil.listFiles'.
    Map<Path, FileStatus> nameToFileStatus = Collections.emptyMap();
    if (listWithLocations) nameToFileStatus = parallelListing(fs);
    List<FileStatus> stats = Lists.newLinkedList();
    for (ContentFile<?> contentFile : icebergFiles_.getAllContentFiles()) {
      Path path = FileSystemUtil.createFullyQualifiedPath(
          new Path(contentFile.path().toString()));
      // If data is in the table location, then we can get LocatedFileStatus from
      // 'nameToFileStatus'. If 'nameToFileStatus' does not include the ContentFile, we
      // try to get LocatedFileStatus based on the specific fs(StorageIds are supported)
      // of the actual ContentFile. If the specific fs does not support StorageIds, then
      // we create FileStatus directly by the method
      // 'org.apache.impala.catalog.IcebergFileMetadataLoader.createFileStatus'.
      if (nameToFileStatus.containsKey(path)) {
        stats.add(nameToFileStatus.get(path));
      } else {
        FileSystem fsForPath = FileSystemUtil.getFileSystemForPath(path);
        if (FileSystemUtil.supportsStorageIds(fsForPath)) {
          stats.add(Utils.createLocatedFileStatus(path, fsForPath));
        } else {
          // To avoid the cost of directory listing on OSS service (e.g. S3A, COS, OSS,
          // etc), we create FileStatus ourselves.
          stats.add(Utils.createFileStatus(contentFile, path));
        }
      }
    }
    return stats;
  }

  private Map<Path, FileStatus> parallelListing(FileSystem fs) throws IOException {
    String logPrefix = "Parallel Iceberg file metadata listing";
    int poolSize = getPoolSize(icebergFiles_.size(), fs);
    ExecutorService pool = createPool(poolSize, logPrefix);
    TOTAL_THREADS.addAndGet(poolSize);
    final Set<Path> partitionPaths;
    Map<Path, FileStatus> nameToFileStatus = Maps.newConcurrentMap();
    try (ThreadNameAnnotator tna = new ThreadNameAnnotator(logPrefix)) {
      partitionPaths = icebergFilesByPartition();
      TOTAL_TASKS.addAndGet(partitionPaths.size());
      List<Future<Void>> tasks =
          partitionPaths.stream()
              .map(path -> pool.submit(() -> {
                try {
                  return listingTask(fs, path, nameToFileStatus);
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

  private Set<Path> icebergFilesByPartition() {
    return StreamSupport.stream(icebergFiles_.getAllContentFiles().spliterator(), false)
        .map(contentFile -> new Path(String.valueOf(contentFile.path())).getParent())
        .collect(Collectors.toSet());
  }

  private Void listingTask(FileSystem fs, Path partitionPath,
      Map<Path, FileStatus> nameToFileStatus) throws IOException {
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

  @VisibleForTesting
  boolean shouldReuseOldFds() throws IOException {
    if (oldFdsByPath_ == null || oldFdsByPath_.isEmpty()) return false;
    if (forceRefreshLocations) return false;

    int oldFdsSize = oldFdsByPath_.size();
    int iceContentFilesSize = icebergFiles_.size();

    if (iceContentFilesSize - oldFdsSize > newFilesThreshold_) {
      LOG.trace("There are at least {} new files under path {}.",
          iceContentFilesSize - oldFdsSize, partDir_);
      return false;
    }

    int newFiles = 0;
    for (ContentFile<?> contentFile : icebergFiles_.getAllContentFiles()) {
      if (getOldFd(contentFile) == null) {
        ++newFiles;
        if (newFiles > newFilesThreshold_) {
          LOG.trace("There are at least {} new files under path {}.", newFiles, partDir_);
          return false;
        }
      }
    }
    LOG.trace("There are only {} new files under path {}.", newFiles, partDir_);
    return true;
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
