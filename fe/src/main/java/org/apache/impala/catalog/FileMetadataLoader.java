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

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.Reference;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.HudiUtil;
import org.apache.impala.util.ListMap;
import org.apache.impala.util.ThreadNameAnnotator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

/**
 * Utility for loading file metadata within a partition directory.
 */
public class FileMetadataLoader {
  private final static Logger LOG = LoggerFactory.getLogger(FileMetadataLoader.class);
  private static final Configuration CONF = new Configuration();

  // The number of unfinished instances. Incremented in the constructor and decremented
  // at the end of load().
  public static final AtomicInteger TOTAL_TASKS = new AtomicInteger();

  protected final Path partDir_;
  protected final boolean recursive_;
  protected final ImmutableMap<String, FileDescriptor> oldFdsByPath_;
  private final ListMap<TNetworkAddress> hostIndex_;
  @Nullable
  private final ValidWriteIdList writeIds_;
  @Nullable
  private final ValidTxnList validTxnList_;
  @Nullable
  private final HdfsFileFormat fileFormat_;

  protected boolean forceRefreshLocations = false;

  protected List<FileDescriptor> loadedFds_;
  private List<FileDescriptor> loadedInsertDeltaFds_;
  private List<FileDescriptor> loadedDeleteDeltaFds_;
  protected LoadStats loadStats_;
  protected String debugAction_;

  /**
   * @param partDir the dir for which to fetch file metadata
   * @param recursive whether to recursively list files
   * @param oldFds any pre-existing file descriptors loaded for this table, used
   *   to optimize refresh if available.
   * @param hostIndex the host index with which to associate the file descriptors
   * @param validTxnList if non-null, it can tell whether a given transaction id is
   *   committed or not. We need it to ignore base directories of in-progress
   *   compactions.
   * @param writeIds if non-null, a write-id list which will filter the returned
   *   file descriptors to only include those indicated to be valid.
   * @param fileFormat if non-null and equal to HdfsFileFormat.HUDI_PARQUET,
   *   this loader will filter files based on Hudi's HoodieROTablePathFilter method
   */
  public FileMetadataLoader(Path partDir, boolean recursive, List<FileDescriptor> oldFds,
      ListMap<TNetworkAddress> hostIndex, @Nullable ValidTxnList validTxnList,
      @Nullable ValidWriteIdList writeIds, @Nullable HdfsFileFormat fileFormat) {
    // Either both validTxnList and writeIds are null, or none of them.
    Preconditions.checkState((validTxnList == null && writeIds == null)
        || (validTxnList != null && writeIds != null));
    partDir_ = Preconditions.checkNotNull(partDir);
    recursive_ = recursive;
    hostIndex_ = Preconditions.checkNotNull(hostIndex);
    oldFdsByPath_ = Maps.uniqueIndex(oldFds, FileDescriptor::getPath);
    writeIds_ = writeIds;
    validTxnList_ = validTxnList;
    fileFormat_ = fileFormat;

    if (writeIds_ != null) {
      Preconditions.checkArgument(recursive_, "ACID tables must be listed recursively");
    }
    TOTAL_TASKS.incrementAndGet();
  }

  public FileMetadataLoader(Path partDir, boolean recursive, List<FileDescriptor> oldFds,
      ListMap<TNetworkAddress> hostIndex, @Nullable ValidTxnList validTxnList,
      @Nullable ValidWriteIdList writeIds) {
    this(partDir, recursive, oldFds, hostIndex, validTxnList, writeIds, null);
  }

  /**
   * If 'refresh' is true, force re-fetching block locations even if a file does not
   * appear to have changed.
   */
  public void setForceRefreshBlockLocations(boolean refresh) {
    forceRefreshLocations = refresh;
  }

  /**
   * @return the file descriptors that were loaded after an invocation of load()
   */
  public List<FileDescriptor> getLoadedFds() {
    Preconditions.checkState(loadedFds_ != null,
        "Must have successfully loaded first");
    return loadedFds_;
  }

  public List<FileDescriptor> getLoadedInsertDeltaFds() {
    return loadedInsertDeltaFds_;
  }

  public List<FileDescriptor> getLoadedDeleteDeltaFds() {
    return loadedDeleteDeltaFds_;
  }

  /**
   * @return statistics about the descriptor loading process, after an invocation of
   * load()
   */
  public LoadStats getStats() {
    Preconditions.checkState(loadedFds_ != null,
        "Must have successfully loaded first");
    return loadStats_;
  }

  Path getPartDir() { return partDir_; }

  /**
   * Load the file descriptors, which may later be fetched using {@link #getLoadedFds()}.
   * After a successful load, stats may be fetched using {@link #getStats()}.
   *
   * If the directory does not exist, this succeeds and yields an empty list of
   * descriptors.
   *
   * @throws IOException if listing fails.
   * @throws CatalogException on ACID errors. TODO: remove this once IMPALA-9042 is
   * resolved.
   */
  public void load() throws CatalogException, IOException {
    try {
      loadInternal();
    } finally {
      TOTAL_TASKS.decrementAndGet();
    }
  }

  private void loadInternal() throws CatalogException, IOException {
    Preconditions.checkState(loadStats_ == null, "already loaded");
    loadStats_ = new LoadStats(partDir_);
    FileSystem fs = partDir_.getFileSystem(CONF);

    // If we don't have any prior FDs from which we could re-use old block location info,
    // we'll need to fetch info for every returned file. In this case we can inline
    // that request with the 'list' call and save a round-trip per file.
    //
    // In the case that we _do_ have existing FDs which we can reuse, we'll optimistically
    // assume that most _can_ be reused, in which case it's faster to _not_ prefetch
    // the locations.
    boolean listWithLocations = FileSystemUtil.supportsStorageIds(fs) &&
        (oldFdsByPath_.isEmpty() || forceRefreshLocations);

    String msg = String.format("%s file metadata%s from path %s",
        oldFdsByPath_.isEmpty() ? "Loading" : "Refreshing",
        listWithLocations ? " with eager location-fetching" : "", partDir_);
    LOG.trace(msg);
    try (ThreadNameAnnotator tna = new ThreadNameAnnotator(msg)) {
      List<FileStatus> fileStatuses = getFileStatuses(fs, listWithLocations);

      loadedFds_ = new ArrayList<>();
      if (fileStatuses == null) return;

      Reference<Long> numUnknownDiskIds = new Reference<>(0L);

      if (writeIds_ != null) {
        fileStatuses = AcidUtils.filterFilesForAcidState(fileStatuses, partDir_,
            validTxnList_, writeIds_, loadStats_);
      }

      if (fileFormat_ == HdfsFileFormat.HUDI_PARQUET) {
        fileStatuses = HudiUtil.filterFilesForHudiROPath(fileStatuses);
      }

      for (FileStatus fileStatus : fileStatuses) {
        if (fileStatus.isDirectory()) {
          continue;
        }

        if (!FileSystemUtil.isValidDataFile(fileStatus)) {
          ++loadStats_.hiddenFiles;
          continue;
        }

        FileDescriptor fd = getFileDescriptor(fs, listWithLocations, numUnknownDiskIds,
            fileStatus);
        loadedFds_.add(Preconditions.checkNotNull(fd));
      }
      if (writeIds_ != null) {
        loadedInsertDeltaFds_ = new ArrayList<>();
        loadedDeleteDeltaFds_ = new ArrayList<>();
        for (FileDescriptor fd : loadedFds_) {
          if (AcidUtils.isDeleteDeltaFd(fd)) {
            loadedDeleteDeltaFds_.add(fd);
          } else {
            loadedInsertDeltaFds_.add(fd);
          }
        }
      }
      loadStats_.unknownDiskIds += numUnknownDiskIds.getRef();
      if (LOG.isTraceEnabled()) {
        LOG.trace(loadStats_.debugString());
      }
    }
  }

  /**
   * Return fd created by the given fileStatus or from the cache(oldFdsByPath_).
   */
  protected FileDescriptor getFileDescriptor(FileSystem fs, boolean listWithLocations,
      Reference<Long> numUnknownDiskIds, FileStatus fileStatus) throws IOException {
    String relPath = FileSystemUtil.relativizePath(fileStatus.getPath(), partDir_);
    FileDescriptor fd = oldFdsByPath_.get(relPath);
    if (listWithLocations || forceRefreshLocations || fd == null ||
        fd.isChanged(fileStatus)) {
      fd = createFd(fs, fileStatus, relPath, numUnknownDiskIds);
      ++loadStats_.loadedFiles;
    } else {
      ++loadStats_.skippedFiles;
    }
    return fd;
  }

  /**
   * Return located file status list when listWithLocations is true.
   */
  protected List<FileStatus> getFileStatuses(FileSystem fs, boolean listWithLocations)
      throws IOException {
    RemoteIterator<? extends FileStatus> fileStatuses;
    if (listWithLocations) {
      fileStatuses = FileSystemUtil
          .listFiles(fs, partDir_, recursive_, debugAction_);
    } else {
      fileStatuses = FileSystemUtil
          .listStatus(fs, partDir_, recursive_, debugAction_);
      // TODO(todd): we could look at the result of listing without locations, and if
      // we see that a substantial number of the files have changed, it may be better
      // to go back and re-list with locations vs doing an RPC per file.
    }
    if (fileStatuses == null) return null;
    List<FileStatus> stats = new ArrayList<>();
    while (fileStatuses.hasNext()) {
      stats.add(fileStatuses.next());
    }
    return stats;
  }

  /**
   * Create a FileDescriptor for the given FileStatus. If the FS supports block locations,
   * and FileStatus is a LocatedFileStatus (i.e. the location was prefetched) this uses
   * the already-loaded information; otherwise, this may have to remotely look up the
   * locations.
   * 'absPath' is null except for the Iceberg tables, because datafiles of the
   * Iceberg tables may not be in the table location.
   */
  protected FileDescriptor createFd(FileSystem fs, FileStatus fileStatus,
      String relPath, Reference<Long> numUnknownDiskIds, String absPath)
      throws IOException {
    if (!FileSystemUtil.supportsStorageIds(fs)) {
      return FileDescriptor.createWithNoBlocks(fileStatus, relPath, absPath);
    }
    BlockLocation[] locations;
    if (fileStatus instanceof LocatedFileStatus) {
      locations = ((LocatedFileStatus) fileStatus).getBlockLocations();
    } else {
      locations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
    }
    return FileDescriptor.create(fileStatus, relPath, locations, hostIndex_,
        fileStatus.isEncrypted(), fileStatus.isErasureCoded(), numUnknownDiskIds,
        absPath);
  }

  private FileDescriptor createFd(FileSystem fs, FileStatus fileStatus,
      String relPath, Reference<Long> numUnknownDiskIds) throws IOException {
    return createFd(fs, fileStatus, relPath, numUnknownDiskIds, null);
  }

  /**
   * Given a file descriptor list 'oldFds', returns true if the loaded file descriptors
   * are the same as them.
   */
  public boolean hasFilesChangedCompareTo(List<FileDescriptor> oldFds) {
    if (oldFds.size() != loadedFds_.size()) return true;
    ImmutableMap<String, FileDescriptor> oldFdsByRelPath =
        Maps.uniqueIndex(oldFds, FileDescriptor::getPath);
    for (FileDescriptor fd : loadedFds_) {
      FileDescriptor oldFd = oldFdsByRelPath.get(fd.getPath());
      if (fd.isChanged(oldFd)) return true;
    }
    return false;
  }

  /**
   * Enables injection of a debug actions to introduce delays in HDFS listStatus or
   * listFiles call during the file-metadata loading.
   */
  public void setDebugAction(String debugAction) {
    this.debugAction_ = debugAction;
  }

  // File/Block metadata loading stats for a single HDFS path.
  public static class LoadStats {
    private final Path partDir_;
    LoadStats(Path partDir) {
      this.partDir_ = Preconditions.checkNotNull(partDir);
    }
    /** Number of files skipped because they pertain to an uncommitted ACID transaction */
    public int uncommittedAcidFilesSkipped = 0;

    /**
     * Number of files skipped because they pertain to ACID directories superseded
     * by compaction or newer base.
     */
    public int filesSupersededByAcidState = 0;

    // Number of files for which the metadata was loaded.
    public int loadedFiles = 0;

    // Number of hidden files excluded from file metadata loading. More details at
    // isValidDataFile().
    public int hiddenFiles = 0;

    // Number of files skipped from file metadata loading because the files have not
    // changed since the last load. More details at hasFileChanged().
    //
    // TODO(todd) rename this to something indicating it was fast-pathed, not skipped
    public int skippedFiles = 0;

    // Number of unknown disk IDs encountered while loading block
    // metadata for this path.
    public int unknownDiskIds = 0;

    public String debugString() {
      return MoreObjects.toStringHelper("")
        .add("path", partDir_)
        .add("loaded files", loadedFiles)
        .add("hidden files", nullIfZero(hiddenFiles))
        .add("skipped files", nullIfZero(skippedFiles))
        .add("uncommited files", nullIfZero(uncommittedAcidFilesSkipped))
        .add("superceded files", nullIfZero(filesSupersededByAcidState))
        .add("unknown diskIds", nullIfZero(unknownDiskIds))
        .omitNullValues()
        .toString();
    }

    private Integer nullIfZero(int x) {
      return x > 0 ? x : null;
    }
  }
}
