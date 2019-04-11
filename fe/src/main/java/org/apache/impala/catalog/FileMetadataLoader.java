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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.Reference;
import org.apache.impala.compat.HdfsShim;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.util.ListMap;
import org.apache.impala.util.ThreadNameAnnotator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * Utility for loading file metadata within a partition directory.
 */
public class FileMetadataLoader {
  private final static Logger LOG = LoggerFactory.getLogger(FileMetadataLoader.class);
  private static final Configuration CONF = new Configuration();

  private final Path partDir_;
  private final boolean recursive_;
  private final ImmutableMap<String, FileDescriptor> oldFdsByRelPath_;
  private final ListMap<TNetworkAddress> hostIndex_;

  private boolean forceRefreshLocations = false;

  private List<FileDescriptor> loadedFds_;
  private LoadStats loadStats_;

  /**
   * @param partDir the dir for which to fetch file metadata
   * @param recursive whether to recursively list files
   * @param oldFds any pre-existing file descriptors loaded for this table, used
   *   to optimize refresh if available.
   * @param hostIndex the host index with which to associate the file descriptors
   */
  public FileMetadataLoader(Path partDir, boolean recursive, List<FileDescriptor> oldFds,
      ListMap<TNetworkAddress> hostIndex) {
    partDir_ = Preconditions.checkNotNull(partDir);
    recursive_ = recursive;
    hostIndex_ = Preconditions.checkNotNull(hostIndex);
    oldFdsByRelPath_ = Maps.uniqueIndex(oldFds, FileDescriptor::getRelativePath);
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
   */
  public void load() throws IOException {
    Preconditions.checkState(loadStats_ == null, "already loaded");
    loadStats_ = new LoadStats();
    FileSystem fs = partDir_.getFileSystem(CONF);

    // If we don't have any prior FDs from which we could re-use old block location info,
    // we'll need to fetch info for every returned file. In this case we can inline
    // that request with the 'list' call and save a round-trip per file.
    //
    // In the case that we _do_ have existing FDs which we can reuse, we'll optimistically
    // assume that most _can_ be reused, in which case it's faster to _not_ prefetch
    // the locations.
    boolean listWithLocations = FileSystemUtil.supportsStorageIds(fs) &&
        (oldFdsByRelPath_.isEmpty() || forceRefreshLocations);

    String msg = String.format("%s file metadata%s from path %s",
          oldFdsByRelPath_.isEmpty() ? "Loading" : "Refreshing",
          listWithLocations ? " with eager location-fetching" : "",
          partDir_);
    LOG.trace(msg);
    try (ThreadNameAnnotator tna = new ThreadNameAnnotator(msg)) {
      RemoteIterator<? extends FileStatus> fileStatuses;
      if (listWithLocations) {
        fileStatuses = FileSystemUtil.listFiles(fs, partDir_, recursive_);
      } else {
        fileStatuses = FileSystemUtil.listStatus(fs, partDir_, recursive_);

        // TODO(todd): we could look at the result of listing without locations, and if
        // we see that a substantial number of the files have changed, it may be better
        // to go back and re-list with locations vs doing an RPC per file.
      }
      loadedFds_ = new ArrayList<>();
      if (fileStatuses == null) return;

      Reference<Long> numUnknownDiskIds = new Reference<Long>(Long.valueOf(0));
      while (fileStatuses.hasNext()) {
        FileStatus fileStatus = fileStatuses.next();
        if (!FileSystemUtil.isValidDataFile(fileStatus)) {
          ++loadStats_.hiddenFiles;
          continue;
        }
        String relPath = relativizePath(fileStatus.getPath());
        FileDescriptor fd = oldFdsByRelPath_.get(relPath);
        if (listWithLocations || forceRefreshLocations ||
            hasFileChanged(fd, fileStatus)) {
          fd = createFd(fs, fileStatus, relPath, numUnknownDiskIds);
          ++loadStats_.loadedFiles;
        } else {
          ++loadStats_.skippedFiles;
        }
        loadedFds_.add(Preconditions.checkNotNull(fd));;
      }
      loadStats_.unknownDiskIds += numUnknownDiskIds.getRef();
      if (LOG.isTraceEnabled()) {
        LOG.trace(loadStats_.debugString());
      }
    }
  }

  /**
   * Return the path of 'path' relative to the partition dir being listed. This may
   * differ from simply the file name in the case of recursive listings.
   */
  private String relativizePath(Path path) {
    URI relUri = partDir_.toUri().relativize(path.toUri());
    if (relUri.isAbsolute() || relUri.getPath().startsWith("/")) {
      throw new RuntimeException("FileSystem returned an unexpected path " +
          path + " for a file within " + partDir_);
    }
    return relUri.getPath();
  }

  /**
   * Create a FileDescriptor for the given FileStatus. If the FS supports block locations,
   * and FileStatus is a LocatedFileStatus (i.e. the location was prefetched) this uses
   * the already-loaded information; otherwise, this may have to remotely look up the
   * locations.
   */
  private FileDescriptor createFd(FileSystem fs, FileStatus fileStatus,
      String relPath, Reference<Long> numUnknownDiskIds) throws IOException {
    if (!FileSystemUtil.supportsStorageIds(fs)) {
      return FileDescriptor.createWithNoBlocks(fileStatus, relPath);
    }
    BlockLocation[] locations;
    if (fileStatus instanceof LocatedFileStatus) {
      locations = ((LocatedFileStatus)fileStatus).getBlockLocations();
    } else {
      locations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
    }
    return FileDescriptor.create(fileStatus, relPath, locations, hostIndex_,
        HdfsShim.isErasureCoded(fileStatus), numUnknownDiskIds);
  }

  /**
   * Compares the modification time and file size between the FileDescriptor and the
   * FileStatus to determine if the file has changed. Returns true if the file has changed
   * and false otherwise.
   */
  private static boolean hasFileChanged(FileDescriptor fd, FileStatus status) {
    return (fd == null) || (fd.getFileLength() != status.getLen()) ||
      (fd.getModificationTime() != status.getModificationTime());
  }

  // File/Block metadata loading stats for a single HDFS path.
  public class LoadStats {
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
    public long unknownDiskIds = 0;

    public String debugString() {
      return String.format("Path: %s: Loaded files: %s Hidden files: %s " +
          "Skipped files: %s Unknown diskIDs: %s", partDir_, loadedFiles, hiddenFiles,
          skippedFiles, unknownDiskIds);
    }
  }
}
