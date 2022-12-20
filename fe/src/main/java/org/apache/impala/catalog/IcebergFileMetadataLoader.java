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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
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
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.util.ListMap;

/**
 * Utility for loading the content files metadata of the Iceberg tables.
 */
public class IcebergFileMetadataLoader extends FileMetadataLoader {
  private final GroupedContentFiles icebergFiles_;
  private final boolean canDataBeOutsideOfTableLocation_;

  public IcebergFileMetadataLoader(Path partDir, boolean recursive,
      List<FileDescriptor> oldFds, ListMap<TNetworkAddress> hostIndex,
      ValidTxnList validTxnList, ValidWriteIdList writeIds,
      GroupedContentFiles icebergFiles, boolean canDataBeOutsideOfTableLocation) {
    super(partDir, recursive, oldFds, hostIndex, validTxnList, writeIds,
        HdfsFileFormat.ICEBERG);
    icebergFiles_ = icebergFiles;
    canDataBeOutsideOfTableLocation_ = canDataBeOutsideOfTableLocation;
  }

  /**
   * Throw exception if the path fails to relativize based on the location of the Iceberg
   * tables, and files is not allowed outside the table location.
   */
  @Override
  protected FileDescriptor getFileDescriptor(FileSystem fs, boolean listWithLocations,
      Reference<Long> numUnknownDiskIds, FileStatus fileStatus) throws IOException {
    String relPath = null;
    String absPath = null;
    URI relUri = partDir_.toUri().relativize(fileStatus.getPath().toUri());
    if (relUri.isAbsolute() || relUri.getPath().startsWith(Path.SEPARATOR)) {
      if (canDataBeOutsideOfTableLocation_) {
        absPath = fileStatus.getPath().toString();
      } else {
        throw new IOException(String.format("Failed to load Iceberg datafile %s, because "
            + "it's outside of the table location", fileStatus.getPath().toUri()));
      }
    } else {
      relPath = relUri.getPath();
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
    RemoteIterator<? extends FileStatus> fileStatuses = null;
    // For the FSs in 'FileSystemUtil#SCHEME_SUPPORT_STORAGE_IDS' (e.g. HDFS, Ozone,
    // Alluxio, etc.) we ensure the file with block location information, so we're going
    // to get the block information through 'FileSystemUtil.listFiles'.
    if (listWithLocations) {
      fileStatuses = FileSystemUtil.listFiles(fs, partDir_, recursive_, debugAction_);
    }
    Map<Path, FileStatus> nameToFileStatus = Maps.newHashMap();
    if (fileStatuses != null) {
      while (fileStatuses.hasNext()) {
        FileStatus status = fileStatuses.next();
        nameToFileStatus.put(status.getPath(), status);
      }
    }

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
}
