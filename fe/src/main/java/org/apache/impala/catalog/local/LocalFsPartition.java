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

package org.apache.impala.catalog.local;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.hadoop.fs.Path;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.HdfsPartitionLocationCompressor;
import org.apache.impala.catalog.HdfsStorageDescriptor;
import org.apache.impala.catalog.PartitionStatsUtil;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.thrift.TAccessLevel;
import org.apache.impala.thrift.THdfsPartitionLocation;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartitionStats;
import org.apache.impala.util.ListMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.impala.util.IcebergUtil;

public class LocalFsPartition implements FeFsPartition {
  private final LocalFsTable table_;
  private final LocalPartitionSpec spec_;
  private final Map<String, String> hmsParameters_;
  private final long writeId_;
  private final HdfsStorageDescriptor hdfsStorageDescriptor_;
  // Prefix-compressed location. The prefix map is carried in TableMetaRef of LocalTable.
  // The special "prototype partition" has a null location.
  private final HdfsPartitionLocationCompressor.Location location_;

  /**
   * Null in the case of a 'prototype partition'.
   */
  @Nullable
  private ImmutableList<FileDescriptor> fileDescriptors_;

  @Nullable
  private ImmutableList<FileDescriptor> insertFileDescriptors_;

  @Nullable
  private ImmutableList<FileDescriptor> deleteFileDescriptors_;

  @Nullable
  private final byte[] partitionStats_;

  // True if partitionStats_ has intermediate_col_stats populated.
  private final boolean hasIncrementalStats_;

  // True if this partition is marked as cached by hdfs caching. Does not necessarily
  // mean the data is cached. Only used in analyzing DDLs or constructing results of
  // SHOW TABLE STATS / SHOW PARTITIONS.
  private final boolean isMarkedCached_;

  public LocalFsPartition(LocalFsTable table, LocalPartitionSpec spec,
      Map<String, String> hmsParameters, long writeId,
      HdfsStorageDescriptor hdfsStorageDescriptor,
      ImmutableList<FileDescriptor> fileDescriptors,
      ImmutableList<FileDescriptor> insertFileDescriptors,
      ImmutableList<FileDescriptor> deleteFileDescriptors,
      byte [] partitionStats, boolean hasIncrementalStats, boolean isMarkedCached,
      HdfsPartitionLocationCompressor.Location location) {
    table_ = Preconditions.checkNotNull(table);
    spec_ = Preconditions.checkNotNull(spec);
    hmsParameters_ = hmsParameters;
    writeId_ = writeId;
    hdfsStorageDescriptor_ = hdfsStorageDescriptor;
    location_ = location;
    fileDescriptors_ = fileDescriptors;
    insertFileDescriptors_ = insertFileDescriptors;
    deleteFileDescriptors_ = deleteFileDescriptors;
    partitionStats_ = partitionStats;
    hasIncrementalStats_ = hasIncrementalStats;
    isMarkedCached_ = isMarkedCached;
  }

  @Override
  public String getPartitionName() {
    return FeCatalogUtils.getPartitionName(this);
  }

  @Override
  public long getId() {
    return spec_.getId();
  }

  @Override
  public FeFsTable getTable() {
    return table_;
  }

  @Override // FeFsPartition
  public ListMap<TNetworkAddress> getHostIndex() { return table_.getHostIndex(); }

  @Override
  public FileSystemUtil.FsType getFsType() {
    Path location = getLocationPath();
    Preconditions.checkNotNull(location.toUri().getScheme(),
        "Cannot get scheme from path " + location);
    return FileSystemUtil.FsType.getFsType(location.toUri().getScheme());
  }

  @Override
  public List<FileDescriptor> getFileDescriptors() {
    if (!fileDescriptors_.isEmpty()) return fileDescriptors_;
    List<FileDescriptor> ret = new ArrayList<>();
    ret.addAll(insertFileDescriptors_);
    ret.addAll(deleteFileDescriptors_);
    return ret;
  }

  @Override
  public List<FileDescriptor> getInsertFileDescriptors() {
    return insertFileDescriptors_;
  }

  @Override
  public List<FileDescriptor> getDeleteFileDescriptors() {
    return deleteFileDescriptors_;
  }

  @Override
  public boolean hasFileDescriptors() {
    return !fileDescriptors_.isEmpty() ||
           !insertFileDescriptors_.isEmpty() ||
           !deleteFileDescriptors_.isEmpty();
  }

  @Override
  public int getNumFileDescriptors() {
    return fileDescriptors_.size() +
           insertFileDescriptors_.size() +
           deleteFileDescriptors_.size();
  }

  @Override
  public String getLocation() {
    return (location_ != null) ? location_.toString() : null;
  }

  @Override
  public THdfsPartitionLocation getLocationAsThrift() {
    return (location_ != null) ? location_.toThrift() : null;
  }

  @Override
  public Path getLocationPath() {
    Preconditions.checkNotNull(location_,
            "LocalFsPartition location is null");
    return new Path(getLocation());
  }

  @Override
  public TAccessLevel getAccessLevel() {
    // TODO(todd): implement me
    return TAccessLevel.READ_WRITE;
  }

  @Override
  public boolean isCacheable() {
    return FileSystemUtil.isPathCacheable(getLocationPath());
  }

  @Override
  public boolean isMarkedCached() {
    return isMarkedCached_;
  }

  @Override
  public HdfsStorageDescriptor getInputFormatDescriptor() {
    return hdfsStorageDescriptor_;
  }

  @Override
  public HdfsFileFormat getFileFormat() {
    HdfsFileFormat format = getInputFormatDescriptor().getFileFormat();
    if (format == HdfsFileFormat.ICEBERG) {
      return IcebergUtil.toHdfsFileFormat(
          IcebergUtil.getIcebergFileFormat(table_.getMetaStoreTable()));
    }
    return format;
  }

  @Override
  public TPartitionStats getPartitionStats() {
    // TODO(todd): after HIVE-19715 is complete, we should defer loading these
    // stats until requested. Even prior to that, we should consider caching them
    // separately to increase effective cache capacity.
    return PartitionStatsUtil.getPartStatsOrWarn(this);
  }

  @Override
  public boolean hasIncrementalStats() { return hasIncrementalStats_; }

  @Override // FeFsPartition
  public byte[] getPartitionStatsCompressed() { return partitionStats_; }

  @Override
  public long getSize() {
    long size = 0;
    for (FileDescriptor fd : getFileDescriptors()) {
      size += fd.getFileLength();
    }
    return size;
  }

  @Override
  public long getNumRows() {
    long rowCount = FeCatalogUtils.getRowCount(hmsParameters_);
    if (table_.getDebugMetadataScale() > 1.0 && rowCount > 0) {
      rowCount *= table_.getDebugMetadataScale();
    }
    return rowCount;
  }

  @Override
  public String getConjunctSql() {
    return FeCatalogUtils.getConjunctSqlForPartition(this);
  }

  @Override
  public List<String> getPartitionValuesAsStrings(boolean mapNullsToHiveKey) {
    return FeCatalogUtils.getPartitionValuesAsStrings(this, mapNullsToHiveKey);
  }

  @Override
  public List<LiteralExpr> getPartitionValues() {
    return spec_.getPartitionValues();
  }

  @Override
  public LiteralExpr getPartitionValue(int pos) {
    return spec_.getPartitionValues().get(pos);
  }

  @Override
  public Map<String, String> getParameters() {
    return hmsParameters_;
  }

  @Override
  public long getWriteId() {
    return writeId_;
  }

  @Override
  public LocalFsPartition genInsertDeltaPartition() {
    ImmutableList<FileDescriptor> fds = insertFileDescriptors_.isEmpty() ?
        fileDescriptors_ : insertFileDescriptors_;
    return new LocalFsPartition(table_, spec_, hmsParameters_, writeId_,
        hdfsStorageDescriptor_, fds, ImmutableList.of(), ImmutableList.of(),
        partitionStats_, hasIncrementalStats_, isMarkedCached_, location_);
  }

  @Override
  public LocalFsPartition genDeleteDeltaPartition() {
    if (deleteFileDescriptors_.isEmpty()) return null;
    return new LocalFsPartition(table_, spec_, hmsParameters_, writeId_,
        hdfsStorageDescriptor_, deleteFileDescriptors_,
        ImmutableList.of(), ImmutableList.of(), partitionStats_,
        hasIncrementalStats_, isMarkedCached_, location_);
  }
}
